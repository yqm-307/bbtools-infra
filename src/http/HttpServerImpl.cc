#include "http/HttpServerImpl.hpp"

#include <sys/socket.h>

#include <exception>
#include <utility>
#include <vector>

#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>

#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/object/CoObject.hpp>

namespace bbt::infra::http_detail {

namespace asio = boost::asio;
namespace http = boost::beast::http;
using tcp      = asio::ip::tcp;

namespace {

// 每次非阻塞读的探测块大小：与就绪等待配对使用，不做预读放大。
constexpr std::size_t kReadChunk = 4096;
// 单次 accept 动作最多接纳的连接数：其余留给下一次就绪事件，避免
// 在 io 域内无界空转。
constexpr int kAcceptBatch = 16;

// 把 beast 解析出的请求转为契约 HttpRequest：头按序保留重复值。
HttpRequest ToInfraRequest(http::request<http::string_body>& msg) {
    HttpRequest out;
    out.method = std::string(msg.method_string());
    out.url    = std::string(msg.target());
    for (const auto& f : msg.base())   // fields 按序枚举，重复头不折叠
        out.headers.emplace_back(std::string(f.name_string()),
                                 std::string(f.value()));
    out.body = std::move(msg.body());
    return out;
}

} // namespace

// ------------------------------------------------------------------ Session

HttpSession::HttpSession(std::shared_ptr<HttpServerImpl> srv,
                         tcp::socket                   sock)
    : server(std::move(srv)),
      socket(std::move(sock)),
      deadline_timer(server->Engine()->Io()) {}

#ifdef BBT_INFRA_STRINGENT_DEBUG
HttpSession::~HttpSession() {
    // issue #62 验收第 5 项：析构即向独立观测组件 debug/InfraDebug.hpp 上报。
    // m_sessions 只保存裸指针，正常路径下 MaybeRelease 已在 Close（io 域门内、
    // inflight 归零时）反登记，故 registered 必为 false。仅在
    // BBT_INFRA_STRINGENT_DEBUG 下存在：Release 无此析构、无计数写入、无符号。
    debug::OnHttpSessionDestroyed(registered);
}
#endif

void HttpSession::Start() {
    std::lock_guard<std::recursive_mutex> io_gate(server->Engine()->IoGate());
    server->RegisterSession(this);
    registered = true;
    // fd 由本会话独占：读/写泵用 MSG_DONTWAIT，socket 自身置非阻塞
    // 使就绪等待语义与 send/receive 返回值一致（EAGAIN→would_block）。
    boost::system::error_code nb;
    socket.non_blocking(true, nb);
    BeginRead();
}

void HttpSession::BeginRead() {
    // fd/parser/buffer/timer 均只许在 Engine 的 io 域门内触碰（与 owner 的
    // 同步 teardown 串行）。
    std::lock_guard<std::recursive_mutex> io_gate(server->Engine()->IoGate());
    if (closed || !server->Accepting() || !server->OpenForIo()) {
        Close();
        return;
    }
    // 本请求在途上下文：读请求起始时刻 + listener 有限预算，排队与处理
    // 都消耗该期限（不信任对端任何自定义 deadline 头）。
    in_flight = std::make_shared<InFlightRequest>();
    in_flight->deadline = std::chrono::steady_clock::now() +
                          server->Engine()->Limits().incoming_timeout;

    // 每条请求一份新解析器（上一份随 Dispatch 移交后即可丢弃）；
    // flat_buffer 不清空：管线内先行到达的字节留给本次解析。
    parser.emplace();
    parser->header_limit(
        static_cast<std::uint32_t>(server->Engine()->Limits().max_header_bytes));
    parser->body_limit(server->Engine()->Limits().max_body_bytes);
    parser->eager(true);   // 手动 put：数据一到就解析进 message

    auto self = shared_from_this();
    // expires_at 抛异常重载在 IoAsyncStart 之前；失败只放弃期限看守，
    // 不得 IoAsyncDone（否则 inflight 变负，session 永不反登记）。
    bool armed_ok = false;
    try {
        deadline_timer.expires_at(in_flight->deadline);
        armed_ok = true;
    } catch (...) {
    }
    if (armed_ok) {
        try {
            // 每次 arm 分配新 token 并记账 +1：完成项带自己的 token，只有它
            // 仍是当前活跃看守时才递减；被 cancel/Close 抢先记账的旧看守的
            // aborted completion 不会消费新看守的记账（keep-alive 重挂关键）。
            const std::uint64_t token = ++timer_gen;
            timer_live                = token;
            IoAsyncStart();
            deadline_timer.async_wait(
                [self, token](boost::system::error_code ec) {
                    self->OnDeadline(ec, token);
                });
        } catch (...) {
            timer_live = 0;
            IoAsyncDone();
        }
    }
    try {
        PumpRead();
    } catch (...) {
        Close();
    }
}

void HttpSession::ArmIoWait(tcp::socket::wait_type type) {
    // 门内调用：只挂一个「不借 payload」的就绪等待——socket 壳 + shared_ptr
    // 保活，不引用 buffer/parser/serializer/message。Close 因此可以在返回
    // 当刻同步释放这些 owner 资源，晚到的完成项只是空壳。
    if (closed || io_wait_armed)
        return;
    auto self = shared_from_this();
    io_wait_armed = true;
    IoAsyncStart();
    try {
        socket.async_wait(type, [self, type](boost::system::error_code ec) {
            self->OnIoWait(ec, type);
        });
    } catch (...) {
        io_wait_armed = false;
        IoAsyncDone();
        Close();
    }
}

void HttpSession::OnIoWait(boost::system::error_code ec,
                           tcp::socket::wait_type     type) {
    std::lock_guard<std::recursive_mutex> io_gate(server->Engine()->IoGate());
    if (!io_wait_armed) {
        // Close（或取消）已在返回当刻记账：本完成项只可能是中止空壳，
        // 不再触碰 fd/parser/serializer/buffer。
        return;
    }
    io_wait_armed = false;
    IoAsyncDone();
    if (closed)
        return;
    if (ec) {
        Close();
        return;
    }
    try {
        if (type == tcp::socket::wait_read)
            PumpRead();
        else
            PumpWrite();
    } catch (...) {
        Close();
    }
}

void HttpSession::PumpRead() {
    std::lock_guard<std::recursive_mutex> io_gate(server->Engine()->IoGate());
    if (closed || !parser)
        return;
    for (;;) {
        if (buffer.size() != 0) {
            boost::system::error_code ec;
            const auto consumed = parser->put(buffer.data(), ec);
            buffer.consume(consumed);
            if (ec == http::error::need_more)
                ec.clear();
            if (ec) {
                // 契约：非法 framing/超限/断连是 Error——对入站侧体现为可观察的
                // 协议拒绝（4xx）或断开，绝不把不足一条的消息当成功请求派发。
                if (ec == http::error::body_limit) {
                    ReplyStatusAndClose(413);
                } else if (ec == http::error::header_limit) {
                    ReplyStatusAndClose(400);
                } else if (IsHttpErrorCode(ec) &&
                           ec != http::error::partial_message &&
                           ec != http::error::end_of_stream) {
                    ReplyStatusAndClose(400);
                } else {
                    Close();
                }
                return;
            }
            if (parser->is_done()) {
                if (!server->Accepting() || !server->OpenForIo()) {
                    Close();   // StopAccepting/Closing 后的在途读不再派发
                    return;
                }
                Dispatch(parser->get().keep_alive());
                return;
            }
        }
        boost::system::error_code ec;
        const auto space = buffer.prepare(kReadChunk);
        if (asio::buffer_size(space) == 0) {
            // 读缓冲已达上限：不静默吞，直接收口本次会话。
            Close();
            return;
        }
        const auto received = socket.receive(space, MSG_DONTWAIT, ec);
        buffer.commit(received);
        if (ec == asio::error::would_block || ec == asio::error::try_again) {
            ArmIoWait(tcp::socket::wait_read);
            return;
        }
        if (ec == asio::error::interrupted)
            continue;
        if (ec) {
            Close();
            return;
        }
        if (received == 0) {
            // 对端 FIN：无可读字节且无错误。
            Close();
            return;
        }
    }
}

void HttpSession::OnDeadline(boost::system::error_code ec,
                             std::uint64_t             token) {
    std::lock_guard<std::recursive_mutex> io_gate(server->Engine()->IoGate());
    if (token != timer_live) {
        // 已被 cancel/Close 抢先记账的旧看守（keep-alive 重挂后常见）：记账
        // 义务已由取消侧履行，这里只消费空壳——既不递减，也不冒充本看守。
        return;
    }
    timer_live = 0;
    IoAsyncDone();
    if (ec || closed)
        return;
    // 期限到点：关闭会话切断回复路径。已接纳 handler 不取消（契约 §N1：
    // StopAccepting/Close 都不抢占 handler），其持有的 InFlightRequest
    // 仍然有效，回复在写路径上按「只交付一次终态」收口。
    Close();
}

void HttpSession::CancelDeadlineWatch() noexcept {
    // 与 Close 的保留位记账同构：取消当前活跃看守并在返回当刻履行其 -1
    // 义务。取消侧抢先记账后，该看守完成项的 token 已不等于活跃值，晚到时
    // 只消费空壳——不会二次递减，也不会消费此后重挂的新看守。
    if (timer_live == 0)
        return;
    try {
        deadline_timer.cancel();
    } catch (...) {
    }
    timer_live = 0;
    IoAsyncDone();
}

void HttpSession::OnPeerWatch(boost::system::error_code ec) {
    std::lock_guard<std::recursive_mutex> io_gate(server->Engine()->IoGate());
    if (!peer_watch_armed) {
        // Close/回复收口侧已记账：晚到空壳。
        return;
    }
    peer_watch_armed = false;
    IoAsyncDone();
    if (closed || ec == asio::error::operation_aborted)
        return;
    bool peer_gone = true;
    if (!ec) {
        // wait_read 触发：可读事件可能是对端 FIN（available==0）或
        // 管线内下一请求的先行字节（available>0，本切片不预读）。
        boost::system::error_code ae;
        const auto avail = socket.available(ae);
        peer_gone = ae || avail == 0;
    }
    if (!peer_gone) {
        // 仅继续观察断连；字节留待下一次 BeginRead 消费。
        auto self = shared_from_this();
        try {
            peer_watch_armed = true;
            IoAsyncStart();
            socket.async_wait(tcp::socket::wait_read,
                [self](boost::system::error_code e) { self->OnPeerWatch(e); });
        } catch (...) {
            peer_watch_armed = false;
            IoAsyncDone();
            Close();
        }
        return;
    }
    // 连接断开：关闭会话切断回复路径；已接纳 handler 不取消。
    Close();
}

void HttpSession::Dispatch(bool keep_alive) {
    std::lock_guard<std::recursive_mutex> io_gate(server->Engine()->IoGate());
    // 锁内原子「drain 检查 + 记账」：拒绝在 teardown 已起步或达到
    // max_inflight 后再派发，保证 pending 计数必然归零可关。
    if (!server->TryAcquireHandler()) {
        // 可观察的接纳失败；不无界创建协程。
        ReplyStatusAndClose(503);
        return;
    }

    IncomingCallContext ctx;
    ctx.deadline       = in_flight->deadline;
    ctx.peer_principal = "";   // 首闭环仅 loopback，未认证身份为空

    HttpRequest req = ToInfraRequest(parser->get());
    auto self = shared_from_this();
    auto req_state = in_flight;   // 协程任务持有在途状态直到 handler 退出
    bool succ = false;
    // 业务 handler 移交受管协程执行；I/O 回调到此为止。
    // req_state 经捕获随 coroutine task 存活：期限信息覆盖 handler 全程，
    // 不会被响应超时或连接断开提前释放（契约 §120）；handler 不被抢占。
    bbt::coroutine::detail::Scheduler::GetInstance()->RegistCoroutineTask(
        [self, req_state, req = std::move(req), ctx, keep_alive]() mutable {
            self->RunHandler(std::move(req), std::move(ctx), keep_alive);
        },
        succ);
    if (!succ) {
        server->OnHandlerExited();   // 派生失败：回收记账并明确拒绝
        ReplyStatusAndClose(503);
        return;
    }
    // handler 期间挂断连观测：对端 FIN 是取消触发源。
    auto self2 = shared_from_this();
    try {
        peer_watch_armed = true;
        IoAsyncStart();
        socket.async_wait(tcp::socket::wait_read,
            [self2](boost::system::error_code e) { self2->OnPeerWatch(e); });
    } catch (...) {
        peer_watch_armed = false;
        IoAsyncDone();
    }
}

void HttpSession::RunHandler(HttpRequest request, IncomingCallContext ctx,
                             bool keep_alive) {
    result<HttpResponse> r = result<HttpResponse>::err(
        MakeError(ErrorCode::InternalError, "handler not run"));
    try {
        r = server->Handler()(std::move(ctx), std::move(request));
    } catch (const std::exception& e) {
        // 边界捕获：异常统一转 InternalError，错误文本脱敏不带原始请求。
        Error err = MakeError(ErrorCode::InternalError,
                              "http handler threw exception");
        err.domain_code = "handler_exception";
        r = result<HttpResponse>::err(std::move(err));
    } catch (...) {
        r = result<HttpResponse>::err(MakeError(ErrorCode::InternalError,
            "http handler threw unknown exception"));
    }

    if (r) {
        auto check = ValidateResponse(r.value(), server->Engine()->Limits());
        if (!check) {
            Error err = MakeError(ErrorCode::InternalError,
                "handler produced invalid response");
            err.domain_code = check.error().domain_code;
            r = result<HttpResponse>::err(std::move(err));
        }
    }

    auto self = shared_from_this();
    auto reply = std::make_shared<QueuedReply>();
    reply->r = std::move(r);
    reply->keep_alive = keep_alive;
    {
        std::lock_guard<std::recursive_mutex> io_gate(
            server->Engine()->IoGate());
        // payload 挂在会话持有的壳上、排队 lambda 只捕获壳：owner Close 在
        // 域门内清空壳即同步释放 body——不把 8MiB 留在排队回调里。
        pending_reply = reply;
        // issue #19：先置 reply_posted 再投递——若 Close 的 Teardown
        // 抢先进 io 队列并 Abort→Close，Close 见 reply_posted 会延迟到本
        // DeliverResult 写完才真关，不再丢弃已产出的响应。
        reply_posted.store(true, std::memory_order_release);
    }
    // TryPost 失败 = io 域已封，回复路径已随 teardown 消失，结果直接丢弃；
    // 在途状态随本 coroutine task 退出释放——token 覆盖 handler 全程。
    server->Engine()->TryPost(
        [self, reply]() mutable { self->DeliverResult(std::move(reply)); });
    server->OnHandlerExited();
}

void HttpSession::DeliverResult(std::shared_ptr<QueuedReply> reply) {
    std::lock_guard<std::recursive_mutex> io_gate(server->Engine()->IoGate());
    if (pending_reply == reply)
        pending_reply.reset();   // 载荷所有权已回到本调用
    if (closed)
        return;
    if (peer_watch_armed) {
        // handler 阶段只挂了 wait_read 观察，取消它不妨碍其它 op；
        // 取消项在此记账，晚到的 OnPeerWatch 只消费空壳。
        peer_watch_armed = false;
        IoAsyncDone();
        boost::system::error_code ignored;
        socket.cancel(ignored);
    }
    // 期限看守同理：取消并在此记账（token 置空）；其完成项仍会到达一次，
    // 但 token 不匹配，只消费空壳，不会消费 keep-alive 重挂后的新看守。
    CancelDeadlineWatch();

    if (!reply || !reply->r)
        return;   // 壳已在 Close 侧被清空：不再写任何字节
    reply_keep_alive = reply->keep_alive;

    http::response<http::string_body> res;
    res.version(11);
    if (*reply->r) {
        HttpResponse& out = reply->r->value();
        res.result(static_cast<http::status>(out.status));
        for (const auto& h : out.headers)
            res.insert(h.first, h.second);
        res.body() = std::move(out.body);   // 载荷随 message 移交，此处不留副本
    } else {
        res.result(static_cast<http::status>(
            ErrorCodeToHttpStatus(reply->r->error().code)));
    }
    res.keep_alive(reply_keep_alive);
    res.prepare_payload();
    reply->r.reset();   // payload 已交给 serializer 引用的 message

    response.emplace(std::move(res));
    try {
        // 序列化器引用 *response：只在本门内、本次写入生命周期内有效，
        // 绝不借给任何 async_* 操作（Close 可同步 reset）。
        serializer.emplace(*response);
        PumpWrite();
    } catch (...) {
        // 写发起失败=本次回复无法落地，清 reply_posted 使 Close 立即生效，
        // 不被 close_after_write 延迟逻辑卡在不关等待。
        reply_posted.store(false, std::memory_order_release);
        Close();
    }
}

void HttpSession::PumpWrite() {
    std::lock_guard<std::recursive_mutex> io_gate(server->Engine()->IoGate());
    if (closed || !serializer)
        return;
    while (!serializer->is_done()) {
        boost::system::error_code ec;
        std::size_t             written = 0;
        serializer->next(ec, [this, &written](boost::system::error_code& error,
                                              const auto& buffers) {
            written = socket.send(buffers, MSG_DONTWAIT | MSG_NOSIGNAL, error);
        });
        if (written != 0)
            serializer->consume(written);
        if (ec == asio::error::interrupted)
            continue;
        if (ec == asio::error::would_block || ec == asio::error::try_again) {
            ArmIoWait(tcp::socket::wait_write);
            return;
        }
        if (ec) {
            reply_posted.store(false, std::memory_order_release);
            Close();
            return;
        }
        if (written == 0) {
            // 无进展也无错误：等下一次可写就绪，不空转。
            ArmIoWait(tcp::socket::wait_write);
            return;
        }
    }
    // 本条回复已全部交给内核：本次请求的回复终结。
    // issue #19：清 reply_posted 让随后 Close（含 close_after_write 延迟路径）
    // 真正关闭 socket。
    reply_posted.store(false, std::memory_order_release);
    if (close_after_write || !reply_keep_alive || !server->Accepting() ||
        !server->OpenForIo()) {
        Close();
        return;
    }
    BeginRead();
}

void HttpSession::ReplyStatusAndClose(unsigned status) {
    std::lock_guard<std::recursive_mutex> io_gate(server->Engine()->IoGate());
    if (closed)
        return;
    if (peer_watch_armed) {
        peer_watch_armed = false;
        IoAsyncDone();       // 取消项在此记账，晚到完成项只消费空壳
        boost::system::error_code ignored;
        socket.cancel(ignored);
    }
    CancelDeadlineWatch();
    // 一次性回复后收口：无论字节是否全部交给内核，本会话都不再读下一条请求。
    close_after_write = true;
    reply_keep_alive  = false;

    http::response<http::string_body> res{
        static_cast<http::status>(status), 11};
    res.keep_alive(false);
    res.prepare_payload();
    response.emplace(std::move(res));
    try {
        serializer.emplace(*response);
        PumpWrite();
    } catch (...) {
        Close();
    }
}

void HttpSession::Abort() noexcept {
    // owner close：断开会话；已接纳 handler 不取消，按只交付一次终态收口。
    // 冻结契约 §6：owner 的 Close 直接丢弃未发送数据并同步释放 buffer，不做
    // flush。因此这里强制同步断开——置 discard_unsent 让 Close 跳过
    // close_after_write 延迟分支（该分支会在 Close 返回后继续写同一 socket）。
    discard_unsent.store(true, std::memory_order_release);
    Close();
}

void HttpSession::IoAsyncDone() {
    // 计数递减与反登记（MaybeRelease → server->UnregisterSession 触碰
    // m_sessions）同属本引擎的 io 域触碰：持门执行才能与 owner 的同步
    // teardown（门内遍历 m_sessions）配对。
    std::lock_guard<std::recursive_mutex> io_gate(server->Engine()->IoGate());
    inflight.fetch_sub(1);
    MaybeRelease();
}

void HttpSession::MaybeRelease() {
    if (!closed || inflight.load() != 0)
        return;
    if (!registered)
        return;
    registered = false;
    server->UnregisterSession(this);
}

void HttpSession::ReleaseBuffers() noexcept {
    // 冻结契约 §6：Close 直接丢弃未发送数据并同步释放 buffer。
    // 顺序：serializer（引用 message）→ message → parser → flat_buffer →
    // 排队中的 handler 结果载荷。以上都不被任何在途完成项引用（在途的只有
    // 「不借 payload 的就绪等待」），因此此处 reset 不会造成悬垂。
    serializer.reset();
    response.reset();
    parser.reset();
    buffer.clear();
    buffer.shrink_to_fit();
    if (pending_reply)
        pending_reply->r.reset();
    pending_reply.reset();
}

void HttpSession::Close() noexcept {
    // 域门内执行：socket/timer 只在此门内触碰（Close 的同步 teardown 也会
    // 经 Abort 走到这里，同线程嵌套由递归门承接）。
    std::lock_guard<std::recursive_mutex> io_gate(server->Engine()->IoGate());
    if (closed)
        return;
    // handler 已产出结果、DeliverResult 仍在 io 队列未写时，线级触发的关闭
    // （对端 FIN / 期限到点）标记 close_after_write，让响应经写路径写完后
    // 回到这里真正关闭。owner teardown 路径（Abort 置 discard_unsent）不走
    // 这条延迟分支：冻结契约 §6 要求 Close 丢弃未发送数据并同步释放。
    if (!discard_unsent.load(std::memory_order_acquire) &&
        reply_posted.load(std::memory_order_acquire) && !close_after_write) {
        close_after_write = true;
        return;
    }
    closed = true;
    // 取消并记账 deadline 看守（token 置空，晚到完成项 token 不匹配 → 空壳）。
    CancelDeadlineWatch();
    boost::system::error_code ignored;
    socket.close(ignored);   // 同时中止 pending 的 async_wait
    // 返回当刻记账被中止的等待项：它们的完成项仍会到达一次，但只消费空壳
    // （OnIoWait/OnDeadline/OnPeerWatch 的守护分支），不再触碰 fd 与
    // buffer。因此此刻 inflight 归零是「后端不再访问本对象资源」的真实判据。
    if (io_wait_armed) {
        io_wait_armed = false;
        IoAsyncDone();
    }
    if (peer_watch_armed) {
        peer_watch_armed = false;
        IoAsyncDone();
    }
    // 同步释放 owner 资源（parser/serializer/message/flat_buffer/排队载荷）。
    ReleaseBuffers();
    server->m_connections.fetch_sub(1);
    MaybeRelease();
}

// ------------------------------------------------------------------ Server

HttpServerImpl::HttpServerImpl(
    std::shared_ptr<HttpIoEngine> engine,
    HttpHandler                   handler,
    bbt::coroutine::CoObjectInfo  info)
    : m_engine(std::move(engine)),
      m_handler(std::move(handler)),
      m_info(std::move(info)),
      m_acceptor(m_engine->Io()) {}

result<void> HttpServerImpl::Bind(ListenAddress address) {
    boost::system::error_code ec;
    const auto addr = asio::ip::make_address(address.host, ec);
    if (ec) {
        Error e = ClassifyBackendError(ec, "listen address: bad host");
        e.code        = ErrorCode::InvalidArgument;
        e.domain_code = "bad_listen_host";
        return result<void>::err(std::move(e));
    }
    const tcp::endpoint ep(addr, address.port);
    m_acceptor.open(ep.protocol(), ec);
    if (!ec)
        m_acceptor.set_option(tcp::acceptor::reuse_address(true), ec);
    if (!ec)
        m_acceptor.bind(ep, ec);
    if (!ec)
        m_acceptor.listen(asio::socket_base::max_listen_connections, ec);
    // 非阻塞 acceptor：接纳由「就绪等待 + 域门内同步 accept」完成，
    // 已接纳连接与交接在同一段同步代码里，不留在 Asio 内部完成项中。
    if (!ec)
        m_acceptor.non_blocking(true, ec);
    if (ec) {
        Error e = ClassifyBackendError(ec, "http listen failed");
        e.code        = ErrorCode::Unavailable;
        e.domain_code = "listen_failed";
        return result<void>::err(std::move(e));
    }
    const auto bound = m_acceptor.local_endpoint();
    m_local.host = bound.address().to_string();
    m_local.port = bound.port();
    return result<void>::ok();
}

void HttpServerImpl::BeginAccept() {
    auto self = shared_from_this();
    m_engine->TryPost([self] { self->DoAccept(); });
}

void HttpServerImpl::DoAccept() {
    std::lock_guard<std::recursive_mutex> io_gate(m_engine->IoGate());
    if (m_teardown_done || !m_accepting.load() || !m_close.IsOpen()) {
        SignalDrain();
        return;
    }
    // accept_inflight 覆盖「本次接纳动作 + 就绪等待」：完成/中止时递减，
    // 使 off-domain 收口的 drain 判定不会把「已 accept 未交接」当成全零。
    m_accept_inflight.fetch_add(1);
    bool keep_accepting = true;
    for (int i = 0; i < kAcceptBatch; ++i) {
        if (m_teardown_done || !m_accepting.load() || !m_close.IsOpen()) {
            keep_accepting = false;
            break;
        }
        boost::system::error_code ec;
        tcp::socket peer(m_engine->Io());
        m_acceptor.accept(peer, ec);
        if (ec == asio::error::would_block || ec == asio::error::try_again) {
            break;   // 无待接纳连接：转就绪等待
        }
        if (ec == asio::error::interrupted)
            continue;
        if (ec) {
            // acceptor 被关/中止（或不可恢复错误）：停止接纳循环，不空转。
            keep_accepting = false;
            break;
        }
        // 已 accept：在此刻同一段同步代码里完成记账与交接。
        HandoffAccepted(std::move(peer));
    }
    if (!keep_accepting) {
        m_accept_inflight.fetch_sub(1);
        SignalDrain();
        return;
    }
    if (m_teardown_done || !m_accepting.load() || !m_close.IsOpen()) {
        m_accept_inflight.fetch_sub(1);
        SignalDrain();
        return;
    }
    // 就绪等待：不借 payload；Teardown/Close 关闭 acceptor 后本完成项只
    // 是空壳，接纳记账已在 Teardown 返回当刻完成。
    auto self = shared_from_this();
    try {
        m_accept_armed = true;
        m_acceptor.async_wait(tcp::socket::wait_read,
            [self](boost::system::error_code e) { self->OnAcceptReady(e); });
    } catch (...) {
        m_accept_armed = false;
        m_accept_inflight.fetch_sub(1);
        SignalDrain();
    }
}

void HttpServerImpl::OnAcceptReady(boost::system::error_code ec) {
    std::lock_guard<std::recursive_mutex> io_gate(m_engine->IoGate());
    if (!m_accept_armed) {
        // Teardown/Close 已在返回当刻记账：晚到空壳，不再接纳任何连接。
        return;
    }
    m_accept_armed = false;
    m_accept_inflight.fetch_sub(1);
    SignalDrain();
    if (ec)
        return;
    DoAccept();
}

void HttpServerImpl::HandoffAccepted(tcp::socket socket) {
    // 门内调用：已 accept 的 fd 与「登记进 server 会话集」在同一段同步代码
    // 内完成，不存在「已接纳未交接」的可观察窗口——owner Close 只能在交接
    // 完成后看到该会话；交接前的失败分支也在此同步关闭 fd。
    if (m_teardown_done || !m_accepting.load() || !m_close.IsOpen()) {
        boost::system::error_code ignored;
        socket.close(ignored);
        return;
    }
    if (m_connections.load() >= m_engine->Limits().max_connections) {
        // 连接数受限：可观察拒绝（对端见 reset），不占用会话资源。
        boost::system::error_code ignored;
        socket.close(ignored);
        return;
    }
    m_connections.fetch_add(1);
    std::shared_ptr<HttpSession> session;
    try {
        session = std::make_shared<HttpSession>(
            shared_from_this(), std::move(socket));
        session->Start();
    } catch (...) {
        // Start 先 RegisterSession 再 BeginRead，后者仍可抛。
        // 已构造则 Close：反登记 + connections--；未构造只回冲连接数。
        if (session)
            session->Close();
        else
            m_connections.fetch_sub(1);
    }
}

void HttpServerImpl::StopAccepting() noexcept {
    m_accepting.store(false);
    auto self = shared_from_this();
    m_engine->TryPost([self] {
        std::lock_guard<std::recursive_mutex> io_gate(self->m_engine->IoGate());
        boost::system::error_code ignored;
        self->m_acceptor.close(ignored);
    });
}

void HttpServerImpl::Close() noexcept {
    // 封口（拒新接纳 + 拒新派发）必须先于物理 teardown 完成：Close 返回后
    // 不再接纳/派发，不依赖 io 域的调度时延。
    m_close.BeginClose();
    m_accepting.store(false);
    {
        std::lock_guard<std::mutex> lk(m_drain_mtx);
        m_draining = true;
    }
    // 物理 teardown 必须在返回前真实发生（契约：返回即本对象资源已释放、
    // 后端不再访问）。不再「投递到 strand 并信任调度」：手动 Tick 模式下
    // strand 没有独立驱动线程，投递的 teardown 根本不会被执行，Close 会在
    // acceptor fd 仍存活时返回（父探针实测：返回后仍可 TCP connect）。
    // teardown 与 io 域 handler 经 Engine()->IoGate() 串行，因此可在调用
    // 线程内同步执行；同线程嵌套（teardown → session::Abort → Close）由
    // 可递归门承接，不会等待自身推进。
    Teardown();
    // 有界等待已派发 handler 与会话在途归零：让 Close 返回时尽量安静。
    // 这不是物理释放判据——契约明确「已接纳 handler 不抢占」，它可能仍在
    // 运行且不拥有本对象的 fd 资源（其回复路径已随 teardown 关闭）。
    {
        std::unique_lock<std::mutex> lk(m_drain_mtx);
        m_drain_cv.wait_for(lk, bbt::infra::detail::kCloseDrainTimeout,
                            [this] { return DrainedLocked(); });
    }
    // 物理释放落定：判据是 teardown 已在返回前真实执行（acceptor 关闭 +
    // 会话中止 + 就绪等待记账 + 会话 owner 资源同步释放），而不是等待是否
    // 超时、也不是逻辑计数是否归零——旧实现正是用「无在途时计数原为 0」
    // 在 fd 仍存活时宣告 Closed。
    // 一次性 closed hook（幂等，仅首次触发）。
    m_close.MarkClosed();
}

bool HttpServerImpl::TryAcquireHandler() noexcept {
    std::lock_guard<std::mutex> lk(m_drain_mtx);
    if (m_draining || m_pending_handlers >= m_engine->Limits().max_inflight)
        return false;
    ++m_pending_handlers;
    return true;
}

void HttpServerImpl::OnHandlerExited() noexcept {
    // 持锁递减并唤醒：Close() 的谓词在 m_drain_mtx 内求值，通知必须与
    // 状态更新同锁，否则可能与等待者的谓词求值竞争产生丢唤醒。
    std::lock_guard<std::mutex> lk(m_drain_mtx);
    if (m_pending_handlers > 0)
        --m_pending_handlers;
    m_drain_cv.notify_all();
}

void HttpServerImpl::RegisterSession(HttpSession* s) {
    m_sessions.insert(s);
    m_session_count.fetch_add(1);
}

void HttpServerImpl::UnregisterSession(HttpSession* s) {
    m_sessions.erase(s);
    m_session_count.fetch_sub(1);
    SignalDrain();
}

void HttpServerImpl::SignalDrain() noexcept {
    // 持锁通知：m_session_count / m_accept_inflight 的更新可能发生在锁外
    // （原子），通知与等待者的谓词求值经 m_drain_mtx 定序，避免丢唤醒。
    std::lock_guard<std::mutex> lk(m_drain_mtx);
    m_drain_cv.notify_all();
}

void HttpServerImpl::Teardown() noexcept {
    // 域门内执行：与 io 域 handler 串行，幂等（仅首次真正回收）。
    std::lock_guard<std::recursive_mutex> io_gate(m_engine->IoGate());
    if (m_teardown_done.load())
        return;
    m_teardown_done.store(true);
    m_accepting.store(false);
    boost::system::error_code ignored;
    m_acceptor.close(ignored);
    if (m_accept_armed) {
        // 关闭 acceptor 中止了就绪等待：在返回当刻记账，晚到的
        // OnAcceptReady 只消费空壳，不会再把 accept_inflight 减错。
        m_accept_armed = false;
        m_accept_inflight.fetch_sub(1);
    }
    // Abort 会经 Close→MaybeRelease 把节点从 m_sessions 抹除（若 inflight
    // 已 0）；inflight>0 的会话留在集合里等 completion。
    const std::vector<HttpSession*> sessions(m_sessions.begin(),
                                             m_sessions.end());
    for (auto* s : sessions)
        s->Abort();   // 中止在途会话：关闭连接；已接纳 handler 不取消
    {
        std::lock_guard<std::mutex> lk(m_drain_mtx);
        m_draining = true;
    }
    SignalDrain();
}

} // namespace bbt::infra::http_detail

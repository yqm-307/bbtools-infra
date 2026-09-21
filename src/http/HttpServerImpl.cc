#include "http/HttpServerImpl.hpp"

#include <exception>
#include <utility>
#include <vector>

#include <boost/asio/error.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/write.hpp>

#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/object/CoObject.hpp>

namespace bbt::infra::http_detail {

namespace asio = boost::asio;
namespace http = boost::beast::http;
using tcp      = asio::ip::tcp;

namespace {

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

void HttpSession::Start() {
    server->RegisterSession(this);
    registered = true;
    BeginRead();
}

void HttpSession::BeginRead() {
    if (closed || !server->Accepting() || !server->OpenForIo()) {
        Close();
        return;
    }
    // 本请求在途上下文：读请求起始时刻 + listener 有限预算，排队与处理
    // 都消耗该期限（不信任对端任何自定义 deadline 头）。
    in_flight = std::make_shared<InFlightRequest>();
    in_flight->deadline = std::chrono::steady_clock::now() +
                          server->Engine()->Limits().incoming_timeout;

    parser.emplace();
    parser->header_limit(
        static_cast<std::uint32_t>(server->Engine()->Limits().max_header_bytes));
    parser->body_limit(server->Engine()->Limits().max_body_bytes);

    auto self = shared_from_this();
    // expires_at 抛异常重载在 IoAsyncStart 之前；失败只放弃期限看守，
    // 不得 IoAsyncDone（否则 inflight 变负，session 永不反登记）。
    bool timer_armed = false;
    try {
        deadline_timer.expires_at(in_flight->deadline);
        timer_armed = true;
    } catch (...) {
    }
    if (timer_armed) {
        try {
            IoAsyncStart();
            deadline_timer.async_wait(
                [self](boost::system::error_code ec) { self->OnDeadline(ec); });
        } catch (...) {
            IoAsyncDone();
        }
    }
    try {
        IoAsyncStart();
        http::async_read(socket, buffer, *parser,
            [self](boost::system::error_code ec, std::size_t bytes) {
                self->OnRead(ec, bytes);
            });
    } catch (...) {
        IoAsyncDone();
        Close();
    }
}

void HttpSession::OnDeadline(boost::system::error_code ec) {
    IoAsyncDone();
    if (ec || closed)
        return;
    // 期限到点：通知在途 handler 取消（协作式），并关闭会话切断回复
    // 路径；handler 协程不抢占，其持有的 InFlightRequest 仍然有效。
    if (in_flight)
        in_flight->cancel_src.RequestCancel();
    Close();
}

void HttpSession::OnPeerWatch(boost::system::error_code ec) {
    IoAsyncDone();
    peer_watch_armed = false;
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
    // 连接断开是取消触发源：通知在途 handler，随后关闭会话。
    if (in_flight)
        in_flight->cancel_src.RequestCancel();
    Close();
}

void HttpSession::OnRead(boost::system::error_code ec, std::size_t) {
    IoAsyncDone();
    if (closed)
        return;
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
    if (!server->Accepting() || !server->OpenForIo()) {
        Close();   // StopAccepting/Closing 后的在途读不再派发新请求
        return;
    }
    Dispatch(parser->get().keep_alive());
}

void HttpSession::Dispatch(bool keep_alive) {
    // 锁内原子「drain 检查 + 记账」：拒绝在 teardown 已起步或达到
    // max_inflight 后再派发，保证 pending 计数必然归零可关。
    if (!server->TryAcquireHandler()) {
        // 可观察的接纳失败；不无界创建协程。
        ReplyStatusAndClose(503);
        return;
    }

    IncomingCallContext ctx;
    ctx.deadline       = in_flight->deadline;
    ctx.cancel         = in_flight->cancel_src.Token();
    ctx.peer_principal = "";   // 首闭环仅 loopback，未认证身份为空

    HttpRequest req = ToInfraRequest(parser->get());
    auto self = shared_from_this();
    auto req_state = in_flight;   // 协程任务持有在途状态直到 handler 退出
    bool succ = false;
    // 业务 handler 移交受管协程执行；I/O 回调到此为止。
    // req_state 经捕获随 coroutine task 存活：取消源/token 状态覆盖
    // handler 全程，不会被响应超时或连接断开提前释放（契约 §120）。
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
    // TryPost 失败 = io 域已封，回复路径已随 teardown 消失，结果直接丢弃；
    // 在途状态随本 coroutine task 退出释放——token 覆盖 handler 全程。
    // issue #19：先置 reply_posted 再投递——若 RequestClose 的 Teardown
    // 抢先进 io 队列并 Abort→Close，Close 见 reply_posted 会延迟到本
    // DeliverResult 写完才真关，不再丢弃已产出的响应。
    reply_posted.store(true, std::memory_order_release);
    server->Engine()->TryPost(
        [self, r = std::move(r), keep_alive]() mutable {
            self->DeliverResult(std::move(r), keep_alive);
        });
    server->OnHandlerExited();
}

void HttpSession::DeliverResult(result<HttpResponse> r, bool keep_alive) {
    if (closed)
        return;
    if (peer_watch_armed) {
        peer_watch_armed = false;
        // handler 阶段只挂了 wait_read 观察，取消它不妨碍其它 op。
        boost::system::error_code ignored;
        socket.cancel(ignored);
    }
    try {
        deadline_timer.cancel();
    } catch (...) {
    }

    http::response<http::string_body> res;
    res.version(11);
    if (r) {
        res.result(static_cast<http::status>(r.value().status));
        for (const auto& h : r.value().headers)
            res.insert(h.first, h.second);
        res.body() = std::move(r.value().body);
    } else {
        res.result(static_cast<http::status>(
            ErrorCodeToHttpStatus(r.error().code)));
    }
    res.keep_alive(keep_alive);
    res.prepare_payload();

    response.emplace(std::move(res));
    auto self = shared_from_this();
    try {
        IoAsyncStart();
        http::async_write(socket, *response,
            [self, keep_alive](boost::system::error_code ec, std::size_t bytes) {
                self->OnWritten(ec, bytes, keep_alive);
            });
    } catch (...) {
        IoAsyncDone();
        // 写发起失败=本次回复无法落地，清 reply_posted 使 Close 立即生效，
        // 不被 close_after_write 延迟逻辑卡在不关等待。
        reply_posted.store(false, std::memory_order_release);
        Close();
    }
}

void HttpSession::OnWritten(boost::system::error_code ec, std::size_t,
                            bool keep_alive) {
    IoAsyncDone();
    // issue #19：响应写完即本次请求的回复终结——清 reply_posted 让随后
    // 的 Close（含 close_after_write 延迟路径）真正关闭 socket。
    reply_posted.store(false, std::memory_order_release);
    if (closed)
        return;
    if (close_after_write || ec || !keep_alive || !server->Accepting() ||
        !server->OpenForIo()) {
        Close();
        return;
    }
    BeginRead();
}

void HttpSession::ReplyStatusAndClose(unsigned status) {
    if (closed)
        return;
    if (peer_watch_armed) {
        peer_watch_armed = false;
        boost::system::error_code ignored;
        socket.cancel(ignored);
    }
    try {
        deadline_timer.cancel();
    } catch (...) {
    }
    http::response<http::string_body> res{
        static_cast<http::status>(status), 11};
    res.keep_alive(false);
    res.prepare_payload();
    response.emplace(std::move(res));
    auto self = shared_from_this();
    try {
        IoAsyncStart();
        http::async_write(socket, *response,
            [self](boost::system::error_code, std::size_t) {
                self->IoAsyncDone();
                self->Close();
            });
    } catch (...) {
        IoAsyncDone();
        Close();
    }
}

void HttpSession::Abort() noexcept {
    // owner close 是取消触发源：先取消在途 handler，再断开会话。
    if (in_flight)
        in_flight->cancel_src.RequestCancel();
    Close();
}

void HttpSession::IoAsyncDone() {
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

void HttpSession::Close() noexcept {
    if (closed)
        return;
    // issue #19：handler 已产出结果、DeliverResult 仍在 io 队列未写时，
    // 立即关 socket 会让已排队响应在 `if(closed)` 被丢弃。改为标记
    // close_after_write 让响应经 OnWritten 写完后回到这里真正关闭；
    // DeliverResult 若发现 close_after_write 已置位则写完直接关。
    if (reply_posted.load(std::memory_order_acquire) && !close_after_write) {
        close_after_write = true;
        return;
    }
    closed = true;
    peer_watch_armed = false;
    try {
        deadline_timer.cancel();
    } catch (...) {
        // cancel 失败不可恢复也不致命：socket 关闭后 wait 不再有意义
    }
    boost::system::error_code ignored;
    socket.close(ignored);   // 同时中止 pending 的 async_wait/read/write
    server->m_connections.fetch_sub(1);
    MaybeRelease();
}

// ------------------------------------------------------------------ Server

HttpServerImpl::HttpServerImpl(
    std::shared_ptr<HttpIoEngine> engine,
    HttpHandler                   handler,
    bbt::coroutine::CoObjectInfo  info,
    std::shared_ptr<bbt::coroutine::CompletionSignal> close_sig)
    : m_engine(std::move(engine)),
      m_handler(std::move(handler)),
      m_info(std::move(info)),
      m_close(std::move(close_sig)),
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
    if (m_teardown_done || !m_accepting.load() || !m_close.IsOpen()) {
        DrainCheckClosed();
        return;
    }
    auto self = shared_from_this();
    try {
        m_accept_inflight.fetch_add(1);
        m_acceptor.async_accept(
            [self](boost::system::error_code ec, tcp::socket socket) {
                self->OnAccept(ec, std::move(socket));
            });
    } catch (...) {
        m_accept_inflight.fetch_sub(1);
        DrainCheckClosed();
    }
}

void HttpServerImpl::OnAccept(boost::system::error_code ec,
                              tcp::socket               socket) {
    // accept_inflight 持有到处理结束：RegisterSession 之前不得 -1，
    // 否则 off-domain DrainCheckClosed 会把「已 accept 未登记」当成全零。
    // 必须先 -1 再 DrainCheckClosed，否则本完成项仍占计数、Closed 永不落定。
    if (ec) {
        // acceptor 被关/中止：停止接纳循环；其他错误也停止（不空转）。
        m_accept_inflight.fetch_sub(1);
        DrainCheckClosed();
        return;
    }
    if (m_teardown_done || !m_accepting.load() || !m_close.IsOpen()) {
        boost::system::error_code ignored;
        socket.close(ignored);
        m_accept_inflight.fetch_sub(1);
        DrainCheckClosed();
        return;
    }
    if (m_connections.load() >= m_engine->Limits().max_connections) {
        // 连接数受限：可观察拒绝（对端见 reset），不占用会话资源。
        boost::system::error_code ignored;
        socket.close(ignored);
        m_accept_inflight.fetch_sub(1);
        DoAccept();
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
        m_accept_inflight.fetch_sub(1);
        DrainCheckClosed();
        return;
    }
    m_accept_inflight.fetch_sub(1);
    DoAccept();
}

void HttpServerImpl::StopAccepting() noexcept {
    m_accepting.store(false);
    auto self = shared_from_this();
    m_engine->TryPost([self] {
        boost::system::error_code ignored;
        self->m_acceptor.close(ignored);
    });
}

void HttpServerImpl::RequestClose() noexcept {
    if (!m_close.BeginClose())
        return;
    m_accepting.store(false);
    auto self = shared_from_this();
    // TryPost 失败：引擎已封则 teardown 已跑过，只复查归零；
    // post 分配失败则走 off-domain 逻辑收口，不得提前 MarkClosed。
    if (!m_engine->TryPost([self] { self->TeardownOnIoDomain(); }))
        TeardownOffDomain();
}

bool HttpServerImpl::TryAcquireHandler() noexcept {
    std::lock_guard<std::mutex> lk(m_drain_mtx);
    if (m_draining || m_pending_handlers >= m_engine->Limits().max_inflight)
        return false;
    ++m_pending_handlers;
    return true;
}

void HttpServerImpl::OnHandlerExited() noexcept {
    bool need = false;
    {
        std::lock_guard<std::mutex> lk(m_drain_mtx);
        if (m_pending_handlers > 0)
            --m_pending_handlers;
        need = m_draining && m_pending_handlers == 0;
    }
    if (!need)
        return;
    auto self = shared_from_this();
    if (!m_engine->TryPost([self] { self->DrainCheckClosed(); }))
        DrainCheckClosed();
}

void HttpServerImpl::RegisterSession(HttpSession* s) {
    m_sessions.insert(s);
    m_session_count.fetch_add(1);
}

void HttpServerImpl::UnregisterSession(HttpSession* s) {
    m_sessions.erase(s);
    m_session_count.fetch_sub(1);
    DrainCheckClosed();
}

void HttpServerImpl::DrainCheckClosed() noexcept {
    bool fin = false;
    {
        std::lock_guard<std::mutex> lk(m_drain_mtx);
        fin = m_draining && m_pending_handlers == 0
            && m_session_count.load() == 0
            && m_accept_inflight.load() == 0;
    }
    if (fin)
        m_close.MarkClosed();
}

void HttpServerImpl::TeardownOffDomain() noexcept {
    m_accepting.store(false);
    {
        std::lock_guard<std::mutex> lk(m_drain_mtx);
        m_draining = true;
    }
    DrainCheckClosed();
}

void HttpServerImpl::TeardownOnIoDomain() noexcept {
    if (m_teardown_done)
        return;
    m_teardown_done = true;
    m_accepting.store(false);
    boost::system::error_code ignored;
    m_acceptor.close(ignored);
    // Abort 会经 Close→MaybeRelease 把节点从 m_sessions 抹除（若 inflight
    // 已 0）；inflight>0 的会话留在集合里等 completion。
    const std::vector<HttpSession*> sessions(m_sessions.begin(),
                                             m_sessions.end());
    for (auto* s : sessions)
        s->Abort();   // 中止在途会话：取消 token + 关闭连接
    {
        std::lock_guard<std::mutex> lk(m_drain_mtx);
        m_draining = true;
    }
    DrainCheckClosed();
}

} // namespace bbt::infra::http_detail

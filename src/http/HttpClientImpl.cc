#include "http/HttpClientImpl.hpp"

#include <sys/socket.h>

#include <cstddef>
#include <iterator>
#include <vector>

#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>

#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>

namespace bbt::infra::http_detail {

namespace asio  = boost::asio;
namespace beast = boost::beast;
namespace http  = boost::beast::http;
using tcp       = asio::ip::tcp;

namespace {
// 每次非阻塞读的探测块大小（与读就绪等待配对，不做预读放大）。
constexpr std::size_t kReadChunk = 4096;
} // namespace

ClientOp::ClientOp(const std::shared_ptr<HttpClientImpl>& owner_,
                   const std::shared_ptr<HttpIoEngine>&   engine_)
    : owner(owner_),
      engine(engine_),
      resolver(engine_->Io()),
      socket(engine_->Io()) {}

// ------------------------------------------------------------------ 请求链

void ClientOp::Begin(std::string host, std::uint16_t port) {
    std::lock_guard<std::recursive_mutex> io_gate(engine->IoGate());
    if (finished || !owner->IsOpenForIo()) {
        // #55 F2：已封口/已落定的早退也是失败出口，先物理释放再注销。
        FailFinish(result<HttpResponse>::err(
            MakeError(ErrorCode::Closed, "http client is closing")));
        return;
    }
    // 用户非阻塞态不在此建立：socket 尚未 open 时 non_blocking() 返回
    // bad_descriptor 且不置位（B1）；每个 endpoint 的 async_connect 会
    // close/open socket，位也必须在那之后重建，见 OnConnect。
    // Issue #64：本次 operation 从此踏入 Connecting；此前（含参数/配额/封口
    // 拒绝与投递失败）阶段保持 NotStarted——这些路径上的失败都是确定失败。
    phase.store(RequestPhase::Connecting);

    auto self = shared_from_this();
    // 先记账再发起：completion 可能同步回调。发起抛异常则立刻冲销。
    try {
        op_armed = true;
        IoAsyncStart();
        if (io_fault)
            io_fault("resolve");
        resolver.async_resolve(
            std::move(host), std::to_string(port),
            [self](boost::system::error_code ec,
                   tcp::resolver::results_type results) {
                self->OnResolve(ec, std::move(results));
            });
    } catch (...) {
        op_armed = false;
        IoAsyncDone();
        FailFinish(result<HttpResponse>::err(
            MakeError(ErrorCode::InternalError,
                      "http request: resolve initiate failed")));
        return;
    }
}

void ClientOp::OnResolve(boost::system::error_code ec,
                         tcp::resolver::results_type  results) {
    // #56 H1 测试 seam：在取 IoGate 之前门控「解析完成项进入处理」，使用例
    // 能在 backend 查询已完成、回调尚未推进的时刻与 Close 竞争。生产恒空。
    if (on_resolve)
        on_resolve();
    std::lock_guard<std::recursive_mutex> io_gate(engine->IoGate());
    if (!op_armed) {
        // Abort/Close 已在返回当刻记账：晚到空壳，不触碰 socket/资源。
        return;
    }
    op_armed = false;
    IoAsyncDone();
    // resolver 完成项可能晚于 Abort/Finish 落定：已收口不再推进。
    if (finished)
        return;
    if (ec) {
        FailFinish(result<HttpResponse>::err(
            ClassifyBackendError(ec, "http request: resolve failed")));
        return;
    }
    // #57 H2：接管 endpoint 迭代，逐条在本门内连接，避免 range composed
    // operation 的内部 close/open 与跨线程 Abort/Close 并发触碰 socket。
    endpoints     = std::move(results);
    next_endpoint = 0;
    TryStartConnect();
}

void ClientOp::TryStartConnect() {
    std::lock_guard<std::recursive_mutex> io_gate(engine->IoGate());
    if (finished)
        return;
    // #57 H2 测试 seam：门控「连接内部步骤」（同一 IoGate 内）。生产恒空。
    if (on_connect_attempt)
        on_connect_attempt();
    // 门控期间 Close/Abort 可能已落定：必须复查，绝不在封口后重开 socket。
    if (finished)
        return;
    // 空解析结果：与 range async_connect 的 not_found 收口语义一致。
    if (next_endpoint >= endpoints.size()) {
        FailFinish(result<HttpResponse>::err(
            ClassifyBackendError(asio::error::not_found,
                                 "http request: connect failed")));
        return;
    }
    // 换 endpoint 可能换协议族：与 range 组合操作相同，先 close 再发起，
    // 使「关旧 socket → 按新 endpoint 打开（async_connect 内自动 open）→
    // 发起」全部落在本门内，与跨线程 Abort/Close 串行。这一步 close 也让
    // 「上一次失败尝试残留的中间 socket」在推进前被物理收口。
    boost::system::error_code cec;
    socket.close(cec);
    auto it = endpoints.begin();
    std::advance(it, static_cast<std::ptrdiff_t>(next_endpoint));
    auto self = shared_from_this();
    try {
        op_armed = true;
        IoAsyncStart();
        ++connect_attempts;
        if (io_fault)
            io_fault("connect");
        socket.async_connect(*it, [self](boost::system::error_code ec) {
            self->OnConnect(ec);
        });
    } catch (...) {
        op_armed = false;
        IoAsyncDone();
        FailFinish(result<HttpResponse>::err(
            MakeError(ErrorCode::InternalError,
                      "http request: connect initiate failed")));
    }
}

void ClientOp::OnConnect(boost::system::error_code ec) {
    std::lock_guard<std::recursive_mutex> io_gate(engine->IoGate());
    if (!op_armed) {
        // Abort/Close 已在返回当刻记账：晚到空壳。
        return;
    }
    op_armed = false;
    IoAsyncDone();
    if (finished)
        return;
    if (ec) {
        // 与 Boost.Asio range async_connect 的既有语义保持一致：其内部在
        // 每次完成后检查 socket；若 endpoint 的 socket 打开失败而仍未 open，
        // 将错误归一为 operation_aborted，并停止继续尝试。单 endpoint
        // async_connect 不执行这层 composed 检查，因此在这里显式保留它。
        // Close/Abort 的竞争不会误入此分支：Abort 先在同一 IoGate 将
        // op_armed 清零，晚到完成项在函数开头直接返回。
        boost::system::error_code connect_ec = ec;
        if (!socket.is_open())
            connect_ec = asio::error::operation_aborted;

        // #57 H2：本 endpoint 失败。若还有下一个 endpoint（且非本地中止），
        // 在同一门内推进重试；否则先物理释放再落定失败。
        if (connect_ec != asio::error::operation_aborted &&
            next_endpoint + 1 < endpoints.size()) {
            ++next_endpoint;
            TryStartConnect();
            return;
        }
        // #55 F2 共同根因：失败收口必须先物理释放再注销，否则 MaybeUnregister
        // 先把 op 移出 owner m_ops，owner Close 的快照随即为空，socket/body
        // 只能等 op 析构才释放——违反「不能依赖析构」。FailFinish 在同一
        // IoGate 内先 Abort（关 socket/resolver + ReleaseBuffers）再 Finish。
        FailFinish(result<HttpResponse>::err(
            ClassifyBackendError(connect_ec, "http request: connect failed")));
        return;
    }
    // B1 根因修复：每个 endpoint 的 async_connect 会对 socket close/open，
    // 令 socket_ops 的 user_set_non_blocking 位丢失；Begin 里在未 open 时设
    // 的位同样无效。真正连接成功后在此重建用户非阻塞态，并检查失败——否则
    // send/receive(MSG_DONTWAIT) 拿到的 EAGAIN 会被 Asio 在应用可见前吸收为
    // 阻塞 poll(-1) 并持 IoGate，ArmWait 不可达、Close 被阻塞。设置失败即
    // 安全收口，不进入写/读泵。
    boost::system::error_code nb;
    socket.non_blocking(true, nb);
    if (nb) {
        // B1-r2 父验收实证路径：此处 connect completion 已把 op_armed 清零、
        // inflight 归零；若直接 Finish，MaybeUnregister 会先把 op 移出
        // owner m_ops，owner Close 快照为空，fd 与 8MiB body 无人物理释放
        // （实测 fd_result=0、body_bytes=8388608）。FailFinish 在同一失败
        // 分支内先 Abort（关 socket/resolver + ReleaseBuffers）再 Finish：
        // 保证 op 离开 m_ops 前资源已物理释放。不进入 PumpWrite/PumpRead，
        // 错误码与消息语义不变。
        FailFinish(result<HttpResponse>::err(
            ClassifyBackendError(nb, "http request: set non-blocking failed")));
        return;
    }
    try {
        // Issue #64：进入写出阶段。此后直到 PumpWrite 判定 serializer
        // is_done() 为止，任何失败（写中断/被中止）都仍在「未完整写出」侧，
        // 保持确定失败。
        phase.store(RequestPhase::Writing);
        // 测试 seam（#64 F-1/F-3）：写出阶段已进入、尚未写出任何字节，同一
        // IoGate 内通知一次。用例据此得到「客户端已进入写出」的因果事件（不靠
        // accept 计数猜 Connecting/Writing），或在同一门内停放 io 域以确定性
        // 构造与放弃/Close 的先后。生产恒空。
        if (on_write_started)
            on_write_started();
        // 序列化器引用本 op 的 request：只在本门内、本次写生命周期内有效，
        // 绝不借给任何 async_* 操作（Abort 可同步 reset）。
        serializer.emplace(request);
        if (io_fault)
            io_fault("write");
        PumpWrite();
    } catch (...) {
        FailFinish(result<HttpResponse>::err(
            MakeError(ErrorCode::InternalError,
                      "http request: write initiate failed")));
        return;
    }
}

void ClientOp::PumpWrite() {
    std::lock_guard<std::recursive_mutex> io_gate(engine->IoGate());
    if (finished || !serializer)
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
            ArmWait(tcp::socket::wait_write);
            return;
        }
        if (ec) {
            FailFinish(result<HttpResponse>::err(
                ClassifyBackendError(ec, "http request: send failed")));
            return;
        }
        if (written == 0) {
            // 无进展也无错误：等下一次可写就绪，不空转。
            ArmWait(tcp::socket::wait_write);
            return;
        }
    }
    // Issue #64：完整写出的唯一线性化点。serializer->is_done() 为真表示请求
    // 消息（头 + body）的全部字节已由 socket.send 交给内核；此后再失去可信
    // 回复终态即为 OutcomeUnknown。不在 connect 成功、也不在首次 send 之前
    // 发布——那时请求可能一字节都没发出去。
    phase.store(RequestPhase::RequestCommitted);
    ArmRead();
}

void ClientOp::ArmRead() {
    std::lock_guard<std::recursive_mutex> io_gate(engine->IoGate());
    if (finished)
        return;
    // 每条响应一份新解析器：上一份（若有）随上次落定丢弃。
    parser.emplace();
    parser->header_limit(
        static_cast<std::uint32_t>(engine->Limits().max_header_bytes));
    parser->body_limit(engine->Limits().max_body_bytes);
    parser->eager(true);
    ArmWait(tcp::socket::wait_read);
    // 读就绪等待已真实武装（发起函数未抛）：此刻 read 完成项必然在途，
    // 只会被 Abort/close/对端写响应催出。测试 seam 在此刻通知，让测试
    // 能在冻结 strand 前确认「物理收口前名额仍占」的前提确实成立。
    if (!finished && op_armed && on_read_armed)
        on_read_armed();
}

void ClientOp::ArmWait(tcp::socket::wait_type type) {
    std::lock_guard<std::recursive_mutex> io_gate(engine->IoGate());
    if (finished || op_armed)
        return;
    auto self = shared_from_this();
    op_armed = true;
    IoAsyncStart();
    try {
        if (io_fault)
            io_fault("armwait");
        socket.async_wait(type, [self, type](boost::system::error_code ec) {
            self->OnIoReady(ec, type);
        });
    } catch (...) {
        op_armed = false;
        IoAsyncDone();
        FailFinish(result<HttpResponse>::err(MakeError(ErrorCode::InternalError,
            type == tcp::socket::wait_read ? "http request: read initiate failed"
                                           : "http request: write initiate failed")));
    }
}

void ClientOp::OnIoReady(boost::system::error_code ec,
                         tcp::socket::wait_type     type) {
    std::lock_guard<std::recursive_mutex> io_gate(engine->IoGate());
    if (!op_armed) {
        // Abort/Close 已在返回当刻记账：晚到空壳，不再触碰 socket/资源。
        return;
    }
    op_armed = false;
    IoAsyncDone();
    if (finished)
        return;
    if (ec) {
        FailFinish(result<HttpResponse>::err(ClassifyBackendError(ec,
            type == tcp::socket::wait_read
                ? "http request: incomplete or bad response"
                : "http request: send failed")));
        return;
    }
    try {
        if (io_fault)
            io_fault("io_pump");
        if (type == tcp::socket::wait_read)
            PumpRead();
        else
            PumpWrite();
    } catch (...) {
        FailFinish(result<HttpResponse>::err(
            MakeError(ErrorCode::InternalError,
                      "http request: io pump failed")));
    }
}

void ClientOp::PumpRead() {
    std::lock_guard<std::recursive_mutex> io_gate(engine->IoGate());
    if (finished || !parser)
        return;
    for (;;) {
        if (buffer.size() != 0) {
            boost::system::error_code ec;
            const auto consumed = parser->put(buffer.data(), ec);
            buffer.consume(consumed);
            if (ec == http::error::need_more)
                ec.clear();
            if (ec) {
                // 不足一条完整消息：eof/reset/partial 一律为 Error，
                // 不伪造成成功。
                FailFinish(result<HttpResponse>::err(ClassifyBackendError(
                    ec, "http request: incomplete or bad response")));
                return;
            }
            if (parser->is_done()) {
                // Issue #64：可信回复终态已取得（完整响应，含明确 4xx/5xx）。
                phase.store(RequestPhase::ReplyTerminal);
                HttpResponse out;
                const auto&  res = parser->get();
                out.status = res.result_int();
                for (const auto& f : res.base())   // fields 按序枚举
                    out.headers.emplace_back(std::string(f.name_string()),
                                             std::string(f.value()));
                out.body = res.body();
                boost::system::error_code ignored;
                socket.close(ignored);
                Finish(result<HttpResponse>::ok(std::move(out)));
                return;
            }
        }
        boost::system::error_code ec;
        const auto space = buffer.prepare(kReadChunk);
        if (asio::buffer_size(space) == 0) {
            FailFinish(result<HttpResponse>::err(
                MakeError(ErrorCode::InternalError,
                          "http request: read buffer limit")));
            return;
        }
        const auto received = socket.receive(space, MSG_DONTWAIT, ec);
        buffer.commit(received);
        if (ec == asio::error::would_block || ec == asio::error::try_again) {
            ArmWait(tcp::socket::wait_read);
            return;
        }
        if (ec == asio::error::interrupted)
            continue;
        if (ec) {
            FailFinish(result<HttpResponse>::err(ClassifyBackendError(
                ec, "http request: incomplete or bad response")));
            return;
        }
        if (received == 0) {
            // 对端 FIN 且无完整消息：按不完整响应收口（不伪造成成功）。
            FailFinish(result<HttpResponse>::err(ClassifyBackendError(
                asio::error::eof,
                "http request: incomplete or bad response")));
            return;
        }
    }
}

// ------------------------------------------------------------------ 收口

void ClientOp::Finish(result<HttpResponse> r) noexcept {
    // 首个落定者独占 outcome 写入与 Notify；完成/取消/deadline/
    // owner close 竞争时先到者的逻辑终态不被覆盖（契约 §128）。
    if (finished.exchange(true))
        return;
    // Issue #64：终态错误绑定本次落定的请求阶段，并在请求已完整写出时把
    // 「失去可信回复终态」升级为 OutcomeUnknown。调用方提前放弃（期限/协程
    // 取消）时已自行决定过阶段，SealError 对已带阶段者不再改写。
    if (!r)
        SealError(r.error(), phase.load());
    outcome = std::move(r);
    MaybeUnregister();
    waiter->Notify();
}

void ClientOp::IoAsyncDone() {
    inflight.fetch_sub(1);
    MaybeUnregister();
}

void ClientOp::MaybeUnregister() {
    // Finish（可能跨线程）与 IoAsyncDone（io 域）各自变化一个条件，
    // 两条落定路径都必须检查。
    if (finished && inflight.load() == 0)
        owner->UnregisterOp(shared_from_this());
}

void ClientOp::ReleaseBuffers() noexcept {
    // 冻结契约 §6：丢弃未发送数据并同步释放本 op 载荷。
    // 顺序：serializer（引用 request）→ request body → parser → flat_buffer。
    // 以上都不被任何在途完成项引用（在途的只有 socket/resolver 的就绪/组合
    // 操作壳），因此此处释放不会造成悬垂。
    serializer.reset();
    // 与空串交换：O(1) 且确定性地把旧缓冲交还给临时对象析构释放
    // （clear()+shrink_to_fit() 的容量回收是实现自定义的非绑定请求）。
    std::string().swap(request.body());
    parser.reset();
    buffer.clear();
    buffer.shrink_to_fit();
    // endpoint 解析结果是本 op 的载荷快照（H2 逐条连接用）：一并释放，避免
    // 离开 m_ops 后仍占住解析结果内存。resolver 对象本身不属于本方法——
    // 其寿命见 .hpp 中的 H1 说明，绝不在此销毁。
    endpoints = {};
}

void ClientOp::Abort() noexcept {
    std::lock_guard<std::recursive_mutex> io_gate(engine->IoGate());
    // 返回当刻记账被中止的完成项：它们的完成项仍会到达一次，但只消费空壳
    // （OnResolve/OnConnect/OnIoReady 的 !op_armed 分支），不再触碰资源。
    if (op_armed) {
        op_armed = false;
        IoAsyncDone();
    }
    boost::system::error_code ec;
    // resolver::cancel 无 error_code 重载且未标 noexcept：
    // noexcept 边界内必须兜底，避免异常逃逸成 terminate。
    // 注意（#56 H1）：cancel 只催出完成项，不 join Asio 解析服务线程；
    // 本 op 由完成 handler 捕获的 shared_from_this() 保活，故晚到完成项
    // 必然落在仍存活的对象上，只走 !op_armed 早退。
    try {
        resolver.cancel();
    } catch (...) {
    }
    socket.close(ec);
    ReleaseBuffers();
}

void ClientOp::FailFinish(result<HttpResponse> r) noexcept {
    // #55 F2 共同根因的唯一收口机制：所有失败路径都先物理释放再注销。
    // 顺序不可交换——Finish 经 MaybeUnregister 在 finished && inflight==0
    // 时把 op 移出 owner m_ops；若先 Finish，op 离开 m_ops 时 socket 仍
    // open、body/parser/flat_buffer 仍被持有，owner Close 的快照随即为空，
    // 这些资源只能等 op 析构才释放（违反「不能依赖析构」）。Abort 在同一
    // IoGate 内关 socket/resolver + ReleaseBuffers，使 op 离开 m_ops 前
    // 资源已物理释放。
    Abort();
    Finish(std::move(r));
}

// ------------------------------------------------------------------ Client

result<HttpResponse> HttpClientImpl::Request(HttpRequest request,
                                             const CallOptions& options) {
    // N-02：非协程上下文明确拒绝；请求等待只在协程上下文有意义。
    if (g_bbt_tls_coroutine_co == nullptr)
        return result<HttpResponse>::err(MakeError(ErrorCode::InvalidContext,
            "HttpClient::Request must run in coroutine context"));
    if (!m_close.IsOpen())
        return result<HttpResponse>::err(
            MakeError(ErrorCode::Closed, "http client is closing or closed"));

    auto parsed = ValidateRequest(request, m_engine->Limits());
    if (!parsed)
        return result<HttpResponse>::err(std::move(parsed).error());

    // Issue #37：在登记 op / 发起任何 async_* 之前，向 owner 原子预留
    // max_connections/max_inflight 名额。名额不足立即 Overloaded——
    // 此时还没建 socket/resolver，也没向 io 域投递，满足「拒绝路径
    // 不建活跃连接」。admit 为空表示本 client 不接 owner 预算。
    // 异常安全：归还钩子在 admit 之前先备好——std::function 复制可抛
    // bad_alloc，若把它放到 admit 成功后执行，名额会在「已占用、无
    // 归还责任者」的窗口中泄漏。release 先于 admit 落定后，admit
    // 成功才 armed 武装：此后任何在「op 接管名额」（on_unregister
    // 挂到 op 且 RegisterOp 成功）之前抛出的异常，都由 guard 析构
    // 归还；admit 失败/抛出时 armed=false，名额未占、guard 不误归还。
    struct AdmitGuard {
        std::function<void()> release;
        bool                  armed{false};
        ~AdmitGuard() { if (armed && release) release(); }
        void Arm()    { armed = true; }
        void Disarm() { armed = false; }
    };
    AdmitGuard slot;
    slot.release = m_release;           // 可抛复制先于 admit 完成
    if (m_admit) {
        auto admitted = m_admit();
        if (!admitted)
            return result<HttpResponse>::err(std::move(admitted).error());
        slot.Arm();                     // admit 成功：guard 接管归还责任
    }

    auto waiter = bbt::coroutine::sync::CoWaiter::Create();
    auto op = std::make_shared<ClientOp>(
        std::static_pointer_cast<HttpClientImpl>(shared_from_this()),
        m_engine);
    op->waiter = waiter;
    // op 与 guard 持有同一份 release：RegisterOp 成功后 Disarm 解除
    // guard 兜底责任，op 物理收口时经 on_unregister 归还；admit 为空
    // （不接 owner 预算）时 release 恒空，行为与基线一致。
    op->on_unregister = slot.release;
    // 测试 seam：把注入的 read-armed 通知交给本 op；写发生在 TryPost
    // （向 io 域派发 Begin）之前，io 域读到的必然是最新值。
    op->on_read_armed = m_read_armed_hook;
    // 测试 seam（#64）：同上，写发生在 TryPost（向 io 域派发 Begin）之前。
    op->on_write_started = m_write_started_hook;

    http::request<http::string_body>& breq = op->request;
    breq.version(11);
    breq.method_string(request.method);   // 任意 tchar method 原样编码
    breq.target(parsed.value().target);
    breq.set(http::field::host, parsed.value().host); // Host 由 adapter 按 URL 写入
    for (const auto& h : request.headers)
        breq.insert(h.first, h.second);        // insert 保留重复头
    breq.body() = std::move(request.body);
    breq.keep_alive(false);
    breq.prepare_payload();

    auto self = std::static_pointer_cast<HttpClientImpl>(shared_from_this());
    const std::string   host = parsed.value().host;
    const std::uint16_t port = parsed.value().port;

    // 契约 §2 请求完成范式：先登记等待事件 → 再执行一次投递回调 → 随后挂起。
    // on_registered 在事件登记成功后、协程真正 park 前调用：其内同步登记
    // op 并投递 Begin（登记先于 post，Close 与提交竞态也能被明确拒绝）。
    // Begin 若在该窗口内落定，Notify 走 PENDING 早到路径，不丢唤醒。
    bbt::coroutine::WaitOptions wait;
    wait.deadline = options.deadline;
    const auto status = waiter->WaitWithCallback(
        wait, [this, &slot, self, op, host, port]() mutable -> bool {
            if (!self->RegisterOp(op)) {
                // op 未进 m_ops，不会经 UnregisterOp 归还名额——guard
                // 析构归还（slot 未 Disarm）。
                op->Finish(result<HttpResponse>::err(
                    MakeError(ErrorCode::Closed, "http client closed")));
                // 仍返回 true：事件已登记，Finish 的 Notify 走 PENDING
                // 早到路径立即兑现，恢复后按 Completed 读 Closed 终态。
                return true;
            }
            // RegisterOp 成功：名额所有权转交 op->on_unregister（物理
            // 收口时归还），解除本作用域的回退责任。
            slot.Disarm();
            if (!m_engine->TryPost([op, host, port]() mutable {
                    op->Begin(std::move(host), port);
                })) {
                // io 域已封：op 由收口路径接管（Finish 幂等），直接落定 Closed。
                op->Finish(result<HttpResponse>::err(
                    MakeError(ErrorCode::Closed, "http client closed")));
            }
            return true;
        });
    if (status == bbt::coroutine::WaitStatus::Completed) {
        // outcome 已由 Finish 写入；Notify 与 Wait 经等待位建立可见性。
        return std::move(*op->outcome);
    }
    // Issue #64：调用方放弃时刻的阶段事实即分界——请求尚未完整写出时保持确定
    // 失败（TimedOut/Cancelled），已完整写出时升级为 OutcomeUnknown。
    // F-1（独立审查必修）：放弃决定必须与「后端不再推进」是同一线性化点。若只
    // 读一次阶段快照就返回、把 Abort 异步投递出去，快照与「未来完整写出」之间
    // 没有同步关系：快照时未提交 ⇒ 返回确定失败，而被投递的 io 域仍可
    // Begin/OnConnect/PumpWrite 把请求字节完整交给内核（甚至收到完整响应）。
    // 这里仿照 Close/TeardownOnIoDomain 的既有范式：调用线程取 engine 的
    // IoGate，在同一门内先 Abort（记账被中止的等待项 + 关 socket/resolver +
    // 释放载荷），再读阶段并落定逻辑终态。返回值因此只可能取自「后端已不可能
    // 再写」之后的阶段：
    //   - 未提交（NotStarted/Connecting/Writing）⇒ io 域此后任何入口都因
    //     finished / !op_armed 早退（Begin 也拒绝推进），确定失败为真；
    //   - 已提交（RequestCommitted/ReplyTerminal）⇒ SealError 升级为
    //     OutcomeUnknown，不谎报确定失败。
    // Abort 在返回当刻记账被中止的等待项并同步释放 op 载荷，故必须同时落定
    // 逻辑终态，否则 op 会以 finished=false 留在 m_ops（配额与账面无法归还）。
    // Finish 必须在门内：它置位的 finished 正是让排队中的 Begin 拒绝推进的那
    // 个判据；门内落定后无需再向 io 域投递收口任务。
    Error decided = WaitStatusToError(status);
    {
        std::lock_guard<std::recursive_mutex> io_gate(m_engine->IoGate());
        op->Abort();
        op->SealError(decided, op->Phase());
        op->Finish(result<HttpResponse>::err(decided));
    }
    return result<HttpResponse>::err(std::move(decided));
}

void HttpClientImpl::Close() noexcept {
    // 先封口再投递 teardown：任何线程进入 Close 都立即拒绝新 op，
    // 「Close 返回后不再有新 I/O 发起」不依赖 io 域的调度时延。
    // 封口先于 teardown：任何线程进入 Close 都立即拒绝新 op，「Close
    // 返回后不再有新 I/O 发起」不依赖 io 域的调度时延。
    m_close.BeginClose();
    {
        std::lock_guard<std::mutex> lk(m_ops_mtx);
        m_io_dead = true;
    }
    // 物理 teardown 必须在返回前真实发生（契约：返回即 op 的 socket 与载荷已
    // 释放；resolver 对象可能由完成项保活，后台查询是否停止不由本层断言，
    // 后端不再访问已释放资源）。不再「投递到 strand 并信任调度」：
    // 手动 Tick 模式下 strand 无独立驱动线程，投递的 teardown 不会被执行。
    // 本调用经 Engine()->IoGate() 与 io 域 handler 串行，故可安全地在调用
    // 线程内同步执行；同线程嵌套由可递归门承接，不等待自身推进。
    TeardownOnIoDomain();
    // 有界等待在途 op 归零（≤ detail::kCloseDrainTimeout）：计数由 completion
    // 落定驱动，本线程只在自身锁 + condition_variable 上等，让返回时尽量
    // 安静。这不是物理释放判据——teardown 已 Abort 掉 op 的 socket/resolver，
    // 并同步释放了 op 载荷。
    {
        std::unique_lock<std::mutex> lk(m_ops_mtx);
        m_drain_cv.wait_for(lk, bbt::infra::detail::kCloseDrainTimeout,
                            [this] { return m_ops.empty(); });
    }
    // 物理释放落定：判据是 teardown 已在返回前真实执行（不再依赖 post 成功
    // 或逻辑计数为 0）；不因等待超时假造 Closed。
    m_close.MarkClosed();
}

void HttpClientImpl::TeardownOnIoDomain() noexcept {
    // 域门内执行：与 io 域 handler 串行（Abort 触碰 socket/resolver）。
    std::lock_guard<std::recursive_mutex> io_gate(m_engine->IoGate());
    // 快照后在锁外逐个收口：Abort 计入被中止的等待项并同步释放 op 载荷，
    // 使计数在返回当刻归零（§Closed 契约：后端不再访问 op 资源）。
    std::vector<std::shared_ptr<ClientOp>> snapshot;
    {
        std::lock_guard<std::mutex> lk(m_ops_mtx);
        if (m_teardown_snapshotted)
            return;
        m_teardown_snapshotted = true;
        m_io_dead = true;
        snapshot.assign(m_ops.begin(), m_ops.end());
    }
    for (auto& op : snapshot) {
        op->Abort();
        op->Finish(result<HttpResponse>::err(
            MakeError(ErrorCode::Closed, "http client closed")));
    }
    m_drain_cv.notify_all();
}

} // namespace bbt::infra::http_detail

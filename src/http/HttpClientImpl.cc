#include "http/HttpClientImpl.hpp"

#include <vector>

#include <boost/asio/connect.hpp>

#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>

namespace bbt::infra::http_detail {

namespace asio  = boost::asio;
namespace beast = boost::beast;
namespace http  = boost::beast::http;
using tcp       = asio::ip::tcp;

ClientOp::ClientOp(const std::shared_ptr<HttpClientImpl>& owner_,
                   const std::shared_ptr<HttpIoEngine>&   engine_)
    : owner(owner_),
      engine(engine_),
      resolver(engine_->Io()),
      socket(engine_->Io()) {}

void ClientOp::Begin(std::string host, std::uint16_t port) {
    if (finished || !owner->IsOpenForIo()) {
        Finish(result<HttpResponse>::err(
            MakeError(ErrorCode::Closed, "http client is closing")));
        return;
    }
    parser.header_limit(
        static_cast<std::uint32_t>(engine->Limits().max_header_bytes));
    parser.body_limit(engine->Limits().max_body_bytes);

    auto self = shared_from_this();
    // 先记账再发起：completion 可能同步回调。发起抛异常则立刻冲销。
    try {
        IoAsyncStart();
        resolver.async_resolve(
            std::move(host), std::to_string(port),
            [self](boost::system::error_code ec,
                   tcp::resolver::results_type results) {
                self->OnResolve(ec, std::move(results));
            });
    } catch (...) {
        IoAsyncDone();
        Finish(result<HttpResponse>::err(
            MakeError(ErrorCode::InternalError,
                      "http request: resolve initiate failed")));
        return;
    }
}

void ClientOp::OnResolve(boost::system::error_code ec,
                         tcp::resolver::results_type  results) {
    IoAsyncDone();
    // resolver 完成项可能晚于 Abort/Finish 落定：已收口不再推进。
    if (finished)
        return;
    if (ec) {
        Finish(result<HttpResponse>::err(
            ClassifyBackendError(ec, "http request: resolve failed")));
        return;
    }
    auto self = shared_from_this();
    try {
        IoAsyncStart();
        asio::async_connect(socket, results,
            [self](boost::system::error_code ec, const tcp::endpoint&) {
                self->OnConnect(ec);
            });
    } catch (...) {
        IoAsyncDone();
        Finish(result<HttpResponse>::err(
            MakeError(ErrorCode::InternalError,
                      "http request: connect initiate failed")));
        return;
    }
}

void ClientOp::OnConnect(boost::system::error_code ec) {
    IoAsyncDone();
    if (finished)
        return;
    if (ec) {
        Finish(result<HttpResponse>::err(
            ClassifyBackendError(ec, "http request: connect failed")));
        return;
    }
    auto self = shared_from_this();
    try {
        IoAsyncStart();
        http::async_write(socket, request,
            [self](boost::system::error_code ec, std::size_t bytes) {
                self->OnWritten(ec, bytes);
            });
    } catch (...) {
        IoAsyncDone();
        Finish(result<HttpResponse>::err(
            MakeError(ErrorCode::InternalError,
                      "http request: write initiate failed")));
        return;
    }
}

void ClientOp::OnWritten(boost::system::error_code ec, std::size_t) {
    IoAsyncDone();
    if (finished)
        return;
    if (ec) {
        Finish(result<HttpResponse>::err(
            ClassifyBackendError(ec, "http request: send failed")));
        return;
    }
    auto self = shared_from_this();
    try {
        IoAsyncStart();
        http::async_read(socket, buffer, parser,
            [self](boost::system::error_code ec, std::size_t bytes) {
                self->OnRead(ec, bytes);
            });
    } catch (...) {
        IoAsyncDone();
        Finish(result<HttpResponse>::err(
            MakeError(ErrorCode::InternalError,
                      "http request: read initiate failed")));
        return;
    }
}

void ClientOp::OnRead(boost::system::error_code ec, std::size_t) {
    IoAsyncDone();
    if (finished)
        return;
    if (ec) {
        // 不足一条完整消息：eof/reset/partial 一律为 Error，不伪造成成功。
        Finish(result<HttpResponse>::err(
            ClassifyBackendError(ec, "http request: incomplete or bad response")));
        return;
    }
    const auto& res = parser.get();
    HttpResponse out;
    out.status = res.result_int();
    for (const auto& f : res.base())   // fields 按序枚举，重复头不折叠
        out.headers.emplace_back(std::string(f.name_string()),
                                 std::string(f.value()));
    out.body = res.body();
    boost::system::error_code ignored;
    socket.close(ignored);
    Finish(result<HttpResponse>::ok(std::move(out)));
}

void ClientOp::Finish(result<HttpResponse> r) noexcept {
    // 首个落定者独占 outcome 写入与 Complete；完成/取消/deadline/
    // owner close 竞争时先到者的逻辑终态不被覆盖（契约 §128）。
    if (finished.exchange(true))
        return;
    outcome = std::move(r);
    MaybeUnregister();
    sig->Complete();
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

void ClientOp::Abort() noexcept {
    boost::system::error_code ec;
    // resolver::cancel 无 error_code 重载且未标 noexcept：
    // noexcept 边界内必须兜底，避免异常逃逸成 terminate。
    try {
        resolver.cancel();
    } catch (...) {
    }
    socket.close(ec);
}

result<HttpResponse> HttpClientImpl::Request(HttpRequest request,
                                             const CallOptions& options) {
    // N-02：非协程上下文明确拒绝；完成信号只支持协程内等待。
    if (g_bbt_tls_coroutine_co == nullptr)
        return result<HttpResponse>::err(MakeError(ErrorCode::InvalidContext,
            "HttpClient::Request must run in coroutine context"));
    if (!m_close.IsOpen())
        return result<HttpResponse>::err(
            MakeError(ErrorCode::Closed, "http client is closing or closed"));

    auto parsed = ValidateRequest(request, m_engine->Limits());
    if (!parsed)
        return result<HttpResponse>::err(std::move(parsed).error());

    std::shared_ptr<bbt::coroutine::CompletionSignal> sig;
    try {
        sig = std::make_shared<bbt::coroutine::CompletionSignal>();
    } catch (const std::logic_error&) {
        return result<HttpResponse>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "coroutine runtime generation unavailable"));
    }

    auto op = std::make_shared<ClientOp>(
        std::static_pointer_cast<HttpClientImpl>(shared_from_this()),
        m_engine);
    op->sig = sig;

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

    // 登记先于 post：与 RequestClose 竞态的提交也能被 teardown 明确拒绝，
    // 不会因 io 域已封而漏通知等待者（见 TeardownOnIoDomain 的 io_dead）。
    auto self = std::static_pointer_cast<HttpClientImpl>(shared_from_this());
    if (!self->RegisterOp(op))
        return result<HttpResponse>::err(
            MakeError(ErrorCode::Closed, "http client closed"));
    const std::string host = parsed.value().host;
    const std::uint16_t port = parsed.value().port;
    if (!m_engine->TryPost([op, host, port]() mutable {
            op->Begin(std::move(host), port);
        })) {
        // io 域已封：op 已被 teardown 收口（Finish 幂等），直接落定 Closed。
        op->Finish(result<HttpResponse>::err(
            MakeError(ErrorCode::Closed, "http client closed")));
        return result<HttpResponse>::err(
            MakeError(ErrorCode::Closed, "http client closed"));
    }

    bbt::coroutine::WaitOptions wait;
    wait.deadline = options.deadline;
    wait.cancel   = options.cancel;
    const auto status = sig->Wait(wait);
    if (status == bbt::coroutine::WaitStatus::Completed) {
        // outcome 已由 Finish 写入；Complete/Wait 经同一互斥建立可见性。
        return std::move(*op->outcome);
    }
    // 逻辑结果先行返回；物理清理继续：io 域中止后端 op。
    // TryPost 失败说明引擎已封，op 已由 teardown 收口，无需再投递。
    m_engine->TryPost([op] { op->Abort(); });
    return result<HttpResponse>::err(WaitStatusToError(status));
}

void HttpClientImpl::RequestClose() noexcept {
    if (!m_close.BeginClose())
        return;
    auto self = std::static_pointer_cast<HttpClientImpl>(shared_from_this());
    // TryPost 失败两种情形：引擎已封 ⇒ teardown 必然已执行过（引擎只有
    // 在全部子对象 Closed 后才封口），兜底路径幂等无害；post 抛异常/
    // 引擎未启动 ⇒ 无法再进 io 域，改走逻辑收口 + 计数门控。
    if (!m_engine->TryPost([self] { self->TeardownOnIoDomain(); }))
        self->TeardownOffDomain();
}

void HttpClientImpl::TeardownOnIoDomain() noexcept {
    if (m_close.IsClosed())
        return;
    // 快照后在锁外逐个收口：Abort 催在途完成项落定；Finish 让可能仍
    // 在 Wait 的调用者以 Closed 落定。io_dead 先于快照置位，此后
    // RegisterOp 一律失败，无漏网 op。
    // 注意：Abort/Finish 都不等于后端完成项已执行——MarkClosed 由
    // 「m_io_dead 且 m_ops 空」门控，在途完成项经 IoAsyncDone 归零后
    // 才允许落定（§Closed 契约：后端不再访问 op 资源）。
    std::vector<std::shared_ptr<ClientOp>> snapshot;
    {
        std::lock_guard<std::mutex> lk(m_ops_mtx);
        m_io_dead = true;
        snapshot.assign(m_ops.begin(), m_ops.end());
    }
    for (auto& op : snapshot) {
        op->Abort();
        op->Finish(result<HttpResponse>::err(
            MakeError(ErrorCode::Closed, "http client closed")));
    }
    DrainCheckClosed();
}

void HttpClientImpl::TeardownOffDomain() noexcept {
    // 投递失败的兜底：socket/resolver 只许 io 域触碰，故不能 Abort；
    // 仅逻辑收口已登记 op（Finish 本就支持跨线程），在途完成项自然
    // 落定后经计数门控汇合 MarkClosed。
    std::vector<std::shared_ptr<ClientOp>> snapshot;
    {
        std::lock_guard<std::mutex> lk(m_ops_mtx);
        if (m_io_dead) {
            // teardown 已执行（io 域或本路径重复进入），不再重复收口。
            return;
        }
        m_io_dead = true;
        snapshot.assign(m_ops.begin(), m_ops.end());
    }
    for (auto& op : snapshot)
        op->Finish(result<HttpResponse>::err(
            MakeError(ErrorCode::Closed, "http client closed")));
    DrainCheckClosed();
}

void HttpClientImpl::DrainCheckClosed() noexcept {
    bool fin;
    {
        std::lock_guard<std::mutex> lk(m_ops_mtx);
        fin = m_io_dead && m_ops.empty();
    }
    if (fin)
        m_close.MarkClosed();
}

} // namespace bbt::infra::http_detail

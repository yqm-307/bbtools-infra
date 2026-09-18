#pragma once
// HttpServerImpl：真实 HTTP/1.1 listener（Boost.Beast/Asio）。
//
// 执行域切分（契约 §N1 核心约束）：
//   - accept/read/write 全部在 engine 的 io 域（共享 executor 的 strand，
//     由 Scheduler 事件循环线程推进）上由 Asio 完成 handler 推进；
//   - 业务 HttpHandler 只在受管协程内调用：read 完成后经
//     Scheduler::RegistCoroutineTask 派生，结果经 TryPost 回到 io 域写出；
//   - I/O 回调自身不运行任何可能挂起的业务代码，不新造事件状态机。
//
// 取消语义（契约 §120）：
//   - 每个已派发请求有独立 InFlightRequest 状态：CancellationSource（写端）
//     与本地 deadline 由它持有，直到 handler 协程真正退出——不随响应
//     超时/连接断开提前释放；
//   - 取消触发源：本地期限（deadline_timer）、连接断开（wait_read 观测）、
//     owner close（session Abort/server teardown）；
//   - deadline 只取 listener 本地预算（接纳读请求时刻 + incoming_timeout），
//     HTTP 首切片不信任任何自定义 deadline 头；协议无取消帧语义，不向
//     对端承诺远端取消必达。

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_set>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/parser.hpp>
#include <boost/beast/http/string_body.hpp>

#include <bbt/coroutine/sync/Cancellation.hpp>
#include <bbt/infra/HttpServer.hpp>

#include "http/HttpDetail.hpp"
#include "http/HttpIoEngine.hpp"

namespace bbt::infra::http_detail {

class HttpServerImpl;

// 一次已派发请求的在途状态。由 session（io 域）与 handler 协程
// （经 lambda 捕获）共同持有：session 在 handler 运行期间持它用于
// deadline/断开/中止时写入取消；handler 协程持有它保证 ctx.cancel
// 的底层状态在 handler 退出前始终有效。
struct InFlightRequest {
    bbt::coroutine::CancellationSource cancel_src;
    bbt::coroutine::Deadline           deadline{};
};

// 一条入站连接的堆上会话状态；跨 io/协程两域共享，由 shared_ptr 保活。
// 域纪律：socket/buffer/parser/response/timer/watch 仅 io 域触碰；
// RunHandler 在协程线程执行，只读已移交的请求并回投结果。
class HttpSession : public std::enable_shared_from_this<HttpSession> {
public:
    HttpSession(std::shared_ptr<HttpServerImpl>      server,
                boost::asio::ip::tcp::socket       sock);

    void Start();              // io 域：登记进 server 会话集 + BeginRead
    void Abort() noexcept;     // io 域：取消在途 token 并关闭会话
    void Close() noexcept;     // io 域：关闭 socket；反登记延到 inflight==0
    bool IsClosed() const noexcept { return closed; }

    // 每次成功发起 async_* +1，对应 completion 入口 -1。
    // Close/Abort 只催完成项；计数归零且 closed 才 UnregisterSession。
    void IoAsyncStart() { inflight.fetch_add(1); }
    void IoAsyncDone();
    void MaybeRelease();

    // ---- 以下均为 io 域 ----
    void BeginRead();
    void OnRead(boost::system::error_code ec, std::size_t bytes);
    void OnDeadline(boost::system::error_code ec);
    void OnPeerWatch(boost::system::error_code ec);
    void Dispatch(bool keep_alive);
    void DeliverResult(result<HttpResponse> r, bool keep_alive);
    void OnWritten(boost::system::error_code ec, std::size_t bytes,
                   bool keep_alive);
    void ReplyStatusAndClose(unsigned status);

    // ---- 协程线程 ----
    // 调用方的 coroutine task 已捕获 InFlightRequest 持其至 handler 退出。
    void RunHandler(HttpRequest request, IncomingCallContext ctx,
                    bool keep_alive);

    const std::shared_ptr<HttpServerImpl>      server;
    boost::asio::ip::tcp::socket               socket;
    boost::beast::flat_buffer                buffer;
    std::optional<
        boost::beast::http::request_parser<
            boost::beast::http::string_body>> parser;
    std::optional<
        boost::beast::http::response<
            boost::beast::http::string_body>> response;
    // 当前在途请求状态（io 域独占访问；协程侧经捕获的 shared_ptr 共享）。
    std::shared_ptr<InFlightRequest>         in_flight;
    boost::asio::steady_timer                deadline_timer;
    bool                                     peer_watch_armed{false};
    bool                                     closed{false};
    bool                                     registered{false};
    std::atomic_int                          inflight{0};
};

class HttpServerImpl : public HttpServer,
                       public IIoTeardown,
                       public std::enable_shared_from_this<HttpServerImpl> {
public:
    HttpServerImpl(std::shared_ptr<HttpIoEngine> engine,
                   HttpHandler                   handler,
                   bbt::coroutine::CoObjectInfo  info,
                   std::shared_ptr<bbt::coroutine::CompletionSignal> close_sig);

    // 控制线程：建 acceptor、bind、listen；成功后 BeginAccept 由工厂发起。
    result<void>   Bind(ListenAddress address);
    void           BeginAccept();   // post accept 循环入口到 io 域

    ListenAddress  LocalAddress() const override { return m_local; }
    void           StopAccepting() noexcept override;

    void           RequestClose() noexcept override;
    bool           IsClosed() const noexcept override {
        return m_close.IsClosed();
    }
    CloseStatus    WaitClosed(bbt::coroutine::Deadline          deadline,
                              bbt::coroutine::CancellationToken cancel) override {
        return m_close.WaitClosed(deadline, std::move(cancel),
                                  m_info.generation);
    }
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override {
        return m_info;
    }

    // IIoTeardown：io 域一次性回收——acceptor 关闭 + 会话中止；
    // MarkClosed 延迟到 handler 退出且 session/accept 的 in-flight
    // async 计数归零（物理清理完成）。
    void TeardownOnIoDomain() noexcept override;
    void TeardownOffDomain() noexcept override;

    // 发布前注册（runtime 聚合物理清理完成）。
    void SetClosedHook(std::function<void()> hook) {
        m_close.SetClosedHook(std::move(hook));
    }

    // ---- 仅 io 域 ----
    void DoAccept();
    void OnAccept(boost::system::error_code ec,
                  boost::asio::ip::tcp::socket socket);
    void RegisterSession(HttpSession* s);
    void UnregisterSession(HttpSession* s);
    // draining 且 pending_handlers==0 且无会话且无在途 accept 才 MarkClosed。
    // 会话数走 atomic，允许 TryPost 失败时从非 io 域复查。
    void DrainCheckClosed() noexcept;

    // 派发记账（io 域）：teardown 已开始或达到 max_inflight 时返回
    // false（调用方回 503 关闭）；成功时 pending+1，与 OnHandlerExited
    // 的 -1 严格配对。锁内原子检查+记账，堵住「teardown 与派发竞争
    // 导致计数无法归零」的洞。
    bool TryAcquireHandler() noexcept;

    // 派发记账：RunHandler 退出（协程线程）时 -1；再回投 DrainCheckClosed。
    void OnHandlerExited() noexcept;

    const HttpHandler& Handler() const { return m_handler; }
    const std::shared_ptr<HttpIoEngine>& Engine() const { return m_engine; }
    bool Accepting() const { return m_accepting.load(); }
    bool OpenForIo() const { return m_close.IsOpen(); }

    std::atomic_size_t m_connections{0};

private:
    std::shared_ptr<HttpIoEngine>                m_engine;
    HttpHandler                                  m_handler;
    bbt::coroutine::CoObjectInfo                 m_info;
    ManagedCloseState                            m_close;
    boost::asio::ip::tcp::acceptor               m_acceptor;
    ListenAddress                                m_local;
    std::atomic_bool                             m_accepting{true};
    std::unordered_set<HttpSession*>             m_sessions;   // 仅 io 域
    std::atomic_int                              m_session_count{0};
    bool                                         m_teardown_done{false}; // io 域
    std::atomic_int                              m_accept_inflight{0}; // 仅 io 域写
    // drain 记账对：已派发未退出的 handler 数 + teardown 标志；
    // 跨 io/协程两线程，一律持锁配对修改，避免丢最后一份退出通知。
    std::mutex                                   m_drain_mtx;
    std::size_t                                  m_pending_handlers{0};
    bool                                         m_draining{false};
};

} // namespace bbt::infra::http_detail

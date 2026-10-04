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
// 关闭语义（进程寿命运行时修订）：
//   - StopAccepting 只停新连接及既有连接上的新请求接纳，不取消已接纳
//     handler、不关闭其回复路径；
//   - Close() 幂等、任意线程可调用，返回即物理释放（acceptor 关闭、
//     会话中止、已派发 handler 退出后在途归零）——teardown 在 Engine 的
//     io 域门内同步执行，不依赖 strand 被独立驱动（手动 Tick 下投递式
//     teardown 不会被执行）——无 RequestClose/
//     WaitClosed、无取消令牌：已接纳 handler 不抢占，其回复在写路径上
//     按「只交付一次终态」收口；
//   - deadline 只取 listener 本地预算（接纳读请求时刻 + incoming_timeout），
//     HTTP 首切片不信任任何自定义 deadline 头；协议无取消帧语义，不向
//     对端承诺远端取消必达。
//
// 资源模型（冻结契约 resource-close-boundary §6：Close 直接丢弃未发送数据
// 并同步释放 buffer，不做 flush）：
//   - parser/serializer/message(buffer)/待发送 payload 全部是会话自身的
//     owner 资源，只在本对象的方法调用（同一把 IoGate 内）被访问；
//   - 所有等待都是「不借 payload 的就绪等待」：socket.async_wait(wait_read/
//     wait_write) 只持有 socket 壳与 shared_ptr 保活，不引用 buffer/parser/
//     serializer/message；
//   - 因此 Close 可以在返回当刻同步交付：中止就绪等待、关闭 fd、释放
//     parser/serializer/message/flat_buffer 与排队中的 handler 结果载荷；
//     晚到的就绪完成项只消费一个空壳（见 OnIoWait/OnPeerWatch 的记账分支），
//     不再触碰任何 fd 或 buffer。
//   - accept 侧同理：就绪等待 + 域门内非阻塞 accept，接纳与交接在同一段
//     同步代码内完成，不存在「已 accept 未交接」的可观察窗口。

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_set>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/parser.hpp>
#include <boost/beast/http/serializer.hpp>
#include <boost/beast/http/string_body.hpp>

#include <bbt/infra/HttpServer.hpp>

#include "http/HttpDetail.hpp"
#include "http/HttpIoEngine.hpp"

namespace bbt::infra::http_detail {

class HttpServerImpl;

// 一次已派发请求的在途状态。由 session（io 域）与 handler 协程
// （经 lambda 捕获）共同持有：session 在 handler 运行期间持它用于
// deadline 判定；handler 协程持有它保证期限信息在 handler 退出前始终有效。
// 不再携带取消令牌：运行时无业务取消令牌，服务端不取消已接纳 handler。
struct InFlightRequest {
    bbt::coroutine::Deadline deadline{};
};

// handler 结果回投的同步可清空载荷。
// RunHandler 在协程线程把结果放进本壳，session 持壳（pending_reply），
// 排队到 io 域的 lambda 只捕获壳、不自己持有 payload。owner Close 在域门内
// 把壳的 payload 清空 ⇒ 即使 io 域此后不再被驱动（NO_LOOP/手动 Tick），
// 8MiB 级响应体在 Close 返回当刻已经释放；晚到的排队 lambda 只看到空壳。
struct QueuedReply {
    std::optional<result<HttpResponse>> r;
    bool                                keep_alive{false};
};

// 一条入站连接的堆上会话状态；跨 io/协程两域共享，由 shared_ptr 保活。
// 域纪律：socket/buffer/parser/serializer/response/timer/watch/pending_reply
// 仅 io 域触碰（含 owner 的同步 teardown，经同一把 IoGate 串行）；
// RunHandler 在协程线程执行，只读已移交的请求并回投结果壳。
class HttpSession : public std::enable_shared_from_this<HttpSession> {
public:
    HttpSession(std::shared_ptr<HttpServerImpl>      server,
                boost::asio::ip::tcp::socket       sock);

    void Start();              // io 域：登记进 server 会话集 + BeginRead
    void Abort() noexcept;     // io 域：关闭会话（丢弃未发送数据）
    void Close() noexcept;     // io 域：关闭 socket + 同步释放 owner 资源
    bool IsClosed() const noexcept { return closed; }

    // 每次成功发起 async_* +1，对应 completion 入口 -1。
    // 就绪/期限等待在 Close/取消侧记账（见各 *_armed 标记）：Close 返回当刻
    // 计数归零，晚到完成项只消费空壳、不再二次递减。
    // Close/Abort 只催完成项；计数归零且 closed 才 UnregisterSession。
    void IoAsyncStart() { inflight.fetch_add(1); }
    void IoAsyncDone();
    void MaybeRelease();

    // ---- 以下均为 io 域 ----
    void BeginRead();
    void PumpRead();                                   // 非阻塞读 + 解析器泵
    void PumpWrite();                                  // 序列化器 + 非阻塞写泵
    void ArmIoWait(boost::asio::ip::tcp::socket::wait_type type);
    void OnIoWait(boost::system::error_code ec,
                  boost::asio::ip::tcp::socket::wait_type type);
    void OnDeadline(boost::system::error_code ec, std::uint64_t token);
    // 取消当前活跃的 deadline 看守并在返回当刻履行其记账义务（token 置空 +
    // IoAsyncDone）；晚到的完成项 token 不匹配，只消费空壳。仅 io 域、门内。
    void CancelDeadlineWatch() noexcept;
    void OnPeerWatch(boost::system::error_code ec);
    void Dispatch(bool keep_alive);
    void DeliverResult(std::shared_ptr<QueuedReply> reply);
    void ReplyStatusAndClose(unsigned status);
    // 释放本会话持有的一切 owner 资源（顺序：serializer → message →
    // parser → flat_buffer → 排队载荷）；仅 io 域、门内调用。
    void ReleaseBuffers() noexcept;

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
    // 引用 *response 的序列化器：它的寿命必须短于 response（ReleaseBuffers
    // 与每次重写回复时都先 reset 它）。它不借给任何 async_* 操作。
    std::optional<
        boost::beast::http::response_serializer<
            boost::beast::http::string_body>> serializer;
    // 当前在途请求状态（io 域独占访问；协程侧经捕获的 shared_ptr 共享）。
    std::shared_ptr<InFlightRequest>         in_flight;
    boost::asio::steady_timer                deadline_timer;
    // 排队等待投递的 handler 结果壳（见 QueuedReply）。
    std::shared_ptr<QueuedReply>             pending_reply;
    bool                                     peer_watch_armed{false};
    bool                                     closed{false};
    bool                                     registered{false};
    std::atomic_int                          inflight{0};
    // 「结果已产出待写」标记：RunHandler 把结果壳挂到会话并投递进 io 队列
    // 后置位；Close 见到它时置 close_after_write 延迟到 OnWritten 真正关闭，
    // 用于「对端 FIN/期限到点」这类线级触发的收口——让已产出响应还能写完。
    // 协程线程写、io 域读 → atomic；close_after_write 仅 io 域触碰。
    std::atomic_bool                         reply_posted{false};
    bool                                     close_after_write{false};
    // owner teardown 标记（Abort 置位）：冻结契约 §6——本对象 Close 直接
    // 丢弃未发送数据并同步释放 buffer，不做 flush。因此 owner 路径必须强制
    // 同步断开，不得走 reply_posted 的「写完再关」延迟分支（该分支会让
    // Close 返回后 socket 仍存活且被后端继续写）。
    std::atomic_bool                         discard_unsent{false};
    // 就绪/期限等待的已武装标记（仅 io 域）：取值者负责递减 inflight。
    // 发起方置位 +1；完成项或 Close（返回当刻记账）置零并 -1，另一侧看到
    // 置零即认定自己是晚到空壳、不再触碰 fd 与 buffer。
    bool                                     io_wait_armed{false};
    // deadline 看守的世代记号（仅 io 域）：每次 arm 分配新 token 并置为当前
    // 活跃值；完成项只在自身 token 仍等于活跃值时递减（记账 -1）并驱动收口，
    // 被 cancel/Close 抢先记账的旧看守完成项 token 已不匹配，只消费空壳。这样
    // keep-alive 重挂不会让旧的 aborted completion 消费新看守的记账：每个
    // outstanding timer completion 恰好递减一次。
    std::uint64_t                            timer_gen{0};
    std::uint64_t                            timer_live{0};
    // 本条回复是否 keep-alive（io 域；PumpWrite 收尾据此决定继续读下一条）。
    bool                                     reply_keep_alive{false};
};

class HttpServerImpl : public HttpServer,
                       public std::enable_shared_from_this<HttpServerImpl> {
public:
    HttpServerImpl(std::shared_ptr<HttpIoEngine> engine,
                   HttpHandler                   handler,
                   bbt::coroutine::CoObjectInfo  info);

    // 控制线程：建 acceptor、bind、listen；成功后 BeginAccept 由工厂发起。
    result<void>   Bind(ListenAddress address);
    void           BeginAccept();   // post accept 循环入口到 io 域

    ListenAddress  LocalAddress() const override { return m_local; }
    void           StopAccepting() noexcept override;

    // 幂等、任意线程；返回即物理释放。owner 域由 Engine()->IoGate() 定义：
    // 所有由本模块入口发生的 fd 触碰（io 域 handler、完成回调、owner 的同步
    // teardown）都持该门，因此 teardown 可在调用线程内同步执行，不需要（也
    // 不能依赖）strand 被独立驱动——手动 Tick 模式下 strand 由调用线程驱动，
    // 投递的 teardown 不会执行。序列：封口（拒新接纳/派发）→ 域门内同步关
    // acceptor + 中止会话（幂等；owner 路径经 Abort 强制同步断开并丢弃未发送
    // 响应，不走 close_after_write 延迟分支）→ 有界等待已派发 handler 退出 →
    // 在途归零才落定一次性 closed hook。handler 卡死交 supervisor，不因超时
    // 假造 Closed。
    void           Close() noexcept override;
    bool           IsClosed() const noexcept override {
        return m_close.IsClosed();
    }
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override {
        return m_info;
    }

    // 同步物理回收（幂等）：域门内关闭 acceptor + 中止全部会话。可在任意
    // 线程调用——与 io 域 handler 经 Engine()->IoGate() 串行。
    void Teardown() noexcept;

    // 发布前注册（runtime 聚合物理清理完成）。
    void SetClosedHook(std::function<void()> hook) {
        m_close.SetClosedHook(std::move(hook));
    }

    // ---- 仅 io 域 ----
    void DoAccept();
    // 就绪等待完成项：已记账（Teardown/Close）则只消费空壳。
    void OnAcceptReady(boost::system::error_code ec);
    // 已 accept 的连接在同一段门内同步交接给会话（无未交接窗口）。
    void HandoffAccepted(boost::asio::ip::tcp::socket socket);
    void RegisterSession(HttpSession* s);
    void UnregisterSession(HttpSession* s);
    // 唤醒在 Close() 上有界等待在途归零的线程（状态由调用方先行更新）。
    // 必须持 m_drain_mtx 通知，避免与等待者的谓词求值竞争产生丢唤醒。
    void SignalDrain() noexcept;

    // 派发记账（io 域）：teardown 已开始或达到 max_inflight 时返回
    // false（调用方回 503 关闭）；成功时 pending+1，与 OnHandlerExited
    // 的 -1 严格配对。锁内原子检查+记账，堵住「teardown 与派发竞争
    // 导致计数无法归零」的洞。
    bool TryAcquireHandler() noexcept;

    // 派发记账：RunHandler 退出（协程线程）时 -1，并唤醒 drain 等待者。
    void OnHandlerExited() noexcept;

    const HttpHandler& Handler() const { return m_handler; }
    const std::shared_ptr<HttpIoEngine>& Engine() const { return m_engine; }
    bool Accepting() const { return m_accepting.load(); }
    bool OpenForIo() const { return m_close.IsOpen(); }

    std::atomic_size_t m_connections{0};

    // 回归/诊断探针：当前仍登记的会话数（已 Start 未反登记）。只读、不参与
    // 任何生产逻辑，供 issue #62 的 deadline timer 记账回归断言使用。
    std::size_t DebugSessionCount() const noexcept {
        return static_cast<std::size_t>(m_session_count.load());
    }

private:
    // drain 判定：封口（m_draining）后已派发 handler 全部退出、会话与
    // accept 在途均归零 ⇒ 物理清理完成。调用方须持 m_drain_mtx。
    bool DrainedLocked() const {
        return m_teardown_done.load() && m_draining
            && m_pending_handlers == 0
            && m_session_count.load() == 0
            && m_accept_inflight.load() == 0;
    }

    std::shared_ptr<HttpIoEngine>                m_engine;
    HttpHandler                                  m_handler;
    bbt::coroutine::CoObjectInfo                 m_info;
    ManagedCloseState                            m_close;
    boost::asio::ip::tcp::acceptor               m_acceptor;
    ListenAddress                                m_local;
    std::atomic_bool                             m_accepting{true};
    std::unordered_set<HttpSession*>             m_sessions;   // 仅 io 域
    std::atomic_int                              m_session_count{0};
    std::atomic_bool                             m_teardown_done{false};
    std::atomic_int                              m_accept_inflight{0}; // 仅 io 域写
    // accept 就绪等待已武装（仅 io 域）：与 m_accept_inflight 配对，
    // Teardown 在返回当刻记账中止的就绪完成项，晚到项只消费空壳。
    bool                                         m_accept_armed{false};
    // drain 记账对：已派发未退出的 handler 数 + teardown 标志；
    // 跨 io/协程两线程，一律持锁配对修改，避免丢最后一份退出通知。
    // m_drain_cv 供 Close() 有界等待（≤ detail::kCloseDrainTimeout）。
    std::mutex                                   m_drain_mtx;
    std::condition_variable                      m_drain_cv;
    std::size_t                                  m_pending_handlers{0};
    bool                                         m_draining{false};
};

} // namespace bbt::infra::http_detail

#pragma once
// HttpClientImpl：真实 HTTP/1.1 客户端（Boost.Beast/Asio）。
//
// 桥接方式：Request 在协程内发起，异步链 resolve→connect→write→read
// 全部运行在 engine 的 io 域（共享 executor 上的 strand，由 Scheduler
// 现有事件循环线程推进）；终态 handler 把 result<HttpResponse>
// 写入堆上 operation state 后 Notify 等待者；调用协程经
// CoWaiter::WaitWithCallback 先登记事件、再投递 Begin、随后挂起
// （契约 §2 请求完成范式）。I/O 回调不触碰业务协程栈，不新造事件
// 状态机，不依赖 Linux Hook。
//
// 关闭语义（进程寿命运行时修订）：Close() 幂等、任意线程可调用，
// 返回即本 client 拥有的物理资源（在途 op 的 socket/resolver）已释放、
// 后端不会再访问；无 RequestClose/WaitClosed、无取消令牌、无 CompletionSignal。
//
// 资源模型（与 server 同源，冻结契约 resource-close-boundary §6：Close 直接
// 丢弃未发送数据并同步释放 buffer，不 flush）：
//   - request/parser/flat_buffer 是 op 自身的 owner 资源，只在 io 域门内被
//     访问；写/读走「非阻塞 send/receive + 序列化器/解析器泵」；
//   - 一切等待都是不借 payload 的就绪等待（socket.async_wait），只有
//     resolve/connect 这两步由 Asio 组合操作推进（它们不借本 op 的
//     payload，只持 socket/resolver 自身）；
//   - Abort（owner Close 的收口入口）在返回当刻记账被中止的等待项、关闭
//     socket、释放 request body/parser/flat_buffer，故 Close 返回即
//     「后端不再访问本 op 资源」；晚到的完成项只消费空壳。

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/parser.hpp>
#include <boost/beast/http/serializer.hpp>
#include <boost/beast/http/string_body.hpp>

#include <bbt/infra/HttpClient.hpp>

#include "http/HttpDetail.hpp"
#include "http/HttpIoEngine.hpp"

namespace bbt::infra::http_detail {

class HttpClientImpl;

// 一次出站的堆上 operation state：晚到回调只访问它，不借用调用者栈。
struct ClientOp : std::enable_shared_from_this<ClientOp> {
    // op 对 owner 持强引用：保证反登记路径上 impl 仍存活；impl 侧仅在
    // m_ops 登记期间反向持有 op，落定后解除。
    std::shared_ptr<HttpClientImpl>       owner;
    std::shared_ptr<HttpIoEngine>         engine;
    // 本请求的唯一等待位（CoWaiter 语义：一个 waiter 一个等待位）。
    // Finish 的 Notify 跨线程安全；早到（协程 park 前）唤醒走
    // CoPollEvent 的 PENDING 路径，不丢唤醒。
    bbt::coroutine::sync::CoWaiter::SPtr  waiter;
    boost::asio::ip::tcp::resolver        resolver;
    boost::asio::ip::tcp::socket          socket;
    boost::beast::flat_buffer             buffer;
    boost::beast::http::request<boost::beast::http::string_body> request;
    // 引用 *request 的序列化器：寿命必须短于 request（ReleaseBuffers 先
    // reset 它）。它不借给任何 async_* 操作。
    std::optional<
        boost::beast::http::request_serializer<
            boost::beast::http::string_body>> serializer;
    std::optional<
        boost::beast::http::response_parser<
            boost::beast::http::string_body>> parser;
    std::optional<result<HttpResponse>>   outcome;
    // 首次发布即逻辑终态：Finish 经 CAS 保证只落定一次（io 域为主，
    // TryPost 失败/调用方提前返回的收口路径可能在其它线程触发）。
    std::atomic_bool                      finished{false};
    // 在途 async_* 计数：每次发起 +1，对应 completion 落定 -1。
    // Abort/close/cancel 只催完成项不直接清零——计数归零且已 Finish
    // 才允许反登记，owner 据此判断后端不再触碰本 op（§Closed 契约）。
    // 发起/落定全在 io 域；唯一跨线程读是「Begin 未发出即 Finish」
    // 的收口路径（该路径计数恒为 0），故用 atomic 保守兜底。
    std::atomic_int                       inflight{0};
    // 是否有「已在 io 域登记完成项的等待」在途（仅 io 域访问）。
    // 取值者负责递减 inflight：完成项或 Abort（返回当刻记账）置零并 -1，
    // 另一侧看到已置零即认定自己是晚到空壳，不再触碰 socket/资源。
    bool                                  op_armed{false};

    ClientOp(const std::shared_ptr<HttpClientImpl>& owner_,
             const std::shared_ptr<HttpIoEngine>&   engine_);

    // 以下全部只在 io 域调用（Finish 除外，见上）
    void Begin(std::string host, std::uint16_t port);
    void PumpWrite();     // 序列化器 + 非阻塞 send
    void PumpRead();      // 非阻塞 receive + 解析器泵
    void ArmRead();       // 写完成后武装读就绪等待（不借 payload）
    void ArmWait(boost::asio::ip::tcp::socket::wait_type type);
    void OnIoReady(boost::system::error_code ec,
                   boost::asio::ip::tcp::socket::wait_type type);
    void Abort() noexcept;
    void Finish(result<HttpResponse> r) noexcept;
    // 释放本 op 持有的 owner 载荷（serializer → request body → parser →
    // flat_buffer）；仅 io 域、门内调用。
    void ReleaseBuffers() noexcept;

    // 发起型 async_* 成功返回即计在途（发起函数抛异常则未发起、
    // 无完成项，不计）。
    void IoAsyncStart() { inflight.fetch_add(1); }
    // 每个 completion 入口调用一次：计数 -1 后按 finished&&归零收口。
    void IoAsyncDone();
    // finished 且 inflight==0 才反登记；两个条件分别在 Finish 与
    // completion 落定路径上变化，两处都必须调用。
    void MaybeUnregister();

    // Issue #37：物理收口归还钩子。op 离开 m_ops（= 后端不再访问
    // 本 op 资源）的瞬间由 UnregisterOp 调一次。TryAdmit 成功才
    // 登记该钩，所以严格配对、每条路径只归还一次。
    std::function<void()> on_unregister;

    // 测试 seam：请求写完成、读就绪等待武装后，在 io 域内调用一次。
    // 测试用它在冻结 strand 前确认 read 已真实在途。默认空即不通知。
    std::function<void()> on_read_armed;

private:
    void OnResolve(boost::system::error_code ec,
                   boost::asio::ip::tcp::resolver::results_type results);
    void OnConnect(boost::system::error_code ec);
};

class HttpClientImpl : public HttpClient,
                       public std::enable_shared_from_this<HttpClientImpl> {
public:
    HttpClientImpl(std::shared_ptr<HttpIoEngine> engine,
                   bbt::coroutine::CoObjectInfo  info)
        : m_engine(std::move(engine)),
          m_info(std::move(info)) {}

    // Issue #37：配额钩子由 owner（NetworkRuntimeImpl）在创建时注入。
    // admit 在「登记 op / 发起 I/O 之前」原子预留名额；release 在 op
    // 物理收口（UnregisterOp）时被调一次。两者都允许为空（空 = 不
    // 计量），便于在不接 owner 预算的上下文构造 client。
    void SetQuotaHooks(std::function<result<void>()> admit,
                       std::function<void()>        release) {
        m_admit  = std::move(admit);
        m_release = std::move(release);
    }

    result<HttpResponse> Request(HttpRequest request,
                                 const CallOptions& options) override;

    // 幂等、任意线程；返回即物理释放。teardown 经 Engine()->IoGate() 与
    // io 域 handler 串行，故在调用线程内同步执行，不依赖 strand 被独立
    // 驱动（手动 Tick 下投递式 teardown 不会被执行）。序列：封口 → 域门内
    /// 同步中止在途 op（Abort：记账等待项 + 关 socket + 释放 op 载荷；唤醒
    // 其等待者）→ 有界等待在途计数归零 → 在途归零才落定一次性 closed hook。
    void Close() noexcept override;
    bool IsClosed() const noexcept override { return m_close.IsClosed(); }
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override {
        return m_info;
    }

    // 同步物理回收（幂等）：域门内中止在途 op（催完成项）并让等待者以
    // Closed 落定。可在任意线程调用，与 io 域 handler 经 IoGate 串行。
    void TeardownOnIoDomain() noexcept;

    // 发布前注册（runtime 聚合物理清理完成）。
    void SetClosedHook(std::function<void()> hook) {
        m_close.SetClosedHook(std::move(hook));
    }

    const std::shared_ptr<HttpIoEngine>& Engine() const { return m_engine; }

    // 测试 seam：给后续新建的 op 注入 read-armed 通知（io 域内、
    // 读就绪等待武装成功后触发）。空 = 不通知。
    void SetReadArmedHookForTest(std::function<void()> hook) {
        m_read_armed_hook = std::move(hook);
    }

    // m_ops 跨线程（调用线程登记、io 域反登记），一律持锁访问。
    // 集合持 op 强引用：快照后可跨线程安全收口，不会迭代到悬垂指针。
    // 返回 false 表示已封口（Close 已置 m_io_dead），调用方按 Closed
    // 拒绝——保证「已登记必被终态收口」，等待者不会因 post 丢失而挂住。
    bool RegisterOp(std::shared_ptr<ClientOp> op) {
        std::lock_guard<std::mutex> lk(m_ops_mtx);
        if (m_io_dead)
            return false;
        m_ops.insert(std::move(op));
        return true;
    }
    // op 在 finished 且 in-flight 归零后才调用本方法离开 m_ops：
    // m_ops 清空即「后端不再访问任何 op 资源」，Close 的在途等待据此返回。
    // Issue #37：这里同时是出站配额的「物理收口」归还点——op 一旦
    // 离开 m_ops，其占用的 max_connections/max_inflight 名额立即
    // 经 op->on_unregister 归还给 owner，晚于此才允许名额复用。
    // 原子一次性：erase 的返回值是本 op 是否「真正从集合移除」的判定。
    // Finish 可跨线程、IoAsyncDone 在 io 域，两条收口路径可并发进入
    // 本方法——只有抢到 erase 成功（返回 1）的一方才执行归还并参与
    // 落定判定；另一方看到已不在集合，直接返回，绝不二次归还。
    void UnregisterOp(std::shared_ptr<ClientOp> op) {
        bool erased = false;
        {
            std::lock_guard<std::mutex> lk(m_ops_mtx);
            erased = (m_ops.erase(op) != 0);
        }
        if (!erased)
            return;                  // 已被并发收口路径移除：不重复归还
        if (op->on_unregister)
            op->on_unregister();
        // 通知可能正在 Close() 中等待在途归零的线程。
        m_drain_cv.notify_all();
    }
    bool IsOpenForIo() const { return m_close.IsOpen(); }

private:
    std::shared_ptr<HttpIoEngine> m_engine;
    bbt::coroutine::CoObjectInfo  m_info;
    ManagedCloseState             m_close;
    std::mutex                    m_ops_mtx;
    std::condition_variable       m_drain_cv;   // 与 m_ops_mtx 配对
    // 未落定 op 集合（持强引用防悬垂）：op 在 finished 且 in-flight
    // 归零后才离开。
    std::unordered_set<std::shared_ptr<ClientOp>> m_ops;
    // 封口标志（m_ops_mtx 保护）：Close 置位后 RegisterOp 一律拒绝，
    // 保证「Close 返回后不再有新 I/O 发起」。与「teardown 是否已快照」
    // 分离——Close 先封口再投递 teardown，off-domain 兜底路径不得因封口
    // 而跳过逻辑收口。
    bool                          m_io_dead{false};
    bool                          m_teardown_snapshotted{false};

    // Issue #37：owner 注入的配额钩子；空表示不计量。
    std::function<result<void>()> m_admit;
    std::function<void()>         m_release;

    // 测试 seam：注入到每个新 op 的 on_read_armed（Request 在 TryPost
    // 前复制给 op）；默认空 = 不通知。仅测试设置，生产路径保持空。
    std::function<void()>         m_read_armed_hook;
};

} // namespace bbt::infra::http_detail

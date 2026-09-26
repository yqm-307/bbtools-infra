#pragma once
// HttpClientImpl：真实 HTTP/1.1 客户端（Boost.Beast/Asio）。
//
// 桥接方式：Request 在协程内发起，异步链 resolve→connect→write→read
// 全部运行在 engine 的 io 域（共享 executor 上的 strand，由 Scheduler
// 现有事件循环线程推进）；终态 handler 把 result<HttpResponse>
// 写入堆上 operation state 后 Complete CompletionSignal；调用协程经
// CompletionSignal::Wait 挂起/恢复。I/O 回调不触碰业务协程栈，
// 不新造事件状态机，不依赖 Linux Hook。

#include <atomic>
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
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/write.hpp>
#include <boost/beast/http/read.hpp>

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
    std::shared_ptr<bbt::coroutine::CompletionSignal> sig;
    boost::asio::ip::tcp::resolver        resolver;
    boost::asio::ip::tcp::socket          socket;
    boost::beast::flat_buffer             buffer;
    boost::beast::http::request<boost::beast::http::string_body> request;
    boost::beast::http::response_parser<boost::beast::http::string_body> parser;
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

    ClientOp(const std::shared_ptr<HttpClientImpl>& owner_,
             const std::shared_ptr<HttpIoEngine>&   engine_);

    // 以下全部只在 io 域调用（Finish 除外，见上）
    void Begin(std::string host, std::uint16_t port);
    void Abort() noexcept;
    void Finish(result<HttpResponse> r) noexcept;

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

    // 测试 seam：OnWritten 发起 async_read 成功后、handler 返回前在
    // io 域内调用一次。Request 在 TryPost(Begin) 前注入（写先于对 io
    // 域的同步派发），故本字段只在 io 域被读、无并发写；默认空即不
    // 通知。测试用它在冻结 strand 前确认 read 已真实在途。
    std::function<void()> on_read_armed;

private:
    void OnResolve(boost::system::error_code ec,
                   boost::asio::ip::tcp::resolver::results_type results);
    void OnConnect(boost::system::error_code ec);
    void OnWritten(boost::system::error_code ec, std::size_t bytes);
    void OnRead(boost::system::error_code ec, std::size_t bytes);
};

class HttpClientImpl : public HttpClient,
                       public IIoTeardown,
                       public std::enable_shared_from_this<HttpClientImpl> {
public:
    HttpClientImpl(std::shared_ptr<HttpIoEngine> engine,
                   bbt::coroutine::CoObjectInfo  info,
                   std::shared_ptr<bbt::coroutine::CompletionSignal> close_sig)
        : m_engine(std::move(engine)),
          m_info(std::move(info)),
          m_close(std::move(close_sig)) {}

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

    void RequestClose() noexcept override;
    bool IsClosed() const noexcept override { return m_close.IsClosed(); }
    CloseStatus WaitClosed(bbt::coroutine::Deadline          deadline,
                           bbt::coroutine::CancellationToken cancel) override {
        return m_close.WaitClosed(deadline, std::move(cancel),
                                  m_info.generation);
    }
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override {
        return m_info;
    }

    // IIoTeardown：io 域一次性回收——中止在途 op（催完成项）；
    // MarkClosed 延迟到全部 op 的 in-flight 计数归零（物理清理落定）。
    void TeardownOnIoDomain() noexcept override;
    // TryPost 失败的兜底收口：不进 io 域（socket/resolver 不触碰），
    // 仅逻辑 Finish 已登记 op，MarkClosed 同样由计数门控。
    void TeardownOffDomain() noexcept override;
    // m_io_dead 且 m_ops 空 ⇒ MarkClosed；teardown 尾部与
    // UnregisterOp 两处都可能成为最后一个归零者。
    void DrainCheckClosed() noexcept;

    // 发布前注册（runtime 聚合物理清理完成）。
    void SetClosedHook(std::function<void()> hook) {
        m_close.SetClosedHook(std::move(hook));
    }

    const std::shared_ptr<HttpIoEngine>& Engine() const { return m_engine; }

    // 测试 seam：给后续新建的 op 注入 read-armed 通知（io 域内、
    // OnWritten 发起 async_read 成功后触发）。空 = 不通知。
    void SetReadArmedHookForTest(std::function<void()> hook) {
        m_read_armed_hook = std::move(hook);
    }

    // m_ops 跨线程（调用线程登记、io 域反登记/teardown），一律持锁访问。
    // 集合持 op 强引用：快照后可跨线程安全收口，不会迭代到悬垂指针。
    // 返回 false 表示 teardown 已快照（io 域已封或将封），调用方按 Closed 拒绝——
    // 保证「已登记必被终态收口」，等待者不会因 post 丢失而挂住。
    bool RegisterOp(std::shared_ptr<ClientOp> op) {
        std::lock_guard<std::mutex> lk(m_ops_mtx);
        if (m_io_dead)
            return false;
        m_ops.insert(std::move(op));
        return true;
    }
    // op 在 finished 且 in-flight 归零后才调用本方法离开 m_ops：
    // teardown 之后 m_ops 清空即「后端不再访问任何 op 资源」，
    // 此时才 MarkClosed。
    // Issue #37：这里同时是出站配额的「物理收口」归还点——op 一旦
    // 离开 m_ops，其占用的 max_connections/max_inflight 名额立即
    // 经 op->on_unregister 归还给 owner，晚于此才允许名额复用。
    // 原子一次性：erase 的返回值是本 op 是否「真正从集合移除」的判定。
    // Finish 可跨线程、IoAsyncDone 在 io 域，两条收口路径可并发进入
    // 本方法——只有抢到 erase 成功（返回 1）的一方才执行归还并参与
    // fin 判定；另一方看到已不在集合，直接返回，绝不二次归还。
    void UnregisterOp(std::shared_ptr<ClientOp> op) {
        bool erased = false;
        bool fin = false;
        {
            std::lock_guard<std::mutex> lk(m_ops_mtx);
            erased = (m_ops.erase(op) != 0);
            if (erased)
                fin = m_io_dead && m_ops.empty();
        }
        if (!erased)
            return;                  // 已被并发收口路径移除：不重复归还
        if (op->on_unregister)
            op->on_unregister();
        if (fin)
            m_close.MarkClosed();
    }
    bool IsOpenForIo() const { return m_close.IsOpen(); }

private:
    std::shared_ptr<HttpIoEngine> m_engine;
    bbt::coroutine::CoObjectInfo  m_info;
    ManagedCloseState             m_close;
    std::mutex                    m_ops_mtx;
    // 未落定 op 集合（持强引用防悬垂）：op 在 finished 且 in-flight
    // 归零后才离开——m_io_dead 置位后集合清空是 MarkClosed 的必要条件。
    std::unordered_set<std::shared_ptr<ClientOp>> m_ops;
    bool                          m_io_dead{false}; // m_ops_mtx 保护

    // Issue #37：owner 注入的配额钩子；空表示不计量。
    std::function<result<void>()> m_admit;
    std::function<void()>         m_release;

    // 测试 seam：注入到每个新 op 的 on_read_armed（Request 在 TryPost
    // 前复制给 op）；默认空 = 不通知。仅测试设置，生产路径保持空。
    std::function<void()>         m_read_armed_hook;
};

} // namespace bbt::infra::http_detail

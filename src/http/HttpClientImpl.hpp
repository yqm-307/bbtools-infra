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
    void UnregisterOp(std::shared_ptr<ClientOp> op) {
        bool fin = false;
        {
            std::lock_guard<std::mutex> lk(m_ops_mtx);
            m_ops.erase(op);
            fin = m_io_dead && m_ops.empty();
        }
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
};

} // namespace bbt::infra::http_detail

#include <bbt/infra/net/Client.hpp>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/system/error_code.hpp>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/detail/CoIoOperation.hpp>
#include <bbt/infra/net/detail/ConnectionBridge.hpp>
#include <bbt/infra/net/detail/EventLoop.hpp>

namespace bbt::infra::net {
namespace {

// 控制线程命令：有界投递 + 有界等待完成（与 ServerEngine.cc 同口径的本地副本；
// 该函数在 ServerEngine.cc 为文件静态，不可跨 TU 复用）。闭包自带 promise 保活，
// 晚到完成不写已离开的调用栈。已 Accepted 后超时只说明未观测完成，不判撤销。
template <typename Command>
std::error_code RunOwner(const std::shared_ptr<EventLoop>& loop, Command&& command,
    std::error_code* command_error, std::chrono::milliseconds timeout)
{
    std::function<std::error_code()> fn;
    std::shared_ptr<std::promise<std::error_code>> promise;
    std::shared_ptr<std::atomic<bool>> claimed;
    try {
        fn = std::function<std::error_code()>(std::forward<Command>(command));
        promise = std::make_shared<std::promise<std::error_code>>();
        claimed = std::make_shared<std::atomic<bool>>(false);
    } catch (const std::bad_alloc&) {
        return std::make_error_code(std::errc::not_enough_memory);
    } catch (...) {
        return std::make_error_code(std::errc::io_error);
    }
    auto future = promise->get_future();
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        SubmitStatus status = SubmitStatus::Invalid;
        std::error_code submit_error;
        try {
            status = loop->Submit([promise, claimed, fn]() {
                std::error_code result;
                try { result = fn(); }
                catch (...) { result = std::make_error_code(std::errc::io_error); }
                if (!claimed->exchange(true)) {
                    try { promise->set_value(result); } catch (...) { }
                }
            }, submit_error);
        } catch (const std::bad_alloc&) {
            return std::make_error_code(std::errc::not_enough_memory);
        } catch (...) {
            return std::make_error_code(std::errc::io_error);
        }
        if (status == SubmitStatus::Accepted) { break; }
        if (submit_error) { return submit_error; }
        if (status != SubmitStatus::Full) {
            switch (status) {
            case SubmitStatus::Full: break;
            case SubmitStatus::Sealed: return std::make_error_code(std::errc::operation_canceled);
            case SubmitStatus::NotStarted: return std::make_error_code(std::errc::operation_not_permitted);
            case SubmitStatus::Invalid: return std::make_error_code(std::errc::invalid_argument);
            case SubmitStatus::Failed: return std::make_error_code(std::errc::io_error);
            case SubmitStatus::NotSubmitted:
            case SubmitStatus::Accepted: return std::make_error_code(std::errc::io_error);
            }
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return std::make_error_code(std::errc::timed_out);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    // 与投递循环同一绝对 deadline：单次 RunOwner 的最坏完成时间有界（单 timeout，
    // 不叠加为 2×timeout），使调用方（Stop 的补偿序列）总预算可推算。
    const auto remaining = deadline - std::chrono::steady_clock::now();
    if (future.wait_for(remaining > std::chrono::milliseconds::zero()
            ? remaining : std::chrono::milliseconds::zero()) != std::future_status::ready) {
        return std::make_error_code(std::errc::timed_out);
    }
    if (command_error != nullptr) { *command_error = future.get(); } else { (void)future.get(); }
    return {};
}

// 未接线请求显式拒绝（不静默忽略配置）；除下面的单 owner 例外，条件集合与
// ServerEngine.cc 同口径。
std::error_code RequireSupportedOptions(const NormalizedOptions& options) {
    const bool unsupported = options.reuse_port
        || options.multicast_interface_index != 0
        || !options.bind_to_device.empty()
        || options.lock_os_thread
        || options.ticker
        || options.tcp_keep_alive.count() != 0
        || options.tcp_keep_interval.count() != 0
        || options.tcp_keep_count != 0
        || options.socket_recv_buffer != 0
        || options.socket_send_buffer != 0
        || options.edge_triggered_io
        || options.edge_triggered_io_chunk != 0
        || !options.log_path.empty()
        || static_cast<bool>(options.logger)
        || options.num_event_loop != 1;   // 单 owner：默认已归一化为 1；>1 不支持
    return unsupported ? std::make_error_code(std::errc::operation_not_supported) : std::error_code{};
}

std::error_code MapWaitStatus(bbt::coroutine::WaitStatus status) {
    using bbt::coroutine::WaitStatus;
    switch (status) {
    case WaitStatus::Completed:          return {};
    case WaitStatus::TimedOut:           return std::make_error_code(std::errc::timed_out);
    case WaitStatus::Cancelled:          return std::make_error_code(std::errc::operation_canceled);
    case WaitStatus::InvalidContext:     return std::make_error_code(std::errc::operation_not_permitted);
    case WaitStatus::AlreadyWaiting:     return std::make_error_code(std::errc::device_or_resource_busy);
    case WaitStatus::RuntimeUnavailable: return std::make_error_code(std::errc::operation_not_supported);
    }
    return std::make_error_code(std::errc::operation_not_permitted);
}

std::error_code ToStd(const boost::system::error_code& error) {
    return std::error_code(error.value(), std::generic_category());
}

// 每次 Dial 的 owner 域状态：fd + readiness watcher + 结果槽。
// 结果槽（shell_）与仲裁位（cancelled_/abandoned_/adopted_）由 m_ 保护，因为
// Dial 协程与 owner 回调会并发访问；其余字段仅 owner 域访问，无需加锁。
struct DialOperation {
    DialOperation(std::shared_ptr<ICoConnectionHandler> handler, Address peer, bool no_delay)
        : handler_(std::move(handler)), peer_(std::move(peer)), no_delay_(no_delay) {}

    std::shared_ptr<ICoConnectionHandler> handler_;
    const Address peer_;
    const bool no_delay_;

    // ---- owner 域专有 ----
    std::shared_ptr<NativeSocket> socket_;                            // 唯一 fd 持有者
    std::shared_ptr<boost::asio::posix::stream_descriptor> wait_;     // dup fd + async_wait
    std::shared_ptr<CoIoOperation> io_;

    // ---- 结果槽 + 仲裁（Dial 协程 / owner 回调并发）----
    std::mutex m_;
    bool cancelled_{false};   // owner 域设置：Stop 取消 / 封门后
    bool abandoned_{false};   // Dial 域设置：deadline 到期放弃
    bool adopted_{false};     // owner 回调已成功 Adopt 并写入 shell_
    std::shared_ptr<CoTcpConnection> shell_;
};

// 在途 Dial 登记簿：Start 前登记，只在「取消责任已实际交付 owner 域」或「完成已由
// owner 域收口」时撤销 —— 取消投递失败时保留登记，Stop 的取消清单仍是兜底；不允许在
// 取消未落地时先摘登记（那会让未决 watcher 拖住无界 Join）。owner 命令在「取清单」
// 同一锁内置 shutdown_，故任何在 shutdown 之后才执行 start 的 Dial 都能看到 shutdown
// 而不武装等待；任何在 shutdown 之前执行 start 的 Dial 都已在清单中，必然被取消。
class DialRegistry {
public:
    void Add(const std::shared_ptr<DialOperation>& op) {
        std::lock_guard<std::mutex> lock(m_);
        dials_[op.get()] = op;
    }
    void Remove(const std::shared_ptr<DialOperation>& op) {
        std::lock_guard<std::mutex> lock(m_);
        dials_.erase(op.get());
    }
    bool Shutdown() const {
        std::lock_guard<std::mutex> lock(m_);
        return shutdown_;
    }
    std::vector<std::shared_ptr<DialOperation>> BeginShutdownAndTake() {
        std::lock_guard<std::mutex> lock(m_);
        shutdown_ = true;
        std::vector<std::shared_ptr<DialOperation>> out;
        out.reserve(dials_.size());
        for (const auto& entry : dials_) { out.push_back(entry.second); }
        return out;
    }

private:
    mutable std::mutex m_;
    std::map<const void*, std::shared_ptr<DialOperation>> dials_;
    bool shutdown_{false};
};

// owner 域：标记取消并撤销在途等待；Complete 首胜，晚到回调只清自有状态。
void CancelDialOwner(const std::shared_ptr<DialOperation>& op) {
    {
        std::lock_guard<std::mutex> lock(op->m_);
        op->cancelled_ = true;
    }
    if (op->wait_) {
        boost::system::error_code ignored;
        op->wait_->cancel(ignored);
    }
    if (op->io_) {
        op->io_->Complete({0, std::make_error_code(std::errc::operation_canceled)});
    }
}

void CancelAllDialsOwner(DialRegistry& registry) {
    const auto dials = registry.BeginShutdownAndTake();
    for (const auto& op : dials) { CancelDialOwner(op); }
}

// owner 域：可写就绪回调。先释放 watcher，再看裁定位（cancelled_/abandoned_），
// 只有未被放弃且 SO_ERROR 为 0 才 Adopt；Adopt 成功后在同一短锁内先写结果槽再
// Complete，保证 Dial 读槽的可见性与「成功移交即有人接收」。
void OnConnectReadyOwner(const std::shared_ptr<DialOperation>& op,
    const std::shared_ptr<CoIoOperation>& io, const std::shared_ptr<ConnectionBridge>& bridge,
    const std::shared_ptr<DialRegistry>& registry, const boost::system::error_code& wait_error)
{
    op->wait_.reset(); // 关 dup watcher fd（不影响连接 socket 自身 fd）
    // owner 域收口点：watcher 已释放，本回调是最后一个 owner 域持有者，登记在此撤销。
    // 即使 Dial 侧的取消投递曾被拒（登记因此保留），晚到回调也在这一刻完成收口。
    registry->Remove(op);
    {
        std::lock_guard<std::mutex> lock(op->m_);
        if (op->cancelled_ || op->abandoned_) {
            io->Complete({0, std::make_error_code(std::errc::operation_canceled)});
            return;
        }
    }
    if (wait_error) {
        io->Complete({0, ToStd(wait_error)});
        return;
    }
    if (!op->socket_) {
        io->Complete({0, std::make_error_code(std::errc::io_error)});
        return;
    }
    // 可写就绪后读 SO_ERROR（读取即清零）：非零 = 连接失败。
    if (const auto pending = op->socket_->PendingError()) {
        io->Complete({0, pending});
        return;
    }
    std::shared_ptr<CoTcpConnection> shell;
    std::error_code adopt_error;
    bool abandon_now = false;
    {
        std::lock_guard<std::mutex> lock(op->m_);
        if (op->cancelled_ || op->abandoned_) {
            abandon_now = true;
        } else {
            shell = bridge->Adopt(std::move(*op->socket_), op->peer_, op->handler_, adopt_error);
            if (shell) {
                op->shell_ = shell;   // 先写结果槽
                op->adopted_ = true;
            }
        }
    }
    if (abandon_now) {
        io->Complete({0, std::make_error_code(std::errc::operation_canceled)});
        return;
    }
    if (shell) {
        io->Complete({0, {}});    // 再 Complete（Dial 侧 Completed 后读槽）
        return;
    }
    io->Complete({0, adopt_error ? adopt_error : std::make_error_code(std::errc::io_error)});
}

// owner 域：Open / 非阻塞 Connect / EINPROGRESS 后武装 wait_write。
void StartDialOwner(const std::shared_ptr<DialOperation>& op,
    const std::shared_ptr<CoIoOperation>& io, EventLoop& loop,
    const std::shared_ptr<ConnectionBridge>& bridge, const std::shared_ptr<DialRegistry>& registry)
{
    op->io_ = io;
    // 封门后到达的 start：不 Open、不武装等待（对应 registry shutdown 竞态窗口）。
    if (registry->Shutdown()) {
        io->Complete({0, std::make_error_code(std::errc::operation_canceled)});
        return;
    }
    {
        std::lock_guard<std::mutex> lock(op->m_);
        if (op->cancelled_ || op->abandoned_) {
            io->Complete({0, std::make_error_code(std::errc::operation_canceled)});
            return;
        }
    }
    std::error_code open_error;
    NativeSocket socket = NativeSocket::Open(op->peer_.Family(), SocketType::Stream, open_error);
    if (open_error || socket.Fd() < 0) {
        io->Complete({0, open_error ? open_error : std::make_error_code(std::errc::io_error)});
        return;
    }
    if (op->no_delay_) { (void)socket.SetNoDelay(true); }
    const auto connect_error = socket.Connect(op->peer_);
    if (!connect_error) {
        // 立即成功（同步完成）：直接走 Adopt 收口路径。
        auto holder = std::make_shared<NativeSocket>(std::move(socket));
        op->socket_ = holder;
        OnConnectReadyOwner(op, io, bridge, registry, boost::system::error_code{});
        return;
    }
    if (connect_error.value() != EINPROGRESS) {
        io->Complete({0, connect_error});
        return;
    }
    std::error_code dup_error;
    NativeSocket watch = socket.Dup(dup_error);
    if (dup_error || watch.Fd() < 0) {
        io->Complete({0, dup_error ? dup_error : std::make_error_code(std::errc::io_error)});
        return;
    }
    auto desc = std::make_shared<boost::asio::posix::stream_descriptor>(loop.Context());
    boost::system::error_code assign_error;
    desc->assign(watch.Fd(), assign_error);
    if (assign_error) {
        io->Complete({0, ToStd(assign_error)});
        return; // watch 未移交，RAII 关 dup fd
    }
    (void)watch.Release(); // fd 所有权移交 desc
    op->socket_ = std::make_shared<NativeSocket>(std::move(socket));
    op->wait_ = desc;
    desc->async_wait(boost::asio::posix::stream_descriptor::wait_write,
        [op, io, bridge, registry](const boost::system::error_code& error) {
            OnConnectReadyOwner(op, io, bridge, registry, error);
        });
}

constexpr std::size_t kLoopCapacity = 256;
constexpr std::size_t kLoopBatch = 16;
constexpr std::chrono::milliseconds kControlDeadline{2000};

} // namespace

// Client 私有实现。phase_ 与资源句柄由 control_ 短锁保护；control_ 绝不跨 owner
// 命令等待/协程等待持有（Stop 的收口序列在锁外推进，见 Client::Stop）。
struct Client::Impl {
    // Starting = 已在锁内预留的启动中相位：同一时刻只有一位启动者，且它也是
    // Stop 判定「不得被启动复活」的依据。
    enum class Phase { Idle, Starting, Running, Stopping, Stopped };

    explicit Impl(std::shared_ptr<ICoConnectionHandler> handler) : handler_(std::move(handler)) {}

    std::mutex                          control_;
    Phase                               phase_{Phase::Idle};
    std::atomic<bool>                   stop_seq_{false};
    std::shared_ptr<ICoConnectionHandler> handler_;
    std::shared_ptr<EventLoop>          loop_;
    std::shared_ptr<ConnectionBridge>   bridge_;
    std::shared_ptr<DialRegistry>       registry_;
    bool                                no_delay_{true};
};

Client::Client(std::shared_ptr<ICoConnectionHandler> handler)
    : impl_(std::make_shared<Impl>(std::move(handler))) {}

Client::~Client()
{
    if (!impl_) { return; }
    Impl::Phase phase{Impl::Phase::Idle};
    {
        std::lock_guard<std::mutex> lock(impl_->control_);
        phase = impl_->phase_;
    }
    if (phase == Impl::Phase::Running || phase == Impl::Phase::Stopping) {
        // 控制域：按 Stop 同序收口（含先取消在途 Dial 再 Join），只调用一次 Stop。
        // 承诺范围与 Stop 相同：单次瞬态投递/执行/关闭故障由 Stop 内的一次有界补偿
        // 推进后真实收口。持续拒绝或 owner 长阻塞属承诺外残余：Stop 返回
        // operation_in_progress 并保留 owner/登记簿，析构返回后最后一个 loop_ 引用
        // 落到 ~EventLoop 的 RequestStop+Join（可能无界）。本实现不承诺该路径有界，
        // 也不在此改 runtime、不加 terminate/daemon/reaper 兜底，只如实报告。
        // 析构无返回值，调用者必须保证本调用在控制线程、且对本对象的 Dial 已返回。
        (void)Stop();
    }
}

std::error_code Client::Start(const Options& options)
{
    if (bbt::coroutine::GetLocalCoroutineId() != 0) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    {
        std::lock_guard<std::mutex> lock(impl_->control_);
        if (impl_->loop_ && impl_->loop_->IsOwner()) {
            return std::make_error_code(std::errc::operation_not_permitted);
        }
        if (impl_->phase_ == Impl::Phase::Starting || impl_->phase_ == Impl::Phase::Running
            || impl_->phase_ == Impl::Phase::Stopping) {
            return std::make_error_code(std::errc::device_or_resource_busy);
        }
        if (impl_->phase_ == Impl::Phase::Stopped) {
            return std::make_error_code(std::errc::operation_canceled);
        }
        if (!impl_->handler_) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        // 原子预留：owner 创建与桥装配都在该预留内完成，避免两个控制线程各起一套。
        impl_->phase_ = Impl::Phase::Starting;
    }
    // phase_ 已在锁内预留为 Starting：此后本调用是唯一启动者。任何失败都必须把预留
    // 复位（仅在仍是自己的预留时），并保证首错优先返回。
    const auto rollback = [this](std::error_code error) {
        std::lock_guard<std::mutex> lock(impl_->control_);
        if (impl_->phase_ == Impl::Phase::Starting) { impl_->phase_ = Impl::Phase::Idle; }
        return error;
    };
    // 回收本次未发布的 owner：调用线程不是 owner，可 RequestStop + Join；全程不碰 impl_。
    const auto reclaim = [](const std::shared_ptr<EventLoop>& loop) {
        if (!loop) { return; }
        loop->RequestStop();
        (void)loop->Join();
    };

    NormalizedOptions normalized;
    int cpu = static_cast<int>(std::thread::hardware_concurrency());
    if (cpu <= 0) { cpu = 1; }
    if (const auto error = NormalizeOptions(options, cpu, normalized)) { return rollback(error); }
    if (const auto error = RequireSupportedOptions(normalized)) { return rollback(error); }

    // owner 资源在本次预留内自建：任何一步失败都回收已起部分，不留半启动状态。
    std::shared_ptr<DialRegistry> registry;
    try {
        registry = std::make_shared<DialRegistry>();
    } catch (const std::bad_alloc&) {
        return rollback(std::make_error_code(std::errc::not_enough_memory));
    } catch (...) {
        return rollback(std::make_error_code(std::errc::io_error));
    }
    std::shared_ptr<EventLoop> loop;
    try {
        loop = std::make_shared<EventLoop>(kLoopCapacity, kLoopBatch);
    } catch (const std::bad_alloc&) {
        return rollback(std::make_error_code(std::errc::not_enough_memory));
    } catch (...) {
        return rollback(std::make_error_code(std::errc::io_error));
    }
    bool started = false;
    try {
        started = loop->Start();
    } catch (const std::bad_alloc&) {
        return rollback(std::make_error_code(std::errc::not_enough_memory));
    } catch (...) {
        return rollback(std::make_error_code(std::errc::io_error));
    }
    if (!started) {
        return rollback(std::make_error_code(std::errc::io_error));
    }
    std::error_code create_error;
    auto bridge = ConnectionBridge::Create(*loop, normalized, create_error);
    if (!bridge) {
        reclaim(loop);
        return rollback(create_error ? create_error : std::make_error_code(std::errc::io_error));
    }
    {
        std::lock_guard<std::mutex> lock(impl_->control_);
        if (impl_->phase_ == Impl::Phase::Starting) {   // 仍是自己的预留才发布
            impl_->loop_ = loop;
            impl_->bridge_ = std::move(bridge);
            impl_->registry_ = std::move(registry);
            impl_->no_delay_ = normalized.tcp_no_delay;
            impl_->phase_ = Impl::Phase::Running;
            return {};
        }
    }
    // Stop 已抢先（Stopping/Stopped）：本次启动不得复活该终态，收回自己起的 owner
    // 并以明确错误失败。
    reclaim(loop);
    return std::make_error_code(std::errc::operation_canceled);
}

std::error_code Client::Stop()
{
    if (bbt::coroutine::GetLocalCoroutineId() != 0) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    std::shared_ptr<EventLoop> loop;
    std::shared_ptr<ConnectionBridge> bridge;
    std::shared_ptr<DialRegistry> registry;
    {
        std::lock_guard<std::mutex> lock(impl_->control_);
        // 域门禁与句柄快照同锁：loop_ 是共享指针，锁外读会与 Start/Stop 的写形成竞争。
        if (impl_->loop_ && impl_->loop_->IsOwner()) {
            return std::make_error_code(std::errc::operation_not_permitted);
        }
        if (impl_->phase_ == Impl::Phase::Stopped) { return {}; }      // 幂等成功
        if (impl_->phase_ == Impl::Phase::Idle) {                     // 未启动亦入终态
            impl_->phase_ = Impl::Phase::Stopped;
            return {};
        }
        impl_->phase_ = Impl::Phase::Stopping;                        // 拒绝新 Dial/新 start
        loop = impl_->loop_;
        bridge = impl_->bridge_;
        registry = impl_->registry_;
    }
    bool expected = false;
    if (!impl_->stop_seq_.compare_exchange_strong(expected, true)) {
        return std::make_error_code(std::errc::operation_in_progress);
    }
    std::error_code result;
    // 单次瞬态故障的一次有界补偿（与 ServerEngine::StopLocked/RollbackStart 同量级）：
    // 首错保留、补偿只为推进未完成项，补偿成功也不改判返回（可资源收口 ≠ 返回成功）。
    // 只补一次；不做无限重试、不建 reaper、不引分离线程、不动 runtime。
    const auto compensate = [&result](const std::shared_ptr<EventLoop>& target_loop,
                               const auto& command) -> bool {
        std::error_code command_error;
        const auto error = RunOwner(target_loop, command, &command_error, kControlDeadline);
        if (!error && !command_error) { return true; }
        if (!result) { result = error ? error : command_error; }
        std::error_code retry_error, retry_command_error;
        retry_error = RunOwner(target_loop, command, &retry_command_error, kControlDeadline);
        return !retry_error && !retry_command_error;
    };
    // 1) 短锁封门：拒绝尚未获得开始授权的业务（新 Adopt/新业务）。
    if (bridge) { bridge->SealService(); }
    // 2) owner 域取消全部在途 Dial 等待（必须在 RequestStop 之前；否则 Join 会被
    //    未决 async_wait 拖到 TCP SYN 超时的分钟级）。取消必须在 owner 域真正执行：
    //    RunOwner 只在命令执行完成时返回空，没有这份收据就不得宣称「已无在途 watcher」。
    //    一次性投递/执行故障由 compensate 有界推进一次。
    bool cancel_settled = !(loop && registry);
    if (loop && registry) {
        cancel_settled = compensate(loop, [registry]() -> std::error_code {
            CancelAllDialsOwner(*registry);
            return {};
        });
    }
    if (!cancel_settled) {
        // 取消仍未取得 owner 域收据（持续拒绝/owner 长阻塞）：不做无界 RequestStop/Join
        // （那正是未决 watcher 的分钟级挂起）。保留 owner 与登记簿、不发布 Stopped，由
        // 再次 Stop 推进；取消责任留在 owner 域（晚到回调收口）与登记簿（下次取清单
        // 覆盖）。该分支为承诺外残余：控制线程只能再 Stop 一次，析构只 Stop 一次。
        impl_->stop_seq_.store(false);
        if (!result) { result = std::make_error_code(std::errc::operation_in_progress); }
        return result;
    }
    // 3) owner 域物理全闭已登记会话（不等 handler）；同样至多一次有界补偿。
    if (loop && bridge) {
        (void)compensate(loop, [bridge]() { return bridge->StopServiceOwner(); });
    }
    // 4) Join 前置：先核真实网络收口（每会话物理已关闭、owned 等待记录已结算）。关闭
    //    步骤失败且补偿后仍未收口时，Join 会被残留 I/O（未撤销的读写等待/未闭 socket）
    //    拖成无界等待 —— 此时不做 Join，保留 owner 与登记簿、返回首错（无首错则
    //    operation_in_progress），由再次 Stop 推进；不新增万能关闭/回收管理器。
    if (loop && bridge && !bridge->NetworkCloseReady()) {
        impl_->stop_seq_.store(false);
        if (!result) { result = std::make_error_code(std::errc::operation_in_progress); }
        return result;
    }
    // 5) 收口 owner：封口并 drain（已无在途 watcher、已无未收口会话）。
    if (loop) {
        loop->RequestStop();
        if (!loop->Join() && !result) {
            result = std::make_error_code(std::errc::operation_not_permitted);
        }
    }
    // 6) owner context 仍活时脱离每会话 owner 域 backend。
    if (bridge) {
        if (const auto detach_error = bridge->DetachOwner()) {
            if (!result) { result = detach_error; }
        }
    }
    // 7) 网络收口判据：未收口不回收 owner/context、不发布 Stopped，可由再次 Stop 推进。
    const bool ready = !bridge || bridge->NetworkCloseReady();
    {
        std::lock_guard<std::mutex> lock(impl_->control_);
        if (ready) {
            impl_->loop_.reset();
            impl_->bridge_.reset();
            impl_->registry_.reset();
            impl_->phase_ = Impl::Phase::Stopped;
        }
    }
    impl_->stop_seq_.store(false);
    if (!ready && !result) {
        result = std::make_error_code(std::errc::operation_in_progress);
    }
    return result;
}

std::error_code Client::Dial(const Address& peer, std::shared_ptr<CoTcpConnection>& out,
    CoDeadline deadline)
{
    if (bbt::coroutine::GetLocalCoroutineId() == 0) {   // 域门禁：副作用之前
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    if (!peer.Valid()) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    std::shared_ptr<EventLoop> loop;
    std::shared_ptr<ConnectionBridge> bridge;
    std::shared_ptr<DialRegistry> registry;
    std::shared_ptr<ICoConnectionHandler> handler;
    bool no_delay = true;
    {
        std::lock_guard<std::mutex> lock(impl_->control_);
        if (impl_->phase_ == Impl::Phase::Idle) {
            return std::make_error_code(std::errc::operation_not_permitted);
        }
        if (impl_->phase_ != Impl::Phase::Running) {
            return std::make_error_code(std::errc::operation_canceled);
        }
        loop = impl_->loop_;
        bridge = impl_->bridge_;
        registry = impl_->registry_;
        handler = impl_->handler_;
        no_delay = impl_->no_delay_;
    }
    if (!loop || !bridge || !registry || !handler) {
        return std::make_error_code(std::errc::operation_canceled);
    }
    auto op = std::make_shared<DialOperation>(handler, peer, no_delay);
    registry->Add(op);   // 登记先于投递（Stop 的取消清单覆盖窗口见 DialRegistry 注释）

    const auto deliver = [loop](std::function<void()> command) {
        return loop->Submit(std::move(command));
    };
    const CoIoResult result = CoIoOperation::Run(deliver, {deadline},
        [op, loop, bridge, registry](std::shared_ptr<CoIoOperation> io) {
            StartDialOwner(op, io, *loop, bridge, registry);
        });

    std::shared_ptr<CoTcpConnection> shell;
    {
        std::lock_guard<std::mutex> lock(op->m_);
        if (op->adopted_) {
            shell = op->shell_;                       // 完成已先赢：返回成功
        } else if (result.wait_status == bbt::coroutine::WaitStatus::TimedOut) {
            op->abandoned_ = true;                    // 超时胜：不再接纳后续 Adopt
        }
    }
    // 撤销登记的条件是「取消责任已明确归属」：完成已由 owner 域收口（成功移交或
    // Completed）时可立即撤销；否则必须把取消真正投进 owner 域（Accepted 即命令入队，
    // RequestStop 的 drain 保证它被执行）后才撤销。投递被拒时保留登记 —— 取消责任仍在
    // owner 域的晚到 watcher 回调与 Stop 的取消清单上，先摘登记会让未决 watcher 拖住
    // 无界 Join（103 P2）。
    const bool settled = shell || result.wait_status == bbt::coroutine::WaitStatus::Completed;
    if (settled) {
        registry->Remove(op);
    } else if (loop->Submit([op, registry]() {
                   CancelDialOwner(op);
                   registry->Remove(op);   // owner 域收口后撤销登记
               }) == SubmitStatus::Accepted) {
        // 取消已交付 owner 域：登记转由 owner 域撤销。
    } else {
        // 投递被拒（Full/Sealed/NotStarted/Failed）：保留登记、不重试、不建 reaper、
        // 不引分离线程；由 owner 域晚到回调或 Stop 的取消清单收口。
    }

    if (shell) {
        out = std::move(shell);
        return {};
    }
    if (result.wait_status == bbt::coroutine::WaitStatus::TimedOut) {
        return std::make_error_code(std::errc::timed_out);
    }
    if (result.wait_status != bbt::coroutine::WaitStatus::Completed) {
        return MapWaitStatus(result.wait_status);
    }
    if (result.io.error) { return result.io.error; }
    if (result.exception) { return std::make_error_code(std::errc::io_error); }
    return std::make_error_code(std::errc::io_error); // 理论不可达
}

} // namespace bbt::infra::net

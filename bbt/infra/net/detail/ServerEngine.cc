#include <bbt/infra/net/detail/ServerEngine.hpp>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <functional>
#include <future>
#include <new>
#include <thread>
#include <utility>

#include <boost/system/error_code.hpp>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/infra/net/NativeSocket.hpp>

namespace bbt::infra::net {

namespace {

using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

// 私有控制常量（不新增配置项；Engine 级预算接线属后续包）。
constexpr std::size_t kOwnerCommandCapacity = 256;
constexpr std::size_t kOwnerCommandBatch = 16;
constexpr int kListenBacklog = 128;
constexpr auto kControlDeadline = 15s;   // 控制线程命令等待
constexpr auto kBootDeadline = 10s;      // OnBoot 完成等待（到期即失败回落，不等业务）
constexpr int kHandoffAttempts = 8;      // 移交投递的有界重试（仅 Full）

// OnTick 返回 delay 的饱和上限（溢出保护，不是业务时限）：asio 的
// basic_waitable_timer::expires_after 内部按 Clock::now()+delay 计算到期时刻，巨大 delay
// 会让该加法在 steady_clock::time_point 上溢出（UB）。上限取两者较小值：
//   1) steady_clock 可表示范围的一半（epoch 起算的 uptime 远小于半程 ⇒ now()+上限不溢出）；
//   2) nanoseconds 可表示范围的一半（duration_cast 的结果必须是合法的 int64 ns）。
// 换算用 long double 先比较再转换：若时钟周期比 1ns 粗（例如 100ns），半程折成 ns 会先
// 超出 int64，整数 duration_cast 会在 constexpr 阶段就溢出；先比较后转换与时钟周期无关。
// <= 上限的用户返回值原样使用，不截断、不归一到任何业务最大间隔（本包不引入业务时限）。
constexpr std::chrono::nanoseconds SaturateTickDelayBound() {
    constexpr auto clock_half = std::chrono::steady_clock::duration::max() / 2;
    constexpr auto nanosecond_half = std::chrono::nanoseconds::max() / 2;
    constexpr long double clock_half_ns = static_cast<long double>(clock_half.count())
        * (static_cast<long double>(std::chrono::steady_clock::period::num)
            / static_cast<long double>(std::chrono::steady_clock::period::den));
    if (clock_half_ns < static_cast<long double>(nanosecond_half.count())) {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(clock_half);
    }
    return nanosecond_half;
}
constexpr std::chrono::nanoseconds kMaxTickDelay = SaturateTickDelayBound();
static_assert(kMaxTickDelay > std::chrono::nanoseconds::zero(),
    "tick 饱和上限必须为正：否则巨大 delay 无法与业务截断区分");

// 移交包：fd 唯一所有权随闭包移动；投递失败/被拒时闭包析构物理关闭 fd。
struct FdHandoff {
    NativeSocket socket;
    Address peer;
};

bool IsWouldBlock(const std::error_code& error) {
    return error == std::errc::resource_unavailable_try_again
        || error == std::errc::operation_would_block
        || error.value() == EAGAIN || error.value() == EWOULDBLOCK;
}

// asio 的 boost::system::error_code → std::error_code：按值映射到
// std::system_category（Linux 同为 errno 值），与 detail/StreamConnection.cc 的
// MapError 同一约定；不把真实装配错误折叠成统一 io_error，也不丢真实 errno。
std::error_code MapAsioError(const boost::system::error_code& error) {
    if (!error) { return {}; }
    return std::error_code(error.value(), std::system_category());
}

std::error_code MapSubmitStatus(SubmitStatus status) {
    switch (status) {
    case SubmitStatus::Accepted: return {};
    case SubmitStatus::Full: return std::make_error_code(std::errc::resource_unavailable_try_again);
    case SubmitStatus::Sealed:
    case SubmitStatus::NotStarted: return std::make_error_code(std::errc::operation_canceled);
    case SubmitStatus::Invalid: return std::make_error_code(std::errc::invalid_argument);
    case SubmitStatus::Failed:
    case SubmitStatus::NotSubmitted: break;
    }
    return std::make_error_code(std::errc::io_error);
}

// 控制线程命令：有界投递 + 有界等待完成。闭包自带 promise 保活，晚到完成
// 不写已离开的调用栈，也不重复兑现。调用点闭包构造（F&& 转发，不经
// std::function 形参转换）、promise/future/claimed 分配与入队全部在本函数
// try 内：一次性 bad_alloc → not_enough_memory、其余 → io_error，不向
// Stop/Rollback/析构调用栈抛出。已 Accepted 后 timeout 只说明未观测完成，
// 不判撤销：晚命令触及的资源由调用方在同一收口函数内 RequestStop+Join 前
// 保持存活，不销毁 context。
template <typename Command>
std::error_code SubmitCommand(EventLoop& loop, Command&& command,
    std::chrono::milliseconds timeout, std::error_code* command_error)
{
    std::function<std::error_code()> fn;
    std::shared_ptr<std::promise<std::error_code>> promise;
    std::shared_ptr<std::atomic<bool>> claimed;
    std::future<std::error_code> future;
    try {
        fn = std::function<std::error_code()>(std::forward<Command>(command));
        promise = std::make_shared<std::promise<std::error_code>>();
        future = promise->get_future();
        claimed = std::make_shared<std::atomic<bool>>(false);
    } catch (const std::bad_alloc&) {
        return std::make_error_code(std::errc::not_enough_memory);
    } catch (...) {
        return std::make_error_code(std::errc::io_error);
    }
    const auto deadline = Clock::now() + timeout;
    for (;;) {
        SubmitStatus status;
        std::error_code submit_error;
        try {
            status = loop.Submit([promise, claimed, fn] {
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
        if (status != SubmitStatus::Full) { return MapSubmitStatus(status); }
        if (Clock::now() >= deadline) { return std::make_error_code(std::errc::timed_out); }
        std::this_thread::sleep_for(1ms);
    }
    if (future.wait_for(timeout) != std::future_status::ready) {
        return std::make_error_code(std::errc::timed_out);
    }
    if (command_error != nullptr) { *command_error = future.get(); } else { (void)future.get(); }
    return {};
}

// 未接线请求显式拒绝（不静默忽略）。
std::error_code RequireSupportedOptions(const NormalizedOptions& options) {
    const bool unsupported = options.reuse_port
        || options.multicast_interface_index != 0
        || !options.bind_to_device.empty()
        || options.lock_os_thread
        || options.tcp_keep_alive.count() != 0
        || options.tcp_keep_interval.count() != 0
        || options.tcp_keep_count != 0
        || options.socket_recv_buffer != 0
        || options.socket_send_buffer != 0
        || options.edge_triggered_io
        || options.edge_triggered_io_chunk != 0
        || !options.log_path.empty()
        || static_cast<bool>(options.logger);
    return unsupported ? std::make_error_code(std::errc::operation_not_supported) : std::error_code{};
}

} // namespace

bool InManagedCoroutine() { return bbt::coroutine::GetLocalCoroutineId() != 0; }

std::shared_ptr<ServerEngine> ServerEngine::Create(std::shared_ptr<EventHandler> handler,
    std::error_code& error)
{
    error.clear();
    return std::shared_ptr<ServerEngine>(new ServerEngine(std::move(handler)));
}

ServerEngine::ServerEngine(std::shared_ptr<EventHandler> handler) : handler_(std::move(handler)) {}

ServerEngine::~ServerEngine()
{
    // 析构域：合法控制线程（非受管协程、非本 Engine owner 线程，见 Engine.hpp）。
    // 仍在运行/启动中则按强停顺序收口；不 terminate、不 detach 线程、不重开 runtime 停机。
    std::lock_guard<std::mutex> guard(control_mutex_);
    if (phase_ == Phase::Starting || phase_ == Phase::Running) { (void)StopLocked(); }
}

std::error_code ServerEngine::Run(const Address& address, const Options& options)
{
    // 域门禁在任何副作用（线程/socket/进程资源）之前。
    if (InManagedCoroutine()) { return std::make_error_code(std::errc::operation_not_permitted); }
    // 空拥有关系在任何 owner/listener/受管任务副作用之前拒绝（与 Engine::Run 同语义）。
    if (!handler_) { return std::make_error_code(std::errc::invalid_argument); }

    std::shared_ptr<BootResult> boot;
    {
        std::lock_guard<std::mutex> guard(control_mutex_);
        // 并发 Run：只有一个可进入 Starting/Running，其余明确 busy；Stopped 是终态，
        // 首胜的 Stop 不会被后续 Run 覆盖回 Running。
        if (phase_ == Phase::Starting || phase_ == Phase::Running) {
            return std::make_error_code(std::errc::device_or_resource_busy);
        }
        if (phase_ == Phase::Stopped) { return std::make_error_code(std::errc::operation_canceled); }
        // 跨代栅栏：上一代 Boot 已开始未完成 ⇒ 拒绝重试（busy），同一 handler 不跨代
        // 重入 OnBoot；只保留窄结果句柄，不保活网络 context。旧任务自然完成后在此清柄
        // 放行重试；Stopped 终态路径已先行返回，不受影响（Stop 仍不等待 Handler）。
        if (pending_boot_) {
            std::lock_guard<std::mutex> boot_guard(pending_boot_->mutex);
            if (pending_boot_->started && !pending_boot_->done) {
                return std::make_error_code(std::errc::device_or_resource_busy);
            }
            pending_boot_.reset();
        }
        if (!address.Valid()) { return std::make_error_code(std::errc::invalid_argument); }
        const int cpu_count = static_cast<int>(std::max(1u, std::thread::hardware_concurrency()));
        NormalizedOptions normalized;
        if (const auto error = NormalizeOptions(options, cpu_count, normalized)) { return error; }
        if (const auto error = RequireSupportedOptions(normalized)) { return error; }
        // 家族门（公共接受集收窄，假成功修正）：当前 backend 的接纳路径只支持 IPv4 stream
        // （StreamConnection::Create 只接受 IPv4 对端并固定 assign ip::tcp::v4()），其余合法家族
        // （Unix/IPv6）必须在 owner 线程/listener socket/OnBoot 登记等**任何副作用之前**明确
        // operation_not_supported；否则 Run 会 bind 成功、发布 bound 快照并返回成功，却永不产生
        // 业务事件（146 探针 2.1 的假成功：error=0、count=0、无 OnOpen/OnTraffic/OnClose）。
        // 位置：仍在既有域门禁（锁外）、相位/终态、空 handler、地址合法性、选项错误之后 ——
        // 不重排任何既有错误优先级，只新增一条拒绝；本处尚未写入任何成员、不起线程、不建 socket，
        // 拒绝不置相位（保持 Idle）⇒ 换地址后可再次 Run。
        if (address.Family() != AddressFamily::IPv4) {
            return std::make_error_code(std::errc::operation_not_supported);
        }
        options_ = normalized;
        tcp_no_delay_ = normalized.tcp_no_delay;
        if (const auto error = StartOwners(normalized)) { ResetOwners(); return error; }
        if (const auto error = BindListener(address, normalized)) { ResetOwners(); return error; }
        if (const auto error = RegisterBoot(boot)) { ResetOwners(); return error; }
        // 发布：bound 快照已就绪 + Starting ⇒ 观测面（LocalAddresses/CountConnections）
        // 与并发 Stop 都能看到稳定 server 与真实绑定地址。登记只做 runtime 排队，
        // 不调用用户代码，因此可以在锁内完成。
        boot_ = boot;
        phase_ = Phase::Starting;
    }
    // 锁外等待 Boot：Stop 可用同一结果对象首胜唤醒并真实收口 Starting 网络，不等业务；
    // 这里没有任何 mutex 跨 OnBoot 等待。
    const bool ready = WaitBoot(boot);

    std::lock_guard<std::mutex> guard(control_mutex_);
    if (phase_ != Phase::Starting) {
        // Stop 首胜：owner/context/listener 已由 Stop 真实收口，这里不再访问任何网络资源、
        // 不再 armAccept；未开始的 Boot 任务不会再进入 OnBoot，已开始的只按值持 handler 自然完成。
        return std::make_error_code(std::errc::operation_canceled);
    }
    boot_.reset();
    if (!ready) {
        // 10s 到期：放弃位已置（未开始的任务不再进入 OnBoot），按原语义回落 Idle 可重试。
        // 若任务已开始未完成：保留窄结果句柄作跨代栅栏（下一次 Run busy，直到旧任务
        // 自然完成后放行；Stop 终态不等待 Handler）。
        {
            std::lock_guard<std::mutex> boot_guard(boot->mutex);
            if (boot->started && !boot->done) { pending_boot_ = boot; }
        }
        (void)RollbackStart();   // 清理首错不覆盖启动原错；未收口则相位保持 Starting
        return std::make_error_code(std::errc::timed_out);
    }
    bool boot_ok = false;
    {
        std::lock_guard<std::mutex> boot_guard(boot->mutex);
        boot_ok = boot->done && boot->ok;
    }
    if (!boot_ok) {
        (void)RollbackStart();
        return std::make_error_code(std::errc::io_error);
    }
    if (const auto error = ArmAccept()) {
        // dup/assign/登记的真实错误原样透传，不伪装成功；回落含撤销已登记的 accept 等待。
        (void)RollbackStart();
        return error;
    }
    phase_ = Phase::Running;
    // ticker 首次投递：进入 Running 且 listener 已挂 accept 就绪等待之后立即投递第一次
    // tick（不等初始 timer、不依赖 OnTick 返回值）。arm 失败与 ArmAccept 失败同语义回落，
    // 不留下「请求了 ticker 却没有 ticker」的假成功。
    if (options_.ticker) {
        if (const auto error = ArmTick(std::chrono::nanoseconds::zero())) {
            (void)RollbackStart();
            return error;
        }
    }
    return {};
}

std::error_code ServerEngine::Stop()
{
    if (InManagedCoroutine()) { return std::make_error_code(std::errc::operation_not_permitted); }
    std::lock_guard<std::mutex> guard(control_mutex_);
    if (IsOwnerThreadLocked()) { return std::make_error_code(std::errc::operation_not_permitted); }
    if (phase_ == Phase::Stopped) { return {}; }   // 已停止：幂等成功
    if (phase_ == Phase::Idle) {
        phase_ = Phase::Stopped;   // 未 Run 同样进入终态：随后 Run 明确 operation_canceled
        return {};
    }
    // Starting/Running：真实收口；Starting 不等 OnBoot 完成（Boot 任务由结果对象放弃/唤醒）。
    return StopLocked();
}

std::error_code ServerEngine::ForceStopForDestruction()
{
    std::lock_guard<std::mutex> guard(control_mutex_);
    if (phase_ == Phase::Starting || phase_ == Phase::Running) { return StopLocked(); }
    ResetOwners();
    return {};
}

std::error_code ServerEngine::LocalAddresses(std::vector<Address>& out) const
{
    std::lock_guard<std::mutex> guard(control_mutex_);
    // Starting（bound 快照已发布）与 Running 可读；未运行/已停止拒绝且不改写 out。
    if ((phase_ != Phase::Starting && phase_ != Phase::Running) || bound_.empty()) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    out = bound_;
    return {};
}

std::size_t ServerEngine::CountConnections() const
{
    std::lock_guard<std::mutex> guard(control_mutex_);
    // 服务已停（subs_ 已清）→ 0；Starting 尚无连接 → 0；否则为本代真实活跃连接数。
    return (subs_.empty() || !live_connections_) ? 0 : live_connections_->load();
}

std::error_code ServerEngine::StartOwners(const NormalizedOptions& options)
{
    // 每次 Run 新代计数：与适配器共享，上一代迟到 OnClose 不会污染新一代。
    live_connections_ = std::make_shared<std::atomic<std::size_t>>(0);
    // 每次 Run 新代 ticker 门（与 live_connections_ 同代语义）：上一代在途 tick 的迟到
    // 重挂被其自身代的门拒绝，不污染新一代。投递句柄在 main owner 起来之后发布（见下）。
    auto tick = std::make_shared<TickState>();
    tick->handler = handler_;
    tick->engine = shared_from_this();
    tick_state_ = tick;
    adapter_ = std::make_shared<EventHandlerAdapter>(handler_, live_connections_);
    main_loop_ = std::make_unique<EventLoop>(kOwnerCommandCapacity, kOwnerCommandBatch);
    if (!main_loop_->Start()) {
        return std::make_error_code(std::errc::io_error);
    }
    {
        // 稳定投递句柄发布：重挂路径只在同一窄门内读 loop/owner（不取 control_mutex_），
        // 撤回由 CloseTickGate 在同一短门内完成（closed 与句柄同锁置空）。
        std::lock_guard<std::mutex> guard(tick->mutex);
        tick->loop = main_loop_.get();
        tick->owner = this;
    }
    const std::size_t loop_count = options.num_event_loop;
    std::error_code balancer_error;
    balancer_ = std::make_unique<LoadBalancer>(options.lb, loop_count, balancer_error);
    if (balancer_error) { return balancer_error; }
    subs_.reserve(loop_count);
    for (std::size_t index = 0; index < loop_count; ++index) {
        SubOwner owner;
        owner.loop = std::make_unique<EventLoop>(kOwnerCommandCapacity, kOwnerCommandBatch);
        if (!owner.loop->Start()) {
            return std::make_error_code(std::errc::io_error);   // 已起 owner 由调用方 ResetOwners 回收
        }
        std::error_code create_error;
        owner.bridge = ConnectionBridge::Create(*owner.loop, options, create_error);
        if (!owner.bridge) {
            return create_error ? create_error : std::make_error_code(std::errc::io_error);
        }
        subs_.push_back(std::move(owner));
    }
    return {};
}

std::error_code ServerEngine::BindListener(const Address& address, const NormalizedOptions& options)
{
    std::error_code error;
    auto socket = NativeSocket::Open(address.Family(), SocketType::Stream, error);
    if (error || socket.Fd() < 0) {
        return error ? error : std::make_error_code(std::errc::io_error);
    }
    listener_ = std::make_unique<NativeSocket>(std::move(socket));
    if (options.reuse_addr) {
        if (const auto set_error = listener_->SetReuseAddr(true)) { return set_error; }
    }
    if (const auto bind_error = listener_->Bind(address)) { return bind_error; }
    if (const auto listen_error = listener_->Listen(kListenBacklog)) { return listen_error; }
    const auto bound = listener_->LocalAddress(error);
    if (error || !bound.Valid()) {
        return error ? error : std::make_error_code(std::errc::io_error);
    }
    bound_.assign(1, bound);   // 实际绑定地址稳定快照（port 0 → 真实端口）
    return {};
}

std::error_code ServerEngine::RegisterBoot(std::shared_ptr<BootResult>& out)
{
    // Boot 独立持有：闭包只按值捕获 handler 与窄结果句柄，不捕 self/this/ServerEngine/
    // adapter 或任何网络资源。因此挂起中的 OnBoot 不延长 Engine 寿命、不需要被取消；
    // 10s 到期只让 Run 回落，任务自身持 handler ⇒ 超时与 Engine 销毁后仍安全。
    auto boot = std::make_shared<BootResult>();
    auto handler = handler_;
    const std::size_t stack = g_bbt_coroutine_config->m_cfg_stack_size;
    try {
        g_scheduler->RegistCoroutineTask(stack, [handler, boot] {
            {
                std::lock_guard<std::mutex> guard(boot->mutex);
                // 窄 CAS：Pending→Running 与 Pending→Abandoned 只有一个先成立，
                // 解决「任务将跑而未跑」与 Stop/超时之间的 check-TOCTOU。
                if (boot->abandoned) { return; }   // 已放弃 ⇒ 不得进入 OnBoot
                boot->started = true;
            }
            bool booted = false;
            try { booted = handler->OnBoot(); }
            catch (...) { booted = false; }
            {
                std::lock_guard<std::mutex> guard(boot->mutex);
                boot->ok = booted;
                boot->done = true;
            }
            boot->cv.notify_all();
        }, "net.engine.boot");
    } catch (...) {
        // runtime 不可用/登记失败：显式失败，不留半启动、不伪造成功。
        return std::make_error_code(std::errc::operation_not_supported);
    }
    out = std::move(boot);
    return {};
}

bool ServerEngine::WaitBoot(const std::shared_ptr<BootResult>& boot)
{
    std::unique_lock<std::mutex> lock(boot->mutex);
    const bool ready = boot->cv.wait_for(lock, kBootDeadline,
        [&boot] { return boot->done || boot->abandoned; });
    if (ready) { return true; }
    // 到期：放弃尚未开始的任务（不得再进入 OnBoot）；已经开始的任务仍按原 10s 语义自然
    // 完成，不无限等待、不强杀、不等业务。
    boot->abandoned = true;
    return false;
}

void ServerEngine::RequestAbandon(const std::shared_ptr<BootResult>& boot)
{
    if (!boot) { return; }
    {
        std::lock_guard<std::mutex> guard(boot->mutex);
        boot->abandoned = true;   // 未开始 ⇒ 不进 OnBoot；已开始 ⇒ 只唤醒等 Boot 的 Run
    }
    boot->cv.notify_all();
}

std::error_code ServerEngine::ArmAccept()
{
    auto self = shared_from_this();
    std::error_code command_error;
    const auto error = SubmitCommand(*main_loop_, [self] {
        return self->ArmAcceptOwner();
    }, kControlDeadline, &command_error);
    if (error) { return error; }
    return command_error;   // owner 内的真实装配错误（dup/assign/登记）不再被吞掉
}

std::error_code ServerEngine::ArmAcceptOwner()
{
    if (stopping_.load()) { return std::make_error_code(std::errc::operation_canceled); }
    if (!watcher_) {
        std::error_code dup_error;
        auto watch = listener_->Dup(dup_error);
        if (dup_error || watch.Fd() < 0) {
            // dup 真实 errno 透传（EMFILE/ENFILE/ENOMEM 等），不折叠、不假成功。
            return dup_error ? dup_error : std::make_error_code(std::errc::io_error);
        }
        boost::system::error_code assign_error;
        watcher_ = std::make_unique<boost::asio::posix::stream_descriptor>(main_loop_->Context());
        watcher_->assign(watch.Fd(), assign_error);
        if (assign_error) {
            watcher_.reset();
            (void)watch.Close();
            return MapAsioError(assign_error);   // 保持 assign 的真实错误
        }
        (void)watch.Release();   // 所有权交给 descriptor（dup fd，不是 listener fd）
    }
    auto self = shared_from_this();
    try {
        watcher_->async_wait(boost::asio::posix::stream_descriptor::wait_read,
            [self](const boost::system::error_code& error) { self->OnAcceptReady(error); });
    } catch (const std::bad_alloc&) {
        return std::make_error_code(std::errc::not_enough_memory);   // 登记分配失败
    } catch (...) {
        return std::make_error_code(std::errc::io_error);            // 登记失败可见
    }
    return {};
}

void ServerEngine::OnAcceptReady(const boost::system::error_code& error)
{
    if (stopping_.load()) { return; }
    if (error) { accept_errors_.fetch_add(1); return; }
    AcceptPendingOwner();
    if (stopping_.load() || !watcher_) { return; }
    auto self = shared_from_this();
    try {
        watcher_->async_wait(boost::asio::posix::stream_descriptor::wait_read,
            [self](const boost::system::error_code& next) { self->OnAcceptReady(next); });
    } catch (...) {
        accept_errors_.fetch_add(1);
    }
}

void ServerEngine::AcceptPendingOwner()
{
    for (;;) {
        if (stopping_.load()) { return; }
        std::error_code error;
        Address peer;
        auto socket = listener_->Accept(peer, error);
        if (error) {
            if (!IsWouldBlock(error)) { accept_errors_.fetch_add(1); }
            return;
        }
        if (socket.Fd() < 0) { return; }
        HandoffOwner(std::move(socket), peer);
    }
}

void ServerEngine::HandoffOwner(NativeSocket&& socket, const Address& peer)
{
    if (subs_.empty() || !balancer_) {
        handoff_rejected_.fetch_add(1);   // fd 随 socket 析构物理关闭
        return;
    }
    std::vector<std::uint64_t> counts;
    if (balancer_->Mode() == LoadBalancing::LeastConnections) {
        counts.reserve(subs_.size());
        for (const auto& sub : subs_) {
            counts.push_back(sub.bridge ? static_cast<std::uint64_t>(sub.bridge->LiveSessionCount()) : 0);
        }
    }
    std::error_code lb_error;
    const auto index = balancer_->Next(peer, counts, lb_error);
    if (lb_error || index >= subs_.size()) {
        handoff_rejected_.fetch_add(1);
        return;
    }
    auto holder = std::make_shared<FdHandoff>();
    holder->socket = std::move(socket);
    holder->peer = peer;
    auto& target = subs_[index];
    const auto bridge = target.bridge;
    const auto adapter = adapter_;
    const auto counter = live_connections_;   // 按值捕获本代计数，不随成员换代错位
    const bool no_delay = tcp_no_delay_;
    auto self = shared_from_this();
    SubmitStatus status = SubmitStatus::NotStarted;
    for (int attempt = 0; attempt < kHandoffAttempts; ++attempt) {
        status = target.loop->Submit([self, holder, bridge, adapter, counter, no_delay] {
            std::error_code error;
            if (no_delay) { (void)holder->socket.SetNoDelay(true); }
            // 唯一 fd 所有权移交给 bridge；校验失败时 fd 仍留在 holder（RAII 物理关闭）。
            auto connection = bridge->Adopt(std::move(holder->socket), holder->peer, adapter, error);
            if (connection && counter) { counter->fetch_add(1); }   // registry 保活 ≠ 活跃
        });
        if (status == SubmitStatus::Accepted) { return; }
        if (status != SubmitStatus::Full) { break; }
    }
    handoff_rejected_.fetch_add(1);   // 未接纳：不发假接纳、不泄漏 fd，计数可见
}

std::error_code ServerEngine::CloseOwnerNetwork()
{
    std::error_code result;
    // ticker：取消并析构 timer，必须先于 RequestStop/Join，否则 Join 会一直等这次已登记的
    // 等待；timer 析构早于 main_loop_（context）析构。取消失败不吞掉：映射后作为本次
    // 收口首错返回（Stop() 如实返回它），但无论取值如何都不跳过 reset —— timer 析构本身
    // 会中止在途等待（asio as-if-cancel），这是 owner 仍能 Join、不因 timer 挂住的必要条件。
    if (tick_timer_) {
        // Boost 1.90 的 basic_waitable_timer 只有 throwing cancel()（无 error_code/promise
        // 重载）。异常分类：asio 失败以 boost::system::system_error 抛出 ⇒ 映射为 error_code
        // 并作为收口首错上报（不吞掉、不伪报已收口）；其余异常同样记为 io_error，不静默。
        // 取消失败的补偿责任落在调用方既有的「有界推进一次」路径：第二次命令里 timer 已析构，
        // 因此不会重复失败。
        try {
            (void)tick_timer_->cancel();
        } catch (const boost::system::system_error& error) {
            result = MapAsioError(error.code());
        } catch (...) {
            if (!result) { result = std::make_error_code(std::errc::io_error); }
        }
        // 无论 cancel 取值如何都不跳过析构：asio 契约规定析构「as if by calling cancel」
        // 中止在途等待（detail/deadline_timer_service.hpp destroy→cancel），这是 owner
        // RequestStop/Join 不因 timer 挂住的必要条件；timer 亦因此早于其 context 析构。
        tick_timer_.reset();
    }
    if (watcher_) {
        boost::system::error_code cancel_error;   // 撤销就绪等待（dup fd 关闭，不影响 listener fd）
        watcher_->cancel(cancel_error);           // error_code 重载：失败不抛、记录首错
        if (cancel_error) { result = MapAsioError(cancel_error); }
        watcher_.reset();
    }
    if (listener_) {
        if (const auto error = listener_->Close(); error && !result) { result = error; }
    }
    return result;
}

std::error_code ServerEngine::RollbackStart()
{
    // 启动回落（仅本处必要收口，不改 StopLocked 通用停机顺序）：先在 main owner 上撤销
    // 可能已登记的 accept 就绪等待并关 listener，再回收 owner/context/FD。否则未 cancel 的
    // Asio 等待会让 RequestStop 之后的 Join 一直等下去。调用方持 control_mutex_，不会与
    // Stop/并发 Run 的资源操作交错。闭包只捕 this：本函数同步驱动 main owner 到 Join，
    // 析构域（最后强引用）不经过 shared_from_this。
    std::error_code cleanup_error;
    // ticker 开始门先收口：回落路径同样不留可重挂的 ticker（timer 由 CloseOwnerNetwork
    // 在 Join 前取消/析构）。
    CloseTickGate();
    if (main_loop_) {
        std::error_code command_error;
        auto error = SubmitCommand(*main_loop_, [this] { return CloseOwnerNetwork(); },
            kControlDeadline, &command_error);
        if (error || command_error) {
            // 首错保留；owner 仍存活且接纳控制清理 ⇒ 有界推进一次（不无限重试）。
            cleanup_error = error ? error : command_error;
            std::error_code retry_error, retry_command_error;
            retry_error = SubmitCommand(*main_loop_, [this] { return CloseOwnerNetwork(); },
                kControlDeadline, &retry_command_error);
            if (retry_error || retry_command_error) {
                // 二次仍失败：网络未收口（可能仍有未撤销的 accept 等待），不回收
                // owner/context（避免无界 Join）、不发布 Idle（未收口不假 Idle）；
                // 相位保持 Starting，重试 Run 得 busy，显式 Stop 可继续推进。
                return cleanup_error;
            }
        }
    }
    boot_.reset();
    ResetOwners();
    phase_ = Phase::Idle;   // 已收口才回落 Idle：同一实例可重试
    return {};
}

std::error_code ServerEngine::StopLocked()
{
    // 首胜：唤醒可能正在等 Boot 的 Run（Stop 不被 Run 后续 Running 覆盖），并让尚未开始
    // 的 Boot 任务不再进入 OnBoot；已经开始的 Boot 只按值持 handler 自然完成，不取消、
    // 不强杀——收口网络与等待业务完成是两件事。
    // ticker 开始门先收口：置 closed 后不再挂 timer、不再投递新 tick；在途 tick 自然完成
    // （不取消协程、不等 handler），timer 由 CloseOwnerNetwork 在 Join 前取消/析构。
    CloseTickGate();
    RequestAbandon(boot_);
    // 控制线程只封开始门，不访问 registry/socket，不等 handler。所有现有桥
    // 均已封门后才发布 Engine 停止线性化点；期间允许提前拒绝，不允许晚授权。
    for (auto& sub : subs_) {
        if (sub.bridge) { sub.bridge->SealService(); }
    }
    stopping_.store(true);
    std::error_code result;
    // 1) 清 listener 确认（命令真实返回即关闭完成）后 join main owner：join 之后
    //    不可能再提交新的 handoff。Starting（未 ArmAccept）同样成立。命令构造/
    //    投递/执行一次性失败：owner 仍存活且接纳控制清理 ⇒ 有界推进一次；首错
    //    保留，恢复成功也返回首错。闭包只捕 this：本函数同步驱动每条 owner 到
    //    Join，析构域（最后强引用）不经过 shared_from_this。
    if (main_loop_) {
        std::error_code command_error;
        auto error = SubmitCommand(*main_loop_, [this] { return CloseOwnerNetwork(); },
            kControlDeadline, &command_error);
        if (error || command_error) {
            if (!result) { result = error ? error : command_error; }
            std::error_code retry_error, retry_command_error;
            retry_error = SubmitCommand(*main_loop_, [this] { return CloseOwnerNetwork(); },
                kControlDeadline, &retry_command_error);
            (void)retry_error;           // 首错已保留；重试只负责推进未完成项
            (void)retry_command_error;
        }
        main_loop_->RequestStop();
        if (!main_loop_->Join() && !result) {
            result = std::make_error_code(std::errc::operation_not_permitted);
        }
    }
    watcher_.reset();
    listener_.reset();
    // 2) 每 sub：单条 StopServiceOwner 命令本身即 FIFO 收口边界（main Join 后不再
    //    产生新 handoff，先于本命令入队的在途 handoff 必然先执行）→ 物理全闭 →
    //    RequestStop/Join → context 仍活时 DetachOwner。一次性失败在同一 owner 上
    //    有界推进一次，不做无限重试。
    for (auto& sub : subs_) {
        if (!sub.loop) { continue; }
        const auto bridge = sub.bridge;
        std::error_code command_error;
        if (bridge) {
            const auto error = SubmitCommand(*sub.loop,
                [bridge] { return bridge->StopServiceOwner(); }, kControlDeadline, &command_error);
            if (error || command_error) {
                if (!result) { result = error ? error : command_error; }
                std::error_code retry_error, retry_command_error;
                retry_error = SubmitCommand(*sub.loop,
                    [bridge] { return bridge->StopServiceOwner(); },
                    kControlDeadline, &retry_command_error);
                (void)retry_error;           // 首错已保留；重试只负责推进未完成项
                (void)retry_command_error;
            }
        }
        sub.loop->RequestStop();
        if (!sub.loop->Join() && !result) {
            result = std::make_error_code(std::errc::operation_not_permitted);
        }
        if (bridge) {
            // Join 后 Detach 重复检查物理关闭必要条件；拒绝时不发布半脱离。
            if (const auto detach_error = bridge->DetachOwner()) {
                if (!result) { result = detach_error; }
            }
        }
    }
    // 3) 网络收口判据（内部只读 readiness）：全部会话物理已关闭且 owned 等待记录
    //    结算；不是 Drained，不等 Handler/OnClose/快照/业务。未收口 ⇒ 不回收
    //    owner/context（残留 shell/backend 仍引用它们）、不发布 Stopped，返回
    //    首错供调用方再次 Stop 推进。最后引用析构下仍失败属无合法闭合残余，
    //    不选牺牲策略（不 reaper/泄漏/terminate/无限重试），按 BLOCKED 残余报告。
    const bool ready = NetworkCloseReady();
    if (!ready && !result) {
        result = std::make_error_code(std::errc::operation_in_progress);
    }
    if (ready) {
        ResetOwners();
        boot_.reset();
        phase_ = Phase::Stopped;
    }
    return result;
}

void ServerEngine::CloseTickGate()
{
    // 控制线程（持 control_mutex_）唯一入口：在同一窄门内完成「停止授权 + 撤空投递句柄」。
    // 撤回后 tick 侧不再 Submit（无需依赖 control_mutex_），也不触碰 timer（owner 域收口）。
    const auto state = tick_state_;
    if (!state) { return; }
    std::lock_guard<std::mutex> guard(state->mutex);
    StopTickerLocked(state, TickState::StopReason::Closed);
}

void ServerEngine::StopTickerLocked(const std::shared_ptr<TickState>& state,
    TickState::StopReason reason)
{
    // 调用方持 state->mutex：closed/首个原因/撤空句柄一次完成，不存在「门已关但仍可投递」
    // 或「句柄已回收但门仍开」的中间态。
    if (!state->closed) {
        state->closed = true;
        state->reason = reason;
    }
    state->loop = nullptr;
    state->owner = nullptr;
    state->handler.reset();
}

std::chrono::nanoseconds ServerEngine::SaturateTickDelay(std::chrono::nanoseconds delay)
{
    return delay > kMaxTickDelay ? kMaxTickDelay : delay;
}

std::error_code ServerEngine::ArmTickOwner(const std::shared_ptr<TickState>& state,
    const std::shared_ptr<EventHandler>& handler, std::chrono::nanoseconds delay)
{
    // owner 域（调用方持 state->mutex）：timer 只在此创建/操作（Context() 只许 owner 线程）。
    // 命令本身不等待 timer 到期，登记完成即返回，因此控制线程的阻塞命令不会被长等待拖住。
    if (!main_loop_) {
        StopTickerLocked(state, TickState::StopReason::ArmFailed);
        return std::make_error_code(std::errc::operation_canceled);
    }
    try {
        if (!tick_timer_) {
            tick_timer_ = std::make_unique<boost::asio::steady_timer>(main_loop_->Context());
        }
        tick_timer_->expires_after(SaturateTickDelay(delay));
        // 完成项只按值持窄门 + handler：不持 ServerEngine/EventLoop/timer，取消后即释放。
        tick_timer_->async_wait([state, handler](const boost::system::error_code& error) {
            if (error) { return; }   // 取消/过期：不再投递
            {
                std::lock_guard<std::mutex> guard(state->mutex);
                if (state->closed) { return; }
            }
            // 只做短事：登记受管协程 tick 任务（只排队，不跑业务、不阻塞 owner）。
            try {
                const std::size_t stack = g_bbt_coroutine_config->m_cfg_stack_size;
                g_scheduler->RegistCoroutineTask(stack, [state, handler] {
                    TickTask(state, handler);
                }, "net.engine.tick");
            } catch (...) {
                // runtime 不可用/登记失败：停止 ticker，保留 Engine 服务，不向 owner 抛出。
                std::lock_guard<std::mutex> guard(state->mutex);
                StopTickerLocked(state, TickState::StopReason::RegistrationFailed);
            }
        });
    } catch (const std::bad_alloc&) {
        StopTickerLocked(state, TickState::StopReason::ArmFailed);
        return std::make_error_code(std::errc::not_enough_memory);
    } catch (...) {
        StopTickerLocked(state, TickState::StopReason::ArmFailed);
        return std::make_error_code(std::errc::io_error);
    }
    return {};
}

std::error_code ServerEngine::ArmTick(std::chrono::nanoseconds delay)
{
    // 控制线程路径（Run 首次投递）：与 ArmAccept 同形状（阻塞等到 owner 命令完成）。
    // 命令内在窄门下装配：关闭门重查与 timer 装配同一临界区，Stop 不会在两者之间插入
    // cancel/reset 后又被重新挂上 timer。
    const auto state = tick_state_;
    const auto handler = handler_;
    auto self = shared_from_this();
    std::error_code command_error;
    const auto error = SubmitCommand(*main_loop_, [self, state, handler, delay] {
        std::lock_guard<std::mutex> guard(state->mutex);
        if (state->closed) { return std::make_error_code(std::errc::operation_canceled); }
        return self->ArmTickOwner(state, handler, delay);
    }, kControlDeadline, &command_error);
    if (error) { return error; }
    return command_error;
}

void ServerEngine::PostArmTick(const std::shared_ptr<TickState>& state, std::chrono::nanoseconds delay)
{
    // 从受管协程重挂：**同一短门内**完成「闭门检查 + 读句柄 + 非阻塞 Submit」，绝不取
    // control_mutex_。Stop 全程持 control_mutex_（CloseOwnerNetwork 的 15s 命令等待 +
    // Join）：若在此排队，唯一 Processer 会被整段停机窗口阻塞，其它连接协程全部停摆
    //（132 审查 P2 的主因）；而不锁外 Submit，则「读到 loop/owner」与「Stop 撤空并销毁
    // context」不在同一同步点，Submit 可落在已析构的 EventLoop 上（135 P1）。
    // 锁序（静态）：control_mutex_ → state->mutex → EventLoop::m_mutex，无反向路径 ——
    // Submit 只取 m_mutex 且非阻塞；owner 执行命令时 Drain 已在锁外（EventLoop.cc Drain
    // 在锁内取出命令、锁外执行），命令内取 state->mutex 不构成环。
    // 闭门与句柄撤空同锁 ⇒ 不存在「门已关但仍可投递」或「句柄已回收但门仍开」的中间态；
    // 命令内重查门：若本命令排在 Stop 的 CloseOwnerNetwork 之后，门已关 ⇒ 不再挂 timer
    //（不会让 Join 重新等待）；若排在之前，则它挂上的 timer 随后被同一 CloseOwnerNetwork
    // 取消/析构（FIFO + 命令内重查 ⇒ 两条路径都收口）。
    std::lock_guard<std::mutex> gate(state->mutex);
    if (state->closed) { return; }
    EventLoop* const loop = state->loop;
    ServerEngine* const owner = state->owner;
    if (loop == nullptr || owner == nullptr) { return; }
    const std::shared_ptr<EventHandler> handler = state->handler;
    SubmitStatus status = SubmitStatus::NotSubmitted;
    try {
        status = loop->Submit([state, owner, handler, delay] {
            std::lock_guard<std::mutex> inner(state->mutex);
            if (state->closed) { return; }
            (void)owner->ArmTickOwner(state, handler, delay);
        });
    } catch (...) {
        // 投递抛出：门内直接停 ticker（用已持有的同一把锁，不重复加锁）。此处析构的
        // std::function/handler 副本都不是最后一个引用（handler 由 ServerEngine::handler_
        // 持有到引擎销毁、state 由 ServerEngine::tick_state_ 持有），故不在门内析构用户对象。
        StopTickerLocked(state, TickState::StopReason::PostFailed);
        return;
    }
    if (status == SubmitStatus::Accepted) { return; }
    // Full/Sealed/NotStarted/Invalid/Failed：明确停 ticker 并记录内部原因，不留
    // 「Engine 仍 Running 但 ticker 无声消失」的不可核状态；不重试、不加 reaper，
    // 连接/接纳不受影响。
    StopTickerLocked(state, TickState::StopReason::PostRejected);
}

void ServerEngine::TickTask(const std::shared_ptr<TickState>& state,
    const std::shared_ptr<EventHandler>& handler)
{
    {
        std::lock_guard<std::mutex> guard(state->mutex);
        if (state->closed) { return; }   // 唯一开始授权：Stop/rollback/前次失败已收口
    }
    std::chrono::nanoseconds delay{0};
    try {
        // 用户回调：受管协程，可挂起（只让出 Processer，不占 owner/网络线程）。
        delay = handler->OnTick();
    } catch (...) {
        std::lock_guard<std::mutex> guard(state->mutex);
        StopTickerLocked(state, TickState::StopReason::UserThrow);   // ticker 停，Engine 继续服务
        return;
    }
    {
        std::lock_guard<std::mutex> guard(state->mutex);
        if (state->closed) { return; }
        if (delay <= std::chrono::nanoseconds::zero()) {
            StopTickerLocked(state, TickState::StopReason::ZeroOrNegative);   // Engine 仍 Running
            return;
        }
    }
    // 重挂（只在 OnTick 返回后发生 ⇒ 结构上不可重入）：只经窄门句柄非阻塞投递。
    // 这里的弱引用提升只覆盖「一次非阻塞 Submit」：不取 control_mutex_、不等 owner、
    // 不跨用户代码或挂起 ⇒ 不会像旧实现那样把提升延长到停机窗口，也不扩持续 OOM 域
    //（不新增 reaper/持有策略）。最后一个强引用仍在 Processer 释放的极端情形沿用 Engine
    // 既有析构域约束（析构只应在合法控制线程，见 Engine/ServerEngine 注释）。
    std::shared_ptr<ServerEngine> self;
    {
        std::lock_guard<std::mutex> guard(state->mutex);
        if (state->closed) { return; }
        self = state->engine.lock();
    }
    if (!self) { return; }
    self->PostArmTick(state, delay);
}

bool ServerEngine::NetworkCloseReady() const
{
    // 内部只读（owner readiness，不扩公共面）：调用方持 control_mutex_；每桥自行
    // 短锁核会话物理关闭与 owned 等待记录结算，不在 Engine 层复制会话状态。
    for (const auto& sub : subs_) {
        if (sub.bridge && !sub.bridge->NetworkCloseReady()) { return false; }
    }
    return true;
}

bool ServerEngine::IsOwnerThreadLocked() const
{
    const auto self = std::this_thread::get_id();
    if (main_loop_ && main_loop_->OwnerId() == self) { return true; }
    for (const auto& sub : subs_) {
        if (sub.loop && sub.loop->OwnerId() == self) { return true; }
    }
    return false;
}

void ServerEngine::ResetOwners()
{
    // 收口 ticker 门并撤空投递句柄：ResetOwners 之后 main_loop_/context 不再存活，
    // 保证任何 tick 侧投递都不可能落到已回收的 owner（本调用在 control_mutex_ 下，
    // 与 Stop/rollback 的 CloseTickGate 幂等）。
    CloseTickGate();
    if (main_loop_) {
        main_loop_->RequestStop();
        (void)main_loop_->Join();
    }
    for (auto& sub : subs_) {
        if (sub.loop) {
            sub.loop->RequestStop();
            (void)sub.loop->Join();
        }
    }
    subs_.clear();   // 先析构 bridge（subs_ 成员逆序），再析构 loop/绑定资源
    balancer_.reset();
    watcher_.reset();
    listener_.reset();
    tick_timer_.reset();   // timer 析构必须早于其 context（main_loop_）
    main_loop_.reset();
    bound_.clear();
    // 不清零仍可能被迟到 OnClose（挂起跨销毁）持有的旧计数：清零会造成 unsigned
    // underflow。改为解引用代际：旧计数由适配器/guard 共享持有到其真正结束；
    // 对外 0 由 subs_ 清空表达；下一次 Run 创建全新计数（无跨代别名）。
    adapter_.reset();
    live_connections_.reset();
}

} // namespace bbt::infra::net

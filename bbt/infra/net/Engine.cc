#include <bbt/infra/net/Engine.hpp>

#include <system_error>
#include <utility>

#include <bbt/infra/net/detail/ServerEngine.hpp>

namespace bbt::infra::net {

Engine::Engine(std::shared_ptr<EventHandler> handler) : handler_(std::move(handler)) {}

Engine::~Engine()
{
    // 析构域：合法控制线程（非受管协程、非本 Engine owner 线程，见 Engine.hpp）。
    // 未 Stop 仍在运行/启动中则按同一强停顺序收口；不 terminate/detach 线程。
    if (server_) { (void)server_->ForceStopForDestruction(); }
}

std::error_code Engine::EnsureServer(std::shared_ptr<ServerEngine>& out)
{
    // 稳定句柄在任何 nativeRun 之前发布：并发的 Stop/观测看到同一个 server（未启动的
    // Stop 也据此进入终态），且 facade 锁不跨 server->Run/Stop。
    std::lock_guard<std::mutex> guard(mutex_);
    if (!server_) {
        std::error_code error;
        auto created = ServerEngine::Create(handler_, error);
        if (!created) { return error ? error : std::make_error_code(std::errc::not_enough_memory); }
        server_ = std::move(created);
    }
    out = server_;
    return {};
}

std::shared_ptr<ServerEngine> Engine::Snapshot() const
{
    std::lock_guard<std::mutex> guard(mutex_);
    return server_;
}

std::error_code Engine::Run(const Address& address, const Options& options)
{
    // 空拥有关系在任何副作用（对象/线程/socket/受管任务）之前明确拒绝。
    if (!handler_) { return std::make_error_code(std::errc::invalid_argument); }
    // 域门禁在任何副作用之前（不建对象、不起线程、不建 socket）。
    if (InManagedCoroutine()) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    std::shared_ptr<ServerEngine> server;
    if (const auto error = EnsureServer(server)) { return error; }
    // 锁外调用：ServerEngine 自己串行化（Starting/Running → busy，Stopped → canceled）。
    return server->Run(address, options);
}

std::error_code Engine::Stop()
{
    if (InManagedCoroutine()) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    std::shared_ptr<ServerEngine> server;
    if (const auto error = EnsureServer(server)) { return error; }
    // 未 Run 时同样进入终态（随后 Run → operation_canceled）；已停止幂等成功。
    return server->Stop();
}

std::error_code Engine::LocalAddresses(std::vector<Address>& out) const
{
    const auto server = Snapshot();   // 短锁只读句柄，锁外做观测调用
    if (!server) { return std::make_error_code(std::errc::operation_not_permitted); }
    return server->LocalAddresses(out);
}

std::size_t Engine::CountConnections() const
{
    const auto server = Snapshot();
    return server ? server->CountConnections() : 0;
}

} // namespace bbt::infra::net

#include <bbt/infra/net/detail/EngineTestAccess.hpp>
#include <bbt/infra/net/detail/Reactor.hpp>
#include <bbt/infra/net/Engine.hpp>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <exception>
#include <functional>
#include <stdexcept>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <unistd.h>

#include <boost/asio/post.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/steady_timer.hpp>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/detail/CloseWait.hpp>
#include <bbt/infra/net/LoadBalancing.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/detail/CoInternal.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/DriverBinder.hpp>
#include <bbt/infra/net/detail/OptionsNormalize.hpp>
#include <bbt/infra/net/detail/ReactorDriver.hpp>
#include <bbt/infra/net/detail/ReclaimRecord.hpp>
#include <bbt/infra/net/detail/ReceiveRing.hpp>
#include <bbt/infra/net/detail/WakeHub.hpp>

namespace bbt::infra::net {

/**
 * ---- EngineTestAccess（私有测试接缝，非产品 API）----
 */
std::error_code EngineTestAccess::InjectDriverFailure(Engine& engine)
{
    auto impl = engine.m_impl;
    std::lock_guard<std::mutex> lock(impl->mutex);
    if (impl->phase != EnginePhase::Running || impl->slots.empty()) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    impl->slots.back().driver->InjectTickFailureOnce();
    return {};
}

void EngineTestAccess::SetConnectLimit(Engine& engine, std::size_t limit)
{
    auto impl = engine.m_impl;
    std::lock_guard<std::mutex> lock(impl->mutex);
    impl->connect_limit = limit == 0 ? 1 : limit;
}

std::size_t EngineTestAccess::ConnectTableSize(Engine& engine)
{
    std::lock_guard<std::mutex> lock(engine.m_impl->connect_mutex);
    return engine.m_impl->connects.size();
}

void EngineTestAccess::SetConnectStartBarrierOnce(Engine& engine, std::function<void()> barrier)
{
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    for (auto& reactor : reactors) {
        reactor->SetConnectStartBarrier(barrier);
    }
}

std::size_t EngineTestAccess::PendingConnectCount(Engine& engine)
{
    auto impl = engine.m_impl;
    std::lock_guard<std::mutex> lock(impl->connect_mutex);
    std::size_t pending = 0;
    for (auto& entry : impl->connects) {
        std::lock_guard<std::mutex> op_lock(entry.second->mutex);
        if (!entry.second->done) {
            ++pending;
        }
    }
    return pending;
}

void EngineTestAccess::SetConnectDeliveryDelay(Engine& engine, std::chrono::milliseconds delay)
{
    engine.m_impl->connect_delivery_delay_ms.store(delay.count());
}

std::size_t EngineTestAccess::DirtyCount(Engine& engine)
{
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    std::size_t total = 0;
    for (auto& reactor : reactors) {
        total += reactor->DirtyCount();
    }
    return total;
}

std::size_t EngineTestAccess::ConnectionCount(Engine& engine)
{
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    std::size_t total = 0;
    for (auto& reactor : reactors) {
        total += reactor->ConnCount();
    }
    return total;
}

void EngineTestAccess::SetSubmitBarrierOnce(Engine& engine, std::function<void()> barrier)
{
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    for (auto& reactor : reactors) {
        reactor->SetSubmitBarrier(barrier);
    }
}

void EngineTestAccess::SetConnectSubmitBarrierOnce(Engine& engine, std::function<void()> barrier)
{
    auto impl = engine.m_impl;
    std::lock_guard<std::mutex> lock(impl->connect_submit_barrier_mutex);
    impl->connect_submit_barrier = std::move(barrier);
}

void EngineTestAccess::SetConnectPostFailureOnce(Engine& engine)
{
    engine.m_impl->connect_post_fail_once.store(true);
}

void EngineTestAccess::SetConnectWaitStatusOnce(Engine& engine, int status)
{
    engine.m_impl->connect_wait_status_once.store(status < 0 ? -1 : status);
}

void EngineTestAccess::ClearConnectInjection(Engine& engine)
{
    auto impl = engine.m_impl;
    impl->connect_post_fail_once.store(false);
    impl->connect_wait_status_once.store(-1);
    std::lock_guard<std::mutex> lock(impl->connect_submit_barrier_mutex);
    impl->connect_submit_barrier = {};
}

void EngineTestAccess::KickOwner(Engine& engine)
{
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    for (auto& reactor : reactors) {
        reactor->KickPumpOwner();
    }
}

void EngineTestAccess::SetSealDelay(Engine& engine, std::chrono::milliseconds delay)
{
    engine.m_impl->seal_delay_ms.store(delay.count());
}

void EngineTestAccess::SetAuthBarriers(Engine& engine, std::function<void()> pre_auth, std::function<void()> post_auth)
{
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    for (auto& reactor : reactors) {
        reactor->SetPreAuthBarrier(pre_auth);
        reactor->SetPostAuthBarrier(post_auth);
    }
}

bool EngineTestAccess::ConnectReceiverClaimed(Engine& engine, std::uint64_t id)
{
    auto op = engine.m_impl->FindConnect(id);
    if (!op) {
        return false;
    }
    std::lock_guard<std::mutex> lock(op->mutex);
    return op->receiver_claimed;
}

std::size_t EngineTestAccess::RecvHoldBytes(Engine& engine)
{
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    std::size_t total = 0;
    for (auto& reactor : reactors) {
        total += reactor->RecvHoldBytes();
    }
    return total;
}

std::size_t EngineTestAccess::FillConnectHub(
    Engine& engine, std::uint64_t id, std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>>& keep)
{
    auto op = engine.m_impl->FindConnect(id);
    if (!op) {
        return 0;
    }
    std::size_t added = 0;
    for (std::size_t i = 0; i < WakeHub::kMaxWaiters + 1; ++i) {
        auto waiter = op->hub.Create();
        if (!op->hub.Add(waiter)) {
            break;
        }
        keep.push_back(std::move(waiter));
        ++added;
    }
    return added;
}

void EngineTestAccess::SetWriteReadyBarrierOnce(Engine& engine, std::function<void()> barrier)
{
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    for (auto& reactor : reactors) {
        reactor->SetWriteReadyBarrier(barrier);
    }
}

void EngineTestAccess::SetConnectStartDelay(Engine& engine, std::chrono::milliseconds delay)
{
    engine.m_impl->connect_start_delay_ms.store(delay.count());
}

void EngineTestAccess::SetCancelPostFailureOnce(Engine& engine)
{
    engine.m_impl->cancel_post_fail_once.store(true);
}

void EngineTestAccess::SetShutdownBarrierOnce(Engine& engine, std::function<void()> barrier)
{
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    for (auto& reactor : reactors) {
        reactor->SetShutdownBarrier(barrier);
    }
}

void EngineTestAccess::SetRemoveBarrierOnce(Engine& engine, std::function<void()> barrier)
{
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    for (auto& reactor : reactors) {
        reactor->SetRemoveBarrier(barrier);
    }
}

void EngineTestAccess::SetDirtyBarrierOnce(Engine& engine, std::function<void()> barrier)
{
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    for (auto& reactor : reactors) {
        reactor->SetDirtyBarrier(barrier);
    }
}

void EngineTestAccess::FailNextConnectWaiterCreateOnce(Engine& engine)
{
    (void)engine;
    WakeHub::SetCreateFaultOnceForTest();
}

void EngineTestAccess::SetWaitDriverFailedOnce(Engine& engine)
{
    engine.m_impl->test_wait_driver_failed.store(true);
}

void EngineTestAccess::SetSealThrowOnce(Engine& engine)
{
    engine.m_impl->seal_throw_once.store(true);
}

void EngineTestAccess::SetAcceptWaiterHook(Engine& engine, std::function<void()> registered)
{
    engine.m_impl->accept_waiter_hook = std::move(registered);
}

void EngineTestAccess::SetAcceptStopPostFailureOnce(Engine& engine, std::function<void()> after_failure)
{
    engine.m_impl->accept_stop_failure_hook = std::move(after_failure);
    engine.m_impl->accept_stop_post_fail_once.store(true);
}

void EngineTestAccess::SetInstallDelay(Engine& engine, std::chrono::milliseconds delay)
{
    engine.m_impl->install_delay_ms.store(delay.count());
}

void EngineTestAccess::SetSealPostFailureOnce(Engine& engine)
{
    engine.m_impl->seal_post_fail_once.store(true);
}

void EngineTestAccess::SetReadAppendHook(Engine& engine, std::function<void()> hook)
{
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    for (auto& reactor : reactors) {
        reactor->SetReadAppendHook(hook);
    }
}

void EngineTestAccess::SetAppendBusyHook(Engine& engine, std::function<void()> hook)
{
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    for (auto& reactor : reactors) {
        reactor->SetAppendBusyHook(hook);
    }
}

int EngineTestAccess::FirstConnectionFd(Engine& engine)
{
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    for (auto& reactor : reactors) {
        const int fd = reactor->FirstConnFd();
        if (fd >= 0) {
            return fd;
        }
    }
    return -1;
}

// clang-format off
}
// clang-format on

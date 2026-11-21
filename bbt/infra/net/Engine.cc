#include <bbt/infra/net/detail/EngineState.hpp>
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
#include <bbt/infra/net/detail/WaitSet.hpp>

namespace bbt::infra::net {

/**
 * ---- Engine ----
 */
Engine::Engine(std::shared_ptr<RawHandler> handler)
    : m_impl(std::make_shared<EngineImpl>(std::move(handler)))
{
}

Engine::~Engine()
{

    /**
     * 合法控制域上的最后兜底：Run 因解绑失败保留 Closing 时，析构前再尝试
     * 同代同步解绑 + 冻结收口；成功才释放责任，失败则无 reaper 可再保证。
     */
    (void)FinalizeNative();
}

std::error_code Engine::Close(const CloseOptions& options) noexcept
{
    return m_impl->Close(options);
}

void Engine::RequestStopAccepting() noexcept
{
    m_impl->RequestStopAccepting();
}

bool Engine::AcceptingStopped() const noexcept
{
    return m_impl->AcceptingStopped();
}

EnginePhase Engine::Phase() const noexcept
{
    return m_impl->Phase();
}

std::error_code Engine::LastError() const
{
    return m_impl->LastError();
}

std::exception_ptr Engine::LastFailure() const
{
    return m_impl->LastFailure();
}

std::error_code Engine::EnableAcceptHandoff(std::size_t capacity)
{
    return m_impl->EnableAcceptHandoff(capacity);
}

std::error_code Engine::Run(const EngineConfig& config)
{
    return m_impl->Run(config);
}

std::error_code Engine::FinalizeNative() noexcept
{
    return m_impl->FinalizeNative();
}

std::error_code Engine::LocalAddresses(std::vector<Address>& out) const
{
    return m_impl->LocalAddresses(out);
}

std::size_t Engine::HandlerErrorCount() const
{
    return m_impl->HandlerErrorCount();
}

std::size_t Engine::CountConnections() const
{
    return m_impl->CountConnections();
}

std::error_code
Engine::ReactorOwnerIds(std::vector<std::uint64_t>& main_owners, std::vector<std::uint64_t>& sub_owners) const
{
    return m_impl->ReactorOwnerIds(main_owners, sub_owners);
}

std::error_code Engine::AsyncConnect(const Address& address, std::uint64_t& out_connect_id)
{

    /**
     * OnConnect 是唯一交付：成功或失败恰一次，operation 等于本次
     * out_connect_id；拒绝路径不改 out_connect_id，也不产生本次回调。
     */
    return StartConnectImpl(m_impl, address, out_connect_id, false);
}

std::error_code Engine::CoSubmitConnect(const Address& address, std::uint64_t& out_connect_id)
{

    /**
     * CoDial 的提交步骤：登记接收意图，完成早到也保留结果供领取。
     */
    return StartConnectImpl(m_impl, address, out_connect_id, true);
}

std::error_code Engine::CoSubmitConnect(
    const Address& address, std::uint64_t& out_connect_id, const std::shared_ptr<ReclaimRecord>& reclaim)
{

    /**
     * 带责任记录的提交：记录随 post 捕获流经 owner 各真实 close/转交点。
     */
    return StartConnectImpl(m_impl, address, out_connect_id, true, reclaim);
}

/**
 * ---- Co 成员入口（docs/decisions/0012-icoconn-member-interface.md）：薄转发到既有私有步骤，不新增第二路径。----
 * 判定优先级依次为门禁、终态、配置。
 * 成员形态返回 ConnectionResult{error, conn, reclaim}：失败不覆盖 conn，
 * reclaim 按提交阶段描述本次 socket 责任（提交前拒绝=无效，提交接纳后=
 * 有效且 Released() 与连接同终态）。
 */
ConnectionResult Engine::CoAccept(const bbt::coroutine::WaitOptions& options)
{
    ConnectionResult result;

    /**
     * 门禁先于一切状态判定：非法域即使
     * handoff 已有连接也返回 operation_not_permitted。
     */
    if (const auto gate = RequireCoroutineContext()) {
        result.error = gate;
        return result;
    }
    Connection conn;
    result.error = CoWaitAccepted(conn, options);
    if (!result.error) {

        /**
         * reclaim 与交付连接同一真源：accept 连接在 AdoptFdOnOwner 建立的
         * owner record 就是 Connection::Released() 的事实——reclaim 取同一
         * 记录才满足「receipt 与连接 IsReleased 同一事实源」。
         */
        if (const auto record = conn.ReclaimForOwner()) {
            result.reclaim = CoAccess::MakeReceipt(record);
        }
        result.connection = CoTcpConnection(std::move(conn));
    }
    return result;
}

ConnectionResult Engine::CoDial(const Address& address, const bbt::coroutine::WaitOptions& options)
{
    ConnectionResult result;

    /**
     * gate 先于提交：非法域不创建责任记录、
     * 不产生任何提交副作用，reclaim 保持无效（Released()=true，无责任）。
     */
    if (const auto gate = RequireCoroutineContext()) {
        result.error = gate;
        return result;
    }

    /**
     * 提交前预分配共享释放记录：一旦接纳，无论超时/取消/未交付/Close 竞态，
     * 记录都随提交流经 owner 各真实 close 点，描述本次物理 socket 责任。
     */
    auto record = std::make_shared<ReclaimRecord>(false);
    std::uint64_t id = 0;
    if (const auto error = CoSubmitConnect(address, id, record)) {
        record->ConfirmReleased();
        result.error = error;

        /**
         * 提交前拒绝：无已提交责任
         */
        result.reclaim = ReclaimReceipt{};
        return result;
    }
    m_impl->InvokeHook(HookPoint::AfterDialSubmitBeforeAttach);
    Connection conn;
    result.error = CoWaitConnect(id, conn, options);
    result.reclaim = CoAccess::MakeReceipt(std::move(record));
    if (!result.error) {
        result.connection = CoTcpConnection(std::move(conn));
    }
    return result;
}

result<void> Engine::CoWaitRunning(const bbt::coroutine::WaitOptions& options)
{
    return m_impl->CoWaitRunning(options);
}

/**
 * CoDial 的领取步骤（内部）：成功才写 out；任何未交付失败都归还 claim 并让
 * owner 回收 fd。有 handler 的 Engine 在写任何等待/取消状态之前拒绝。
 */
std::error_code
Engine::CoWaitConnect(std::uint64_t connect_id, Connection& out, const bbt::coroutine::WaitOptions& options)
{
    return m_impl->CoWaitConnect(connect_id, out, options);
}

std::error_code Engine::CoWaitAccepted(Connection& out, const bbt::coroutine::WaitOptions& options)
{
    return m_impl->CoWaitAccepted(out, options);
}

// clang-format off
}
// clang-format on

#include <bbt/infra/net/detail/TestAccess.hpp>

#include <cassert>
#include <utility>

#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/ReceiveRing.hpp>

// 内部测试接缝实现：与产品实现同库（bbt_infra_net），但归属 detail 头，
// 不进公共头自检白名单。CoTestAccess 的屏障定义在 CoInternal.cc，
// 这里只做声明链接。

namespace bbt::infra::net {

Connection ConnectionTestAccess::Make(std::shared_ptr<ConnectionState> state)
{
    return Connection(std::move(state));
}

std::shared_ptr<ConnectionState> ConnectionTestAccess::StateOf(const Connection& connection)
{
    return connection.m_state;
}

WriteOperation ConnectionTestAccess::MakeWrite(std::shared_ptr<WriteOpRecord> record,
                                               std::shared_ptr<ConnectionState> state)
{
    return WriteOperation(std::move(record), std::move(state));
}

std::unique_lock<std::mutex> ConnectionTestAccess::HoldForTest(std::shared_ptr<ConnectionState> state)
{
    return state ? state->ring.TryHoldForTest() : std::unique_lock<std::mutex>();
}

void ConnectionTestAccess::Reset(const std::shared_ptr<ConnectionState>& state, std::unique_lock<std::mutex>& lock)
{
    if (lock.owns_lock()) {
        assert(state && lock.mutex() == &state->ring.m_mutex);
        lock.unlock();
        // 测试保护也必须完成释放协议，否则 Busy 后的等待者和 owner 无法继续。
        state->ring.NotifyRelease();
        state->ring.WakeOwnerOnly();
    }
}

Buffer BufferTestAccess::Make(std::shared_ptr<void> backing, std::vector<BufferSegment> segments)
{
    return Buffer(std::move(backing), std::move(segments));
}

} // namespace bbt::infra::net

#include <bbt/infra/net/detail/TestAccess.hpp>

#include <cassert>
#include <utility>

#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/ReceiveRing.hpp>

/**
 * 内部测试接缝实现：与产品实现同库（bbt_infra_net），但归属 detail 头，
 * 不进公共头自检白名单。CoTestAccess 的屏障定义在 CoInternal.cc，
 * 这里只做声明链接。
 */
namespace bbt::infra::net {

Connection ConnectionTestAccess::Make(std::shared_ptr<ConnectionState> state)
{
    return Connection(std::move(state));
}

ConnectionTestAccess::Observation ConnectionTestAccess::Inspect(const Connection& connection)
{
    const auto [identity, bytes, pending, generation, close_requested, can_accept] = connection.InspectForTest();
    return {identity, bytes, pending, generation, close_requested, can_accept};
}

bool ConnectionTestAccess::ExerciseReadContention(const Connection& connection, const std::function<void()>& during)
{
    return connection.ExerciseReadContentionForTest(
        [](const void* context) { (*static_cast<const std::function<void()>*>(context))(); }, &during);
}

bool ConnectionTestAccess::ExerciseReadContention(
    const std::shared_ptr<ConnectionState>& state,
    const std::function<void()>& during)
{
    return state && state->ExerciseReadContentionForTest(during);
}

bool ConnectionTestAccess::DeferReceived(const Connection& connection, const std::string& data)
{
    return connection.DeferReceivedForTest(data);
}

Buffer BufferTestAccess::Make(std::shared_ptr<void> backing, std::vector<BufferSegment> segments)
{
    return Buffer(std::move(backing), std::move(segments));
}

// clang-format off
}
// clang-format on

#include <bbt/infra/net/detail/TestAccess.hpp>

#include <utility>

#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/ReceiveRing.hpp>

// 内部测试接缝实现：与产品实现同库（bbt_infra_net），但归属 detail 头，
// 不进公共头自检白名单。CoTestAccess 的屏障定义在 CoConnection.cc，
// 这里只做声明链接。

namespace bbt::infra::net {

Connection ConnectionTestAccess::Make(std::shared_ptr<ConnectionState> state) {
    return Connection(std::move(state));
}

std::shared_ptr<ConnectionState> ConnectionTestAccess::StateOf(const Connection& connection) {
    return connection.m_state;
}

WriteOperation ConnectionTestAccess::MakeWrite(std::shared_ptr<WriteOpRecord> record,
                                               std::shared_ptr<ConnectionState> state) {
    return WriteOperation(std::move(record), std::move(state));
}

ReadAccess ConnectionTestAccess::HoldForTest(std::shared_ptr<ConnectionState> state) {
    if (!state) {
        return {};
    }
    auto attempt = state->ring.TryHoldAttemptForTest();
    return ReadAccess(std::move(state), std::move(attempt.lock), attempt.status, attempt.error);
}

void ConnectionTestAccess::Reset(ReadAccess& access) { access = ReadAccess(); }

} // namespace bbt::infra::net

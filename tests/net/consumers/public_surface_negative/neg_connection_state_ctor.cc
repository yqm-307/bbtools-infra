// expect-error: is private within this context
#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
void Probe(std::shared_ptr<bbt::infra::net::ConnectionState> state) {
    bbt::infra::net::Connection connection(state);   // 只能经 Engine/TestAccess 构造
    (void)connection;
}

// expect-error: AsyncConnectAwaitable
#include <bbt/infra/net/Engine.hpp>
void Probe(bbt::infra::net::Engine& engine, const bbt::infra::net::Address& address) {
    std::uint64_t id = 0;
    (void)engine.AsyncConnectAwaitable(address, id);   // 两段式建连是 CoDial 内部步骤
}

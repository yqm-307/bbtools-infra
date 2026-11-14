// expect-error: WaitConnect
#include <bbt/infra/net/Engine.hpp>

void Probe(bbt::infra::net::Engine& engine)
{
    bbt::infra::net::Connection connection;

    /**
     * 领取步骤不再公开
     */
    (void)engine.WaitConnect(1, connection, {});
}

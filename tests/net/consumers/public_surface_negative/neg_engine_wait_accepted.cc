// expect-error: WaitAccepted
#include <bbt/infra/net/Engine.hpp>

void Probe(bbt::infra::net::Engine& engine)
{
    bbt::infra::net::Connection connection;

    /**
     * accept 等待是 CoAccept 内部步骤
     */
    (void)engine.WaitAccepted(connection, {});
}

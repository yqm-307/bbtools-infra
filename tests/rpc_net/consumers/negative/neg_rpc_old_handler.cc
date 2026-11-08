// expect-error: RpcHandler
#include <bbt/infra/NetworkTypes.hpp>

void Probe()
{
    bbt::infra::RpcHandler handler;
    (void)handler;
}

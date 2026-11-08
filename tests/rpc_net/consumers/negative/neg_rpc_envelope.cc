// expect-error: RpcEnvelope
#include <bbt/infra/NetworkTypes.hpp>

void Probe()
{
    bbt::infra::RpcEnvelope envelope;
    (void)envelope;
}

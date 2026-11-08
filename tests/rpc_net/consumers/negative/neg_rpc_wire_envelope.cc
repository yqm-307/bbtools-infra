// expect-error: RpcWireEnvelope
#include <bbt/infra/rpc/SessionCodec.hpp>

void Probe()
{
    bbt::infra::rpc::RpcWireEnvelope envelope;
    (void)envelope;
}

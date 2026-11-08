// expect-error: IRpcInboundTransport
#include <bbt/infra/rpc/CoRpcServer.hpp>

void Probe()
{
    bbt::infra::rpc::IRpcInboundTransport* inbound = nullptr;
    (void)inbound;
}

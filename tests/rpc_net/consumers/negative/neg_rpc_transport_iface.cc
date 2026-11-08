// expect-error: IRpcTransport
#include <bbt/infra/rpc/CoRpcClient.hpp>

void Probe()
{
    bbt::infra::rpc::IRpcTransport* transport = nullptr;
    (void)transport;
}

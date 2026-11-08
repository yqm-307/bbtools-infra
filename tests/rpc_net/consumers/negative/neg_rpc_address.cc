// expect-error: RpcAddress
#include <bbt/infra/NetworkTypes.hpp>

void Probe()
{
    bbt::infra::RpcAddress address;
    (void)address;
}

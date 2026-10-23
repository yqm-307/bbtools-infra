// expect-error: TryAccess
#include <bbt/infra/net/detail/ReceiveRing.hpp>
void Probe(bbt::infra::net::ReceiveRing& ring) { (void)ring.TryAccess(); }

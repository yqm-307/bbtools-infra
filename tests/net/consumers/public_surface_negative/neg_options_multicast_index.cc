// expect-error: multicast_interface_index
#include <bbt/infra/net/Options.hpp>
void Probe(bbt::infra::net::Options& options) { options.multicast_interface_index = 1; }

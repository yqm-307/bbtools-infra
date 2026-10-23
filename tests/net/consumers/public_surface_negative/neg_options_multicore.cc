// expect-error: multicore
#include <bbt/infra/net/Options.hpp>
void Probe(bbt::infra::net::Options& options) { options.multicore = true; }

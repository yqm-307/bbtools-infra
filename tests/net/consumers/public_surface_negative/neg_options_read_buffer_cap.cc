// expect-error: read_buffer_cap
#include <bbt/infra/net/Options.hpp>
void Probe(bbt::infra::net::Options& options) { options.read_buffer_cap = 1024; }

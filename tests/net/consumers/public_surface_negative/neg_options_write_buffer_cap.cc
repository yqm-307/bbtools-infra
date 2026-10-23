// expect-error: write_buffer_cap
#include <bbt/infra/net/Options.hpp>
void Probe(bbt::infra::net::Options& options) { options.write_buffer_cap = 1024; }

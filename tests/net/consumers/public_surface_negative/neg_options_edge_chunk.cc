// expect-error: edge_triggered_io_chunk
#include <bbt/infra/net/Options.hpp>
void Probe(bbt::infra::net::Options& options) { options.edge_triggered_io_chunk = 128; }

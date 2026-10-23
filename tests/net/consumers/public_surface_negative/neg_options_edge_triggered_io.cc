// expect-error: edge_triggered_io
#include <bbt/infra/net/Options.hpp>
void Probe(bbt::infra::net::Options& options) { options.edge_triggered_io = true; }

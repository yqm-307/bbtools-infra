// expect-error: ticker
#include <bbt/infra/net/Options.hpp>
void Probe(bbt::infra::net::Options& options) { options.ticker = true; }

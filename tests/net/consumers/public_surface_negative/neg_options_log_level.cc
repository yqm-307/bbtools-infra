// expect-error: log_level
#include <bbt/infra/net/Options.hpp>
void Probe(bbt::infra::net::Options& options) { options.log_level = 1; }

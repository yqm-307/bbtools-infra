// expect-error: logger
#include <bbt/infra/net/Options.hpp>
void Probe(bbt::infra::net::Options& options) { options.logger = {}; }

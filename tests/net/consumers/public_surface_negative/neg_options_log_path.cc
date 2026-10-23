// expect-error: log_path
#include <bbt/infra/net/Options.hpp>
#include <string>
void Probe(bbt::infra::net::Options& options) { options.log_path = std::string("/tmp/x"); }

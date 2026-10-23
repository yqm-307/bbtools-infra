// expect-error: bind_to_device
#include <bbt/infra/net/Options.hpp>
#include <string>
void Probe(bbt::infra::net::Options& options) { options.bind_to_device = std::string("eth0"); }

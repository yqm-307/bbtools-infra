// expect-error: lock_os_thread
#include <bbt/infra/net/Options.hpp>
void Probe(bbt::infra::net::Options& options) { options.lock_os_thread = true; }

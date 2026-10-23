// expect-error: LoggerCallback
#include <bbt/infra/net/Options.hpp>
using namespace bbt::infra::net;
void Probe() { LoggerCallback callback; (void)callback; }

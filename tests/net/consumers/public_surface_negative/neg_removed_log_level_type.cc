// expect-error: LogLevel
#include <bbt/infra/net/Options.hpp>
using namespace bbt::infra::net;
void Probe() { LogLevel level = {}; (void)level; }

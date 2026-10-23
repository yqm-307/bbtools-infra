// expect-error: was not declared in this scope
#include <bbt/infra/net/CoConnection.hpp>
#include <bbt/coroutine/coroutine.hpp>
using namespace bbt::infra::net;
void Probe() { SetCoDialBarrierForTest({}); }

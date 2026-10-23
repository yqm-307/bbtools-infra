// expect-error: no member named
#include <bbt/infra/net/Connection.hpp>
void Probe(bbt::infra::net::Connection& connection) {
    (void)connection.State();   // 公共句柄不再暴露内部状态
}

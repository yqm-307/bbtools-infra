// expect-error: Wait
#include <bbt/infra/net/CoTcpConnection.hpp>

void Probe(bbt::infra::net::CoTcpConnection& connection)
{
    auto submission = connection.AsyncWrite("fire");

    /**
     * AsyncWrite 返回接纳错误，不交付等待票。
     */
    (void)submission.Wait({});
}

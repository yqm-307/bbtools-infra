// expect-error: CoRead
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/coroutine/coroutine.hpp>
using namespace bbt::infra::net;

void Probe(Engine& engine, Connection& connection, const Address& address)
{
    std::string out;

    /**
     * 已删除：协程读是 conn.Read()
     */
    (void)CoRead(connection, out, {});
    WriteOutcome outcome;
    WriteOperation op;

    /**
     * 已删除：conn.Write()
     */
    (void)CoWrite(connection, "x", {}, outcome, op);

    /**
     * 已删除：engine.CoAccept()
     */
    (void)CoAccept(engine, connection, {});

    /**
     * 已删除：engine.CoDial()
     */
    (void)CoDial(engine, address, connection, {});
}

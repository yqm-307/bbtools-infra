// expect-error: is private within this context
#include <bbt/infra/net/Connection.hpp>

void Probe(std::shared_ptr<bbt::infra::net::WriteOpRecord> record)
{

    /**
     * 未接纳的操作不可自行构造
     */
    bbt::infra::net::WriteOperation operation(record);
    (void)operation;
}

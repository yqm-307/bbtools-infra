// expect-error: is private within this context
#include <bbt/infra/net/Connection.hpp>
void Probe(std::shared_ptr<bbt::infra::net::WriteOpRecord> record) {
    bbt::infra::net::WriteOperation operation(record);   // 未接纳的操作不可自行构造
    (void)operation;
}

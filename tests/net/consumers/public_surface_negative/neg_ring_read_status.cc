// expect-error: ReadStatus
#include <bbt/infra/net/detail/ReceiveRing.hpp>
// 内部 ring 不再自带 ReadStatus：公共值类型集中在 Types.hpp，ring 只返回它。
void Probe(bbt::infra::net::ReceiveRing& ring) {
    auto result = ring.TryAppend("a");
    (void)(result.status == bbt::infra::net::ReceiveRing::ReadStatus::Ok);
}

// expect-error: deleted
#include <bbt/infra/net/ReadAccess.hpp>
void Probe(bbt::infra::net::ReadAccess& guard) {
    // 守卫是 move-only：复制会让两个对象持有同一份 ring 保护与通知责任。
    bbt::infra::net::ReadAccess copy(guard);
    (void)copy;
}

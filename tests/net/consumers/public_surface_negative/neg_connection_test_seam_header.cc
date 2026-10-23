// expect-missing-header: bbt/infra/net/EngineTestAccess.hpp
#include <bbt/infra/net/EngineTestAccess.hpp>
// 公共路径不再存在该测试接缝头；唯一入口在 detail/ 下，不属公共面。
void Probe() {}

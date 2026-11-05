#pragma once

/**
 * @file Udp.hpp
 * @brief 高层 UDP 的编译期拒绝入口。
 *
 * 高层 UDP 尚未交付。Engine 与 Options 都没有数据报参数或入口，接受端口一律按 IPv4 TCP stream
 * 处理。若放行，调用方只会得到一条被当作 stream 的连接，表现为假成功，因此在这里、且只在这个显式
 * 使用点直接拒绝。本头不声明任何类、函数、类型别名或模板，不构成 UDP API。
 *
 * 使用约束：
 * - 显式 include 本头即触发下面的 #error。
 * - 本头不得被聚合头、产品编译单元或头自检 include（见 bbt/infra/net/CMakeLists.txt 中 _headers 的注释），
 *   否则已交付的 IPv4 TCP 高层路径会一并编译失败。
 * - 底层数据报原语不受此拒绝影响：NativeSocket 的 SocketType::Datagram、SendTo/ReceiveFrom、零长与截断
 *   语义照常可用，并由 tests/net/Test_net_foundation.cc、tests/net/consumers/coroutine_first/main.cc 与
 *   examples/net_foundation_consumer 使用。它们不构成高层连接 API。
 * - 实现高层 UDP 时，删除本拒绝，并按 docs/decisions/0010-gnet-network-foundation.md 的目标语义新增真实入口：
 *   报文边界、截断、零长、完成项与错误传递、Wake 与事件位、预算与背压、地址族门。
 *
 * 相关文档：README 的模块交付状态、docs/decisions/0010-gnet-network-foundation.md、
 * docs/architecture/gnet-network-execution.md（其中记录了 UDP 未交付的范围）。
 */

#error                                                                                                                 \
    "UDP is not supported yet / UDP 暂不支持: bbt::infra::net has no high-level UDP entry (see bbt/infra/net/Udp.hpp)"

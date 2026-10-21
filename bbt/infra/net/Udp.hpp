#pragma once

// 高层 GNet（bbt::infra::net）UDP 的编译期拒绝入口。
//
// 用户裁定（2026-10-17）：本轮不实现高层 UDP。Engine/Client/Options 都没有
// datagram 参数或入口（接受端口一律按 IPv4 TCP stream 处理），若放行，调用方只
// 会得到一条被当作 stream 的连接（假成功）。因此在这里、且仅在这个显式使用点
// 直接拒绝：不改产品实现/公共声明，也不发明运行时协议选择或泛型 Engine。
//
// 用法与边界：
// - 显式 #include <bbt/infra/net/Udp.hpp> 即触发下面的 #error。本头不声明任何类、
//   函数、类型别名或模板，不是可调用接口，也不构成 UDP API。
// - 本头不得被任何聚合头、产品 TU 或头自检 include（见 bbt/infra/net/CMakeLists.txt
//   的 _headers 注释）：否则会把已交付的 IPv4 TCP 高层路径一并编译失败。
// - 底层 datagram 原语不在暂缓范围，保持现状可用：NativeSocket 的 SocketType::Datagram、
//   SendTo/ReceiveFrom、零长与 truncation，及其既有消费者（tests/net/net_foundation.cc、
//   tests/net/net_stream.cc、examples/net_foundation_consumer）。
//   「高层无 UDP 入口」不等于「UDP 整体未实现」：旧模块 bbt::infra_udp 的 CoUDP
//   （include/bbt/infra/CoUDP.hpp）是独立已交付实现，不属本头范围。
// - 将来实现高层 UDP 时：删除本拒绝，按 docs/decisions/0010-gnet-network-foundation.md
//   的目标语义（报文边界、截断、零长、完成项与错误传递、Wake 与事件位、预算与背压、
//   家族门）新增真实入口。
//
// 相关记录：README「模块交付状态」、docs/decisions/0010-gnet-network-foundation.md、
// docs/architecture/gnet-network-execution.md
// 的 UDP 暂缓注记。
#error "UDP is not supported yet / UDP 暂不支持: bbt::infra::net has no high-level UDP entry (see bbt/infra/net/Udp.hpp)"

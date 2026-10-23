#pragma once

// Co 薄封装（R2）：只复用既有 CoWaiter 与同一 raw 内核，不建第二收包队列。
//
// 执行域（D4）：四个入口都在任何消费、接纳、提交、创建等待责任之前校验当前
// 执行域；非协程上下文（普通线程、runtime idle、raw owner 回调）统一返回
// operation_not_permitted，包括带有现成数据/零长度写/已就绪 accept 的快路径。
// gate 不构造 CoObject、不创建 Processer、不产生网络副作用。
//
// 建连（D5）：CoDial 是唯一 Co 建连交付。它支持无 RawHandler 的 server 或
// client-only Engine；配置了 handler 的 Engine 在任何提交前拒绝，因为此时
// OnConnect 是唯一交付路径，Co 等待不得取消它。两段式（提交 → 领取）是内部
// 细节，不公开中间 ID；任何未交付失败都由内部归还 claim 并让 owner 回收未
// 交付的 socket。
//
// 输出（D4）：CoWrite 先置 Rejected/0/本次错误与无效 operation，接纳后才赋
// 本次 op；CoRead 失败不改 out 且不消费；CoAccept/CoDial 用临时结果，成功才
// 交付给调用者，失败不覆盖调用者原 Connection。
//
// 不自动为连接建业务协程、不等业务回包、不自动关闭/重连/重发。

#include <cstdint>
#include <string>
#include <system_error>

#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/Error.hpp>
#include <bbt/infra/net/Types.hpp>

namespace bbt::infra::net {

// 同一 ring 真源；WouldBlock 等数据/终态，Busy 等访问代次释放。
// 超时/取消不消费、不改 out。options 的 deadline 只影响本次等待，不撤销
// 已接纳的 I/O，也不表示"取消后永远不走快路径"。
std::error_code CoRead(Connection& connection, std::string& out,
                       const bbt::coroutine::WaitOptions& options = {});

// 组合 AsyncWrite 与容量/本地完成通知；同一绝对 deadline 沿重试保持。
//   - {} + outcome.phase==Done：本地完成（仍非对端回包），成功还须 error 为空。
//   - timed_out + outcome.phase==InFlight：已接纳、等待超时仍在途，operation
//     可查最终结果；不能仅凭 timed_out 推断未发出。
//   - timed_out + outcome.phase==Rejected：容量等待超时，本次未提交。
// 返回错误描述等待结果，outcome.error 描述该次发送结果。
std::error_code CoWrite(Connection& connection, const std::string& data,
                        const bbt::coroutine::WaitOptions& options,
                        WriteOutcome& outcome, WriteOperation& operation);

// 从有界 handoff 队列取已接纳连接（队列满时暂停 native accept，不丢 fd）。
// 配置/生命周期错误立即返回，不等待永远不会来的事件。判定优先级是契约：
//   0. 执行域非法（非协程上下文）：operation_not_permitted，先于任何状态判定。
//   1. 已封口 / Closing / Stopped：operation_canceled（终态优先；Stopped 时
//      main accept owner 已解绑，但结论仍是"取消"，不得当作 client-only）。
//   2. Idle / Starting：operation_not_permitted（尚未建立 accept 生产者）。
//   3. Running 但未开启 handoff：operation_not_permitted（配置缺失）。
//   4. Running + handoff 但无 main accept owner（client-only）：operation_not_supported。
// 失败不覆盖调用者原有 out。
std::error_code CoAccept(Engine& engine, Connection& out,
                         const bbt::coroutine::WaitOptions& options = {});

// 协程内建连；失败不覆盖调用者原有 out，不遗留待回收责任。
std::error_code CoDial(Engine& engine, const Address& address, Connection& out,
                       const bbt::coroutine::WaitOptions& options = {});

} // namespace bbt::infra::net

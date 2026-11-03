#pragma once
// RPC v2 session 内部接缝（bbt/infra/rpc 实现 TU 专用，非公开头）：
// Client.cc/Server.cc 的 v1 生命周期方法与 Session.cc 的 v2 owner/入站
// 状态之间的跨 TU 挂钩。只放声明；实现唯一在 Session.cc。

#include <memory>
#include <string>
#include <cstdint>
#include <functional>

#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/ICoConnection.hpp>
#include <bbt/infra/rpc/Codec.hpp>
#include <bbt/infra/rpc/Options.hpp>

namespace bbt::infra::rpc {

class Server;
struct SessionOwnerState;
struct ServerV2State;

// 测试接缝（detail 私有头，非公共钩子）：直接驱动匿名 InboundHandshake
// ——伪造 ICoConnection 可交付确定性单批 ReadOutcome，不依赖 TCP 分段
// 时机；只声明不导出，实现唯一在 Session.cc。
result<std::uint64_t> InboundHandshakeForTest(
    net::ICoConnection& conn, const ClientOptions& options,
    std::uint64_t proposal, std::uint64_t nonce,
    std::string& out_peer_identity, SessionFrameDecoder& out_decoder,
    bbt::coroutine::Deadline deadline);

// v2 owner 逻辑封口：幂等、不可重开；立即拒绝新 Call/Send，对在途 link
// RequestClose，已提交无可信终态的调用结算（committed → OutcomeUnknown）。
// 这是「简单 Close」的 owner 侧：封口即在途任务不可用的边界——等待顺序与
// 业务任务的停止由用户控制，infra 不新增 drain/receipt/多阶段协议。
void SessionOwnerSeal(const std::shared_ptr<SessionOwnerState>& owner) noexcept;
bool SessionOwnerClosed(const SessionOwnerState& owner) noexcept;

// v2 Server 立即逻辑封口（幂等）：StopAccepting + admission 封口 + 入站
// link RequestClose；不等待收口（收口等待走 Close(deadline)）。
void ServerV2Seal(const std::shared_ptr<ServerV2State>& v2) noexcept;
bool ServerV2Closed(const ServerV2State& v2) noexcept;
// v2 Start：登记 admission worker 并启动 accept 任务（Engine 必须 Running
// 且有监听；native 宿主域）。
result<void> ServerV2Start(const std::shared_ptr<ServerV2State>& v2);
// native 宿主等待收口（Drain/Close(deadline)/WaitStopped 共用谓词检查由
// Session.cc 内部分流）。
result<void> ServerV2Drain(const std::shared_ptr<ServerV2State>& v2,
                           bbt::coroutine::Deadline deadline);
result<void> ServerV2WaitStopped(const std::shared_ptr<ServerV2State>& v2,
                                 bbt::coroutine::Deadline deadline);
void ServerV2StopAccepting(const std::shared_ptr<ServerV2State>& v2) noexcept;

// 内部接缝（仅 infra TU）：server 台账全部已登记 link 的未结算写责任
// 计数之和——含 dead link 的残留（writer 手中在写项不属排队丢弃口径）。
std::uint64_t ServerV2WritesOutstanding(
    const std::shared_ptr<ServerV2State>& v2) noexcept;

// 测试接缝：上一条的 Server 句柄入口（v1 装配/未启动返回 0）。
// 实现在 Server.cc（member 访问 m_v2）转调上一接缝。
std::uint64_t ServerV2WritesOutstandingForTest(const Server& server) noexcept;

// 测试接缝（私有）：SessionCore::WriterLoop 取出 OutItem 后、conn.Write
// 前调用；传空 function 清除。用于确定性钉住「已排队/写入中未终态」交错。
void SessionSetWriterGateForTest(std::function<void()> gate);

// 测试接缝（私有）：出站 EstablishOutbound 拨号成功后、入站 ServeInbound
// 开始握手 I/O 前调用；传空 function 清除。用于确定性钉住「建立中」交错
// （额度已占/link 未发布）。
void SessionSetDialGateForTest(std::function<void()> gate);

namespace detail {
// 测试接缝（私有）：v2 server 台账快照——已发布 link 数与建立中握手
// conn 数（v1 装配/未启动返回 {0,0}）。
struct ServerV2Ledger {
    std::uint64_t links{0};
    std::uint64_t handshakes{0};
};
// 测试接缝：Server 句柄入口（实现在 Server.cc，member 访问 m_v2，
// 转调 Session.cc 的 rpc::ServerV2LinkLedger）。
ServerV2Ledger ServerV2LinkLedgerForTest(const Server& server) noexcept;
} // namespace detail

// 内部接缝（仅 infra TU）：ServerV2State 直参版本；返回类型在
// rpc::detail 下，用限定名声明。
detail::ServerV2Ledger ServerV2LinkLedger(
    const std::shared_ptr<ServerV2State>& v2) noexcept;

} // namespace bbt::infra::rpc

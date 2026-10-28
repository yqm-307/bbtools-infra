#pragma once

// RPC core 出站客户端（bbt::infra::rpc，消费新 bbt::infra::http::Client）。
//
// 职责边界：Client 只做 envelope wire 编解码、endpoint 边界检查、请求/回复
// 关联校验与剩余预算换算；网络、连接池、阶段超时与错误分类全部归公开
// http::Client owner。protobuf envelope、POST /rpc、Content-Type 与
// remaining_budget_ms 语义沿用既有 wire profile（bbt/infra/rpc/RpcWire.hpp）。
//
// 生命周期沿用 http::Client 语义：Close() 立即逻辑封口（幂等，拒绝新 Call、
// 在飞请求按已持有借用完成或失败）；IsClosed() 查询当前状态。析构=Close。
// 不新增第二种关闭入口，也不承诺物理排空（退役连接由 Engine owner 收口）。
//
// 错误面（透传优先，RPC 不重新覆盖物理错误）：
//   - Closed：本 Client 已 Close 后调用；
//   - InvalidArgument：address transport/endpoint 非法、envelope 不可编码；
//   - TimedOut：调用方 deadline 已过期（确定失败，不发请求）；
//   - Unavailable：对端返回非 200（backend_category=http、backend_code=状态码）；
//   - ProtocolError：回复关联字段不匹配或 wire 解析失败；
//   - 传输/取消/未知结果原样透传 http::Client 错误（含 OutcomeUnknown 与
//     request_phase 事实），RPC 不把提交后错误改写成其他终态。

#include <memory>

#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/http/Client.hpp>

namespace bbt::infra::rpc {

class Client {
public:
    // 只装配：校验并持有 transport，不联网。Engine/执行底座由应用装配并
    // 最终收口（与 http::Client 同责）。
    static result<std::shared_ptr<Client>> Create(
        std::shared_ptr<http::Client> transport);
    ~Client() noexcept;
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    // 仅在受管协程内调用（bbt_co_await 消费）；期限由 options.deadline 表达，
    // 必须是未来时刻（epoch/已过期确定 TimedOut）。请求经 wire profile 编码后
    // 以 POST {endpoint}/rpc 发出；共享 transport 的默认 headers/params 参与合
    // 并，但线上目标始终是精确 /rpc、Content-Type 恰好一个 application/x-protobuf
    // （其余默认配置保留）。回复按五字段关联校验（request_id/service/method/
    // request_schema/response_schema），不一致报 ProtocolError。
    bbt_co_async(result<RpcEnvelope>) Call(const RpcAddress& address,
                                           RpcEnvelope request,
                                           const CallOptions& options);

    // 立即逻辑封口：转发给 http::Client（幂等、不可重开）。
    void Close() noexcept;
    bool IsClosed() const noexcept;

private:
    explicit Client(std::shared_ptr<http::Client> transport) noexcept;
    std::shared_ptr<http::Client> m_transport;
};

} // namespace bbt::infra::rpc

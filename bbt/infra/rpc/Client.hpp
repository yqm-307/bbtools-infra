#pragma once

// RPC core 出站客户端（bbt::infra::rpc，只依赖 IRpcTransport seam，不依赖
// 任何物理栈；HTTP adapter 见 bbt/infra/rpc/HttpTransport.hpp）。
//
// 职责边界：Client 只做 envelope wire 编解码、address 边界检查、请求/回复
// 五字段关联校验与剩余预算换算；网络、连接池、阶段超时与错误分类全部归
// IRpcTransport 实现（architecture §5.2/§5.3）。wire profile 语义沿用
// bbt/infra/rpc/RpcWire.hpp。
//
// 生命周期（Open → Closing → Closed，无重开）：Close() 立即逻辑封口（幂等，
// 委托给 adapter；未提交调用 Closed，已提交无回复 OutcomeUnknown）；
// IsClosed() 查询当前状态。析构=Close。不新增第二种关闭入口，也不承诺物理
// 排空（退役连接由 adapter/Engine owner 收口）。
//
// 错误面（透传优先，RPC 不重新覆盖物理错误）：
//   - Closed：本 Client/adapter 已 Close 后调用；
//   - InvalidArgument：address transport/endpoint 非法、envelope 不可编码；
//   - TimedOut：调用方 deadline 已过期（确定失败，不发请求）；
//   - ProtocolError：回复五字段关联不匹配或 wire 解析失败；
//   - 传输/取消/未知结果原样透传 adapter 错误（含 OutcomeUnknown 与
//     request_phase 事实），RPC 不把提交后错误改写成其他终态；
//   - HTTP adapter 另将非 200 映射为 Unavailable（backend_category=http）。

#include <memory>

#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/rpc/Transport.hpp>

// 源兼容工厂（HTTP adapter 实现，见 HttpTransport.hpp）：完整声明需要
// bbt::infra::http 类型，这里仅前向声明；定义与实现在 HTTP adapter TU。
namespace bbt::infra::http { class Client; }

namespace bbt::infra::rpc {

class Client {
public:
    // 只装配：校验并持有 transport，不联网。Engine/执行底座由应用装配并
    // 最终收口（与 adapter owner 同责）。
    static result<std::shared_ptr<Client>> Create(
        std::shared_ptr<IRpcTransport> transport);
    // 源兼容转接：包装 HttpRpcTransport；下一迁移窗口结束前保留
    // （architecture §10）。定义在 HTTP adapter（bbt/infra/rpc/HttpTransport.cc）。
    static result<std::shared_ptr<Client>> Create(
        std::shared_ptr<http::Client> http_client);
    ~Client() noexcept;
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    // 仅在受管协程内调用（bbt_co_await 消费）；期限由 options.deadline 表达，
    // 必须是未来时刻（epoch/已过期确定 TimedOut）。地址 transport 只接受
    // "http"/"tcp"，endpoint 合法性归 adapter；请求经 wire profile 编码后交
    // adapter Exchange。回复按五字段关联校验（request_id/service/method/
    // request_schema/response_schema），不一致报 ProtocolError。
    bbt_co_async(result<RpcEnvelope>) Call(const RpcAddress& address,
                                           RpcEnvelope request,
                                           const CallOptions& options);

    // 立即逻辑封口：转发给 adapter（幂等、不可重开）。
    void Close() noexcept;
    bool IsClosed() const noexcept;

private:
    explicit Client(std::shared_ptr<IRpcTransport> transport) noexcept;
    std::shared_ptr<IRpcTransport> m_transport;
};

} // namespace bbt::infra::rpc

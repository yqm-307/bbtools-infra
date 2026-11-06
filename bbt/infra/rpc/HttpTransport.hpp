#pragma once

/**
 * @file HttpTransport.hpp
 * @brief RPC HTTP adapter（bbt::infra::rpc）：IRpcTransport/IRpcInboundTransport
 *        在 bbt::infra::http 栈上的实现 + HTTP body 绑定助手。
 *
 * 职责边界：
 *   * HttpRpcTransport 包装既有 http::Client 并经 shared_ptr 持有其池；
 *     强制 POST /rpc 与恰好一个 application/x-protobuf，保留 client 默认
 *     授权头；非 200 → Unavailable（backend_category="http"、
 *     backend_code=状态码）；Content-Type 缺失/不符 → 协议层错误；成功只
 *     返回 body 字节。传输/取消/未知结果错误（含 request_phase 事实）原样
 *     透传 http::Client，不改写终态。
 *   * HttpRpcInboundTransport 持有一个 http::Server，经既有 HTTP request
 *     owner 强制精确 POST /rpc/Content-Type/body 上限，转成 WireRequest 交
 *     WireInboundHandler，并写回编码后响应。不拥有、不关闭 Engine；其
 *     Drain/WaitStopped 继承 http::Server 的 native 宿主域限制与物理回收
 *     语义。
 *   * 本头还承载 HTTP body 绑定助手：MakeRpcWireHttpResponse /
 *     ParseRpcWireHttpRequest / ParseRpcWireHttpResponse（放在本头以免 core
 *     公共头依赖 HTTP 类型）。
 *
 * 生命周期全部继承所包装 http::Client/http::Server 语义（Close 幂等不可
 * 重开；InboundTransport::IsClosed 观测 Close 已调用）。adapter 不关闭共享
 * Engine，不承诺物理排空。
 */

#include <memory>
#include <string>

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/http/Client.hpp>
#include <bbt/infra/http/Server.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/rpc/RpcWire.hpp>
#include <bbt/infra/rpc/Transport.hpp>

namespace bbt::infra::rpc {

// --- HTTP body 绑定（bbt::infra::http 值类型） -------------------------------

/**
 * @brief 服务端 → 客户端：把 envelope 编码进 200 响应。
 *
 * Content-Type: application/x-protobuf、body=wire bytes。仅填标准传输字段；
 * 不设置任何 x-bbt-* 头。
 * @param env 待发送的 wire envelope
 * @return 可直接交给 http handler 的响应，或编码错误
 */
result<http::Response> MakeRpcWireHttpResponse(const RpcWireEnvelope& env);

/**
 * @brief 服务端入站解析。
 *
 * 仅接受正式 POST + 精确 target /rpc（origin-form，无 query）
 * + Content-Type: application/x-protobuf + 非空 body；违规返回
 * InvalidArgument/ProtocolError；不读取任何 x-bbt-* 字段。
 * @param req 已解析的入站 HTTP 请求
 * @return 解出的 envelope 或协议错误
 */
result<RpcWireEnvelope> ParseRpcWireHttpRequest(const http::IncomingRequest& req);

/**
 * @brief 客户端回包解析。
 *
 * 仅接受 HTTP 200 + Content-Type: application/x-protobuf + 非空 body；违规返回
 * InvalidArgument/ProtocolError。
 * @param resp 收到的 HTTP 响应
 * @return 解出的 envelope 或协议错误
 */
result<RpcWireEnvelope> ParseRpcWireHttpResponse(const http::Response& resp);

// --- IRpcTransport 实现 -------------------------------------------------------

/**
 * @brief IRpcTransport 的 HTTP 实现：把 wire envelope 映射到 POST /rpc。
 */
class HttpRpcTransport final : public IRpcTransport {
public:
    /// 只装配：校验并持有 http::Client（含其连接池）。Engine 由应用装配收口。
    /// @param http_client 被包装的客户端（共享其连接池）
    /// @return 装配失败时为错误
    static result<std::shared_ptr<HttpRpcTransport>> Create(std::shared_ptr<http::Client> http_client);
    ~HttpRpcTransport() noexcept override;
    HttpRpcTransport(const HttpRpcTransport&) = delete;
    HttpRpcTransport& operator=(const HttpRpcTransport&) = delete;

    /// 交换一次请求：固定 POST {endpoint}/rpc、Content-Type 恰好一个
    /// application/x-protobuf（其余共享默认头保留）；回复收敛到 200 + 精确
    /// Content-Type，只交付 body 字节，不做 envelope 层解码（归 rpc::Client）。
    ///
    /// @param address 目标地址与端点
    /// @param request 待发送的 wire 请求
    /// @param options 本次调用的期限与流量选项
    bbt_co_async(result<WireReply>)
        Exchange(const RpcAddress& address, WireRequest request, const CallOptions& options) override;
    /// 立即逻辑封口：转发 http::Client（幂等、不可重开）。
    void Close() noexcept override;
    /// 关闭原因版本：原因发布到底座 HTTP 池。
    /// @param reason 首个关闭原因
    void Close(Error reason) noexcept override;
    /// 封口 + 等底层池排空（native 宿主域；协程域由底座返回 InvalidContext）。
    /// @param deadline 等待排空的期限
    /// @return 排空完成、已关闭或超时
    result<void> Shutdown(bbt::coroutine::Deadline deadline) override;
    /// 是否已封口。
    bool IsClosed() const noexcept override;

private:
    explicit HttpRpcTransport(std::shared_ptr<http::Client> http_client) noexcept;
    std::shared_ptr<http::Client> m_http; // owning client pool
};

// --- IRpcInboundTransport 实现 -------------------------------------------------

/**
 * @brief IRpcInboundTransport 的 HTTP 实现：持有 http::Server 并把请求转成
 *        WireRequest。
 */
class HttpRpcInboundTransport final : public IRpcInboundTransport {
public:
    /// 只装配：在给定 Engine 上创建底层 http::Server（Engine 必须 Idle，只配置
    /// handoff，不 Run/监听/建线程）。Run/Start/收口责任与 http::Server 相同。
    /// @param engine 应用独占并推进的 net::Engine
    /// @param options 底层 http::Server 的装配选项
    /// @return 装配失败时为错误
    static result<std::shared_ptr<HttpRpcInboundTransport>> Create(std::shared_ptr<net::Engine> engine,
                                                                   http::ServerOptions options = {});
    ~HttpRpcInboundTransport() noexcept override;
    HttpRpcInboundTransport(const HttpRpcInboundTransport&) = delete;
    HttpRpcInboundTransport& operator=(const HttpRpcInboundTransport&) = delete;

    /// Start 一次性：登记 WireInboundHandler。入站请求经 HTTP 形状校验与
    /// body 上限后交付 handler，回复按 wire bytes 写回。
    /// @param handler 入站 wire 处理器
    /// @return 重复 Start 或登记失败时为错误
    result<void> Start(WireInboundHandler handler) override;
    /// 停止接纳新请求；已授权请求继续。
    void StopAccepting() noexcept override;
    /// 排空已授权请求（native 宿主域）。
    /// @param deadline 等待排空的期限
    /// @return 排空完成、已关闭或超时
    result<void> Drain(bbt::coroutine::Deadline deadline) override;
    /// 立即逻辑封口（转发底层 http::Server，幂等）。
    void Close() noexcept override;
    /// 关闭原因版本：原因发布到底座 http::Server。
    /// @param reason 首个关闭原因
    void Close(Error reason) noexcept override;
    /// 单入口封口 + 排空，语义即 http::Server::Shutdown（native 宿主域）。
    /// @param deadline 等待已授权处理的期限
    /// @return 排空完成、已关闭或相应错误
    result<void> Shutdown(bbt::coroutine::Deadline deadline) override;
    /// 等待 listener/回调/连接全部真实回收（native 宿主域）。
    /// @param deadline 等待回收的期限
    /// @return 回收完成、超时或已关闭
    result<void> WaitStopped(bbt::coroutine::Deadline deadline) override;
    /// 是否已封口。
    bool IsClosed() const noexcept override;

private:
    struct State;
    explicit HttpRpcInboundTransport(std::shared_ptr<State> state) noexcept;
    std::shared_ptr<State> m_state; // gate + owned http::Server
};

} // namespace bbt::infra::rpc

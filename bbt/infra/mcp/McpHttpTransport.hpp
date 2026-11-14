#pragma once

/**
 * @file McpHttpTransport.hpp
 * @brief MCP Streamable HTTP transport 公共接入面。
 *
 * 契约来源：官方 MCP 规范 2026-07-28 Streamable HTTP + 官方 Python SDK v2.3.0
 * （streamable_http.py / shared/inbound.py）。实现形态：
 *   - 精确路径 POST /mcp（POST-only；GET/DELETE/其余方法 → 405，其余路径 → 404）；
 *     Accept 必须同时含 application/json 与 text/event-stream → 否则 406；
 *     Origin 显式白名单（缺 Origin 放行，给出 Origin 必须命中）→ 403；
 *     allowed_hosts 非空、Host 头不匹配 → 421；
 *     请求体超 max_body_bytes → 413；
 *   - modern 路由头校验（每 POST）：MCP-Protocol-Version 必须等于
 *     params._meta.io.modelcontextprotocol/protocolVersion；Mcp-Method 必须等于
 *     帧 method；tools/call、prompts/get、resources/read 的 Mcp-Name 解码后必须
 *     等于 params.name/params.uri（非 ASCII 用 =?base64?…?= sentinel 逐字解码，
 *     与 SDK shared/inbound.py 同规则）；失配 → 400 + JSON-RPC -32020 body
 *     （Content-Type: application/json，可关联 id 时原值回包）；
 *   - modern 未知方法 → 404 + -32601 body；不支持的 modern 协议版本 → 400 +
 *     -32022 body（SDK 对 4xx+JSON-RPC body 原样上抛；schema.ts「MUST be 400
 *     Bad Request」）。两状态均按响应体结构化 error.code 判定，非子串匹配；
 *     合法通知 POST（含 notifications/cancelled）→ 202 空体；普通请求 → 200
 *     application/json；subscriptions/listen → 200 text/event-stream（SSE，
 *     X-Accel-Buffering: no，「: 」心跳，事件逐帧 data: …\n\n；无 id 字段、无
 *     Last-Event-ID 重放、绝不 mint/echo Mcp-Session-Id）；
 *   - legacy（无 modern _meta 信封）：头门禁采用 legacy revision 采纳会话门禁
 *     （AdoptLegacyHeaderGate：MCP-Protocol-Version == 2025-11-25 的
 *     POST 为每连接会话回填 negotiated version，不回填 capabilities、不伪造
 *     initialize；2025-06-18 不在服务矩阵——规范对照未核对，按
 *     未服务 revision 拒绝）。无 session profile：与 modern 同一条 POST 管线，
 *     无 GET 流、无会话表、无重放。
 *
 * 生命周期：Create 要求 server 已 Start、engine 为 Idle（与 http::Server 同一
 * 规则）；Start 要求 engine Running 且有监听。transport 持 server 强引用，core
 * 对 transport 只经会话 sink 弱引用（无环）。`Close(CloseOptions)`、`Close(Immediate)`
 * 与 `Close(wait_release=true)` 与 http::Server 同一契约，直接委托内部 server：逻辑封口/在途
 * 请求排空由 server 承担，本层不重复实现等待；Close 不关 Engine、幂等、取消每个
 * 活动流源一次（http::Server 的 session 收口完成）。
 * 断连驱动订阅结束（HTTP 帧取消不支持，连接终止经 EndAllSubscriptions 结束本会话
 * 订阅）。
 *
 * 有界流：每 SSE 流一个 McpEventBody（私有类，实现 http::ResponseBody 拉取源），
 * 内部事件队列按 max_stream_events 有界：sink 非阻塞入队，满即丢弃 + stderr
 * 诊断；流期间到达的发布事件只有本地队列接纳语义，不承诺 peer 交付。
 * max_stream_lifetime/stream_write_timeout/max_chunk_bytes 透传
 * http::StreamingOptions（http 层负责 chunk 上限与逐写/流期限）。
 *
 * 当前不提供：Mcp-Session-Id、GET 事件流、Last-Event-ID 重放、TLS、DNS、
 * 2024-11-05 HTTP+SSE、Mcp-Param-* 头映射（仅路由头三件套校验）、2025-06-18
 * 兼容面（规范对照未核对，故不接受）。非 loopback 监听默认被拒；确需对外暴露时
 * 由部署方显式 opt-in（McpHttpOptions::allow_non_loopback）并自带外层认证/反代。
 */
#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <bbt/infra/CloseOptions.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/mcp/McpServer.hpp>
#include <bbt/infra/net/Engine.hpp>

namespace bbt::infra {
namespace http {
class Server;
}

// clang-format off
}
// clang-format on

namespace bbt::infra::mcp {

/**
 * @brief Streamable HTTP transport 的安全门禁、请求体上限与 SSE 流配额。
 */
struct McpHttpOptions {

    /**
     * Host 白名单（必须非空）：匹配配置的监听（如 {"127.0.0.1:PORT","localhost:PORT"}，
     * 匹配 host[:port]、大小写不敏感、剥离 [v6] 括号与尾随点）；空 → Create InvalidArgument。
     * 同一门禁里的 Origin 白名单是显式浏览器 allowlist：空表表示「允许无 Origin，
     * 拒绝任何给出的 Origin」；命中按精确值比较（大小写敏感）。
     */
    std::vector<std::string> allowed_hosts;

    /**
     * 浏览器 Origin allowlist（可空）；比较规则同上（精确值、大小写敏感）。
     */
    std::vector<std::string> allowed_origins;

    /**
     * 请求体上限（>0，≤ http 层 max_body_bytes；Create 同步收紧 http limits），默认 1 MiB。
     */
    std::size_t max_body_bytes = 1U << 20;

    /**
     * SSE 单块字节上限；流配额（max_stream_lifetime / stream_write_timeout）透传
     * http::StreamingOptions 前先经本包校验为正且 ≤24h。
     */
    std::size_t max_chunk_bytes = 16U << 10;

    /**
     * SSE 流总期限。
     */
    std::chrono::milliseconds max_stream_lifetime{60000};

    /**
     * 每次 wire 写期限。
     */
    std::chrono::milliseconds stream_write_timeout{5000};

    /**
     * 每 SSE 流事件队列上限（条数，>0）；满即丢帧 + 诊断（有界，不阻塞发布者）。
     */
    std::size_t max_stream_events = 64;

    /**
     * SSE 「: 」注释心跳间隔（>0；keepalive 不重置流期限）。
     */
    std::chrono::milliseconds keepalive_interval{15000};

    /**
     * 非 loopback 监听 opt-in（默认 false）。SHTTP 规范 SHOULD 本地运行时只绑 localhost；
     * 默认 Start 对任何非 loopback 监听地址（0.0.0.0、::、公网 IP）直接失败
     * （ErrorCode::UnsupportedRoute，domain_code mcp.http.non_loopback），只有显式置 true
     * 才放行——该开关是「我知道在做什么」的声明，不是 Host/Origin 白名单（白名单防的是
     * Host 头伪造，挡不住 bind 暴露）。
     */
    bool allow_non_loopback = false;
};

/**
 * @brief MCP Streamable HTTP transport：POST /mcp 的 modern + legacy 路由。
 *
 * 持 server 强引用，core 对 transport 只经会话 sink 弱引用（无环）；收口与
 * http::Server 同一契约，不关共享 Engine。
 */
class McpHttpTransport {
public:
    /**
     * @brief 校验 options（白名单/字节/期限全为正且有限）并创建内部 streaming http::Server。
     *
     * engine 必须 Idle；http limits.max_body_bytes 收紧到 options.max_body_bytes。失败不留
     * 半装配 transport。
     * @param server 已 Start 的服务实例
     * @param engine 应用独占并推进的 net::Engine
     * @param options 安全门禁、请求体上限与 SSE 流配额
     * @return transport 或装配失败原因
     */
    static result<std::shared_ptr<McpHttpTransport>>
    Create(std::shared_ptr<McpServer> server, std::shared_ptr<net::Engine> engine, McpHttpOptions options);
    ~McpHttpTransport();

    /**
     * @brief 开始接纳（一次性；要求 engine Running 且有监听地址，与 http::Server::Start 同契约）。
     *
     * 默认要求所有监听地址为 loopback（127.0.0.0/8、::1、Unix）；任一非 loopback 监听地址
     * 且未显式 opt-in（McpHttpOptions::allow_non_loopback）→ ErrorCode::UnsupportedRoute
     * （domain_code mcp.http.non_loopback）。
     * @return 状态不满足或非 loopback 未 opt-in 时为错误
     */
    result<void> Start();

    /**
     * @brief 统一逻辑关闭入口，语义与 http::Server 相同（本层委托，不另造等待）。
     *
     * Close 逻辑封口并按模式等待在途请求：Graceful 到写终态，Immediate 直接失败在途并取消
     * 每流源一次；wait_release=true 在同一 Close 内等物理回收到同一期限（listener 停止、
     * callback/源销毁、连接 Released）。Close(Immediate) 任意域非阻塞封口。均不关共享 Engine。
     * @param options 模式、首因与优雅关闭的有限绝对期限
     */
    result<void> Close(const CloseOptions& options = {});

    /**
     * @brief 关闭原因；任意模式首次发布后固定。
     */
    std::optional<Error> CloseReason() const noexcept;

    /**
     * @brief 是否已封口。
     */
    bool IsClosed() const noexcept;

private:
    void _BeginClose(Error reason = MakeError(ErrorCode::Closed, "mcp.http: closed by owner")) noexcept;
    result<void> _WaitReleased(bbt::coroutine::Deadline deadline);

    struct Impl;
    explicit McpHttpTransport(std::shared_ptr<Impl> impl) noexcept;
    std::shared_ptr<Impl> m_impl;
};

// clang-format off
}
// clang-format on

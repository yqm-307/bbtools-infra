#pragma once
// MCP Streamable HTTP transport 公共接入面（3-2 切片）。
//
// 契约来源：WFER mcp-server-foundation（specs R9/S18-S20、R10/S21、design
// 「核心与transport」「执行域和关闭」）。单 POST MCP endpoint：普通请求 JSON，
// 订阅（subscriptions/listen）用真 event-stream；现代 GET/DELETE 405；不 mint /
// echo session（无 Mcp-Session-Id）。HTTP 头-body 一致校验与自定义 header 注解
// 编码规则（Base64 sentinel）见 docs/decisions/0010 决策 11-16。
//
// 执行域（design）：fd/配额/Close 组合公开 TransportRuntime/CoTCP（listener 与
// accepted TCP 全部受管：max_connections 含 listener、op 配额允许并发
// reader/writer/accept）；framing 复用 Boost.Beast（实现私有，公共头不泄漏任何
// Beast/Boost 类型）；SSE 与请求级取消由 MCP 自有。不新增 I/O 线程、不建独立
// io_context（共享协程运行时；accept/读写均为受管协程 op）。
//
// 生命周期：Listen 要求 server 已 Start 且共享协程运行时已启动；transport 持
// core 强引用，core 对 transport 无反向引用。Close() 同步物理收口自有资源
// （listener + 全部 accepted 连接的 fd、输出队列、协程有界汇合、runtime 账本），
// 任意线程幂等/并发 Close 均等首次真实落定；不触碰 server core 生命周期
// （core.Close 由业务决定）。LocalAddress() 返回实际绑定的动态端口。
//
// 半关契约（RFC 9112 §9.6；specs S20）：完整 POST 后客户端 SHUT_WR 不界定请求
// 取消——仍返回 JSON/SSE；正文未完整时读 EOF 是明确截断失败；SSE 流只以
// 「写失败/流结束响应/Close」收口，不以读 EOF 取消（无法立即确认的全关用
// 心跳写探测 + 写期限有界收口，不承诺 TCP 断连瞬时可知）。
//
// 本切片未实现、故不在此宣告：跨连接的客户端 cancel 通知路由到在途订阅
// （单 request/connection 下订阅与 cancel 在不同连接/会话，属 3-3 request
// 栈外状态范围）、keep-alive、TLS、Last-Event-ID 重放。

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/mcp/McpServer.hpp>

namespace bbt::infra::mcp {

struct HttpOptions {
    // 监听地址。默认且只允许 loopback（127.0.0.1/::1/localhost）；其余一律
    // InvalidArgument——未支持 TLS 的 transport 不做默认公网监听（design）。
    std::string listen_host = "127.0.0.1";
    std::uint16_t listen_port = 0;  // 0 = 动态端口（LocalAddress 取实际值）
    // 单 POST MCP endpoint 路径（精确匹配；带 query 的目标按 404 处理）。
    std::string endpoint_path = "/mcp";
    // Origin 白名单（精确串匹配）。请求携带 Origin 时：重复/语法非法/不在
    // 白名单 → 403。默认只放行本机来源。
    std::vector<std::string> allowed_origins = {"http://127.0.0.1", "http://localhost"};
    // 受管配额（组合 TransportRuntime）：连接容量含 listener 本身；在途 op 配额
    // 须允许并发 accept/reader/writer。
    std::size_t max_connections = 64;
    std::size_t max_inflight = 256;
    int backlog = 64;
    // 单请求 framing 上限（Beast header/body limit）与读期限。
    std::size_t max_header_bytes = 16U << 10;
    std::size_t max_body_bytes = 4U << 20;
    std::chrono::milliseconds request_read_timeout{10000};
    // SSE：心跳注释间隔（不作为 JSON 的探测写，兼作断连有界探测）。
    std::chrono::milliseconds sse_heartbeat_interval{15000};
    // 有界输出：每连接挂起队列字节上限；背压下出队等待上限（超限丢弃并计数）。
    std::size_t max_pending_bytes = 1U << 20;
    std::chrono::milliseconds write_deadline{5000};
    // Close 的有界协程汇合上限（超时放弃等待并继续物理释放，与仓内 Close 排空口径一致）。
    std::chrono::milliseconds close_drain_timeout{2000};
};

// Close 真实落定当刻的资源状态快照（S21「返回当刻原生资源」在本切片的 HTTP 形态）。
// 快照在收口末尾（listener/连接 fd 已物理关闭、协程已汇合或确认超时、runtime
// 账本已归还之后）一次性拷贝；LastCloseEvidence() 返回该当刻值拷贝。
struct HttpCloseEvidence {
    bool listener_closed = false;         // listener 物理关闭（之后 connect 不可达）
    bool runtime_closed = false;          // TransportRuntime owner 已收口
    bool accept_loop_exited = false;      // accept 协程已退出
    bool connection_loops_exited = false; // 全部连接协程有界汇合（超时为 false）
    bool output_sealed = false;           // sink 出队/入队路径已封
    std::size_t connections_closed = 0;   // 物理关闭的 accepted 连接数（含中断在途）
    std::size_t live_connections = 0;     // 收口当刻仍未退出的连接协程数
    std::size_t dropped_frames = 0;       // 满队/超限丢弃的输出帧
    std::size_t abandoned_frames = 0;     // 封口放弃的未发帧
    bool truncated_request_seen = false;  // 曾见「正文未完整即 EOF」的截断请求
    bool write_error_seen = false;        // 曾见响应/流写失败（请求取消路径）
};

class McpHttpTransport {
public:
    // 要求 server 已 Start、共享协程运行时已启动、选项合法（loopback 监听、
    // 各上限/期限 >0）；失败不留半启动 transport。
    static result<std::shared_ptr<McpHttpTransport>> Listen(
        std::shared_ptr<McpServer> server, HttpOptions options = {});
    ~McpHttpTransport();

    // 同步物理收口：停 accept → 封全部连接输出/唤醒等待 → listener 与全部
    // accepted CoTCP 物理 Close → 协程有界汇合 → runtime 收口 → 当刻快照。
    // 并发/重复 Close 均等首次真实落定。
    void Close() noexcept;
    bool IsClosed() const noexcept;

    // 实际监听地址（动态端口）。Close 后返回最后一次有效值。
    SocketAddress LocalAddress() const noexcept;

    // 线程契约：任意线程可调用；返回收口当刻快照的值拷贝。Close 尚未落定时
    // 返回默认构造。
    HttpCloseEvidence LastCloseEvidence() const noexcept;

private:
    struct Impl;
    std::shared_ptr<Impl> m_impl;
    explicit McpHttpTransport(std::shared_ptr<Impl> impl) noexcept;
};

} // namespace bbt::infra::mcp

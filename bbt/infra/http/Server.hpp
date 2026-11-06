#pragma once

/**
 * @file Server.hpp
 * @brief 原生协程 HTTP 服务端（bbt::infra::http）：装配、请求处理与流式响应。
 *
 * Server 只接管 Engine 的 accept 消费：Create 时 Engine 必须 Idle，Start 时
 * 必须 Running 且已有监听；对象本身不 Run、不建线程、不建 Engine，最终由
 * 应用关闭。同一 Engine 不得另设 raw reader 或第二个 CoAccept 消费者。
 *
 * 请求处理只有一个入口（ServerHandler 或 StreamingServerHandler），回复可以
 * 是完整 Response，也可以是需要拉取的 ResponseBody（chunked）。
 */

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <variant>

#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/infra/http/Client.hpp>

namespace bbt::infra::http {

/// 已解析的入站请求。
struct IncomingRequest {
    std::string method; ///< 请求方法
    std::string target; ///< owning origin-form，非代理/隧道目标
    Headers headers;    ///< 请求头，保持线上顺序
    std::string body;   ///< 请求体字节
};

/// 一次性回复的处理函数：返回完整 Response。
using ServerHandler = std::function<result<Response>(IncomingCallContext, IncomingRequest)>;

// ---- 服务端响应流（仅 server 侧；Client::Send 不变）----
//
// ResponseBody 是拉取源而非回调写器：Start 在响应头提交前选择 buffered 或
// chunked；chunked 开始后 Next 逐个交还自拥 chunk（nullopt=EOF）。HTTP
// session 经 ICoConnection::Write 写 wire chunk，调用方不持 socket/fd/序列化器。

/// 响应流首帧：确定缓冲或分块，并给出状态码与头。
struct StreamStart {
    /// 响应体形态：整块缓冲或分块。
    enum class Mode { Buffered, Chunked } mode; ///< Buffered=整块一次交付；Chunked=逐块拉取
    int status_code;                            ///< HTTP 状态码
    Headers headers;                            ///< 响应头
    std::string body;                           ///< Mode::Buffered 有效（含允许的空体）
    std::string first_chunk;                    ///< Mode::Chunked 的首块，可为空（只提交头）
};

/**
 * @brief 流式响应的拉取源。
 *
 * Start/Next 必须在受管协程内挂起消费；Cancel 幂等、可在任意域调用，只表达
 * 「session 不再拉取」，由实现保证不再交付后续结果。
 */
class ResponseBody {
public:
    virtual ~ResponseBody() = default;
    /// 拉取首帧：必须在给定 WaitOptions 的 deadline 内产出；err 即未提交头、走
    /// 普通错误响应。
    virtual bbt_co_async(result<StreamStart>) Start(const bbt::coroutine::WaitOptions&) = 0;
    /// Chunked 模式下逐块拉取；nullopt 为正常 EOF（写 0-chunk 收尾）。
    virtual bbt_co_async(result<std::optional<std::string>>) Next(const bbt::coroutine::WaitOptions&) = 0;
    /// 停止拉取；幂等，可在任意域调用。
    virtual void Cancel() noexcept = 0;
};

/// 流式回复的限额。
struct StreamingOptions {
    std::size_t max_chunk_bytes{16 * 1024};               ///< Next 单块上界（0 拒绝）
    std::chrono::milliseconds max_stream_lifetime{60000}; ///< 头提交后流总期限
    std::chrono::milliseconds stream_write_timeout{5000}; ///< 每次 wire 写期限
};

/// 流式处理函数的回复：完整响应或待拉取的响应体。
using ServerReply = std::variant<Response, std::shared_ptr<ResponseBody>>;
/// 可返回流式回复的处理函数。
using StreamingServerHandler = std::function<result<ServerReply>(IncomingCallContext, IncomingRequest)>;

/// 服务端装配选项。
struct ServerOptions {
    NetworkLimits limits{64, 64, 64 * 1024, 8 * 1024 * 1024,
                         std::chrono::milliseconds{5000}}; ///< 连接/在飞/头/体上限与入站超时
    std::size_t handoff_capacity{16};                      ///< accept 交给 session 的交接队列容量
    std::size_t max_target_bytes{8192};                    ///< 请求行 target 字节上限
    std::chrono::milliseconds write_timeout{5000};         ///< 单次 wire 写期限
};

/// 服务端观测快照。
struct ServerStats {
    std::size_t accepted{0}; ///< 已接受的连接累计数
    std::size_t requests{0}; ///< 已授权 handler 的累计数
    std::size_t sessions{0}; ///< 活 session + 未物理回收退役连接，共占额度
    std::size_t retiring{0}; ///< 退役中连接
    std::size_t inflight{0}; ///< 授权起至 handler 返回且回复写终态/明确放弃
    std::size_t tasks{0};    ///< accept/session callback 寿命，非 Engine driver
};

class ServerTestAccess;

/**
 * @brief 原生协程 HTTP 服务端，独占 Engine 的 accept 消费。
 *
 * 生命周期：Create（Engine Idle）→ Start（Engine Running 且有监听）→
 * StopAccepting/Drain/Shutdown 逐步收口 → Close。封口单向，不可重开。
 * 封口动作不等待 handler、不关 Engine，物理回收由 Engine owner 完成。
 */
class Server {
public:
    /// 装配服务端：Engine 必须 Idle；只配置 handoff，不 Run/监听/建线程。Engine
    /// 由应用独占接纳消费（不得另设 raw reader 或第二个 CoAccept 消费者），
    /// 最终由应用关闭。
    /// @param engine 应用独占并推进的 net::Engine，Server 只接管其 accept 消费
    /// @param handler 一次性回复的处理函数
    /// @param options 会话/头/体上限与交接队列配置
    /// @return 装配失败时为错误
    static result<std::shared_ptr<Server>> Create(std::shared_ptr<net::Engine> engine, ServerHandler handler,
                                                  ServerOptions options = {});
    /// 流式工厂：handler 可返回完整 Response 或 ResponseBody 拉取源。
    ///
    /// 独立工厂签名（不与 Create 重载）。StreamingOptions 校验：max_chunk_bytes>0、
    /// max_stream_lifetime/stream_write_timeout 有限正值且 <=24h。
    /// @param engine 应用独占并推进的 net::Engine
    /// @param handler 可返回流式回复的处理函数
    /// @param options 会话/头/体上限与交接队列配置
    /// @param streaming 流式回复的限额
    /// @return 装配失败或流式限额非法时为错误
    static result<std::shared_ptr<Server>> CreateStreaming(std::shared_ptr<net::Engine> engine,
                                                           StreamingServerHandler handler, ServerOptions options = {},
                                                           StreamingOptions streaming = {});
    ~Server() noexcept;
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    /// 开始接纳：Engine Running 且有监听后调用。一次性；登记失败封口并显式报错。
    /// @return 登记失败或状态不满足时为错误
    result<void> Start();
    /// 同门封新 handler；已授权 handler 及回复可继续。idle/半包会话被关闭。
    void StopAccepting() noexcept;
    /// 单入口封口 + 排空：持久拒绝新任务，等已授权 handler 及其回复写终态。
    ///
    /// native 宿主专用；协程/Processer 域返回 InvalidContext。有限期限超时返回
    /// TimedOut 且保持封口；等待期间被 Close 打断返回其关闭原因；先排空后
    /// Close 返回 ok（成功不被追改）。
    /// @param deadline 等待已授权处理的期限
    /// @return 排空完成、已关闭或相应错误
    result<void> Shutdown(bbt::coroutine::Deadline deadline);
    /// native 宿主专用：必须先 StopAccepting；协程/Processer 域返回 InvalidContext。
    /// 被 Close 唤醒时立即返回关闭原因，不再无限等 inflight。
    /// @param deadline 等待 inflight 归零的期限
    /// @return 排空完成、已关闭或相应错误
    result<void> Drain(bbt::coroutine::Deadline deadline);
    /// 立即逻辑封口：强封自身连接、取消活动流源（恰好一次）、发布关闭原因；
    /// 不等待 handler、不关 Engine。首个原因固定，幂等不可重开。
    /// @param reason 首个关闭原因；之后调用不再覆盖
    void Close(Error reason = MakeError(ErrorCode::Closed, "http server: closed by owner")) noexcept;
    /// Close 发布的原因；仅 Shutdown 封口时为空。
    std::optional<Error> CloseReason() const noexcept;
    /// native 宿主专用：listener 停止、全部本 Server callback 已销毁、连接 Released。
    /// 超时保留任务/共享状态责任，宿主必须继续驱动 runtime 和 Engine。
    /// @param deadline 等待全部回收的期限
    /// @return 回收完成、超时或已关闭
    result<void> WaitStopped(bbt::coroutine::Deadline deadline);
    /// 当前服务端观测快照（只读）。
    ServerStats Stats() const noexcept;

private:
    friend class ServerTestAccess;
    struct State;
    explicit Server(std::shared_ptr<State> state) noexcept;
    std::shared_ptr<State> m_state;
};

} // namespace bbt::infra::http

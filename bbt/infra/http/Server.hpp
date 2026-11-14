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
 *
 * 每连接固定 reader/parser + 请求执行任务 + 单 writer/sequencer：handler 可以
 * 重叠执行，线上响应保持请求顺序；writer 是唯一写 socket、唯一拉取流源的角色。
 * 请求记录、inflight 与队列按同一接纳预算有界，字节上界为
 * `limits.max_inflight * limits.max_body_bytes`（每个响应体另有单响应上限）。
 */
#pragma once

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

/**
 * @brief 已解析的入站请求。
 *
 * target 为 owning origin-form，不是代理/隧道目标；headers 保持线上顺序；body
 * 已在校验上限内收全。
 */
struct IncomingRequest {
    std::string method;
    std::string target;
    Headers headers;
    std::string body;
};

/**
 * @brief 一次性回复的处理函数：返回完整 Response。
 */
using ServerHandler = std::function<result<Response>(IncomingCallContext, IncomingRequest)>;

/**
 * @brief 响应流首帧：确定缓冲或分块，并给出状态码与头。
 *
 * Mode::Buffered 时 body 为完整响应体（允许空体），first_chunk 不用；Mode::Chunked
 * 时 first_chunk 是头提交后立即写出的首块，可为空（只提交头）。后者只在 session
 * 真正拉取时才有意义，因此两种形态不能合成一个字段。
 */
struct StreamStart {
    enum class Mode { Buffered, Chunked } mode;
    int status_code;
    Headers headers;
    std::string body;
    std::string first_chunk;
};

/**
 * @brief 流式响应的拉取源（仅 server 侧；Client::Send 不消费）。
 *
 * ResponseBody 是拉取源而非回调写器：Start 在响应头提交前选择 buffered 或
 * chunked；chunked 开始后 Next 逐个交还自拥 chunk（nullopt=EOF）。HTTP session
 * 经 ICoConnection::Write 写 wire chunk，调用方不持 socket/fd/序列化器。
 *
 * Start/Next 必须在受管协程内挂起消费；Cancel 幂等、可在任意域调用，只表达
 * 「session 不再拉取」，由实现保证不再交付后续结果。
 */
class ResponseBody {
public:
    virtual ~ResponseBody() = default;

    /**
     * @brief 拉取首帧。
     *
     * 必须在给定 WaitOptions 的 deadline 内产出；err 即未提交头、走普通错误响应。
     */
    virtual bbt_co_async(result<StreamStart>) Start(const bbt::coroutine::WaitOptions&) = 0;

    /**
     * @brief Chunked 模式下逐块拉取；nullopt 为正常 EOF（写 0-chunk 收尾）。
     */
    virtual bbt_co_async(result<std::optional<std::string>>) Next(const bbt::coroutine::WaitOptions&) = 0;

    /**
     * @brief 停止拉取；幂等，可在任意域调用。
     */
    virtual void Cancel() noexcept = 0;
};

/**
 * @brief 流式回复的限额。
 *
 * max_chunk_bytes 限制 Next 单块（0 拒绝）；max_stream_lifetime 是头提交后流总期限；
 * stream_write_timeout 是每次 wire 写的期限。
 */
struct StreamingOptions {
    std::size_t max_chunk_bytes{16 * 1024};
    std::chrono::milliseconds max_stream_lifetime{60000};
    std::chrono::milliseconds stream_write_timeout{5000};
};

/**
 * @brief 流式处理函数的回复：完整 Response 或待拉取的响应体。
 */
using ServerReply = std::variant<Response, std::shared_ptr<ResponseBody>>;

/**
 * @brief 可返回流式回复的处理函数。
 */
using StreamingServerHandler = std::function<result<ServerReply>(IncomingCallContext, IncomingRequest)>;

/**
 * @brief 服务端装配选项。
 *
 * limits 给出连接/在飞/头/体上限与入站超时；handoff_capacity 是 accept 交给
 * session 的交接队列容量；max_target_bytes 限制请求行 target；write_timeout
 * 是单次 wire 写的期限。
 */
struct ServerOptions {
    NetworkLimits limits{64, 64, 64 * 1024, 8 * 1024 * 1024, std::chrono::milliseconds{5000}};
    std::size_t handoff_capacity{16};
    std::size_t max_target_bytes{8192};
    std::chrono::milliseconds write_timeout{5000};
};

/**
 * @brief 服务端观测快照。
 *
 * accepted/requests 是累计数；sessions 是活 session 与未物理回收的退役连接，
 * 两者共占额度；retiring 是已请求关闭但尚未真实回收的连接；inflight 覆盖授权到
 * 回复写终态或明确放弃，放弃时 handler 仍存活则额度保留到其退出；tasks 是
 * accept/reader/writer/请求执行等受管协程寿命，不含 Engine driver。
 */
struct ServerStats {
    std::size_t accepted{0};
    std::size_t requests{0};
    std::size_t sessions{0};
    std::size_t retiring{0};
    std::size_t inflight{0};
    std::size_t tasks{0};
};

class ServerTestAccess;

/**
 * @brief 原生协程 HTTP 服务端，独占 Engine 的 accept 消费。
 *
 * 生命周期：Create（Engine Idle）→ Start（Engine Running 且有监听）→
 * Close(CloseOptions)/Close(Immediate) 逻辑封口 → Close(wait_release=true) 物理回收。封口单向，
 * 不可重开。关闭动作不等待 handler 之外的对象、不关 Engine，物理回收由 Engine
 * owner 完成。
 */
class Server {
public:
    /**
     * @brief 装配服务端：Engine 必须 Idle；只配置 handoff，不 Run/监听/建线程。
     *
     * Engine 由应用独占接纳消费（不得另设 raw reader 或第二个 CoAccept 消费者），
     * 最终由应用关闭。
     * @param engine 应用独占并推进的 net::Engine，Server 只接管其 accept 消费
     * @param handler 一次性回复的处理函数
     * @param options 会话/头/体上限与交接队列配置
     * @return 装配失败时为错误
     */
    static result<std::shared_ptr<Server>>
    Create(std::shared_ptr<net::Engine> engine, ServerHandler handler, ServerOptions options = {});

    /**
     * @brief 流式工厂：handler 可返回完整 Response 或 ResponseBody 拉取源。
     *
     * 独立工厂签名（不与 Create 重载）。StreamingOptions 校验：max_chunk_bytes>0、
     * max_stream_lifetime/stream_write_timeout 有限正值且 <=24h。
     * @param engine 应用独占并推进的 net::Engine
     * @param handler 可返回流式回复的处理函数
     * @param options 会话/头/体上限与交接队列配置
     * @param streaming 流式回复的限额
     * @return 装配失败或流式限额非法时为错误
     */
    static result<std::shared_ptr<Server>> CreateStreaming(
        std::shared_ptr<net::Engine> engine,
        StreamingServerHandler handler,
        ServerOptions options = {},
        StreamingOptions streaming = {});
    ~Server() noexcept;
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    /**
     * @brief 开始接纳：Engine Running 且有监听后调用。
     *
     * 一次性；登记失败封口并显式报错。
     * @return 登记失败或状态不满足时为错误
     */
    result<void> Start();

    /**
     * @brief 同门封新 handler；已授权 handler 及回复可继续。idle/半包会话被关闭。
     */
    void StopAccepting() noexcept;

    /**
     * @brief 统一关闭入口；Immediate 不等待时任意线程可调用。
     *
     * - `Immediate`：封新、强封本 Server 连接、取消活动流源（恰好一次）、发布
     *   首个关闭原因并唤醒等待者；不等待 handler、不关 Engine。
     * - `Graceful`：封新并发布首个原因，然后在绝对期限前排空已授权请求
     *   （handler 返回且回复写终态/明确放弃）；到期只返回 `TimedOut` 并保留
     *   已接纳责任，不自动升级。
     * - `wait_release=true`：模式工作完成后在同一 `Close` 内继续等连接
     *   `Released()` 与 callback 销毁到同一绝对期限；false 时物理事实仍由
     *   `Close(wait_release=true)` 证明。
     *
     * 需要等待时，本对象已接纳的 handler/session 任务内调用（自等待）在副作用前返回
     * `InvalidContext`。重复调用幂等、首因固定；`Graceful → Immediate` 升级由
     * 任一次 Immediate/Close(Immediate) 触发，反向不允许。
     * @param options 模式、首因与优雅关闭的有限绝对期限
     * @return 排空完成、已关闭或相应错误
     */
    result<void> Close(const CloseOptions& options = {});

    /**
     * @brief 关闭原因；任意模式首次发布后固定。
     */
    std::optional<Error> CloseReason() const noexcept;

    /**
     * @brief 当前服务端观测快照（只读）。
     */
    ServerStats Stats() const noexcept;

private:
    void _BeginClose(Error reason = MakeError(ErrorCode::Closed, "http server: closed by owner")) noexcept;
    result<void> _WaitReleased(bbt::coroutine::Deadline deadline);

    friend class ServerTestAccess;
    struct State;
    explicit Server(std::shared_ptr<State> state) noexcept;
    std::shared_ptr<State> m_state;
};

// clang-format off
}
// clang-format on

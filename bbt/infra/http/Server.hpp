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

struct IncomingRequest {
    std::string method;
    std::string target;  // owning origin-form，非代理/隧道目标
    Headers headers;
    std::string body;
};

using ServerHandler = std::function<result<Response>(IncomingCallContext, IncomingRequest)>;

// ---- 服务端响应流（architecture §7，仅 server 侧；Client::Send 不变）----
//
// ResponseBody 是拉取源而非回调写器：Start 在响应头提交前选择 buffered 或
// chunked；chunked 开始后 Next 逐个交还自拥 chunk（nullopt=EOF）。HTTP
// session 经 ICoConnection::Write 写 wire chunk，调用方不持 socket/fd/序列化器。
// Start/Next 必须在受管协程内挂起消费；Cancel 幂等、可在任意域调用，只表达
// 「session 不再拉取」，由实现保证不再交付后续结果。

struct StreamStart {
    enum class Mode { Buffered, Chunked } mode;
    int status_code;
    Headers headers;
    std::string body;        // Mode::Buffered 有效（含允许的空体）
    std::string first_chunk; // Mode::Chunked 的首块，可为空（只提交头）
};

class ResponseBody {
public:
    virtual ~ResponseBody() = default;
    // 首个结果必须在给定 deadline 内产出；err 即未提交头、走普通错误响应。
    virtual bbt_co_async(result<StreamStart>) Start(
        const bbt::coroutine::WaitOptions&) = 0;
    // Chunked 模式下逐块拉取；nullopt 为正常 EOF（写 0-chunk 收尾）。
    virtual bbt_co_async(result<std::optional<std::string>>) Next(
        const bbt::coroutine::WaitOptions&) = 0;
    virtual void Cancel() noexcept = 0;
};

struct StreamingOptions {
    std::size_t max_chunk_bytes{16 * 1024};          // Next 单块上界（0 拒绝）
    std::chrono::milliseconds max_stream_lifetime{60000}; // 头提交后流总期限
    std::chrono::milliseconds stream_write_timeout{5000}; // 每次 wire 写期限
};

using ServerReply = std::variant<Response, std::shared_ptr<ResponseBody>>;
using StreamingServerHandler = std::function<result<ServerReply>(
    IncomingCallContext, IncomingRequest)>;

struct ServerOptions {
    NetworkLimits limits{64, 64, 64 * 1024, 8 * 1024 * 1024, std::chrono::milliseconds{5000}};
    std::size_t handoff_capacity{16};
    std::size_t max_target_bytes{8192};
    std::chrono::milliseconds write_timeout{5000};
};

struct ServerStats {
    std::size_t accepted{0};
    std::size_t requests{0};  // 已授权 handler 的累计数
    std::size_t sessions{0}; // 活 session + 未物理回收退役连接，共占额度
    std::size_t retiring{0};
    std::size_t inflight{0}; // 授权起至 handler 返回且回复写终态/明确放弃
    std::size_t tasks{0};   // accept/session callback 寿命，非 Engine driver
};

class ServerTestAccess;

class Server {
public:
    // Engine 必须 Idle；只配置 handoff，不 Run/监听/建线程。Engine 由应用独占
    // 接纳消费（不得另设 raw reader 或第二个 CoAccept 消费者），最终由应用关闭。
    static result<std::shared_ptr<Server>> Create(std::shared_ptr<net::Engine> engine,
                                                  ServerHandler handler, ServerOptions options = {});
    // Streaming 工厂：handler 可返回完整 Response 或 ResponseBody 拉取源。
    // 独立工厂签名（不与 Create 重载），StreamingOptions 校验：max_chunk_bytes>0、
    // max_stream_lifetime/stream_write_timeout 有限正值且 <=24h。
    static result<std::shared_ptr<Server>> CreateStreaming(
        std::shared_ptr<net::Engine> engine, StreamingServerHandler handler,
        ServerOptions options = {}, StreamingOptions streaming = {});
    ~Server() noexcept;
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    // Engine Running 且有监听后调用。一次性；登记失败封口并显式报错。
    result<void> Start();
    // 同门封新 handler；已授权 handler 及回复可继续。idle/半包会话被关闭。
    void StopAccepting() noexcept;
    // native 宿主专用：必须先 StopAccepting；协程/Processer 域返回 InvalidContext。
    result<void> Drain(bbt::coroutine::Deadline deadline);
    // 幂等强制逻辑封口自身连接，不关 Engine，不强杀 handler。
    void Close() noexcept;
    // native 宿主专用：listener 停止、全部本 Server callback 已销毁、连接 Released。
    // 超时保留任务/共享状态责任，宿主必须继续驱动 runtime 和 Engine。
    result<void> WaitStopped(bbt::coroutine::Deadline deadline);
    ServerStats Stats() const noexcept;

private:
    friend class ServerTestAccess;
    struct State;
    explicit Server(std::shared_ptr<State> state) noexcept;
    std::shared_ptr<State> m_state;
};

} // namespace bbt::infra::http

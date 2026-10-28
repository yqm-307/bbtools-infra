#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>

#include <bbt/infra/http/Client.hpp>

namespace bbt::infra::http {

struct IncomingRequest {
    std::string method;
    std::string target;  // owning origin-form，非代理/隧道目标
    Headers headers;
    std::string body;
};

using ServerHandler = std::function<result<Response>(IncomingCallContext, IncomingRequest)>;

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

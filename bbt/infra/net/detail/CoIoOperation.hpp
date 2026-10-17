#pragma once

#include <exception>
#include <functional>
#include <memory>
#include <mutex>

#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/detail/EventLoop.hpp>

namespace bbt::infra::net {

struct CoIoResult {
    bbt::coroutine::WaitStatus wait_status{bbt::coroutine::WaitStatus::InvalidContext};
    SubmitStatus submit_status{SubmitStatus::NotSubmitted};
    IoResult io;
    std::exception_ptr exception;
};

// 每次 Run 独立 waiter/state；backend 必须捕获 shared_ptr 直到末次 callback 退出。
// 超时/取消只结束 runtime 等待，不表示 backend 已取消。结果只在 Completed 交付。
// Asio callback 的非阻塞结果计算须经 CompleteFrom，异常也必须完成并通知等待者。
class CoIoOperation {
public:
    using Start = std::function<void(std::shared_ptr<CoIoOperation>)>;
    // 窄提交：把 owner 投递抽象为调用方提供的 deliver（例如在共享门内取
    // 有效 owner 并完成入队），使门只覆盖投递、不跨挂起等待/用户回调；
    // 默认变体直接转发 EventLoop::Submit，语义与既有入口一致。
    using Deliver = std::function<SubmitStatus(std::function<void()>)>;
    static CoIoResult   Run(EventLoop& owner, const bbt::coroutine::WaitOptions& options, Start start);
    static CoIoResult   Run(Deliver deliver, const bbt::coroutine::WaitOptions& options, Start start);
    bool                Complete(IoResult result, std::exception_ptr error = {});

    template<class Callback> bool CompleteFrom(Callback&& callback) {
        try { return Complete(callback()); }
        catch (...) {
            return Complete({0, std::make_error_code(std::errc::io_error)}, std::current_exception());
        }
    }

private:
    std::mutex          m_mutex;
    bbt::coroutine::sync::CoWaiter m_waiter;
    bool                m_completed{false};
    IoResult            m_result;
    std::exception_ptr  m_exception;
};

} // namespace bbt::infra::net

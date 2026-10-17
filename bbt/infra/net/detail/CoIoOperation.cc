#include <bbt/infra/net/detail/CoIoOperation.hpp>

#include <utility>

#include <bbt/coroutine/coroutine.hpp>

namespace bbt::infra::net {
namespace {

// combined 纯信号等待显式映射回既有窄口径 WaitStatus；无 fd interest 时
// InvalidOptions/FdReadable/FdWriteable 不可达，显式归入 RuntimeUnavailable，
// 不假成功、不引入新错误集合。
bbt::coroutine::WaitStatus MapCombinedStatus(bbt::coroutine::sync::CombinedWaitStatus status) {
    using bbt::coroutine::sync::CombinedWaitStatus;
    using bbt::coroutine::WaitStatus;
    switch (status) {
    case CombinedWaitStatus::Completed:          return WaitStatus::Completed;
    case CombinedWaitStatus::TimedOut:           return WaitStatus::TimedOut;
    case CombinedWaitStatus::Cancelled:          return WaitStatus::Cancelled;
    case CombinedWaitStatus::InvalidContext:     return WaitStatus::InvalidContext;
    case CombinedWaitStatus::AlreadyWaiting:     return WaitStatus::AlreadyWaiting;
    case CombinedWaitStatus::RuntimeUnavailable: return WaitStatus::RuntimeUnavailable;
    case CombinedWaitStatus::InvalidOptions:
    case CombinedWaitStatus::FdReadable:
    case CombinedWaitStatus::FdWriteable:        return WaitStatus::RuntimeUnavailable;
    }
    return WaitStatus::RuntimeUnavailable;
}

} // namespace

CoIoResult CoIoOperation::Run(EventLoop& owner, const bbt::coroutine::WaitOptions& options, Start start) {
    // 既有入口委托给窄提交变体：投递仍是无门直连 owner，语义不变。
    return Run([&owner](std::function<void()> command) { return owner.Submit(std::move(command)); },
        options, std::move(start));
}

CoIoResult CoIoOperation::Run(Deliver deliver, const bbt::coroutine::WaitOptions& options, Start start) {
    CoIoResult result;
    // 精确查询，无隐式 Processer 创建；非法环境先于分配/投递及 start 副作用。
    if (bbt::coroutine::GetLocalCoroutineId() == 0) { return result; }
    if (!start || !deliver) {
        result.submit_status = SubmitStatus::Invalid;
        return result;
    }
    auto operation = std::make_shared<CoIoOperation>();
    // 组合等待的纯信号切片：不申请 fd interest，仅透传同一 deadline。窄入口
    // 在“过期检查”与“剩余期限采样”两次采样之间到期时，ceil 得 0ms 会建立
    // 不挂定时器的等待（可无限延长），明显负值又被登记拒绝；组合入口对
    // remain<=0 复查，两个窗口都收敛为 TimedOut。登记先于投递、Notify 首胜
    // 与取消语义保持不变。
    bbt::coroutine::sync::CombinedWaitOptions combined_options;
    combined_options.deadline = options.deadline;
    result.wait_status = MapCombinedStatus(operation->m_waiter.Wait(combined_options,
        [&result, operation, start, deliver] {
        try {
            result.submit_status = deliver([operation, start] {
                try { start(operation); }
                catch (...) {
                    operation->Complete({0, std::make_error_code(std::errc::io_error)}, std::current_exception());
                }
            });
            if (result.submit_status != SubmitStatus::Accepted) {
                const auto error = result.submit_status == SubmitStatus::Full
                    ? std::errc::no_buffer_space : std::errc::operation_canceled;
                operation->Complete({0, std::make_error_code(error)});
            }
        } catch (...) {
            result.submit_status = SubmitStatus::Failed;
            operation->Complete({0, std::make_error_code(std::errc::io_error)}, std::current_exception());
        }
        return true;
        }));
    if (result.wait_status == bbt::coroutine::WaitStatus::Completed) {
        std::lock_guard<std::mutex> lock(operation->m_mutex);
        result.io = operation->m_result;
        result.exception = operation->m_exception;
    }
    return result;
}

bool CoIoOperation::Complete(IoResult result, std::exception_ptr error) {
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_completed) { return false; }
        m_result = result;
        m_exception = std::move(error);
        m_completed = true;
    }
    // 无借用 payload，不把 CoEventValue 的指针当寿命；Notify 只交既有 runtime。
    m_waiter.Notify(bbt::coroutine::CoEventValue::From(true));
    return true;
}

} // namespace bbt::infra::net

#pragma once

namespace bbt::infra::detail {

inline CloseWaitDomain CurrentCloseWaitDomain() noexcept
{
    if (bbt::coroutine::GetLocalCoroutineId() != 0) {
        return CloseWaitDomain::Co;
    }
    if (bbt::coroutine::detail::Processer::TryGetLocalProcesser() == nullptr) {
        return CloseWaitDomain::NativeHost;
    }
    return CloseWaitDomain::Invalid;
}

inline bbt::coroutine::Deadline NextCloseSlice(bbt::coroutine::Deadline deadline) noexcept
{
    const auto now = std::chrono::steady_clock::now();
    if (deadline == bbt::coroutine::Deadline::max()) {
        return now + std::chrono::milliseconds(50);
    }
    return std::min(deadline, now + std::chrono::milliseconds(50));
}

inline result<bool>
WaitCloseSlice(const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& signal, bbt::coroutine::Deadline until)
{
    using bbt::coroutine::WaitStatus;
    if (!signal) {
        return result<bool>::err(MakeError(ErrorCode::RuntimeUnavailable, "close wait signal missing"));
    }
    switch (signal->Wait({until})) {
    case WaitStatus::Completed:
        return result<bool>::ok(true);
    case WaitStatus::TimedOut:
        return result<bool>::ok(false);
    case WaitStatus::AlreadyWaiting:
        bbtco_sleep(5);
        return result<bool>::ok(false);
    case WaitStatus::Cancelled:
        return result<bool>::err(MakeError(ErrorCode::Cancelled, "close wait cancelled"));
    case WaitStatus::InvalidContext:
        return result<bool>::err(MakeError(ErrorCode::InvalidContext, "close wait requires coroutine context"));
    case WaitStatus::RuntimeUnavailable:
        return result<bool>::err(MakeError(ErrorCode::RuntimeUnavailable, "close wait unavailable"));
    }
    return result<bool>::err(MakeError(ErrorCode::RuntimeUnavailable, "close wait unavailable"));
}

inline result<void> ValidateCloseOptions(const CloseOptions& options)
{
    const bool waits = options.mode == CloseMode::Graceful || options.wait_release;
    if (waits && options.deadline == bbt::coroutine::Deadline::max()) {
        return result<void>::err(
            MakeError(ErrorCode::InvalidArgument, "close with waiting requires a finite absolute deadline"));
    }
    return result<void>::ok();
}

template <typename WaitFn> inline result<void> FinishCloseWithRelease(const CloseOptions& options, WaitFn&& wait)
{
    if (!options.wait_release) {
        return result<void>::ok();
    }
    return wait(options.deadline);
}

inline Error SettleCloseError(const std::optional<Error>& reason, const char* default_message)
{
    if (reason && !reason->message.empty()) {
        return *reason;
    }
    return MakeError(ErrorCode::Closed, default_message);
}

// clang-format off
}
// clang-format on

#pragma once
// co-network/v1 N0：关闭契约。Open → Closing → Closed，不重开。
// RequestClose 幂等、可从普通线程调用；WaitClosed 只在协程内等待，
// 每个对象最多一个并发等待者，超时/取消不撤销关闭。

#include <bbt/infra/ICoObject.hpp>

namespace bbt::infra {

enum class CloseStatus {
    Closed, TimedOut, Cancelled, InvalidContext,
    AlreadyWaiting, RuntimeUnavailable
};

class ICoCloseable {
public:
    virtual ~ICoCloseable() = default;
    virtual void RequestClose() noexcept = 0;
    virtual bool IsClosed() const noexcept = 0;
    virtual CloseStatus WaitClosed(
        bbt::coroutine::Deadline deadline,
        bbt::coroutine::CancellationToken cancel) = 0;
};

} // namespace bbt::infra

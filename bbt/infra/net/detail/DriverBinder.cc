#include <bbt/infra/net/detail/DriverBinder.hpp>

#include <new>
#include <utility>

#include <bbt/coroutine/coroutine.hpp>

namespace bbt::infra::net {

namespace {

using bbt::coroutine::detail::ProcesserDriverStatus;
using bbt::coroutine::detail::ProcesserDriverToken;
using bbt::coroutine::detail::Scheduler;

Scheduler* SchedulerInstance() { return Scheduler::GetInstance().get(); }

// 调用域预校验：不得在受管业务协程或已登记 worker 线程上绑定/解绑，
// 否则可能在目标 P 自己身上建立 driver、或在自身上同步解绑等待自己。
bool InOwnerOrManagedDomain() {
    if (bbt::coroutine::GetLocalCoroutineId() != 0) {
        return true;
    }
    return bbt::coroutine::detail::Processer::TryGetLocalProcesser() != nullptr;
}

} // namespace

std::error_code DriverBinder::MapStatus(ProcesserDriverStatus status) {
    switch (status) {
        case ProcesserDriverStatus::Ok:
            return {};
        case ProcesserDriverStatus::NotInitialized:
            return std::make_error_code(std::errc::operation_not_supported);
        case ProcesserDriverStatus::InvalidTarget:
        case ProcesserDriverStatus::InvalidDriver:
            return std::make_error_code(std::errc::invalid_argument);
        case ProcesserDriverStatus::Occupied:
            return std::make_error_code(std::errc::device_or_resource_busy);
        case ProcesserDriverStatus::StaleToken:
            return std::make_error_code(std::errc::operation_canceled);
        case ProcesserDriverStatus::SelfDetach:
            return std::make_error_code(std::errc::operation_not_permitted);
    }
    return std::make_error_code(std::errc::io_error);
}

std::error_code DriverBinder::BindSlots(std::size_t count, std::vector<DriverSlot>& out,
                                        std::vector<DriverSlot>* residual) {
    if (count == 0) {
        return {};
    }
    if (InOwnerOrManagedDomain()) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    Scheduler* scheduler = SchedulerInstance();
    if (scheduler == nullptr || !scheduler->IsInitialized()) {
        return std::make_error_code(std::errc::operation_not_supported);
    }

    std::vector<bbt::coroutine::detail::ProcesserId> ids;
    try {
        ids = scheduler->GetWorkerProcesserIds();
    } catch (const std::bad_alloc&) {
        return std::make_error_code(std::errc::not_enough_memory);
    }

    // 请求量超过实际可登记的 worker 槽数：这个请求在语义上不可能满足，
    // 先拒绝，避免为一个不可能成功的绑定做 count 量级的 reserve。
    if (count > ids.size()) {
        return std::make_error_code(std::errc::no_buffer_space);
    }

    // 本地构建新绑定：失败时调用者 out 中既存绑定保持原样，不丢责任。
    std::vector<DriverSlot> bound;
    try {
        bound.reserve(count);
    } catch (const std::bad_alloc&) {
        return std::make_error_code(std::errc::not_enough_memory);
    }

    auto rollback = [&](std::error_code reason) {
        const auto unbind_error = UnbindSlots(bound);
        if (unbind_error && residual != nullptr) {
            // 未解绑的残项责任移交给调用者；不让局部 vector 析构吞掉。
            residual->insert(residual->end(),
                             std::make_move_iterator(bound.begin()),
                             std::make_move_iterator(bound.end()));
            bound.clear();
        }
        return reason ? reason : unbind_error;
    };

    for (auto id : ids) {
        if (bound.size() >= count) {
            break;
        }
        std::shared_ptr<ReactorDriver> driver;
        try {
            driver = std::make_shared<ReactorDriver>();
        } catch (const std::bad_alloc&) {
            return rollback(std::make_error_code(std::errc::not_enough_memory));
        }
        ProcesserDriverToken token{};
        ProcesserDriverStatus status = ProcesserDriverStatus::InvalidDriver;
        try {
            status = scheduler->RegisterProcesserDriver(id, driver, token);
        } catch (...) {
            return rollback(std::make_error_code(std::errc::io_error));
        }
        if (status == ProcesserDriverStatus::Ok) {
            bound.push_back(DriverSlot{id, token, std::move(driver)});
            continue;
        }
        // 已占用 / 目标失效：本次换下一个空闲槽，不回滚其它槽。
        if (status == ProcesserDriverStatus::Occupied
            || status == ProcesserDriverStatus::InvalidTarget) {
            continue;
        }
        return rollback(MapStatus(status));
    }

    if (bound.size() < count) {
        // 空闲槽不足或绑定中竞争失败：回滚本次全部，不缩数量、不抢占。
        return rollback(std::make_error_code(std::errc::no_buffer_space));
    }

    // 只在完全成功后才追加到 out：reserve 抛出时 out 不变，回滚本次绑定。
    try {
        out.reserve(out.size() + bound.size());
    } catch (const std::bad_alloc&) {
        return rollback(std::make_error_code(std::errc::not_enough_memory));
    }
    for (auto& slot : bound) {
        out.push_back(std::move(slot));  // capacity 已足，不再分配
    }
    return {};
}

std::error_code DriverBinder::UnbindSlots(std::vector<DriverSlot>& slots) {
    std::error_code first;
    Scheduler* scheduler = SchedulerInstance();

    // 原地移除成功项（swap-pop），不分配临时容器。
    std::size_t index = 0;
    while (index < slots.size()) {
        DriverSlot& slot = slots[index];
        if (!slot.driver) {
            slot = std::move(slots.back());
            slots.pop_back();
            continue;
        }
        ProcesserDriverStatus status = ProcesserDriverStatus::StaleToken;
        if (scheduler == nullptr) {
            status = ProcesserDriverStatus::NotInitialized;
        } else {
            try {
                status = scheduler->UnregisterProcesserDriver(slot.token);
            } catch (...) {
                status = ProcesserDriverStatus::InvalidDriver;
            }
        }
        // Ok：确已解绑。StaleToken：上游契约保证该代归属已不存在，同样安全移出
        // （旧 token 不能解绑新一代，见上游 coroutine 核心契约）。其它状态保留责任。
        if (status == ProcesserDriverStatus::Ok
            || status == ProcesserDriverStatus::StaleToken) {
            slot = std::move(slots.back());
            slots.pop_back();
            continue;
        }
        if (!first) {
            first = MapStatus(status);
        }
        ++index;
    }
    return first;
}

} // namespace bbt::infra::net

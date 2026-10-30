#pragma once

// 自动槽绑定（detail 私有部件，不是公共 API）。
//
// 契约：docs/architecture/gnet-network-execution.md「Engine 自动枚举并绑定空闲已登记 P；
// 查询空槽不是预留，以注册成功的同代 token 为准。槽不足或竞争失败回滚本次
// 绑定，不抢占、不扩 worker、不缩数量、不退回独立网络线程。」
//
// 绑定失败回滚时，若本次已绑定项无法解绑（SelfDetach/未初始化等），必须由
// 调用者的可达对象继续持有责任：通过 residual 输出回滚残项，绝不以销毁局部
// vector 冒充已清理。解绑成功或上游契约明确的 StaleToken（该代归属已不存在）
// 才移出。

#include <cstddef>
#include <memory>
#include <system_error>
#include <vector>

#include <bbt/coroutine/detail/Scheduler.hpp>

#include <bbt/infra/net/detail/ReactorDriver.hpp>

namespace bbt::infra::net {

struct DriverSlot {
    bbt::coroutine::detail::ProcesserId processer_id{BBT_COROUTINE_INVALID_PROCESSER_ID};
    bbt::coroutine::detail::ProcesserDriverToken token{};
    std::shared_ptr<ReactorDriver> driver;
};

class DriverBinder {
public:
    // 成功时把恰好 count 个新槽追加到 out（调用者既存绑定不变）。
    // 失败时 out 不变；若本次已绑定项因解绑被拒而无法回收，移入 *residual
    // （residual 非空时不再承担；为空则返回 no_buffer_space 表示责任未收回）。
    static std::error_code BindSlots(std::size_t count, std::vector<DriverSlot>& out,
                                     std::vector<DriverSlot>* residual = nullptr);

    // 同代同步解绑：成功项原地移出（无分配、无临时容器）；解绑被拒项保留在
    // slots 中由调用者继续持有。返回首个失败映射。
    static std::error_code UnbindSlots(std::vector<DriverSlot>& slots);

    static std::error_code MapStatus(bbt::coroutine::detail::ProcesserDriverStatus status);
};

} // namespace bbt::infra::net

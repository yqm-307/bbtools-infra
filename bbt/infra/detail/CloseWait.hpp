/**
 * @file CloseWait.hpp
 * @brief infra 私有关闭等待夹具：执行域判定、50ms 分片与 CloseOptions 校验。
 *
 * 唯一的分片实现，供 http/rpc/redis 与 Engine::CoWaitRunning 共用：
 *   - 受管 Co 域用 `CoWaiter` 等待，每轮绝对截止时间取
 *     `min(deadline, now + 50ms)`，不阻塞 worker、也不退回 native CV；
 *   - native 宿主域允许同一谓词的有界 native 等待（由模块用自己的条件变量实现）；
 *   - 其它域（Processer 上但不在协程）不提供等待，必须返回 InvalidContext。
 *
 * 全部实现留在头内并标 inline：这些函数被八个模块 target 共同使用，放进独立 TU 就要
 * 在每个 target 的源清单里重复注册，任何一处漏登记都是链接错误；inline 让链接面保持
 * 不变（ODR 由 inline 保证）。
 *
 * 本头是 detail，不属于冻结的公共 API。
 */
#pragma once

#include <algorithm>
#include <chrono>
#include <memory>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/infra/CloseOptions.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra::detail {

/**
 * 关闭调用的执行域。
 */
enum class CloseWaitDomain {

    /**
     * 受管协程：必须用 CoWaiter 分片等待。
     */
    Co,

    /**
     * native 宿主：允许同一谓词的有界 native 等待。
     */
    NativeHost,

    /**
     * Processer 线程上但不在协程：不允许阻塞等待。
     */
    Invalid
};

CloseWaitDomain CurrentCloseWaitDomain() noexcept;

/**
 * 下一片等待的绝对时间点：`min(deadline, now + 50ms)`；无期限时固定 50ms。
 */
bbt::coroutine::Deadline NextCloseSlice(bbt::coroutine::Deadline deadline) noexcept;

/**
 * @brief 单次 Co 分片等待。
 *
 * `Completed`/`TimedOut` 用 bool 区分是否命中唤醒；`AlreadyWaiting` 表示同一
 * 单等待位另有等待者，本片让出 5ms 后按未命中处理，由调用方复查谓词。
 * 取消、非法域与登记失败如实映射为错误。
 */
result<bool>
WaitCloseSlice(const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& signal, bbt::coroutine::Deadline until);

/**
 * @brief CloseOptions 值校验：需要等待的组合必须给有限绝对期限。
 *
 * `Graceful` 与 `wait_release=true` 都要在同一绝对期限内等待，因此两者都拒绝
 * `Deadline::max()`；`Immediate` 且不等待物理回收时不读期限。模块不支持的组合
 * 由各模块在自己的副作用之前补判，本函数只做与模块无关的值校验。
 */
result<void> ValidateCloseOptions(const CloseOptions& options);

/**
 * @brief 模式工作成功后按 `wait_release` 合并本对象的物理回收等待。
 *
 * 同一 `Close` 调用内沿用同一绝对期限（不续预算）；`wait_release=false` 时立即
 * 返回，物理事实仍留给调用方的 `WaitStopped`。失败/超时的模式结果不由本函数
 * 改写，也不在失败后偷换成强制关闭。
 */
template <typename WaitFn> inline result<void> FinishCloseWithRelease(const CloseOptions& options, WaitFn&& wait);

/**
 * @brief 首个关闭原因：空时补模块默认 `Closed`。
 */
Error SettleCloseError(const std::optional<Error>& reason, const char* default_message);

// clang-format off
}
// clang-format on

#include "__TCloseWait.hpp"

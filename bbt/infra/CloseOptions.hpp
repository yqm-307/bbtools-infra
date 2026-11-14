/**
 * @file CloseOptions.hpp
 * @brief 统一关闭控制面：模式、首因与绝对期限（infra 各模块唯一关闭输入）。
 *
 * 语义唯一真源是 docs/architecture/close-options-contract.md。本头只定义值类型，
 * 不执行等待、不持有模块状态：
 *   - `Immediate`：默认模式。逻辑封口并发布首因，不等待在途责任；不读 deadline。
 *   - `Graceful`：逻辑封口、停止接纳，等待本模块承诺的用户可见责任归零；
 *     必须给出有限绝对期限（`Deadline::max()` 被拒绝），到期只报 `TimedOut`，
 *     不自动升级为 Immediate。
 *   - `wait_release`：默认 false。true 时在模式工作完成后，继续在本对象自己的
 *     物理回收责任上等待到同一绝对期限（同样要求有限期限），不改模式、不续预算。
 *   - `reason` 是首个发布的关闭原因；空时由各模块补默认 `Closed`。
 *
 * 三种终态（逻辑封口 / 用户责任终态 / 物理回收）由模块成员 `Close`、
 * `Close(Immediate)` 与 `Close(wait_release=true)` 分别表达，不能互相替代。
 */
#pragma once

#include <optional>

#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra {

/**
 * @brief 关闭模式：只有立即与优雅两种，不提供第二套超时模式。
 */
enum class CloseMode { Immediate, Graceful };

/**
 * @brief 统一关闭输入。
 *
 * 默认值是 `Immediate` + `wait_release=false`：裸 `CloseOptions{}` 就是「立即封口、
 * 不等」的强制关闭。需要优雅排空时用 `Graceful(deadline)` 显式给出有限绝对期限；
 * `wait_release=true` 也必须给出有限期限，否则任何模块都在副作用前返回
 * `InvalidArgument`。
 */
struct CloseOptions {

    /**
     * 关闭模式：立即或优雅。
     */
    CloseMode mode{CloseMode::Immediate};

    /**
     * 是否把本对象物理回收等待并入本次 Close；true 时要求有限期限。
     */
    bool wait_release{false};

    /**
     * 首个关闭原因；空时由各模块补默认 Closed。
     */
    std::optional<Error> reason;

    /**
     * 优雅关闭与物理等待共用的有限绝对期限；Immediate 模式不读。
     */
    bbt::coroutine::Deadline deadline{bbt::coroutine::Deadline::max()};

    /**
     * @brief 立即关闭：只发布原因，不读 deadline。
     */
    static CloseOptions Immediate(Error reason = {})
    {
        CloseOptions options;
        options.mode = CloseMode::Immediate;
        options.reason = std::move(reason);
        return options;
    }

    /**
     * @brief 优雅关闭：在绝对期限前排空用户可见责任。
     */
    static CloseOptions Graceful(bbt::coroutine::Deadline deadline, Error reason = {})
    {
        CloseOptions options;
        options.mode = CloseMode::Graceful;
        options.reason = std::move(reason);
        options.deadline = deadline;
        return options;
    }
};

// clang-format off
}
// clang-format on

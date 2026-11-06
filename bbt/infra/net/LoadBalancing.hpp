#pragma once

/**
 * @file LoadBalancing.hpp
 * @brief 连接在 owner 之间分配的选择逻辑。
 *
 * 三种算法移植自 gnet v2.10.0 的 load_balancer.go（Apache-2.0，来源证明见
 * docs/dependencies/gnet-2.10.0.provenance.md）。这里只做选择，不维护完整的 owner 注册表。
 * 跨 owner 的注册串行化与连接计数由 Engine 层负责，因此 LoadBalancer 明确不是线程安全的，
 * 不得误标为线程安全。
 */

#include <cstddef>
#include <cstdint>
#include <system_error>
#include <vector>

namespace bbt::infra::net {

class Address; // 值借用：Next 只读取其 ToString 的字节。

/** @brief 连接分配策略。 */
enum class LoadBalancing : int {
    RoundRobin = 0,       ///< 依次轮询
    LeastConnections = 1, ///< 选择连接数最少的 owner
    SourceAddrHash = 2,   ///< 按远端地址哈希
};

/**
 * @brief 在若干个 owner 之间选择下一个索引。非线程安全。
 *
 * 构造时校验参数：loop_count 不能为零，mode 必须是已定义的枚举值。失败时对象保持不可用，
 * 之后的 Next 都会报错。
 */
class LoadBalancer {
public:
    /**
     * @brief 构造选择器。
     *
     * @param mode 分配策略，必须是已定义的枚举值。
     * @param loop_count owner 数量，不能为零。
     * @param[out] error 成功时清空；失败时写入 invalid_argument。调用方可以复用携带旧错误的变量。
     */
    LoadBalancer(LoadBalancing mode, std::size_t loop_count, std::error_code& error);

    /** @brief owner 数量；构造失败时为 0。 */
    std::size_t Size() const noexcept
    {
        return loop_count_;
    }

    /** @brief 当前策略。 */
    LoadBalancing Mode() const noexcept
    {
        return mode_;
    }

    /**
     * @brief 选择下一个 owner 的索引。
     *
     * connection_counts 只在 LeastConnections 模式下使用，且大小必须等于 Size()。其它模式忽略传入的计数，
     * 不会静默修正调用方的输入。任何错误路径都不推进 RoundRobin 的游标。成功时清空 error，
     * 失败时返回 Size() 作为哨兵值。
     *
     * @param remote 远端地址，只用于 SourceAddrHash。
     * @param connection_counts 每个 owner 当前的连接数。
     * @param[out] error 成功时清空，失败时写入原因。
     * @return 被选中的 owner 索引；失败时为 Size()。
     */
    std::size_t Next(const Address& remote, const std::vector<std::uint64_t>& connection_counts,
                     std::error_code& error);

private:
    LoadBalancing mode_{LoadBalancing::RoundRobin};
    std::size_t loop_count_{0};
    std::size_t next_offset_{0}; // RoundRobin 游标，恒在 [0, loop_count_) 内，不会溢出
};

} // namespace bbt::infra::net

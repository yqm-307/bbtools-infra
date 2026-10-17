#pragma once

// gnet v2.10.0 load_balancer.go（Apache-2.0，见
// docs/dependencies/gnet-2.10.0.provenance.md）三算法的纯选择逻辑移植。
// 只做选择，不预建完整 owner 注册表；跨域 Register 的串行化与连接计数
// 维护由后续 Engine 层负责（0010 D3），因此本类显式非线程安全，
// 不得误标为线程安全。

#include <cstddef>
#include <cstdint>
#include <system_error>
#include <vector>

namespace bbt::infra::net {

class Address;  // 值借用：Next 只读其 ToString 字节。

enum class LoadBalancing : int {
    RoundRobin = 0,         // 逐个轮询
    LeastConnections = 1,   // 连接数最少者
    SourceAddrHash = 2,     // 远端地址哈希
};

class LoadBalancer {
public:
    // loop_count 必须非空（空集合拒绝）；mode 必须是已定义枚举值
    // （拒绝任意 cast 进来的值）。成功时清空传入 error（调用方可能复用
    // 携带陈旧错误的变量）；失败时对象保持不可用，后续 Next 一律报错。
    LoadBalancer(LoadBalancing mode, std::size_t loop_count, std::error_code& error);

    std::size_t Size() const noexcept { return loop_count_; }
    LoadBalancing Mode() const noexcept { return mode_; }

    // 选择下一个 owner 索引。connection_counts 仅 LeastConnections 消费，
    // 大小必须等于 Size()；其余模式忽略传入计数（不静默修正调用输入）。
    // 任何错误路径都不推进 RoundRobin 游标。成功路径清空 error（调用方
    // 可复用同一变量）；错误时返回 Size() 作为哨兵。
    std::size_t Next(const Address& remote,
        const std::vector<std::uint64_t>& connection_counts, std::error_code& error);

private:
    LoadBalancing mode_{LoadBalancing::RoundRobin};
    std::size_t loop_count_{0};
    std::size_t next_offset_{0};    // RoundRobin 游标，恒在 [0, loop_count_)，无溢出
};

} // namespace bbt::infra::net

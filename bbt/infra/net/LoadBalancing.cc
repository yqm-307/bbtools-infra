#include <bbt/infra/net/LoadBalancing.hpp>

#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/detail/SourceAddrHash.hpp>

namespace bbt::infra::net {

LoadBalancer::LoadBalancer(LoadBalancing mode, std::size_t loop_count, std::error_code& error)
{
    switch (mode) {
    case LoadBalancing::RoundRobin:
    case LoadBalancing::LeastConnections:
    case LoadBalancing::SourceAddrHash:
        break;
    default:
        error = std::make_error_code(std::errc::invalid_argument);
        return;
    }
    if (loop_count == 0) {
        error = std::make_error_code(std::errc::invalid_argument);
        return;
    }
    mode_ = mode;
    loop_count_ = loop_count;
    error.clear();      // 成功路径清掉调用方预置的陈旧错误（error 复用契约）。
}

std::size_t LoadBalancer::Next(const Address& remote,
    const std::vector<std::uint64_t>& connection_counts, std::error_code& error)
{
    // 同一 error 变量在调用间复用：成功路径必须清空，失败路径在下方
    // 重新赋明确错误。
    error.clear();
    if (loop_count_ == 0) {
        error = std::make_error_code(std::errc::invalid_argument);
        return loop_count_;
    }
    switch (mode_) {
    case LoadBalancing::RoundRobin: {
        // 序列与参考 nextIndex % size 一致；游标存于 [0, loop_count_)，无溢出。
        const std::size_t chosen = next_offset_;
        next_offset_ = (next_offset_ + 1) % loop_count_;
        return chosen;
    }
    case LoadBalancing::LeastConnections: {
        if (connection_counts.size() != loop_count_) {
            error = std::make_error_code(std::errc::invalid_argument);
            return loop_count_;
        }
        std::size_t best = 0;
        for (std::size_t i = 1; i < loop_count_; ++i) {
            // 严格小于：同值优先第一个，与参考 leastConnectionsLoadBalancer 一致。
            if (connection_counts[i] < connection_counts[best]) {
                best = i;
            }
        }
        return best;
    }
    case LoadBalancing::SourceAddrHash: {
        // 对 remote Address::ToString 的完整字节（含端口/IPv6/Unix 形态）
        // 做 IEEE CRC32 后按 size 取模，对应 gnet load_balancer.go:128-140。
        // 与 64 位 Go int 语义一致：uint32 CRC 转 int64 恒非负，模 size
        // 结果相同；32 位 Go 会先转负再 abs，本迁移不复现该路径（0010：
        // 不承诺 Go 二进制兼容）。
        return static_cast<std::size_t>(Crc32Checksum(remote.ToString())) % loop_count_;
    }
    }
    error = std::make_error_code(std::errc::invalid_argument);
    return loop_count_;
}

} // namespace bbt::infra::net

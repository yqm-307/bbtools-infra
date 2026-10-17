#pragma once

// owner 待发队列：贴近 gnet「静态 ring + 溢出 list」分段结构
// （gnet v2.10.0 pkg/buffer 的 ring/list 语义参考，MIT，见
// docs/dependencies/gnet-2.10.0.provenance.md；未复制其实现）。
//
// 与 0010 有界资源要求对应：总待发字节有界、段/元数据也有实际界，
// 拒绝发生在任何修改之前（已有数据在拒绝后不变）。ring 容量是静态段
// 容量，不是总量上限（与 gnet WriteBufferCap 语义一致，总量另设）。
//
// 非线程安全；单 owner 域使用。Append 拷贝数据取得所有权——异步调用的
// 数据必须复制/转移，不得保存调用栈上的 view。
// 不得把指向本队列内部存储（如 GatherViews 返回的视图或其子串）的
// 别名数据再传入本队列：aliasing 输入未定义。

#include <cstddef>
#include <cstring>
#include <deque>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace bbt::infra::net {

class OutboundQueue {
public:
    // 尾段合并上限：小写入合并进尾段，避免大量零长/一字节段绕开
    // 元数据限额；超过该上限的溢出数据自成一段（受段数界约束）。
    static constexpr std::size_t kMaxMergedSegmentBytes = 4096;

    // 几何契约：ring_bytes / max_total_bytes / max_segments 任一为 0，或
    // ring_bytes > max_total_bytes 时，构造不失败但队列不可用——任何非空
    // 追加都被保守拒绝，且不为无效几何分配不受 total 约束的 ring。合法
    // 几何构造即分配 ring（受 total 约束的静态段容量）。
    OutboundQueue(std::size_t ring_bytes, std::size_t max_total_bytes, std::size_t max_segments)
        : max_total_bytes_(max_total_bytes), max_segments_(max_segments)
    {
        if (ring_bytes != 0 && max_total_bytes != 0 && max_segments != 0
            && ring_bytes <= max_total_bytes) {
            geometry_valid_ = true;
            ring_.resize(ring_bytes);
        }
    }

    // 单 owner：拷贝被禁止，转移走显式移动。移动后来源保持合法空队列
    // （同几何可继续追加；ring 段容量随存储转移归零，后续写入走溢出段），
    // 自移动安全；不留「容器已转移而标量未清」的隐式 move。
    OutboundQueue(const OutboundQueue&) = delete;
    OutboundQueue& operator=(const OutboundQueue&) = delete;
    // 空目标的 deque 默认构造也可能分配：允许 bad_alloc，全部成员
    // 初始化成功前不触碰源；之后 swap/exchange 不分配、不抛。
    // 构造失败无目标对象，源字节/几何完整保持原样。
    OutboundQueue(OutboundQueue&& other)
        : max_total_bytes_(other.max_total_bytes_),
          max_segments_(other.max_segments_),
          geometry_valid_(other.geometry_valid_)
    {
        ring_.swap(other.ring_);
        spill_.swap(other.spill_);
        ring_head_ = std::exchange(other.ring_head_, 0);
        ring_size_ = std::exchange(other.ring_size_, 0);
        list_bytes_ = std::exchange(other.list_bytes_, 0);
    }
    OutboundQueue& operator=(OutboundQueue&& other) noexcept
    {
        if (this != &other) {
            ring_ = std::move(other.ring_);
            spill_ = std::move(other.spill_);
            max_total_bytes_ = other.max_total_bytes_;
            max_segments_ = other.max_segments_;
            geometry_valid_ = other.geometry_valid_;
            ring_head_ = std::exchange(other.ring_head_, 0);
            ring_size_ = std::exchange(other.ring_size_, 0);
            list_bytes_ = std::exchange(other.list_bytes_, 0);
        }
        return *this;
    }

    std::size_t RingCapacity() const noexcept { return ring_.size(); }
    std::size_t MaxTotalBytes() const noexcept { return max_total_bytes_; }
    // 段语义：MaxSegments 是溢出 list 的段数上限；SegmentCount 是逻辑
    // 总段数（ring 非空计 1 段 + 溢出段数），可达 MaxSegments()+1。
    std::size_t MaxSegments() const noexcept { return max_segments_; }
    std::size_t TotalBytes() const noexcept { return ring_size_ + list_bytes_; }
    std::size_t SegmentCount() const noexcept { return (ring_size_ != 0 ? 1 : 0) + spill_.size(); }
    bool Empty() const noexcept { return TotalBytes() == 0; }

    // 拷贝追加。空数据合法且不产生任何段。无效几何、超总量界，或溢出
    // 部分既不能合并进尾段又已达到段数界时，在修改前拒绝
    // （no_buffer_space），已有数据不变。
    //
    // 保序（FIFO）：数据只能追加到逻辑队尾。ring 段是逻辑队首，仅当
    // spill 为空（没有更早的未发数据）时新数据才可进入 ring；spill 非空
    // 时任何新数据一律入溢出段尾（合并或新段），不得复用 ring 中已释放
    // 的空位插到未发数据之前（父级 probe 观察到的 cdijEFGH 缺陷）。
    //
    // 异常边界：可能抛 bad_alloc 的溢出段提交先于 noexcept 的 ring 拷贝
    // 提交，提交失败时队列保持原样，无半提交状态。
    std::error_code Append(std::string_view data)
    {
        if (data.empty()) {
            return {};
        }
        if (!geometry_valid_) {
            return std::make_error_code(std::errc::no_buffer_space);
        }
        if (data.size() > max_total_bytes_ - TotalBytes()) {
            return std::make_error_code(std::errc::no_buffer_space);
        }
        const std::size_t ring_space =
            spill_.empty() ? ring_.size() - ring_size_ : 0;
        const std::size_t to_ring = data.size() < ring_space ? data.size() : ring_space;
        const std::string_view spill_part = data.substr(to_ring);
        const bool can_merge = !spill_part.empty() && !spill_.empty()
            && spill_.back().size() + spill_part.size() <= kMaxMergedSegmentBytes;
        if (!spill_part.empty() && !can_merge && spill_.size() >= max_segments_) {
            return std::make_error_code(std::errc::no_buffer_space);
        }

        if (!spill_part.empty()) {
            if (can_merge) {
                spill_.back().append(spill_part.data(), spill_part.size());
            } else {
                spill_.emplace_back(spill_part);
            }
            list_bytes_ += spill_part.size();
        }
        if (to_ring != 0) {
            const std::size_t write_pos = (ring_head_ + ring_size_) % ring_.size();
            const std::size_t tail_space = ring_.size() - write_pos;
            const std::size_t first = to_ring < tail_space ? to_ring : tail_space;
            std::memcpy(ring_.data() + write_pos, data.data(), first);
            if (to_ring > first) {
                std::memcpy(ring_.data(), data.data() + first, to_ring - first);
            }
            ring_size_ += to_ring;
        }
        return {};
    }

    // writev 浏览：按发送顺序填最多 max_views 个连续段（ring 绕环时至多
    // 2 段 + list 各 1 段），返回实际填入数。视图仅在下一次本队列非 const
    // 调用前有效；跨非阻塞 sendv 的部分写用 ConsumeSent 表达，0 字节进展
    // 合法。段数不足 max_views 时调用者可循环重试，不得假定一次写完。
    std::size_t GatherViews(std::string_view* views, std::size_t max_views) const
    {
        std::size_t filled = 0;
        if (ring_size_ != 0 && filled < max_views) {
            const std::size_t contiguous =
                ring_size_ < ring_.size() - ring_head_ ? ring_size_ : ring_.size() - ring_head_;
            views[filled++] = std::string_view(
                reinterpret_cast<const char*>(ring_.data() + ring_head_), contiguous);
            if (ring_size_ > contiguous && filled < max_views) {
                views[filled++] = std::string_view(
                    reinterpret_cast<const char*>(ring_.data()), ring_size_ - contiguous);
            }
        }
        for (const auto& segment : spill_) {
            if (filled >= max_views) {
                break;
            }
            views[filled++] = std::string_view(segment.data(), segment.size());
        }
        return filled;
    }

    // 消费已发送前缀 n 字节：先 ring（可跨绕环），再按序消费 list 段；
    // 支持段内部分消费。n > TotalBytes 拒绝且不变；n == 0 合法。
    std::error_code ConsumeSent(std::size_t n)
    {
        if (n > TotalBytes()) {
            return std::make_error_code(std::errc::result_out_of_range);
        }
        if (n != 0 && ring_size_ != 0) {
            const std::size_t contiguous =
                ring_size_ < ring_.size() - ring_head_ ? ring_size_ : ring_.size() - ring_head_;
            const std::size_t first = n < contiguous ? n : contiguous;
            ring_head_ = (ring_head_ + first) % ring_.size();
            ring_size_ -= first;
            n -= first;
        }
        if (n != 0 && ring_size_ != 0) {
            // 绕环剩余部分（此时 head 已回卷到 0 之后仍用通式，不出错）。
            const std::size_t second = n < ring_size_ ? n : ring_size_;
            ring_head_ = (ring_head_ + second) % ring_.size();
            ring_size_ -= second;
            n -= second;
        }
        while (n != 0) {
            auto& segment = spill_.front();
            const std::size_t take = n < segment.size() ? n : segment.size();
            // ponytail: 段内部分消费用 erase(0)，O(段长) 拷贝；段受
            // kMaxMergedSegmentBytes/追加大小约束，后续 Engine 出现性能
            // 证据时再改段内偏移游标。
            segment.erase(0, take);
            list_bytes_ -= take;
            n -= take;
            if (segment.empty()) {
                spill_.pop_front();
            }
        }
        return {};
    }

private:
    std::vector<unsigned char> ring_;
    std::size_t ring_head_{0};
    std::size_t ring_size_{0};
    std::deque<std::string> spill_;     // 溢出段，按发送顺序
    std::size_t list_bytes_{0};
    std::size_t max_total_bytes_;
    std::size_t max_segments_;
    bool geometry_valid_{false};        // 构造时几何校验结果
};

} // namespace bbt::infra::net

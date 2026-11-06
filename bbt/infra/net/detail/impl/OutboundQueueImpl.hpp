#pragma once

// 成员实现由声明头末尾包含，保留调用点可见性与 inline。

namespace bbt::infra::net {

inline OutboundQueue::OutboundQueue(std::size_t ring_bytes, std::size_t max_total_bytes, std::size_t max_segments)
    : max_total_bytes_(max_total_bytes), max_segments_(max_segments)
{
    if (ring_bytes != 0 && max_total_bytes != 0 && max_segments != 0 && ring_bytes <= max_total_bytes) {
        geometry_valid_ = true;
        ring_.resize(ring_bytes);
    }
}

inline OutboundQueue::OutboundQueue(OutboundQueue&& other)
    : max_total_bytes_(other.max_total_bytes_), max_segments_(other.max_segments_),
      geometry_valid_(other.geometry_valid_)
{
    ring_.swap(other.ring_);
    spill_.swap(other.spill_);
    ring_head_ = std::exchange(other.ring_head_, 0);
    ring_size_ = std::exchange(other.ring_size_, 0);
    list_bytes_ = std::exchange(other.list_bytes_, 0);
}

inline OutboundQueue& OutboundQueue::operator=(OutboundQueue&& other) noexcept
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

inline std::size_t OutboundQueue::SegmentCount() const noexcept
{
    return (ring_size_ != 0 ? 1 : 0) + spill_.size();
}

inline std::error_code OutboundQueue::Append(std::string_view data)
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
    const std::size_t ring_space = spill_.empty() ? ring_.size() - ring_size_ : 0;
    const std::size_t to_ring = data.size() < ring_space ? data.size() : ring_space;
    const std::string_view spill_part = data.substr(to_ring);
    const bool can_merge =
        !spill_part.empty() && !spill_.empty() && spill_.back().size() + spill_part.size() <= kMaxMergedSegmentBytes;
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

inline std::size_t OutboundQueue::GatherViews(std::string_view* views, std::size_t max_views) const
{
    std::size_t filled = 0;
    if (ring_size_ != 0 && filled < max_views) {
        const std::size_t contiguous = ring_size_ < ring_.size() - ring_head_ ? ring_size_ : ring_.size() - ring_head_;
        views[filled++] = std::string_view(reinterpret_cast<const char*>(ring_.data() + ring_head_), contiguous);
        if (ring_size_ > contiguous && filled < max_views) {
            views[filled++] = std::string_view(reinterpret_cast<const char*>(ring_.data()), ring_size_ - contiguous);
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

inline std::error_code OutboundQueue::ConsumeSent(std::size_t n)
{
    if (n > TotalBytes()) {
        return std::make_error_code(std::errc::result_out_of_range);
    }
    if (n != 0 && ring_size_ != 0) {
        const std::size_t contiguous = ring_size_ < ring_.size() - ring_head_ ? ring_size_ : ring_.size() - ring_head_;
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

} // namespace bbt::infra::net

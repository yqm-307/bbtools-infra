#pragma once

// 成员实现由声明头末尾包含，保留调用点可见性与 inline。

namespace bbt::infra::net {

inline EventSnapshot::EventSnapshot(EventSnapshot&& other) noexcept
    : data_(std::move(other.data_)), cursor_(std::exchange(other.cursor_, 0))
{
}

inline EventSnapshot& EventSnapshot::operator=(EventSnapshot&& other) noexcept
{
    if (this != &other) {
        data_ = std::move(other.data_);
        cursor_ = std::exchange(other.cursor_, 0);
    }
    return *this;
}

inline std::error_code EventSnapshot::Discard(std::size_t n)
{
    if (n > Remaining()) {
        return std::make_error_code(std::errc::result_out_of_range);
    }
    cursor_ += n;
    return {};
}

inline std::error_code EventSnapshot::Consume(std::size_t n, std::string& out)
{
    if (n > Remaining()) {
        return std::make_error_code(std::errc::result_out_of_range);
    }
    out.append(data_, cursor_, n);
    cursor_ += n;
    return {};
}

inline EventSnapshot::EventSnapshot(std::string data) : data_(std::move(data))
{
}

inline EventRingBuffer::EventRingBuffer(std::size_t max_bytes) noexcept : max_bytes_(max_bytes)
{
}

inline EventRingBuffer::EventRingBuffer(EventRingBuffer&& other) noexcept
    : storage_(std::move(other.storage_)), max_bytes_(other.max_bytes_)
{
}

inline EventRingBuffer& EventRingBuffer::operator=(EventRingBuffer&& other) noexcept
{
    if (this != &other) {
        storage_ = std::move(other.storage_);
        max_bytes_ = other.max_bytes_;
    }
    return *this;
}

inline std::error_code EventRingBuffer::Append(std::string_view data)
{
    if (data.empty()) {
        return {};
    }
    if (data.size() > max_bytes_ - Size()) {
        return std::make_error_code(std::errc::no_buffer_space);
    }
    const auto overlaps = [data](const auto& segment) {
        // std::less 提供不相关指针的全序；只在各有效区间内求尾指针。
        const std::less<const char*> less;
        return segment.second != 0 && less(data.data(), segment.first + segment.second)
               && less(segment.first, data.data() + data.size());
    };
    std::string owned_input;
    if (overlaps(storage_.array_one()) || overlaps(storage_.array_two())) {
        owned_input.assign(data.data(), data.size());
        data = owned_input;
    }
    const std::size_t need = Size() + data.size();
    if (need > Capacity()) {
        storage_.set_capacity(GrowTarget(need)); // 只增长，强异常保证
    }
    // 足够空位且 char 复制不抛；不会进入 Boost 满缓冲的覆盖语义。
    storage_.insert(storage_.end(), data.begin(), data.end());
    return {};
}

inline std::error_code EventRingBuffer::Reserve(std::size_t total_capacity)
{
    if (total_capacity <= Capacity()) {
        return {};
    }
    if (total_capacity > max_bytes_) {
        return std::make_error_code(std::errc::no_buffer_space);
    }
    storage_.set_capacity(GrowTarget(total_capacity)); // 只增长，强异常保证
    return {};
}

inline std::string_view EventRingBuffer::Peek() const noexcept
{
    if (Empty()) {
        return {};
    }
    const auto segment = storage_.array_one();
    return std::string_view(segment.first, segment.second);
}

inline std::error_code EventRingBuffer::Discard(std::size_t n)
{
    if (n > Size()) {
        return std::make_error_code(std::errc::result_out_of_range);
    }
    storage_.erase_begin(n);
    return {};
}

inline std::error_code EventRingBuffer::Consume(std::size_t n, std::string& out)
{
    if (n > Size()) {
        return std::make_error_code(std::errc::result_out_of_range);
    }
    if (n == 0) {
        return {};
    }
    const auto one = storage_.array_one();
    const std::size_t first = n < one.second ? n : one.second;
    out.append(one.first, first);
    if (n > first) {
        out.append(storage_.array_two().first, n - first);
    }
    storage_.erase_begin(n);
    return {};
}

inline EventSnapshot EventRingBuffer::TakeSnapshot() const
{
    std::string data;
    const auto one = storage_.array_one();
    const auto two = storage_.array_two();
    if (one.second != 0) {
        data.append(one.first, one.second);
    }
    if (two.second != 0) {
        data.append(two.first, two.second);
    }
    return EventSnapshot(std::move(data));
}

inline std::shared_ptr<const boost::circular_buffer<char>> EventRingBuffer::TakeStorage()
{
    // boost::circular_buffer 的移动构造转移底层分配指针；移后 storage_
    // Size/Capacity 为 0（与类头移动语义注释一致），可直接复用追加。
    return std::make_shared<const boost::circular_buffer<char>>(std::move(storage_));
}

inline std::size_t EventRingBuffer::GrowTarget(std::size_t need) const noexcept
{
    std::size_t target = Capacity() == 0 ? (max_bytes_ < kInitialCapacity ? max_bytes_ : kInitialCapacity) : Capacity();
    while (target < need) {
        if (target > max_bytes_ / 2) {
            return max_bytes_;
        }
        target *= 2;
    }
    return target;
}

} // namespace bbt::infra::net

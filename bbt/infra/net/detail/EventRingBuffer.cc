#include <bbt/infra/net/detail/EventRingBuffer.hpp>

namespace bbt::infra::net {

EventSnapshot::EventSnapshot(EventSnapshot&& other) noexcept
    : data_(std::move(other.data_))
    , cursor_(std::exchange(other.cursor_, 0))
{
}

EventSnapshot& EventSnapshot::operator=(EventSnapshot&& other) noexcept
{
    if (this != &other) {
        data_ = std::move(other.data_);
        cursor_ = std::exchange(other.cursor_, 0);
    }
    return *this;
}

std::error_code EventSnapshot::Discard(std::size_t n)
{
    if (n > Remaining()) {
        return std::make_error_code(std::errc::result_out_of_range);
    }
    cursor_ += n;
    return {};
}

std::error_code EventSnapshot::Consume(std::size_t n, std::string& out)
{
    if (n > Remaining()) {
        return std::make_error_code(std::errc::result_out_of_range);
    }
    out.append(data_, cursor_, n);
    cursor_ += n;
    return {};
}

EventSnapshot::EventSnapshot(std::string data)
    : data_(std::move(data))
{
}

EventRingBuffer::EventRingBuffer(std::size_t max_bytes) noexcept
    : storage_(max_bytes)
    , max_bytes_(max_bytes)
{
}

EventRingBuffer::EventRingBuffer(EventRingBuffer&& other) noexcept
    : storage_(std::move(other.storage_))
    , max_bytes_(other.max_bytes_)
{
}

EventRingBuffer& EventRingBuffer::operator=(EventRingBuffer&& other) noexcept
{
    if (this != &other) {
        storage_ = std::move(other.storage_);
        max_bytes_ = other.max_bytes_;
    }
    return *this;
}

std::error_code EventRingBuffer::Append(std::string_view data)
{
    if (data.empty()) {
        return {};
    }
    if (data.size() > max_bytes_ - Size()) {
        return std::make_error_code(std::errc::no_buffer_space);
    }
    const auto overlaps = [this, data] {
        const std::less<const char*> less;
        for (const auto segment : storage_.data()) {
            const auto* begin = static_cast<const char*>(segment.data());
            if (segment.size() != 0 && less(data.data(), begin + segment.size())
                && less(begin, data.data() + data.size())) {
                return true;
            }
        }
        return false;
    };
    std::string owned_input;
    if (overlaps()) {
        owned_input.assign(data.data(), data.size());
        data = owned_input;
    }

    /**
     * prepare 保留可读段地址，输出区与可读区不重叠；别名输入已在上面复制。
     */
    const auto output = storage_.prepare(data.size());
    boost::asio::buffer_copy(output, boost::asio::buffer(data.data(), data.size()));
    storage_.commit(data.size());
    return {};
}

std::error_code EventRingBuffer::Reserve(std::size_t total_capacity)
{
    if (total_capacity <= Capacity()) {
        return {};
    }
    if (total_capacity > max_bytes_) {
        return std::make_error_code(std::errc::no_buffer_space);
    }
    storage_.reserve(total_capacity);
    return {};
}

std::string_view EventRingBuffer::Peek() const noexcept
{
    if (Empty()) {
        return {};
    }
    const auto segments = storage_.data();
    const auto segment = *segments.begin();
    return std::string_view(static_cast<const char*>(segment.data()), segment.size());
}

std::error_code EventRingBuffer::Discard(std::size_t n)
{
    if (n > Size()) {
        return std::make_error_code(std::errc::result_out_of_range);
    }
    storage_.consume(n);
    return {};
}

std::error_code EventRingBuffer::Consume(std::size_t n, std::string& out)
{
    if (n > Size()) {
        return std::make_error_code(std::errc::result_out_of_range);
    }
    if (n == 0) {
        return {};
    }

    /**
     * 先确保输出容量；分段追加期间不得因分配失败留下部分输出。
     */
    if (n > out.max_size() - out.size()) {
        throw std::length_error("buffer output too long");
    }
    out.reserve(out.size() + n);
    std::size_t remaining = n;
    for (const auto segment : storage_.data()) {
        const auto count = (std::min)(remaining, segment.size());
        out.append(static_cast<const char*>(segment.data()), count);
        remaining -= count;
        if (remaining == 0) {
            break;
        }
    }
    storage_.consume(n);
    return {};
}

EventSnapshot EventRingBuffer::TakeSnapshot() const
{
    std::string data;
    data.reserve(Size());
    for (const auto segment : storage_.data()) {
        data.append(static_cast<const char*>(segment.data()), segment.size());
    }
    return EventSnapshot(std::move(data));
}

// clang-format off
}
// clang-format on

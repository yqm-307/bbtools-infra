#include <bbt/infra/net/Buffer.hpp>

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <utility>

#include <boost/asio/buffer.hpp>
#include <boost/beast/core/multi_buffer.hpp>

namespace bbt::infra::net {

struct Buffer::WriteStorage {
    boost::beast::multi_buffer bytes;
};

Buffer::Buffer() noexcept = default;
Buffer::~Buffer() = default;

Buffer::Buffer(Buffer&& other) noexcept
    : Buffer()
{
    *this = std::move(other);
}

Buffer& Buffer::operator=(Buffer&& other) noexcept
{
    if (this != &other) {
        m_backing = std::move(other.m_backing);
        m_segments = std::move(other.m_segments);
        m_sourceBytes = std::exchange(other.m_sourceBytes, 0);
        m_writeStorage = std::move(other.m_writeStorage);
        m_view = std::move(other.m_view);
        m_error = std::exchange(other.m_error, std::error_code{});
        other.m_segments.clear();
        other.m_view.clear();
    }
    return *this;
}

std::size_t Buffer::Size() const noexcept
{
    return m_sourceBytes + (m_writeStorage ? m_writeStorage->bytes.size() : 0);
}

std::size_t Buffer::SegmentCount() const noexcept
{
    auto count = m_segments.size();
    if (m_writeStorage) {
        for (const auto segment : m_writeStorage->bytes.cdata()) {
            if (segment.size() != 0) {
                ++count;
            }
        }
    }
    return count;
}

const std::vector<BufferSegment>& Buffer::Segments() const
{
    if (!m_writeStorage || m_writeStorage->bytes.size() == 0) {
        return m_segments;
    }
    if (m_view.empty()) {

        /**
         * 写热路径只操作 multi_buffer；消费者实际请求视图时才生成段元数据。
         */
        m_view.reserve(SegmentCount());
        m_view.insert(m_view.end(), m_segments.begin(), m_segments.end());
        for (const auto segment : m_writeStorage->bytes.cdata()) {
            if (segment.size() != 0) {
                m_view.push_back({static_cast<const char*>(segment.data()), segment.size()});
            }
        }
    }
    return m_view;
}

std::string Buffer::CopyTo() const
{
    std::string out;
    AppendTo(out);
    return out;
}

void Buffer::AppendTo(std::string& out) const
{
    if (Size() > out.max_size() - out.size()) {
        throw std::length_error("buffer output too long");
    }
    out.reserve(out.size() + Size());
    for (const auto& segment : m_segments) {
        out.append(segment.data, segment.size);
    }
    if (m_writeStorage) {
        for (const auto segment : m_writeStorage->bytes.cdata()) {
            out.append(static_cast<const char*>(segment.data()), segment.size());
        }
    }
}

bool Buffer::Reserve(std::size_t additionalBytes)
{
    if (m_error) {
        return false;
    }
    if (additionalBytes > (std::numeric_limits<std::size_t>::max)() - Size()) {
        return _Fail(std::errc::value_too_large);
    }
    if (additionalBytes == 0) {
        return true;
    }
    if (!m_writeStorage) {
        m_writeStorage = std::make_unique<WriteStorage>();
    }
    m_writeStorage->bytes.reserve(m_writeStorage->bytes.size() + additionalBytes);
    return true;
}

bool Buffer::_Append(std::string_view prefix, std::string_view bytes)
{
    if (m_error) {
        return false;
    }
    const auto available = (std::numeric_limits<std::size_t>::max)() - Size();
    if (prefix.size() > available || bytes.size() > available - prefix.size()) {
        return _Fail(std::errc::value_too_large);
    }
    const auto count = prefix.size() + bytes.size();
    if (count == 0) {
        return true;
    }
    if (!m_writeStorage) {
        m_writeStorage = std::make_unique<WriteStorage>();
    }

    /**
     * prepare 保持已有可读地址，且输出区不重叠；自追加也不必复制输入临时串。
     * 所有分配先于 commit，字符串长度和正文不会被半提交。
     */
    const auto output = m_writeStorage->bytes.prepare(count);
    const std::array<boost::asio::const_buffer, 2> input{
        boost::asio::buffer(prefix.data(), prefix.size()), boost::asio::buffer(bytes.data(), bytes.size())};
    boost::asio::buffer_copy(output, input);
    m_writeStorage->bytes.commit(count);
    m_view.clear();
    return true;
}

bool Buffer::WriteBytes(std::string_view bytes)
{
    return _Append({}, bytes);
}

bool Buffer::_Fail(std::errc error) noexcept
{
    if (!m_error) {
        m_error = std::make_error_code(error);
    }
    return false;
}

bool Buffer::_RequireReadable(std::size_t count) noexcept
{
    if (m_error) {
        return false;
    }
    return count <= Size() ? true : _Fail(std::errc::result_out_of_range);
}

void Buffer::_CopyBytes(std::size_t offset, void* out, std::size_t count) const noexcept
{
    auto* destination = static_cast<char*>(out);
    const auto copy = [&](const char* data, std::size_t size) {
        if (offset >= size) {
            offset -= size;
            return;
        }
        const auto take = (std::min)(count, size - offset);
        if (take != 0) {
            std::memcpy(destination, data + offset, take);
            destination += take;
            count -= take;
        }
        offset = 0;
    };
    for (const auto& segment : m_segments) {
        if (count == 0) {
            return;
        }
        copy(segment.data, segment.size);
    }
    if (m_writeStorage) {
        for (const auto segment : m_writeStorage->bytes.cdata()) {
            if (count == 0) {
                return;
            }
            copy(static_cast<const char*>(segment.data()), segment.size());
        }
    }
}

void Buffer::_Consume(std::size_t count) noexcept
{
    const auto fromSource = (std::min)(count, m_sourceBytes);
    auto remaining = fromSource;
    std::size_t removed = 0;
    while (remaining != 0) {
        auto& segment = m_segments[removed];
        const auto take = (std::min)(remaining, segment.size);
        remaining -= take;
        segment.data += take;
        segment.size -= take;
        if (segment.size == 0) {
            ++removed;
        }
    }

    /**
     * 只在跨过存储段时移动段元数据，逐字段消费不搬移载荷。
     */
    m_segments.erase(m_segments.begin(), m_segments.begin() + removed);
    m_sourceBytes -= fromSource;
    if (m_sourceBytes == 0) {
        m_backing.reset();
    }
    if (m_writeStorage && count > fromSource) {
        m_writeStorage->bytes.consume(count - fromSource);
    }
    m_view.clear();
}

bool Buffer::ReadBytes(void* out, std::size_t count)
{
    if (!_RequireReadable(count)) {
        return false;
    }
    if (count != 0 && out == nullptr) {
        return _Fail(std::errc::invalid_argument);
    }
    _CopyBytes(0, out, count);
    _Consume(count);
    return true;
}

bool Buffer::Consume(std::size_t count) noexcept
{
    if (!_RequireReadable(count)) {
        return false;
    }
    _Consume(count);
    return true;
}

void Buffer::Clear() noexcept
{
    m_segments.clear();
    m_sourceBytes = 0;
    m_backing.reset();
    if (m_writeStorage) {
        m_writeStorage->bytes.clear();
    }
    m_view.clear();
    m_error.clear();
}

Buffer& Buffer::operator<<(std::string_view value)
{
    if (m_error) {
        return *this;
    }
    if (value.size() > (std::numeric_limits<std::uint32_t>::max)()) {
        _Fail(std::errc::value_too_large);
        return *this;
    }
    const auto length = boost::endian::native_to_big(static_cast<std::uint32_t>(value.size()));
    _Append({reinterpret_cast<const char*>(&length), sizeof(length)}, value);
    return *this;
}

Buffer& Buffer::operator<<(const char* value)
{
    if (value == nullptr) {
        _Fail(std::errc::invalid_argument);
        return *this;
    }
    return *this << std::string_view(value);
}

Buffer& Buffer::operator>>(std::string& value)
{
    std::uint32_t encoded{};
    if (!_RequireReadable(sizeof(encoded))) {
        return *this;
    }
    _CopyBytes(0, &encoded, sizeof(encoded));
    const auto length = boost::endian::big_to_native(encoded);

    /**
     * 减法检查先于正文分配，截断输入和不可信长度不能导致越界或提前消费。
     */
    if (length > Size() - sizeof(encoded)) {
        _Fail(std::errc::result_out_of_range);
        return *this;
    }
    std::string decoded(length, '\0');
    _CopyBytes(sizeof(encoded), decoded.data(), length);
    value = std::move(decoded);
    _Consume(sizeof(encoded) + length);
    return *this;
}

Buffer& Buffer::operator<<(bool value)
{
    const char encoded = value ? 1 : 0;
    WriteBytes({&encoded, 1});
    return *this;
}

Buffer& Buffer::operator>>(bool& value)
{
    if (!_RequireReadable(1)) {
        return *this;
    }
    unsigned char encoded{};
    _CopyBytes(0, &encoded, 1);
    if (encoded > 1) {
        _Fail(std::errc::illegal_byte_sequence);
        return *this;
    }
    value = encoded != 0;
    _Consume(1);
    return *this;
}

Buffer& Buffer::operator<<(float value)
{
    static_assert(
        sizeof(value) == sizeof(std::uint32_t) && std::numeric_limits<float>::is_iec559,
        "Buffer requires IEEE 754 binary32");
    std::uint32_t bits{};
    std::memcpy(&bits, &value, sizeof(bits));
    return *this << bits;
}

Buffer& Buffer::operator>>(float& value)
{
    std::uint32_t bits{};
    if (*this >> bits) {
        std::memcpy(&value, &bits, sizeof(value));
    }
    return *this;
}

Buffer& Buffer::operator<<(double value)
{
    static_assert(
        sizeof(value) == sizeof(std::uint64_t) && std::numeric_limits<double>::is_iec559,
        "Buffer requires IEEE 754 binary64");
    std::uint64_t bits{};
    std::memcpy(&bits, &value, sizeof(bits));
    return *this << bits;
}

Buffer& Buffer::operator>>(double& value)
{
    std::uint64_t bits{};
    if (*this >> bits) {
        std::memcpy(&value, &bits, sizeof(value));
    }
    return *this;
}

Buffer::Buffer(std::shared_ptr<void> backing, std::vector<BufferSegment> segments)
    : m_backing(std::move(backing))
    , m_segments(std::move(segments))
{
    for (const auto& segment : m_segments) {
        m_sourceBytes += segment.size;
    }
}

// clang-format off
}
// clang-format on

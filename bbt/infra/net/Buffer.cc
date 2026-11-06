#include <bbt/infra/net/Buffer.hpp>
#include <utility>

namespace bbt::infra::net {

std::size_t Buffer::Size() const noexcept
{
    std::size_t total = 0;
    for (const auto& seg : m_segments) {
        total += seg.size;
    }
    return total;
}

std::string Buffer::CopyTo() const
{
    std::string out;
    out.reserve(Size());
    for (const auto& seg : m_segments) {
        out.append(seg.data, seg.size);
    }
    return out;
}

void Buffer::AppendTo(std::string& out) const
{
    for (const auto& seg : m_segments) {
        out.append(seg.data, seg.size);
    }
}

Buffer::Buffer(std::shared_ptr<void> backing, std::vector<BufferSegment> segments)
    : m_backing(std::move(backing)), m_segments(std::move(segments))
{
}

} // namespace bbt::infra::net

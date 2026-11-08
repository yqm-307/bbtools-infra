#pragma once

namespace bbt::infra::http {
namespace detail {

template <class ConstBufferSequence> inline std::size_t StringSink::write_some(const ConstBufferSequence& buffers)
{
    std::size_t total = 0;
    for (auto it = boost::asio::buffer_sequence_begin(buffers); it != boost::asio::buffer_sequence_end(buffers); ++it) {
        const boost::asio::const_buffer& buffer = *it; // 迭代器可能无 operator->
        m_out.append(static_cast<const char*>(buffer.data()), buffer.size());
        total += buffer.size();
    }
    return total;
}

template <class ConstBufferSequence>
inline std::size_t StringSink::write_some(const ConstBufferSequence& buffers, boost::system::error_code& ec)
{
    ec = {};
    return write_some(buffers);
}

} // namespace detail
} // namespace bbt::infra::http

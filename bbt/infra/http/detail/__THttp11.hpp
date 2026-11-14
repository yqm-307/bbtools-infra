/**
 * @file __THttp11.hpp
 * @brief StringSink 的模板定义，由 Http11.hpp 末尾包含。
 */
#pragma once

namespace bbt::infra::http {
namespace detail {

template <class ConstBufferSequence> inline std::size_t StringSink::write_some(const ConstBufferSequence& buffers)
{
    std::size_t total = 0;
    for (auto it = boost::asio::buffer_sequence_begin(buffers); it != boost::asio::buffer_sequence_end(buffers); ++it) {

        /**
         * buffer_sequence 的迭代器不一定提供 operator->，先解引用成 const_buffer
         * 再取 data()/size()，避免依赖具体序列类型。
         */
        const boost::asio::const_buffer& buffer = *it;
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

// clang-format off
}
// clang-format on

// clang-format off
}
// clang-format on

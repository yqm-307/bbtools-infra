#pragma once

/**
 * @file ReadView.hpp
 * @brief 接收 ring 的短生命周期只读视图。
 *
 * View 不持有连接状态、锁或底层存储，也不消费数据。返回的字节只在本次同步使用期间有效；
 * 同一连接的下一次 I/O、View 或并发 owner 写入都可能使视图失效。需要跨调用保存时使用 Read。
 */
#include <cstddef>
#include <string_view>
#include <system_error>

#include <bbt/infra/net/Types.hpp>

namespace bbt::infra::net {

class ReadView {
public:
    ReadView() noexcept = default;

    /**
     * @brief 是否取得了非空的可读视图。
     */
    explicit operator bool() const noexcept { return m_status == ReadStatus::Ok; }

    /**
     * @brief 本次视图状态；Ok 时 Bytes() 可读。
     */
    ReadStatus Status() const noexcept { return m_status; }

    /**
     * @brief 当前连续可读区域；不复制、不消费。
     */
    std::string_view Bytes() const noexcept { return m_bytes; }

    /**
     * @brief 当前连续区域长度。
     */
    std::size_t Size() const noexcept { return m_bytes.size(); }

    /**
     * @brief Busy/WouldBlock/Eof/Error 的错误原因；Ok 时为空。
     */
    std::error_code Error() const noexcept { return m_error; }

private:
    friend class Connection;
    friend class ReceiveRing;

    ReadView(ReadStatus status, std::string_view bytes, std::error_code error)
        : m_status(status)
        , m_bytes(bytes)
        , m_error(error)
    {
    }

    ReadStatus m_status{ReadStatus::WouldBlock};
    std::string_view m_bytes;
    std::error_code m_error;
};

// clang-format off
}
// clang-format on

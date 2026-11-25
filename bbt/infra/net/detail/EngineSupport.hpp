#pragma once

#include <memory>
#include <system_error>
#include <unistd.h>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <bbt/infra/net/detail/ReclaimRecord.hpp>

namespace bbt::infra::net {

namespace asio = boost::asio;
using TcpWait = asio::posix::stream_descriptor;

/**
 * 可移动 fd RAII：post 失败/关闭竞争时兜底关闭，交接时 Release。
 */
class FdHandle {
public:
    FdHandle() = default;

    explicit FdHandle(int fd);

    ~FdHandle();

    FdHandle(const FdHandle&) = delete;
    FdHandle& operator=(const FdHandle&) = delete;

    FdHandle(FdHandle&& other) noexcept;

    FdHandle& operator=(FdHandle&& other) noexcept;

    int Get() const noexcept { return m_fd; }

    int Release() noexcept;

    /**
     * 显式物理 close 点：责任转移/失败收口前调用，使「记录释放」晚于真实关闭。
     */
    void Close() noexcept;

private:
    int m_fd{-1};
};

std::error_code FromBoost(const boost::system::error_code& error);
void MarkReclaimReleased(const std::shared_ptr<ReclaimRecord>& record);

// clang-format off
}
// clang-format on

#include <bbt/infra/net/detail/EngineSupport.hpp>

namespace bbt::infra::net {

FdHandle::FdHandle(int fd)
    : m_fd(fd)
{
}

FdHandle::~FdHandle()
{
    Close();
}

FdHandle::FdHandle(FdHandle&& other) noexcept
    : m_fd(other.m_fd)
{
    other.m_fd = -1;
}

FdHandle& FdHandle::operator=(FdHandle&& other) noexcept
{
    if (this != &other) {
        Close();
        m_fd = other.m_fd;
        other.m_fd = -1;
    }
    return *this;
}

int FdHandle::Release() noexcept
{
    const int fd = m_fd;
    m_fd = -1;
    return fd;
}

void FdHandle::Close() noexcept
{
    if (m_fd >= 0) {
        ::close(m_fd);
        m_fd = -1;
    }
}

// clang-format off
}
// clang-format on
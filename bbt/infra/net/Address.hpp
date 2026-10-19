#pragma once

#include <sys/socket.h>

#include <cstdint>
#include <string>
#include <system_error>

namespace bbt::infra::net {

enum class AddressFamily { Unspecified, IPv4, IPv6, Unix };

class Address {
public:
    // Numeric only：不调用 DNS，不截断非法端口。
    // IPv6 scope 只支持严格十进制数字（完整消费、u32 范围，C++17 from_chars）；
    // 拒绝负号、空、尾随垃圾、溢出、重复 '%'、IPv4/主机名带 scope 与接口名
    // （无 if_nametoindex/atoi 回退）。scope 0 合法且 ToString 不输出。
    static Address  Ip(const std::string& host, int port, std::error_code& error);
    static Address  Unix(const std::string& path, std::error_code& error, bool abstract = false);
    // 内部/适配使用：从已 accept 的 native sockaddr 构造值快照（不保留指针）。
    // 失败时返回无效 Address（长度超 storage 或空）。
    static Address  FromNative(const sockaddr* addr, socklen_t length);
    AddressFamily   Family() const noexcept;
    std::uint16_t   Port() const noexcept;
    std::string     ToString() const;
    bool            Valid() const noexcept { return m_size != 0; }

private:
    friend class NativeSocket;
    sockaddr_storage m_storage{};
    socklen_t        m_size{0};
};

} // namespace bbt::infra::net

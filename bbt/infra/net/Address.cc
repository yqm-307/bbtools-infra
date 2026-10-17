#include <bbt/infra/net/Address.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/un.h>

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include <boost/asio/ip/address.hpp>

namespace {
// L3：Boost 的 scope 解析带 atoi 回退（%4294967296→0、%-1→u32max、%3garbage→3），
// 因此 scope 在进入 Boost 前自行严格校验：只支持完整十进制数字（C++17 from_chars，
// u32 范围），拒绝负号、空、尾随垃圾、溢出、重复 '%' 与 IPv4/主机名带 scope；
// 不支持接口名（无 if_nametoindex/atoi 回退）。返回 false 表示已置 error。
bool SplitV6Scope(const std::string& host, std::string* body, std::uint32_t* scope, std::error_code& error) {
    const auto percent = host.find('%');
    if (percent == std::string::npos) { return true; }
    const auto invalid = std::make_error_code(std::errc::invalid_argument);
    if (host.find('%', percent + 1) != std::string::npos) { error = invalid; return false; } // 重复 %
    *body = host.substr(0, percent);
    if (body->find(':') == std::string::npos) { error = invalid; return false; }             // IPv4/主机名不支持 scope
    const std::string suffix = host.substr(percent + 1);
    if (suffix.empty()) { error = invalid; return false; }
    const char* first = suffix.data();
    const char* last = first + suffix.size();
    std::uint32_t value = 0;
    const auto parsed = std::from_chars(first, last, value, 10);
    if (parsed.ec != std::errc{} || parsed.ptr != last) { error = invalid; return false; }   // 非数字/垃圾/溢出
    *scope = value;
    return true;
}
} // namespace

namespace bbt::infra::net {

Address Address::Ip(const std::string& host, int port, std::error_code& error) {
    error.clear();
    Address result;
    if (host.empty() || host.find('\0') != std::string::npos || port < 0 || port > 65535) {
        error = std::make_error_code(std::errc::invalid_argument);
        return result;
    }
    std::string body = host;
    std::uint32_t scope = 0;
    if (!SplitV6Scope(host, &body, &scope, error)) { return result; }
    boost::system::error_code parsed_error;
    const auto parsed = boost::asio::ip::make_address(body, parsed_error);
    if (parsed_error) {
        error = std::make_error_code(std::errc::invalid_argument);
        return result;
    }
    if (parsed.is_v4()) {
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<std::uint16_t>(port));
        const auto bytes = parsed.to_v4().to_bytes();
        std::memcpy(&addr.sin_addr, bytes.data(), bytes.size());
        std::memcpy(&result.m_storage, &addr, sizeof(addr));
        result.m_size = sizeof(addr);
    } else {
        sockaddr_in6 addr{};
        addr.sin6_family = AF_INET6;
        addr.sin6_port = htons(static_cast<std::uint16_t>(port));
        const auto v6 = parsed.to_v6();
        const auto bytes = v6.to_bytes();
        addr.sin6_scope_id = scope;
        std::memcpy(&addr.sin6_addr, bytes.data(), bytes.size());
        std::memcpy(&result.m_storage, &addr, sizeof(addr));
        result.m_size = sizeof(addr);
    }
    return result;
}

Address Address::Unix(const std::string& path, std::error_code& error, bool abstract) {
    error.clear();
    Address result;
    sockaddr_un addr{};
    if (path.empty() || path.find('\0') != std::string::npos || path.size() >= sizeof(addr.sun_path)) {
        error = std::make_error_code(std::errc::invalid_argument);
        return result;
    }
    addr.sun_family = AF_UNIX;
    const std::size_t offset = abstract ? 1 : 0;
    std::memcpy(addr.sun_path + offset, path.data(), path.size());
    result.m_size = static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + path.size() + 1);
    std::memcpy(&result.m_storage, &addr, sizeof(addr));
    return result;
}

AddressFamily Address::Family() const noexcept {
    switch (m_storage.ss_family) {
    case AF_INET: return AddressFamily::IPv4;
    case AF_INET6: return AddressFamily::IPv6;
    case AF_UNIX: return AddressFamily::Unix;
    default: return AddressFamily::Unspecified;
    }
}

std::uint16_t Address::Port() const noexcept {
    if (Family() == AddressFamily::IPv4) {
        sockaddr_in addr{};
        std::memcpy(&addr, &m_storage, sizeof(addr));
        return ntohs(addr.sin_port);
    }
    if (Family() == AddressFamily::IPv6) {
        sockaddr_in6 addr{};
        std::memcpy(&addr, &m_storage, sizeof(addr));
        return ntohs(addr.sin6_port);
    }
    return 0;
}

std::string Address::ToString() const {
    char text[INET6_ADDRSTRLEN]{};
    if (Family() == AddressFamily::IPv4) {
        sockaddr_in addr{};
        std::memcpy(&addr, &m_storage, sizeof(addr));
        if (!inet_ntop(AF_INET, &addr.sin_addr, text, sizeof(text))) { return {}; }
        return std::string(text) + ':' + std::to_string(Port());
    }
    if (Family() == AddressFamily::IPv6) {
        sockaddr_in6 addr{};
        std::memcpy(&addr, &m_storage, sizeof(addr));
        if (!inet_ntop(AF_INET6, &addr.sin6_addr, text, sizeof(text))) { return {}; }
        const std::string scope = addr.sin6_scope_id ? '%' + std::to_string(addr.sin6_scope_id) : "";
        return '[' + std::string(text) + scope + "]:" + std::to_string(Port());
    }
    if (Family() == AddressFamily::Unix) {
        sockaddr_un addr{};
        std::memcpy(&addr, &m_storage, sizeof(addr));
        const auto size = m_size > offsetof(sockaddr_un, sun_path) ? m_size - offsetof(sockaddr_un, sun_path) : 0;
        if (!size) { return {}; }
        if (!addr.sun_path[0]) { return '@' + std::string(addr.sun_path + 1, size - 1); }
        return std::string(addr.sun_path, strnlen(addr.sun_path, size));
    }
    return {};
}

} // namespace bbt::infra::net

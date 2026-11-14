#pragma once

/**
 * @file Address.hpp
 * @brief 网络端点地址值类型：IPv4、IPv6 与 Unix 域路径。
 *
 * 只做数字地址的解析与保存，不做 DNS 查询，也不做任何 I/O。
 */
#include <sys/socket.h>

#include <cstdint>
#include <string>
#include <system_error>

namespace bbt::infra::net {

/**
 * @brief 地址族。Unix 指 AF_UNIX 路径，包括 Linux 抽象命名空间。
 */
enum class AddressFamily { Unspecified, IPv4, IPv6, Unix };

/**
 * @brief 网络端点地址值。
 *
 * 值语义：内部保存一份 sockaddr 快照，不保留调用方传入的指针。解析只接受数字字面量，
 * 主机名直接失败，因此同一输入得到的地址与宿主的 DNS 和网卡配置无关。
 */
class Address {
public:
    /**
     * @brief 由数字主机字面量与端口构造 IPv4 或 IPv6 地址。
     *
     * 主机名不会被解析，直接报 invalid_argument。IPv6 scope 只接受完整消费、落在 u32 范围内的
     * 十进制数字：负号、空串、尾随字符、溢出、重复的 '%'，以及 IPv4 或主机名携带 scope，都会被拒绝。
     * 不支持接口名，因为 if_nametoindex 与 atoi 会把非法输入悄悄变成别的值。scope 为 0 合法，
     * 且 ToString 不输出它。
     *
     * @param host 点分 IPv4 或 IPv6 字面量。
     * @param port 端口号；超出 [0, 65535] 时报错，不截断。
     * @param[out] error 成功时清空，失败时写入 invalid_argument。
     * @return 解析得到的地址；失败时 Valid() 为 false。
     */
    static Address Ip(const std::string& host, int port, std::error_code& error);

    /**
     * @brief 构造 Unix 域路径地址。
     *
     * @param path 文件系统路径；abstract 为 true 时表示 Linux 抽象命名空间，此时 path 不对应文件系统节点。
     * @param[out] error 成功时清空；空串、含 '\0' 或超过 sun_path 容量时写入 invalid_argument。
     * @param abstract 是否放入抽象命名空间。
     * @return 解析得到的地址；失败时 Valid() 为 false。
     */
    static Address Unix(const std::string& path, std::error_code& error, bool abstract = false);

    /**
     * @brief 从 native sockaddr 构造地址快照，通常用于 accept 或 getsockname 之后。
     *
     * 数据按值拷贝，返回后 addr 指针即可失效。addr 为空、length 为 0，或超出 sockaddr_storage 时
     * 返回无效地址。
     *
     * @param addr 指向 native sockaddr 的指针。
     * @param length addr 的有效长度，单位为字节。
     * @return 地址快照；无效时 Valid() 为 false。
     */
    static Address FromNative(const sockaddr* addr, socklen_t length);

    /**
     * @brief 地址族；无效地址返回 Unspecified。
     */
    AddressFamily Family() const noexcept;

    /**
     * @brief 端口号，已从网络字节序转为主机字节序；Unix 地址与无效地址返回 0。
     */
    std::uint16_t Port() const noexcept;

    /**
     * @brief 文本表示，供日志与诊断使用。
     *
     * IPv4 为 "a.b.c.d:port"；IPv6 为 "[addr]:port"，scope 非 0 时为 "[addr%scope]:port"；
     * Unix 抽象地址以 '@' 开头。
     */
    std::string ToString() const;

    /**
     * @brief 地址是否有效，即保存了非零长度的 sockaddr。
     */
    bool Valid() const noexcept { return m_size != 0; }

    /**
     * @brief 地址是否仅限本机可达。
     *
     * 本机地址包括 IPv4 127.0.0.0/8、IPv6 ::1 与 Unix 域路径。其他地址，包括 0.0.0.0、::、
     * IPv4 映射的 IPv6 以及无法识别的地址族，一律返回 false。无法确认的地址宁可判为非本机，
     * 也不当作本机放行：靠名称或字符串比较判断 loopback 容易漏判，偏保守的那一侧才是安全的。
     */
    bool IsLoopback() const noexcept;

private:
    friend class NativeSocket;
    sockaddr_storage m_storage{};
    socklen_t m_size{0};
};

// clang-format off
}
// clang-format on

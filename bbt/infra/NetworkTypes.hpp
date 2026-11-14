/**
 * @file NetworkTypes.hpp
 * @brief 调用选项、网络装配值类型与协议消息形态。
 *
 * 签名契约见 docs/decisions/0002-co-network-contract.md。
 */
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include <bbt/infra/ICoObject.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra {

/**
 * @brief 调用选项，只携带单调时钟的绝对期限。
 *
 * 业务取消不由 infra 令牌表达：等待中的取消来自协程级 RequestCancel（WaitStatus::Cancelled），
 * 业务级取消由上层经带载荷的 Notify（CoEventValue）表达，再把 Completed 解释为业务终态。
 */
struct CallOptions {

    /**
     * 绝对截止时刻（单调时钟）。
     */
    bbt::coroutine::Deadline deadline;
};

/**
 * @brief 最大连接数上限。
 */
inline constexpr std::size_t kNetworkLimitsMaxConnections = 1000000;

/**
 * @brief 最大在途请求数上限。
 */
inline constexpr std::size_t kNetworkLimitsMaxInflight = 1000000;

/**
 * @brief 报文头上限（字节）的上限。
 */
inline constexpr std::size_t kNetworkLimitsMaxHeaderBytes = 16 * 1024 * 1024;

/**
 * @brief 报文体上限（字节）的上限。
 */
inline constexpr std::size_t kNetworkLimitsMaxBodyBytes = 1024 * 1024 * 1024;

/**
 * @brief 入站请求超时上限（24 小时）。
 */
inline constexpr std::chrono::milliseconds kNetworkLimitsMaxIncomingTimeout{std::chrono::hours{24}};

/**
 * @brief 网络层装配上限，由 ValidateNetworkLimits 校验。
 *
 * 各项须显式给出、大于零且不超过对应上限；incoming_timeout 另须不超过 24 小时。
 */
struct NetworkLimits {

    /**
     * 最大连接数，范围 (0, kNetworkLimitsMaxConnections]。
     */
    std::size_t max_connections;

    /**
     * 最大在途请求数，范围 (0, kNetworkLimitsMaxInflight]。
     */
    std::size_t max_inflight;

    /**
     * 报文头上限（字节），范围 (0, kNetworkLimitsMaxHeaderBytes]。
     */
    std::size_t max_header_bytes;

    /**
     * 报文体上限（字节），范围 (0, kNetworkLimitsMaxBodyBytes]。
     */
    std::size_t max_body_bytes;

    /**
     * 入站请求超时，范围 (0, kNetworkLimitsMaxIncomingTimeout]。
     */
    std::chrono::milliseconds incoming_timeout;
};

/**
 * @brief 监听地址：主机名或地址与端口。
 */
struct ListenAddress {

    /**
     * 监听主机。
     */
    std::string host;

    /**
     * 监听端口。
     */
    std::uint16_t port;
};

/**
 * @brief 入站调用的上下文：绝对截止时刻与对端主体。
 */
struct IncomingCallContext {

    /**
     * 入站调用的绝对截止时刻。
     */
    bbt::coroutine::Deadline deadline;

    /**
     * 对端主体，由可信传输提供；trusted-network 模式下恒为空。
     */
    std::string peer_principal;
};

/**
 * @brief HTTP 请求的协议消息形态。
 */
struct HttpRequest {

    /**
     * 请求方法，如 GET、POST。
     */
    std::string method;

    /**
     * 请求 URL。
     */
    std::string url;

    /**
     * 请求头键值对。
     */
    std::vector<std::pair<std::string, std::string>> headers;

    /**
     * 请求体。
     */
    std::string body;
};

/**
 * @brief HTTP 响应的协议消息形态。
 */
struct HttpResponse {

    /**
     * HTTP 状态码。
     */
    unsigned status;

    /**
     * 响应头键值对。
     */
    std::vector<std::pair<std::string, std::string>> headers;

    /**
     * 响应体。
     */
    std::string body;
};

/**
 * @brief 非拥有的可写缓冲视图：不持有内存，使用期间由调用方保证数据有效。
 */
struct MutableBytes {

    /**
     * 缓冲区首地址。
     */
    void* data;

    /**
     * 字节数。
     */
    std::size_t size;
};

/**
 * @brief 非拥有的只读缓冲视图：不持有内存，使用期间由调用方保证数据有效。
 */
struct ConstBytes {

    /**
     * 缓冲区首地址。
     */
    const void* data;

    /**
     * 字节数。
     */
    std::size_t size;
};

/**
 * @brief TCP 端点：主机与端口。
 */
struct TcpEndpoint {

    /**
     * 数值 IP 或主机名。
     */
    std::string host;

    /**
     * 端口号。
     */
    std::uint16_t port = 0;
};

/**
 * @brief 已解析的套接字地址：数值 IP 与端口。
 */
struct SocketAddress {

    /**
     * 数值 IP 地址。
     */
    std::string ip;

    /**
     * 端口号。
     */
    std::uint16_t port = 0;
};

/**
 * @brief 单次 I/O 的状态：Ok 表示有进展，WouldBlock 表示需等待就绪，Eof 表示对端已结束。
 */
enum class IoState { Ok, WouldBlock, Eof };

/**
 * @brief 单次 I/O 的进展：状态与本次传输字节数。
 */
struct IoProgress {

    /**
     * 本次 I/O 的状态。
     */
    IoState state;

    /**
     * 本次传输的字节数。
     */
    std::size_t bytes;
};

/**
 * @brief 单次 I/O 的结果：成功为 IoProgress，失败为 Error。
 */
using IoResult = result<IoProgress>;

/**
 * @brief 数据报读取的结果：状态、字节数、来源地址与截断标记。
 */
struct DatagramRead {

    /**
     * I/O 状态。
     */
    IoState state;

    /**
     * 拷贝进缓冲区的字节数，不计被截断的部分。
     */
    std::size_t bytes = 0;

    /**
     * 数据报来源地址。
     */
    SocketAddress peer;

    /**
     * 数据报超过缓冲区被截断时为 true。
     */
    bool truncated = false;
};

/**
 * @brief HTTP 入站处理函数：接收调用上下文与请求，返回响应或错误。
 */
using HttpHandler = std::function<result<HttpResponse>(IncomingCallContext, HttpRequest)>;

/**
 * @brief 校验 NetworkLimits 装配值：各项须大于零且不超过对应上限。
 * @param limits 待校验的装配值。
 * @return 通过返回 ok；违规返回 InvalidArgument。
 */
result<void> ValidateNetworkLimits(const NetworkLimits& limits);

// clang-format off
}
// clang-format on

#include <bbt/infra/CloseOptions.hpp>
#include <bbt/infra/config/Watch.hpp>
#include <bbt/infra/http/Client.hpp>
#include <bbt/infra/http/Server.hpp>
#include <bbt/infra/mongo/CoMongoDb.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/redis/CoRedisCli.hpp>
#include <bbt/infra/redis/CoRedisPool.hpp>
#include <bbt/infra/rpc/CoRpcClient.hpp>
#include <bbt/infra/rpc/CoRpcServer.hpp>

#include <type_traits>
#include <utility>

#ifdef BBT_CLOSE_MCP
#include <bbt/infra/mcp/McpHttpTransport.hpp>
#include <bbt/infra/mcp/McpStdioTransport.hpp>
#endif
#ifdef BBT_CLOSE_CONSUL
#include <bbt/infra/consul/Client.hpp>
#endif

namespace {
template <class T, class = void> struct HasRequestClose : std::false_type {};

template <class T>
struct HasRequestClose<T, std::void_t<decltype(std::declval<T&>().RequestClose())>> : std::true_type {};

template <class T, class = void> struct HasWaitStopped : std::false_type {};

template <class T>
struct HasWaitStopped<T, std::void_t<decltype(std::declval<T&>().WaitStopped(bbt::coroutine::Deadline{}))>>
    : std::true_type {};

template <class T> constexpr bool SingleClose()
{
    static_assert(!HasRequestClose<T>::value, "RequestClose must not be public");
    static_assert(!HasWaitStopped<T>::value, "WaitStopped must not be public");
    using CloseResult = decltype(std::declval<T&>().Close(bbt::infra::CloseOptions{}));
    return !std::is_void_v<CloseResult>;
}

static_assert(SingleClose<bbt::infra::http::Client>());
static_assert(SingleClose<bbt::infra::http::Server>());
static_assert(SingleClose<bbt::infra::config::Watcher>());
static_assert(SingleClose<bbt::infra::mongo::CoMongoDb>());
static_assert(SingleClose<bbt::infra::redis::CoRedisCli>());
static_assert(SingleClose<bbt::infra::redis::CoRedisPool>());
static_assert(SingleClose<bbt::infra::rpc::CoRpcClient>());
static_assert(SingleClose<bbt::infra::rpc::CoRpcServer>());
static_assert(SingleClose<bbt::infra::net::Connection>());
static_assert(SingleClose<bbt::infra::net::ICoConnection>());
static_assert(SingleClose<bbt::infra::net::Engine>());
#ifdef BBT_CLOSE_MCP
static_assert(SingleClose<bbt::infra::mcp::McpHttpTransport>());
static_assert(SingleClose<bbt::infra::mcp::McpStdioTransport>());
#endif
#ifdef BBT_CLOSE_CONSUL
static_assert(SingleClose<bbt::infra::consul::Client>());
#endif
// clang-format off
}
// clang-format on

int main()
{
    bbt::infra::net::Connection connection;
    bbt::infra::CloseOptions release;
    release.wait_release = true;
    if (connection.Close(release) != std::make_error_code(std::errc::invalid_argument))
        return 1;
    release.deadline = std::chrono::steady_clock::now();
    if (connection.Close(release) != std::make_error_code(std::errc::operation_not_permitted))
        return 2;

    /**
     * 默认 Immediate 必须能在没有运行时、也没有受管协程的进程里直接使用。
     */
    return connection.Close() ? 3 : 0;
}

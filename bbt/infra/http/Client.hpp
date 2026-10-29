#pragma once

// HTTPX 形态原生协程 HTTP 客户端（bbt::infra::http）：单一有栈协程 Client、
// 内建连接池，调用形态借鉴 httpx AsyncClient；不是其完整功能兼容实现。
// 已支持范围与验证入口见 docs/http-net.md，未实现能力在下文逐项声明。
//
// 职责边界：
//   - Client 只装配，不联网、不建线程、不建 Engine、无全局单例；执行底座
//     （coroutine scheduler + client-only net::Engine + 宿主 Run 线程）由应用
//     一次性装配并最终收口。多个 Client 可共享同一 Engine，池/默认配置互不共享。
//   - HTTP 4xx/5xx 是有效 Response（不是错误）；result 错误只表达传输/排队/
//     装配失败。错误码沿用 bbt::infra::ErrorCode。
//   - 编解码用 Beast；网络只经 bbt::infra::net 协程成员接口（Engine::CoDial、
//     CoTcpConnection::Read/Write），不触碰 Asio socket/native fd/net detail。
//
// 里程碑范围（诚实边界，未实现不得暗示）：
//   - 支持：GET/POST/Request（任意方法）、raw body、base_url/默认与请求级
//     headers/params 合并、分阶段超时、内建连接池（复用/上限/排队/退役/过期）。
//   - 不支持：https/TLS、主机名 DNS（host 仅数值 IP，net Address 同界）、
//     json/form 编码、流式响应、重定向、代理、HTTP/2、请求自动重试。

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/coroutine/syntax/Async.hpp>

namespace bbt::infra::http {

using Headers = std::vector<std::pair<std::string, std::string>>;
using QueryParams = std::vector<std::pair<std::string, std::string>>;

// 单档超时三态：Inherit（未指定，解析时取 Client 档）、有限值、Disabled
// （显式禁用该档）。单层 optional 表达不了三态，不用。
class PhaseTimeout {
public:
    static PhaseTimeout Inherit() noexcept { return PhaseTimeout{Kind::Inherit, {}}; }
    static PhaseTimeout Disabled() noexcept { return PhaseTimeout{Kind::Disabled, {}}; }
    // 有限值；非正值拒绝（调用方 Get/Post 返回 InvalidArgument）。
    static PhaseTimeout Milliseconds(std::chrono::milliseconds value) noexcept {
        return PhaseTimeout{Kind::Finite, value};
    }

    bool IsInherit() const noexcept { return m_kind == Kind::Inherit; }
    bool IsDisabled() const noexcept { return m_kind == Kind::Disabled; }
    // 仅有限值有输出；Inherit/Disabled 返回 nullopt。
    std::optional<std::chrono::milliseconds> Finite() const noexcept {
        if (m_kind != Kind::Finite) { return std::nullopt; }
        return m_value;
    }

private:
    enum class Kind { Inherit, Finite, Disabled };
    PhaseTimeout(Kind kind, std::chrono::milliseconds value) noexcept
        : m_kind(kind), m_value(value) {}
    Kind m_kind;
    std::chrono::milliseconds m_value{};
};

// 分阶段超时。默认四档均 5s（对齐 httpx Timeout(5.0) 均匀档），逐字段显式
// 写出，避免「一个字段默认值被误读为四档」。
struct Timeout {
    PhaseTimeout connect{PhaseTimeout::Milliseconds(std::chrono::milliseconds{5000})};
    PhaseTimeout read   {PhaseTimeout::Milliseconds(std::chrono::milliseconds{5000})};
    PhaseTimeout write  {PhaseTimeout::Milliseconds(std::chrono::milliseconds{5000})};
    PhaseTimeout pool   {PhaseTimeout::Milliseconds(std::chrono::milliseconds{5000})};

    static Timeout Uniform(PhaseTimeout phase) noexcept {
        Timeout value;
        value.connect = phase;
        value.read = phase;
        value.write = phase;
        value.pool = phase;
        return value;
    }
};

// 连接池配额：全局配额（active+idle+dialing+retiring 计入 max_connections），
// 无每 origin 限额。max_queue 是排队人数上界，满即 Overloaded（不静默无限排队）。
struct PoolLimits {
    std::size_t max_connections{8};
    std::size_t max_keepalive_connections{4};
    // 空闲连接保活过期；nullopt 表示不过期。过期在池 sweep 中惰性判定。
    std::optional<std::chrono::milliseconds> keepalive_expiry{
        std::chrono::milliseconds{15000}};
    std::size_t max_queue{32};
};

struct ClientOptions {
    // 例 "http://127.0.0.1:8080"；scheme 仅 http，host 仅数值 IP（DNS 未交付）。
    std::string base_url;
    Headers headers;      // client 级默认头（请求级同名覆盖）
    QueryParams params;   // client 级默认查询参数（请求级追加在后）
    Timeout timeout{};
    PoolLimits limits{};
    // 单响应体上限（Beast body_limit）；上限含 header 上界兜底。
    std::uint64_t max_body_bytes{8 * 1024 * 1024};
};

// 请求体（原始内容里程碑）。content 原样发送；content_type 非空时写入
// Content-Type（无请求级覆盖时）。json/form 编码未实现，不用互斥 union 假装。
struct RequestBody {
    std::string content;
    std::string content_type;
};

struct RequestOptions {
    Headers headers;                 // merge：client 打底，同名覆盖
    QueryParams params;              // merge：client 在前，请求级追加在后
    std::optional<Timeout> timeout;  // 分档覆盖；档为 Inherit 时取 Client 档
};

struct Response {
    int status_code{0};
    Headers headers;
    std::string body;
    bool Ok() const noexcept { return status_code >= 200 && status_code < 300; }
};

// BuildRequest 产物：已合并 base_url/headers/params、已解析超时（无 Inherit
// 残留）、已解析 origin/数值地址。Send 消费；纯值不联网。
// （命名说明：方法名 Client::Request 与值类型不能同名，值类型用 PreparedRequest。）
struct PreparedRequest {
    std::string method;
    std::string url;            // 合并 base_url 后的绝对 URL
    std::string target_for_wire;  // origin-form（path[?query]），HTTP/1.1 wire 目标
    net::Address address;       // 数值地址（Engine::CoDial 用）
    std::string host_header;    // 默认 Host（无用户覆盖时）
    Headers headers;
    std::string body;
    Timeout timeout;            // 已解析：任何档都不是 Inherit
};

// 连接池观测（产品公开面，供容量/退役验证；httpcore 池内省的对应物）。
struct PoolStats {
    std::size_t active{0};    // 在飞请求占用
    std::size_t idle{0};      // 空闲可复用
    std::size_t dialing{0};   // 建连中
    std::size_t retiring{0};  // 退役中（已请求关闭，真实回收凭据尚未完成）
    std::size_t waiting{0};   // 排队等待者
    std::size_t budget{0};    // 以上连接之和，受 max_connections 约束
};

class Client {
public:
    // 只装配：校验 options、构造池；不联网。Engine 由应用装配并保证可推进。
    static result<std::shared_ptr<Client>> Create(std::shared_ptr<net::Engine> engine,
                                                  ClientOptions options);
    ~Client() noexcept;
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    // BuildRequest 纯构造，不挂起（可在协程外调用）。URL/base_url 合并、参数
    // 编码、headers/超时解析都在此完成。
    result<PreparedRequest> BuildRequest(std::string_view method, std::string_view url,
                                         const RequestOptions& options) const;

    // 以下入口挂起等待，须在协程上下文调用（bbt_co_await 消费）。
    // 省略 call 时默认无总期限（Deadline::max()），各阶段默认超时限速仍生效；
    // 显式传 CallOptions{} 表示已过期（deadline 为 epoch），会被拒绝——
    // 省略与显式零值是两种语义，不是同一缺省。
    bbt_co_async(result<Response>) Get(std::string_view url, const RequestOptions& options,
                                       const CallOptions& call = {
                                           bbt::coroutine::Deadline::max()});
    bbt_co_async(result<Response>) Post(std::string_view url, RequestBody body,
                                        const RequestOptions& options,
                                        const CallOptions& call = {
                                            bbt::coroutine::Deadline::max()});
    bbt_co_async(result<Response>) Request(std::string_view method, std::string_view url,
                                           const RequestOptions& options,
                                           const CallOptions& call = {
                                               bbt::coroutine::Deadline::max()});
    bbt_co_async(result<Response>) Send(const PreparedRequest& request,
                                        const CallOptions& call = {
                                            bbt::coroutine::Deadline::max()});

    // 立即逻辑封口：拒绝新请求、唤醒全部排队等待者（Closed）、退役空闲连接；
    // 幂等不可重开。在飞请求按已持有借用完成或失败；不关共享 Engine，不假装
    // 物理排空（退役连接的 fd 由 Engine owner 收口）。
    void Close() noexcept;
    bool IsClosed() const noexcept;

    PoolStats Stats() const noexcept;

private:
    struct State;
    explicit Client(std::shared_ptr<State> state) noexcept;
    std::shared_ptr<State> m_state;
};

} // namespace bbt::infra::http

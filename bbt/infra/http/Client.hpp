#pragma once

/**
 * @file Client.hpp
 * @brief 原生协程 HTTP 客户端的装配面与请求/响应值类型（bbt::infra::http）。
 *
 * 形态借鉴 httpx AsyncClient：单一有栈协程 Client、内建连接池；不是其完整
 * 功能兼容实现。已支持范围与验证入口见 docs/guides/http-net.md。
 *
 * 职责边界：
 *   - Client 只装配，不联网、不建线程、不建 Engine、无全局单例；执行底座
 *     （coroutine scheduler + client-only net::Engine + 宿主 Run 线程）由应用
 *     一次性装配并最终收口。多个 Client 可共享同一 Engine，池/默认配置互不共享。
 *   - HTTP 4xx/5xx 是有效 Response（不是错误）；result 错误只表达传输/排队/
 *     装配失败。错误码沿用 bbt::infra::ErrorCode。
 *   - 编解码用 Beast；网络只经 bbt::infra::net 协程成员接口（Engine::CoDial、
 *     CoTcpConnection::Read/Write），不触碰 Asio socket/native fd/net detail。
 *
 * 支持范围（未实现能力不得在注释或接口上暗示）：
 *   - 支持：GET/POST/Request（任意方法）、raw body、base_url/默认与请求级
 *     headers/params 合并、分阶段超时、内建连接池（复用/上限/排队/退役/过期）。
 *   - 不支持：https/TLS、主机名 DNS（host 仅数值 IP，net Address 同界）、
 *     json/form 编码、流式响应、重定向、代理、HTTP/2、请求自动重试。
 */

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

/// 请求/响应头序列；保留插入顺序，允许重复键。
using Headers = std::vector<std::pair<std::string, std::string>>;
/// 查询参数序列；编码按此顺序拼接。
using QueryParams = std::vector<std::pair<std::string, std::string>>;

/**
 * @brief 单档超时的三态取值：继承 Client 档、有限值、显式禁用。
 *
 * 单层 optional 表达不了三态（「未指定」与「禁用」必须区分），所以用独立
 * 类型承载。
 */
class PhaseTimeout {
public:
    /// 未指定该档：解析时取 Client 级同档取值。
    static PhaseTimeout Inherit() noexcept
    {
        return PhaseTimeout{Kind::Inherit, {}};
    }
    /// 显式禁用该档超时（只看该档以外的限速）。
    static PhaseTimeout Disabled() noexcept
    {
        return PhaseTimeout{Kind::Disabled, {}};
    }
    /// 有限值；非正值被拒绝（调用方 Get/Post 返回 InvalidArgument）。
    static PhaseTimeout Milliseconds(std::chrono::milliseconds value) noexcept
    {
        return PhaseTimeout{Kind::Finite, value};
    }

    /// 是否为继承档。
    bool IsInherit() const noexcept
    {
        return m_kind == Kind::Inherit;
    }
    /// 是否为显式禁用档。
    bool IsDisabled() const noexcept
    {
        return m_kind == Kind::Disabled;
    }
    /// 有限值档的取值；Inherit/Disabled 返回 nullopt。
    std::optional<std::chrono::milliseconds> Finite() const noexcept
    {
        if (m_kind != Kind::Finite) {
            return std::nullopt;
        }
        return m_value;
    }

private:
    enum class Kind { Inherit, Finite, Disabled };
    PhaseTimeout(Kind kind, std::chrono::milliseconds value) noexcept : m_kind(kind), m_value(value)
    {
    }
    Kind m_kind;
    std::chrono::milliseconds m_value{};
};

/**
 * @brief 分阶段超时：连接、读、写、池排队四档。
 *
 * 默认四档均 5s（对齐 httpx Timeout(5.0) 的均匀档），逐字段显式写出，
 * 避免「一个字段默认值被误读为四档」。
 */
struct Timeout {
    PhaseTimeout connect{PhaseTimeout::Milliseconds(std::chrono::milliseconds{5000})}; ///< 建连
    PhaseTimeout read{PhaseTimeout::Milliseconds(std::chrono::milliseconds{5000})};    ///< 读响应
    PhaseTimeout write{PhaseTimeout::Milliseconds(std::chrono::milliseconds{5000})};   ///< 写请求
    PhaseTimeout pool{PhaseTimeout::Milliseconds(std::chrono::milliseconds{5000})};    ///< 等池内可用连接

    /// 四档取同一个 PhaseTimeout 值。
    static Timeout Uniform(PhaseTimeout phase) noexcept
    {
        Timeout value;
        value.connect = phase;
        value.read = phase;
        value.write = phase;
        value.pool = phase;
        return value;
    }
};

/**
 * @brief 连接池配额。
 *
 * 全局配额（active+idle+dialing+retiring 计入 max_connections），无每 origin
 * 限额。max_queue 是排队人数上界，满即 Overloaded（不静默无限排队）。
 */
struct PoolLimits {
    std::size_t max_connections{8};           ///< 全局连接上限，含在飞、空闲、建连中与退役中
    std::size_t max_keepalive_connections{4}; ///< 空闲保活上限
    /// 空闲连接保活过期；nullopt 表示不过期。过期在池 sweep 中惰性判定。
    std::optional<std::chrono::milliseconds> keepalive_expiry{std::chrono::milliseconds{15000}};
    std::size_t max_queue{32}; ///< 排队等待人数上界，满即 Overloaded
};

/**
 * @brief Client 级装配选项：默认地址、默认头/参数、分档超时与池配额。
 */
struct ClientOptions {
    /// 例 "http://127.0.0.1:8080"；scheme 仅 http，host 仅数值 IP（DNS 未交付）。
    std::string base_url;
    Headers headers;     ///< client 级默认头（请求级同名覆盖）
    QueryParams params;  ///< client 级默认查询参数（请求级追加在后）
    Timeout timeout{};   ///< 默认分档超时
    PoolLimits limits{}; ///< 连接池配额
    /// 单响应体上限（Beast body_limit）。
    std::uint64_t max_body_bytes{8 * 1024 * 1024};
    /// 请求 body 字节上限；未设置时保留原有行为。
    std::optional<std::uint64_t> max_request_body_bytes;
    /// 响应 header 字节上限；未设置时保留原有行为。
    std::optional<std::uint64_t> max_header_bytes;
};

/**
 * @brief 原始请求体。
 *
 * content 原样发送；content_type 非空时写入 Content-Type（无请求级覆盖时）。
 * json/form 编码未实现，因此不用互斥 union 假装两种形态。
 */
struct RequestBody {
    std::string content;      ///< 原样发送的 body 字节
    std::string content_type; ///< 非空时作为 Content-Type 的默认值
};

/**
 * @brief 单请求覆盖项：头、查询参数与分档超时。
 */
struct RequestOptions {
    Headers headers;                ///< merge：client 打底，同名覆盖
    QueryParams params;             ///< merge：client 在前，请求级追加在后
    std::optional<Timeout> timeout; ///< 分档覆盖；档为 Inherit 时取 Client 档
};

/**
 * @brief 一次 HTTP 响应（含 4xx/5xx，它们不是错误）。
 */
struct Response {
    int status_code{0}; ///< HTTP 状态码
    Headers headers;    ///< 响应头，保持线上顺序
    std::string body;   ///< 响应体字节（受 max_body_bytes 限制）
    /// 2xx 为 true；其余（含 4xx/5xx）为 false，但仍是有效响应。
    bool Ok() const noexcept
    {
        return status_code >= 200 && status_code < 300;
    }
};

/**
 * @brief BuildRequest 产物：已合并 base_url/headers/params、已解析超时
 *        （无 Inherit 残留）、已解析 origin/数值地址的待发请求。
 *
 * Send 消费；纯值不联网。（命名说明：方法名 Client::Request 与值类型不能
 * 同名，值类型用 PreparedRequest。）
 */
struct PreparedRequest {
    std::string method;          ///< 请求方法
    std::string url;             ///< 合并 base_url 后的绝对 URL
    std::string target_for_wire; ///< origin-form（path[?query]），HTTP/1.1 wire 目标
    net::Address address;        ///< 数值地址（Engine::CoDial 用）
    std::string host_header;     ///< 默认 Host（无用户覆盖时）
    Headers headers;             ///< 合并后的请求头
    std::string body;            ///< 请求体字节
    Timeout timeout;             ///< 已解析：任何档都不是 Inherit
};

/**
 * @brief 连接池观测快照（产品公开面，供容量/退役验证）。
 */
struct PoolStats {
    std::size_t active{0};   ///< 在飞请求占用
    std::size_t idle{0};     ///< 空闲可复用
    std::size_t dialing{0};  ///< 建连中
    std::size_t retiring{0}; ///< 退役中（已请求关闭，真实回收凭据尚未完成）
    std::size_t waiting{0};  ///< 排队等待者
    std::size_t budget{0};   ///< 以上连接之和，受 max_connections 约束
};

/**
 * @brief 单一有栈协程 HTTP 客户端，持有连接池与默认配置。
 *
 * 生命周期单向：Open → Draining → Closed，不可重开。对象不拥有 Engine，
 * 物理回收（fd 关闭）由 Engine owner 收口。
 */
class Client {
public:
    /// 只装配：校验 options、构造池；不联网。Engine 由应用装配并保证可推进。
    /// @param engine 应用装配并推进的 net::Engine，Client 只借用
    /// @param options 默认地址、头/参数、分档超时与池配额
    /// @return 装配失败时为错误（参数非法等）
    static result<std::shared_ptr<Client>> Create(std::shared_ptr<net::Engine> engine, ClientOptions options);
    ~Client() noexcept;
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    /// BuildRequest 纯构造，不挂起（可在协程外调用）。URL/base_url 合并、参数
    /// 编码、headers/超时解析都在此完成。
    /// @param method 请求方法
    /// @param url 绝对 URL 或相对 base_url 的路径
    /// @param options 请求级覆盖项
    /// @return 已解析的待发请求；解析失败（非法 URL、非正值超时等）为错误
    result<PreparedRequest> BuildRequest(std::string_view method, std::string_view url,
                                         const RequestOptions& options) const;

    // 以下入口挂起等待，须在协程上下文调用（bbt_co_await 消费）。
    // 省略 call 时默认无总期限（Deadline::max()），各阶段默认超时限速仍生效；
    // 显式传 CallOptions{} 表示已过期（deadline 为 epoch），会被拒绝——
    // 省略与显式零值是两种语义，不是同一缺省。
    /// GET 请求：url 为绝对 URL 或相对 base_url 的路径，options 为请求级覆盖；
    /// call 为总期限，省略表示无总期限。返回有效响应（含 4xx/5xx）或传输/排队/
    /// 装配错误。
    bbt_co_async(result<Response>) Get(std::string_view url, const RequestOptions& options,
                                       const CallOptions& call = {bbt::coroutine::Deadline::max()});
    /// POST 请求（原始 body）：body 携带内容与默认 Content-Type，options 为请求级
    /// 覆盖，call 为总期限（省略表示无总期限）。返回有效响应（含 4xx/5xx）或
    /// 传输/排队/装配错误。
    bbt_co_async(result<Response>) Post(std::string_view url, RequestBody body, const RequestOptions& options,
                                        const CallOptions& call = {bbt::coroutine::Deadline::max()});
    /// 任意方法的请求，body 由 RequestBody 承载（无 body 语义时传空 content）；
    /// call 为总期限（省略表示无总期限）。返回有效响应（含 4xx/5xx）或传输/排队/
    /// 装配错误。
    bbt_co_async(result<Response>) Request(std::string_view method, std::string_view url, const RequestOptions& options,
                                           const CallOptions& call = {bbt::coroutine::Deadline::max()});
    /// 发送已构建的请求（BuildRequest 产物），不再做 URL/超时解析；返回有效响应
    /// （含 4xx/5xx）或传输/排队/装配错误。
    ///
    /// @param request 已解析的待发请求
    /// @param call 总期限（省略表示无总期限）
    bbt_co_async(result<Response>)
        Send(const PreparedRequest& request, const CallOptions& call = {bbt::coroutine::Deadline::max()});

    /// 立即逻辑封口（Open/Draining → Closed），幂等且不可重开。
    ///
    /// 拒绝新请求、唤醒全部排队等待者、对在飞与拨号条目请求关闭，并结算全部
    /// 未终结操作（「失败在途」恰好一次，Draining 下同样执行，与 Shutdown
    /// 不共用位）、发布首个关闭原因。不等待在飞请求，不关共享 Engine，
    /// 不假装物理排空（退役连接的 fd 由 Engine owner 收口）。
    /// @param reason 首个关闭原因；之后调用不再覆盖
    void Close(Error reason = MakeError(ErrorCode::Closed, "http client: closed by owner")) noexcept;
    /// 非 Open 即 true：Shutdown 置位 Draining 后同样报已封口。
    bool IsClosed() const noexcept;
    /// Close 发布的原因；Shutdown 只置 Draining 不发布原因，故未 Close 时为空。
    std::optional<Error> CloseReason() const noexcept;
    /// 置 Draining（拒新）并等已接纳调用责任归零（在途借用与排队，不看条目
    /// phase；Retiring 属延迟物理回收不计入）。
    ///
    /// 有限期限超时返回 TimedOut 且保持拒新；等待期间被 Close 且仍有在途返回
    /// 其关闭原因，先排空先胜返回 ok（成功不被追改）；重复调用仍 Draining 且
    /// 有在途返回 Overloaded（Busy）；已 Closed 返回首个关闭原因。native 宿主
    /// 专用；协程/Processer 域返回 InvalidContext。
    /// @param deadline 等待已接纳调用归零的期限
    /// @return 排空完成、已关闭或相应错误
    result<void> Shutdown(bbt::coroutine::Deadline deadline);

    /// 当前池占用快照（只读，不阻塞）。
    PoolStats Stats() const noexcept;

private:
    struct State;
    explicit Client(std::shared_ptr<State> state) noexcept;
    std::shared_ptr<State> m_state;
};

} // namespace bbt::infra::http

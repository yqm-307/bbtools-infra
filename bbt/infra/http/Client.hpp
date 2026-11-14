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
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <bbt/infra/CloseOptions.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/coroutine/syntax/Async.hpp>

namespace bbt::infra::http {

/**
 * @brief 请求/响应头序列；保留插入顺序，允许重复键。
 */
using Headers = std::vector<std::pair<std::string, std::string>>;

/**
 * @brief 查询参数序列；编码按此顺序拼接。
 */
using QueryParams = std::vector<std::pair<std::string, std::string>>;

/**
 * @brief 连接池配额。
 *
 * 全局配额（active+idle+dialing+retiring 计入 max_connections），无每 origin
 * 限额。max_queue 是排队人数上界，满即 Overloaded（不静默无限排队）。
 *
 * max_connections 含在飞、空闲、建连中与退役中；max_keepalive_connections 是空闲
 * 保活上限；keepalive_expiry 为 nullopt 表示不过期，过期在池 sweep 中惰性判定。
 */
struct PoolLimits {
    std::size_t max_connections{8};
    std::size_t max_keepalive_connections{4};
    std::optional<std::chrono::milliseconds> keepalive_expiry{std::chrono::milliseconds{15000}};
    std::size_t max_queue{32};
};

/**
 * @brief Client 级装配选项：默认地址、默认头/参数、分档超时与池配额。
 *
 * base_url 形如 "http://127.0.0.1:8080"，scheme 仅 http，host 仅数值 IP（DNS 未
 * 交付）；headers/params 是 client 级默认值，请求级同名覆盖、参数追加在后；
 * 各阶段超时独立限时，max() 表示该阶段不设上限，默认均为 5 秒；limits 是连接池
 * 配额；max_body_bytes 是单响应体上限（Beast body_limit）；
 * max_request_body_bytes / max_header_bytes 未设置时保留原有行为。
 *
 * 超时配置只在 Create 前有意义，Create 后不再读取改动。
 */
struct ClientOptions {
    std::string base_url;
    Headers headers;
    QueryParams params;
    std::chrono::milliseconds connect_timeout{5000};
    std::chrono::milliseconds read_timeout{5000};
    std::chrono::milliseconds write_timeout{5000};
    std::chrono::milliseconds pool_timeout{5000};
    PoolLimits limits{};
    std::uint64_t max_body_bytes{8 * 1024 * 1024};
    std::optional<std::uint64_t> max_request_body_bytes;
    std::optional<std::uint64_t> max_header_bytes;

    /**
     * @name 超时设置与禁用
     * @brief 覆盖 Client 级默认阶段超时；时长须为正，由 Create 校验。
     *
     * 共享语义：SetTimeout 一次设置全部四个阶段；SetXxxTimeout 只改对应阶段；
     * DisableXxxTimeout 只禁用该阶段的超时，不取消 CallOptions 的总期限。禁用把
     * 该阶段保存为 milliseconds::max()，即这个阶段不再有上限。
     * @{
     */

    /**
     * @brief 同时设置 connect/read/write/pool 四个阶段。
     */
    ClientOptions& SetTimeout(std::chrono::milliseconds value) noexcept;

    /**
     * @brief 只设置建连阶段超时。
     */
    ClientOptions& SetConnectTimeout(std::chrono::milliseconds value) noexcept;

    /**
     * @brief 只设置读阶段超时。
     */
    ClientOptions& SetReadTimeout(std::chrono::milliseconds value) noexcept;

    /**
     * @brief 只设置写阶段超时。
     */
    ClientOptions& SetWriteTimeout(std::chrono::milliseconds value) noexcept;

    /**
     * @brief 只设置排队等池的阶段超时。
     */
    ClientOptions& SetPoolTimeout(std::chrono::milliseconds value) noexcept;

    /**
     * @brief 禁用全部四个阶段的超时。
     */
    ClientOptions& DisableTimeout() noexcept;

    /**
     * @brief 禁用建连阶段超时。
     */
    ClientOptions& DisableConnectTimeout() noexcept;

    /**
     * @brief 禁用读阶段超时。
     */
    ClientOptions& DisableReadTimeout() noexcept;

    /**
     * @brief 禁用写阶段超时。
     */
    ClientOptions& DisableWriteTimeout() noexcept;

    /**
     * @brief 禁用排队等池的阶段超时。
     */
    ClientOptions& DisablePoolTimeout() noexcept;

    /**
     * @}
     */
};

/**
 * @brief 原始请求体。
 *
 * content 原样发送；content_type 非空时写入 Content-Type（无请求级覆盖时）。
 * json/form 编码未实现，因此不用互斥 union 假装两种形态。
 */
struct RequestBody {
    std::string content;
    std::string content_type;
};

/**
 * @brief 单请求覆盖项：头、查询参数与分档超时。
 *
 * headers 按「client 打底、同名覆盖」合并；params 按「client 在前、请求级追加
 * 在后」拼接；四个 timeout 字段 nullopt 表示继承 Client 配置。
 */
struct RequestOptions {
    Headers headers;
    QueryParams params;
    std::optional<std::chrono::milliseconds> connect_timeout;
    std::optional<std::chrono::milliseconds> read_timeout;
    std::optional<std::chrono::milliseconds> write_timeout;
    std::optional<std::chrono::milliseconds> pool_timeout;

    /**
     * @name 超时覆盖与禁用
     * @brief 只覆盖指定阶段；未设置的阶段继承 Client 配置。
     *
     * 共享语义：SetTimeout 覆盖全部阶段；SetXxxTimeout 只改对应阶段；
     * DisableXxxTimeout 显式禁用该阶段（不等于继承）。时长须为正，由
     * BuildRequest 校验；对字段 reset() 即恢复继承。
     * @{
     */

    /**
     * @brief 同时覆盖 connect/read/write/pool 四个阶段。
     */
    RequestOptions& SetTimeout(std::chrono::milliseconds value) noexcept;

    /**
     * @brief 只覆盖建连阶段超时。
     */
    RequestOptions& SetConnectTimeout(std::chrono::milliseconds value) noexcept;

    /**
     * @brief 只覆盖读阶段超时。
     */
    RequestOptions& SetReadTimeout(std::chrono::milliseconds value) noexcept;

    /**
     * @brief 只覆盖写阶段超时。
     */
    RequestOptions& SetWriteTimeout(std::chrono::milliseconds value) noexcept;

    /**
     * @brief 只覆盖排队等池的阶段超时。
     */
    RequestOptions& SetPoolTimeout(std::chrono::milliseconds value) noexcept;

    /**
     * @brief 禁用全部四个阶段的超时。
     */
    RequestOptions& DisableTimeout() noexcept;

    /**
     * @brief 禁用建连阶段超时。
     */
    RequestOptions& DisableConnectTimeout() noexcept;

    /**
     * @brief 禁用读阶段超时。
     */
    RequestOptions& DisableReadTimeout() noexcept;

    /**
     * @brief 禁用写阶段超时。
     */
    RequestOptions& DisableWriteTimeout() noexcept;

    /**
     * @brief 禁用排队等池的阶段超时。
     */
    RequestOptions& DisablePoolTimeout() noexcept;

    /**
     * @}
     */
};

/**
 * @brief 一次 HTTP 响应（含 4xx/5xx，它们不是错误）。
 *
 * status_code 是 HTTP 状态码；headers 保持线上顺序；body 受 max_body_bytes 限制。
 */
struct Response {
    int status_code{0};
    Headers headers;
    std::string body;

    /**
     * @brief 2xx 为 true；其余（含 4xx/5xx）为 false，但仍是有效响应。
     */
    bool Ok() const noexcept { return status_code >= 200 && status_code < 300; }
};

/**
 * @brief BuildRequest 产物：已合并 base_url/headers/params、已解析超时
 *        （无待继承项）、已解析 origin/数值地址的待发请求。
 *
 * Send 消费；纯值不联网。命名说明：方法名 Client::Request 与值类型不能同名，
 * 值类型用 PreparedRequest。
 *
 * target_for_wire 是 origin-form（path[?query]），即 HTTP/1.1 wire 目标；
 * address 是 Engine::CoDial 用的数值地址；host_header 是无用户覆盖时的默认 Host。
 */
struct PreparedRequest {
    std::string method;
    std::string url;
    std::string target_for_wire;
    net::Address address;
    std::string host_header;
    Headers headers;
    std::string body;
    std::chrono::milliseconds connect_timeout{5000};
    std::chrono::milliseconds read_timeout{5000};
    std::chrono::milliseconds write_timeout{5000};
    std::chrono::milliseconds pool_timeout{5000};
};

/**
 * @brief 连接池观测快照（产品公开面，供容量/退役验证）。
 *
 * active 是在飞请求占用；idle 空闲可复用；dialing 建连中；retiring 已请求关闭、
 * 真实回收凭据尚未完成；waiting 是排队等待者；budget 为以上连接之和，受
 * max_connections 约束。
 */
struct PoolStats {
    std::size_t active{0};
    std::size_t idle{0};
    std::size_t dialing{0};
    std::size_t retiring{0};
    std::size_t waiting{0};
    std::size_t budget{0};
};

/**
 * @brief 单一有栈协程 HTTP 客户端，持有连接池与默认配置。
 *
 * 生命周期单向：Open → Draining → Closed，不可重开。对象不拥有 Engine，
 * 物理回收（fd 关闭）由 Engine owner 收口。
 */
class Client {
public:
    /**
     * @brief 只装配：校验 options、构造池；不联网。
     *
     * Engine 由应用装配并保证可推进。
     * @param engine 应用装配并推进的 net::Engine，Client 只借用
     * @param options 默认地址、头/参数、分档超时与池配额
     * @return 装配失败时为错误（参数非法等）
     */
    static result<std::shared_ptr<Client>> Create(std::shared_ptr<net::Engine> engine, ClientOptions options);
    ~Client() noexcept;
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    /**
     * @brief BuildRequest 纯构造，不挂起（可在协程外调用）。
     *
     * URL/base_url 合并、参数编码、headers/超时解析都在此完成。
     * @param method 请求方法
     * @param url 绝对 URL 或相对 base_url 的路径
     * @param options 请求级覆盖项
     * @return 已解析的待发请求；解析失败（非法 URL、非正值超时等）为错误
     */
    result<PreparedRequest>
    BuildRequest(std::string_view method, std::string_view url, const RequestOptions& options) const;

    /**
     * @name 请求入口
     * @brief 在协程上下文中执行请求，阶段超时受 CallOptions 的非重置总期限共同约束。
     *
     * 省略 call 表示无总期限；显式 CallOptions{} 的 epoch deadline 会被拒绝。
     * 各入口只在请求形态、body 和已构建请求方面不同。
     * @{
     */

    /**
     * @brief GET 请求。
     *
     * 这一族入口挂起等待，须在协程上下文调用（bbt_co_await 消费）。省略 call 时
     * 默认无总期限（Deadline::max()），各阶段默认超时限速仍生效；显式传
     * CallOptions{} 表示已过期（deadline 为 epoch），会被拒绝——省略与显式零值是
     * 两种语义，不是同一缺省。
     *
     * @param url 绝对 URL 或相对 base_url 的路径
     * @param options 请求级覆盖
     * @param call 总期限，省略表示无总期限
     * @return 有效响应（含 4xx/5xx）或传输/排队/装配错误
     */
    bbt_co_async(result<Response>)
        Get(std::string_view url,
            const RequestOptions& options,
            const CallOptions& call = {bbt::coroutine::Deadline::max()});

    /**
     * @brief POST 请求（原始 body）。
     *
     * @param url 绝对 URL 或相对 base_url 的路径
     * @param body 内容与默认 Content-Type
     * @param options 请求级覆盖
     * @param call 总期限，省略表示无总期限
     * @return 有效响应（含 4xx/5xx）或传输/排队/装配错误
     */
    bbt_co_async(result<Response>) Post(
        std::string_view url,
        RequestBody body,
        const RequestOptions& options,
        const CallOptions& call = {bbt::coroutine::Deadline::max()});

    /**
     * @brief 任意方法的请求，body 由 RequestBody 承载（无 body 语义时传空 content）。
     *
     * @param method 请求方法
     * @param url 绝对 URL 或相对 base_url 的路径
     * @param options 请求级覆盖
     * @param call 总期限，省略表示无总期限
     * @return 有效响应（含 4xx/5xx）或传输/排队/装配错误
     */
    bbt_co_async(result<Response>) Request(
        std::string_view method,
        std::string_view url,
        const RequestOptions& options,
        const CallOptions& call = {bbt::coroutine::Deadline::max()});

    /**
     * @brief 发送已构建的请求（BuildRequest 产物），不再做 URL/超时解析。
     *
     * @param request 已解析的待发请求
     * @param call 总期限，省略表示无总期限
     * @return 有效响应（含 4xx/5xx）或传输/排队/装配错误
     */
    bbt_co_async(result<Response>)
        Send(const PreparedRequest& request, const CallOptions& call = {bbt::coroutine::Deadline::max()});

    // clang-format off

    /**
     * @}
     */
    // clang-format on

    /**
     * @brief 统一关闭入口；Immediate 不等待时任意线程可调用。
     *
     * - `Immediate`：等价 `Close(Immediate)`，不等待在飞请求。
     * - `Graceful`：封新、拒绝排队者并发布首因，然后在绝对期限前等已借出租借
     *   与在途操作归零；排空后退役池内剩余条目。到期只返回 `TimedOut` 并保留
     *   在途责任，不自动升级。
     * - `wait_release=true`：模式工作完成后在同一 `Close` 内继续等池条目按
     *   真实回收出账到同一绝对期限；false 时物理事实仍由 `Close(wait_release=true)` 证明。
     *
     * 重复调用幂等、首因固定；`Graceful → Immediate` 升级由任一次
     * Immediate/Close(Immediate) 触发。不关共享 Engine。
     * @param options 模式、首因与优雅关闭的有限绝对期限
     * @return 排空完成、已关闭或相应错误
     */
    result<void> Close(const CloseOptions& options = {});

    /**
     * @brief 非 Open 即 true：优雅关闭置 Draining 后同样报已封口。
     */
    bool IsClosed() const noexcept;

    /**
     * @brief 关闭原因；任意模式首次发布后固定。
     */
    std::optional<Error> CloseReason() const noexcept;

    /**
     * @brief 当前池占用快照（只读，不阻塞）。
     */
    PoolStats Stats() const noexcept;

private:
    void _BeginClose(Error reason = MakeError(ErrorCode::Closed, "http client: closed by owner")) noexcept;
    result<void> _WaitReleased(bbt::coroutine::Deadline deadline);

    struct State;
    explicit Client(std::shared_ptr<State> state) noexcept;
    std::shared_ptr<State> m_state;
};

// clang-format off
}
// clang-format on

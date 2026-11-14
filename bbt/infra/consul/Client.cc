#include <bbt/infra/consul/Client.hpp>
#include <bbt/infra/consul/detail/ClientTestAccess.hpp>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

/**
 * Boost.JSON 以源码方式内联进本模块（仓内唯一一处之外的第二个 TU 会与 MCP 的
 * JsonValue.cc 重复定义；顶层 CMake 对 BBT_INFRA_CONSUL + BBT_INFRA_MCP 同开做
 * fail-closed 门禁）。不链接 libboost_json，不新增第三方依赖。
 */
#include <boost/json/src.hpp>

#include <arpa/inet.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/syntax/Async.hpp>

/**
 * 执行域判定 / 50ms 分片 / CloseOptions 校验
 */
#include <bbt/infra/detail/CloseWait.hpp>

namespace bbt::infra::consul {
namespace {

using bbt::infra::Error;
using bbt::infra::ErrorCode;

std::mutex health_delivery_mutex;
const Client* health_delivery_client = nullptr;
std::weak_ptr<HealthDeliveryPause> health_delivery_pause;

std::shared_ptr<HealthDeliveryPause> TakeHealthDeliveryPause(const Client& client)
{
    std::lock_guard<std::mutex> lock(health_delivery_mutex);
    if (health_delivery_client != &client) {
        return {};
    }
    health_delivery_client = nullptr;
    return std::exchange(health_delivery_pause, {}).lock();
}

Error InvalidArgument(std::string message)
{
    return MakeError(ErrorCode::InvalidArgument, "consul: " + std::move(message));
}

Error Protocol(std::string message)
{
    return MakeError(ErrorCode::ProtocolError, "consul: " + std::move(message));
}

Error ClosedError()
{
    return MakeError(ErrorCode::Closed, "consul: client closed");
}

bool EqualsNoCase(std::string_view left, std::string_view right)
{
    if (left.size() != right.size()) {
        return false;
    }
    for (std::size_t i = 0; i < left.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(left[i])) != std::tolower(static_cast<unsigned char>(right[i]))) {
            return false;
        }
    }
    return true;
}

/**
 * RFC 3986 unreserved 之外的字节一律 %XX：service 名与实例 ID 是用户输入，
 * 直接拼进路径会让 '/'、'?'、'#' 改变目标路由。
 */
std::string PercentEncode(std::string_view value)
{
    static constexpr char kHex[] = "0123456789ABCDEF";
    std::string out;
    out.reserve(value.size());
    for (const unsigned char c : value) {
        const bool unreserved = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-'
                                || c == '_' || c == '.' || c == '~';
        if (unreserved) {
            out.push_back(static_cast<char>(c));
            continue;
        }
        out.push_back('%');
        out.push_back(kHex[c >> 4]);
        out.push_back(kHex[c & 0x0F]);
    }
    return out;
}

bool IsNumericIpv4(std::string_view address)
{
    if (address.empty()) {
        return false;
    }
    in_addr parsed{};
    const std::string text(address);
    return ::inet_pton(AF_INET, text.c_str(), &parsed) == 1;
}

bool IsLoopbackIpv4(std::string_view address)
{
    in_addr parsed{};
    const std::string text(address);
    if (::inet_pton(AF_INET, text.c_str(), &parsed) != 1) {
        return false;
    }
    return (ntohl(parsed.s_addr) >> 24) == 127;
}

/**
 * agent 地址归一为无尾斜杠的 "http://<ipv4>:<port>"：https、主机名、路径、远端
 * 地址都在此拒绝——该层只承诺本机 agent，远端注册中心不是它假装支持的形态。
 */
result<void> NormalizeAgentUrl(std::string_view url, std::string* out)
{
    constexpr std::string_view kScheme = "http://";
    if (url.substr(0, kScheme.size()) != kScheme) {
        return result<void>::err(InvalidArgument("agent_url must start with http://"));
    }
    std::string rest(url.substr(kScheme.size()));
    while (!rest.empty() && rest.back() == '/') {
        rest.pop_back();
    }
    if (rest.empty()) {
        return result<void>::err(InvalidArgument("agent_url has no host"));
    }
    if (rest.find_first_of("/?#") != std::string::npos) {
        return result<void>::err(InvalidArgument("agent_url must be origin only"));
    }
    const auto colon = rest.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 >= rest.size()) {
        return result<void>::err(InvalidArgument("agent_url must be host:port"));
    }
    const std::string host = rest.substr(0, colon);
    const std::string port_text = rest.substr(colon + 1);
    if (!IsNumericIpv4(host)) {
        return result<void>::err(InvalidArgument("agent_url host must be numeric ipv4"));
    }
    if (!IsLoopbackIpv4(host)) {
        return result<void>::err(InvalidArgument("agent_url must be loopback"));
    }
    std::uint32_t port = 0;
    const auto parsed = std::from_chars(port_text.data(), port_text.data() + port_text.size(), port);
    if (parsed.ec != std::errc() || parsed.ptr != port_text.data() + port_text.size() || port == 0 || port > 65535) {
        return result<void>::err(InvalidArgument("agent_url port must be 1..65535"));
    }
    out->assign("http://" + host + ":" + std::to_string(port));
    return result<void>::ok();
}

result<void> ValidateRegistration(const ServiceRegistration& registration)
{
    if (registration.id.empty()) {
        return result<void>::err(InvalidArgument("registration id must not be empty"));
    }
    if (registration.name.empty()) {
        return result<void>::err(InvalidArgument("registration name must not be empty"));
    }
    if (!IsNumericIpv4(registration.address)) {
        return result<void>::err(InvalidArgument("registration address must be numeric ipv4"));
    }
    if (registration.port == 0) {
        return result<void>::err(InvalidArgument("registration port must be non-zero"));
    }
    return result<void>::ok();
}

/**
 * 200/2xx 之外一律 TransportError 并保留状态码：Consul 的拒绝不是传输失败，但
 * 对本层调用人是「后端明确拒绝」，与协议错（ProtocolError）、超时（TimedOut）
 * 可区分即可；不把响应体拼进消息（可能含注册数据）。
 */
result<void> CheckStatus(const http::Response& response, std::string_view action)
{
    if (response.Ok()) {
        return result<void>::ok();
    }
    Error error = MakeError(
        ErrorCode::TransportError,
        "consul: " + std::string(action) + " rejected with HTTP " + std::to_string(response.status_code));
    error.backend_category = "consul.http";
    error.backend_code = response.status_code;
    return result<void>::err(std::move(error));
}

std::optional<std::string> HeaderValue(const http::Headers& headers, std::string_view name)
{
    for (const auto& header : headers) {
        if (EqualsNoCase(header.first, name)) {
            return header.second;
        }
    }
    return std::nullopt;
}

const boost::json::value* Member(const boost::json::object& object, std::string_view key)
{
    return object.if_contains(key);
}

std::optional<std::string> StringMember(const boost::json::object& object, std::string_view key)
{
    const auto* value = Member(object, key);
    if (value == nullptr || !value->is_string()) {
        return std::nullopt;
    }
    return std::string(value->get_string());
}

std::optional<std::uint16_t> PortMember(const boost::json::object& object, std::string_view key)
{
    const auto* value = Member(object, key);
    if (value == nullptr) {
        return std::nullopt;
    }
    std::int64_t port = 0;
    if (value->is_int64()) {
        port = value->get_int64();
    } else if (value->is_uint64()) {
        const auto raw = value->get_uint64();
        if (raw > 65535) {
            return std::nullopt;
        }
        port = static_cast<std::int64_t>(raw);
    } else {
        return std::nullopt;
    }
    if (port <= 0 || port > 65535) {
        return std::nullopt;
    }
    return static_cast<std::uint16_t>(port);
}

// clang-format off
}
// clang-format on

std::shared_ptr<HealthDeliveryPause> ClientTestAccess::PauseHealthDelivery(const Client& client)
{
    auto pause = std::make_shared<HealthDeliveryPause>();
    std::lock_guard<std::mutex> lock(health_delivery_mutex);
    health_delivery_client = &client;
    health_delivery_pause = pause;
    return pause;
}

struct Client::State {
    State(std::shared_ptr<bbt::infra::http::Client> client, Options opts)
        : http(std::move(client))
        , options(std::move(opts))
    {
    }

    /**
     * 单次 HTTP 交换：token 头、分阶段超时、总期限在此统一收口，业务路径只给
     * 方法与 target（target 以 '/' 开头的 origin-form）。
     */
    bbt_co_async(result<http::Response>) Send(
        std::string_view method,
        const std::string& target,
        std::string body,
        std::chrono::milliseconds read_budget) const
    {
        http::RequestOptions request;
        request.headers.emplace_back("Accept", "application/json");
        if (!options.token.empty()) {
            request.headers.emplace_back("X-Consul-Token", options.token);
        }
        request.SetTimeout(options.request_timeout).SetReadTimeout(read_budget);

        auto prepared = http->BuildRequest(method, base + target, request);
        if (!prepared) {
            return result<http::Response>::err(std::move(prepared.error()));
        }
        if (!body.empty()) {
            prepared.value().body = std::move(body);
            prepared.value().headers.emplace_back("Content-Type", "application/json");
        }
        CallOptions call;
        call.deadline = std::chrono::steady_clock::now() + options.request_timeout;
        return bbt_co_await http->Send(prepared.value(), call);
    }

    /**
     * 阻塞查询的读档必须容纳 Consul 允许的 wait/16 附加抖动，否则「阻塞返回」
     * 会先被读档超时打断；非阻塞快照使用整个 request_timeout。
     */
    std::chrono::milliseconds ReadBudgetForQuery(bool blocking) const
    {
        if (!blocking || options.query_wait <= std::chrono::milliseconds{0}) {
            return options.request_timeout;
        }
        const auto jitter = std::chrono::milliseconds{(options.query_wait.count() + 15) / 16};
        const auto budget = options.query_wait + jitter + std::chrono::milliseconds{250};
        return std::min(options.request_timeout, budget);
    }

    const std::shared_ptr<bbt::infra::http::Client> http;
    const Options options;
    std::string base;

    /**
     * 保护 inflight/terminal 与协程等待者登记
     */
    mutable std::mutex mutex;

    /**
     * 宿主线程等待真实终态
     */
    std::condition_variable idle;

    /**
     * 协程域等待者
     */
    std::vector<bbt::coroutine::sync::CoWaiter::SPtr> co_waiters;
    std::size_t inflight{0};

    /**
     * 封口：拒绝新操作（IsClosed 同此语义）
     */
    std::atomic<bool> closed{false};

    /**
     * 专属 HTTP Client 已封口
     */
    bool http_closed{false};

    /**
     * 与结果交付共用 mutex：Immediate 后迟到结果只能消费，不能交付成功。
     */
    bool immediate{false};

    /**
     * 仅一个路径负责关闭专属 HTTP Client
     */
    bool http_close_started{false};

    /**
     * 封口 && http_closed && inflight==0，单调不可逆
     */
    bool terminal{false};

    /**
     * 首个关闭原因（首因固定）
     */
    std::optional<Error> reason;

    /**
     * 终态发布（锁内调用）：仅当三条件同时成立才置位，并摘下全部协程等待者。
     * 返回 true 表示本次发布；woken 供调用方在锁外 Notify（不持锁唤醒）。
     */
    bool MarkTerminalLocked(std::vector<bbt::coroutine::sync::CoWaiter::SPtr>* woken)
    {
        if (terminal || inflight != 0 || !closed.load() || !http_closed) {
            return false;
        }
        terminal = true;
        woken->insert(woken->end(), co_waiters.begin(), co_waiters.end());
        co_waiters.clear();
        return true;
    }

    /**
     * 在途归零发布（锁内调用）：Graceful 只等这个谓词，不等专属 HTTP Client 的
     * 物理退役；终态未成立时被唤醒者复查后自行重新登记。
     */
    bool MarkSettledLocked(std::vector<bbt::coroutine::sync::CoWaiter::SPtr>* woken)
    {
        if (!closed.load() || inflight != 0) {
            return false;
        }
        woken->insert(woken->end(), co_waiters.begin(), co_waiters.end());
        co_waiters.clear();
        return true;
    }

    /**
     * Graceful 等已接纳操作离开后才关闭专属 HTTP Client；Immediate 立即中断交换，
     * 但操作的状态和 owner 仍保留到各自安全退出。
     */
    bool BeginHttpCloseLocked()
    {
        if (!closed.load() || (!immediate && inflight != 0) || http_close_started) {
            return false;
        }
        http_close_started = true;
        return true;
    }

    void CompleteHttpClose() noexcept
    {

        /**
         * 只关闭专属 HTTP Client，不关共享 Engine；Immediate 可在操作仍在途时进入，
         * 物理凭据仍须等操作退出和 HTTP 连接真实退役。
         */
        (void)http->Close(CloseOptions::Immediate(MakeError(ErrorCode::Closed, "consul: owned http client closed")));
        std::vector<bbt::coroutine::sync::CoWaiter::SPtr> woken;
        bool published = false;
        {
            std::lock_guard<std::mutex> lock(mutex);
            http_closed = true;
            published = MarkTerminalLocked(&woken);
        }
        if (published) {
            WakeAll(woken);
        }
    }

    /**
     * 锁外唤醒：宿主线程 cv + 协程等待者。Notify 对已决议/已解绑的 waiter 返回
     * -1，无副作用，可安全重复。
     */
    void WakeAll(const std::vector<bbt::coroutine::sync::CoWaiter::SPtr>& woken) noexcept
    {
        idle.notify_all();
        for (const auto& waiter : woken) {
            waiter->Notify();
        }
    }
};

/**
 * 在飞登记：封口后拒绝新操作；析构归还名额，最后一个离开的操作关闭专属 HTTP
 * Client 并发布终态、唤醒 Close 的等待者。状态由 shared_ptr 持有，Close 等待期间
 * Client 析构也不会悬空。
 */
class Client::OpScope {
public:
    explicit OpScope(std::shared_ptr<State> state)
        : m_state(std::move(state))
    {
        std::lock_guard<std::mutex> lock(m_state->mutex);
        if (m_state->closed.load()) {
            m_error = ClosedError();
            return;
        }
        ++m_state->inflight;
        m_entered = true;
    }

    ~OpScope()
    {
        if (!m_entered) {
            return;
        }
        std::vector<bbt::coroutine::sync::CoWaiter::SPtr> woken;
        bool published = false;
        bool settled = false;
        bool close_http = false;
        {
            std::lock_guard<std::mutex> lock(m_state->mutex);
            --m_state->inflight;

            /**
             * 归零即唤醒 Graceful 等待者：它只等在途责任发布结果，不等物理退役。
             */
            settled = m_state->MarkSettledLocked(&woken);
            close_http = m_state->BeginHttpCloseLocked();
            if (!close_http) {
                published = m_state->MarkTerminalLocked(&woken);
            }
        }

        /**
         * 锁外关闭专属 HTTP Client；最后一个离开的操作负责启动它，随后再发布终态。
         */
        if (close_http) {
            m_state->CompleteHttpClose();
        } else if (settled || published) {
            m_state->WakeAll(woken);
        }
    }

    OpScope(const OpScope&) = delete;
    OpScope& operator=(const OpScope&) = delete;

    bool entered() const noexcept { return m_entered; }

    Error error() const { return m_error; }

    template <typename T> result<T> Deliver(result<T> outcome) const
    {
        std::lock_guard<std::mutex> lock(m_state->mutex);
        if (m_state->immediate) {
            return result<T>::err(*m_state->reason);
        }
        return outcome;
    }

    const std::shared_ptr<State>& state() const noexcept { return m_state; }

private:
    std::shared_ptr<State> m_state;
    Error m_error;
    bool m_entered{false};
};

result<std::shared_ptr<Client>> Client::Create(std::shared_ptr<bbt::infra::http::Client> http, Options options)
{
    if (!http) {
        return result<std::shared_ptr<Client>>::err(InvalidArgument("http client required"));
    }
    if (options.request_timeout <= std::chrono::milliseconds{0}) {
        return result<std::shared_ptr<Client>>::err(InvalidArgument("request_timeout must be positive"));
    }
    if (options.query_wait < std::chrono::milliseconds{0}) {
        return result<std::shared_ptr<Client>>::err(InvalidArgument("query_wait must not be negative"));
    }
    if (options.query_wait > std::chrono::milliseconds{0}) {
        constexpr auto kQueryMargin = std::chrono::milliseconds{250};
        const auto wait_count = options.query_wait.count();
        const auto jitter = std::chrono::milliseconds{wait_count / 16 + (wait_count % 16 == 0 ? 0 : 1)};
        if (options.request_timeout <= kQueryMargin || jitter >= options.request_timeout - kQueryMargin
            || options.query_wait >= options.request_timeout - kQueryMargin - jitter) {
            return result<std::shared_ptr<Client>>::err(
                InvalidArgument("query_wait must leave wait/16 jitter and 250ms margin"));
        }
    }
    std::string base;
    if (auto normalized = NormalizeAgentUrl(options.agent_url, &base); !normalized) {
        return result<std::shared_ptr<Client>>::err(std::move(normalized.error()));
    }
    auto state = std::make_shared<State>(std::move(http), std::move(options));
    state->base = std::move(base);
    return result<std::shared_ptr<Client>>::ok(std::shared_ptr<Client>(new Client(state)));
}

Client::Client(std::shared_ptr<State> state) noexcept
    : m_state(std::move(state))
{
}

Client::~Client() noexcept
{
    (void)Close();
}

bbt_co_async(result<void>) Client::Register(ServiceRegistration registration)
{
    OpScope scope(m_state);
    if (!scope.entered()) {
        return result<void>::err(scope.error());
    }
    const auto& state = scope.state();
    if (auto valid = ValidateRegistration(registration); !valid) {
        return scope.Deliver(result<void>::err(std::move(valid.error())));
    }
    boost::json::object payload;
    payload["ID"] = registration.id;
    payload["Name"] = registration.name;
    payload["Address"] = registration.address;
    payload["Port"] = registration.port;
    boost::json::array tags;
    for (const auto& tag : registration.tags) {
        tags.emplace_back(tag);
    }
    payload["Tags"] = std::move(tags);

    /**
     * TCP check 只证明端口可达，不宣称业务 readiness；DeregisterCriticalServiceAfter
     * 让崩溃进程的登记不会永久残留。
     */
    boost::json::object check;
    check["TCP"] = registration.address + ":" + std::to_string(registration.port);
    check["Interval"] = "1s";
    check["Timeout"] = "500ms";
    check["DeregisterCriticalServiceAfter"] = "1m";
    payload["Check"] = std::move(check);

    auto response = bbt_co_await state->Send(
        "PUT", "/v1/agent/service/register", boost::json::serialize(payload), state->options.request_timeout);
    if (!response) {
        return scope.Deliver(result<void>::err(std::move(response.error())));
    }
    return scope.Deliver(CheckStatus(response.value(), "register"));
}

bbt_co_async(result<void>) Client::Deregister(std::string_view id)
{
    OpScope scope(m_state);
    if (!scope.entered()) {
        return result<void>::err(scope.error());
    }
    if (id.empty()) {
        return scope.Deliver(result<void>::err(InvalidArgument("deregister id must not be empty")));
    }
    const auto& state = scope.state();
    auto response = bbt_co_await state->Send(
        "PUT", "/v1/agent/service/deregister/" + PercentEncode(id), std::string{}, state->options.request_timeout);
    if (!response) {
        return scope.Deliver(result<void>::err(std::move(response.error())));
    }

    /**
     * agent 对已不存在的 ID 返回 404（实测 1.20.2："Unknown service ID"）。注销的
     * 语义是「结束登记」，只有 agent 明确返回该 body 才视为目标已不存在；其它 404
     * 仍是明确拒绝，避免把错误代理/路径误判为幂等成功。
     */
    if (response.value().status_code == 404 && response.value().body.find("Unknown service ID") != std::string::npos) {
        return scope.Deliver(result<void>::ok());
    }
    return scope.Deliver(CheckStatus(response.value(), "deregister"));
}

bbt_co_async(result<ServiceSnapshot>) Client::HealthyInstances(std::string_view service, std::uint64_t index)
{
    OpScope scope(m_state);
    if (!scope.entered()) {
        return result<ServiceSnapshot>::err(scope.error());
    }
    if (service.empty()) {
        return scope.Deliver(result<ServiceSnapshot>::err(InvalidArgument("service name must not be empty")));
    }
    const auto& state = scope.state();
    http::QueryParams params;
    params.emplace_back("passing", "true");
    if (!state->options.datacenter.empty()) {
        params.emplace_back("dc", state->options.datacenter);
    }
    if (index != 0 && state->options.query_wait > std::chrono::milliseconds{0}) {
        params.emplace_back("index", std::to_string(index));

        /**
         * 用 ms 表达 wait：避免秒级取整引入额外 jitter，仍是 Consul 接受的时长。
         */
        params.emplace_back("wait", std::to_string(state->options.query_wait.count()) + "ms");
    }
    auto target = "/v1/health/service/" + PercentEncode(service);
    if (!params.empty()) {
        target.push_back('?');
        for (std::size_t i = 0; i < params.size(); ++i) {
            if (i != 0) {
                target.push_back('&');
            }
            target += PercentEncode(params[i].first) + "=" + PercentEncode(params[i].second);
        }
    }

    const bool blocking = index != 0 && state->options.query_wait > std::chrono::milliseconds{0};
    auto response = bbt_co_await state->Send("GET", target, std::string{}, state->ReadBudgetForQuery(blocking));
    if (!response) {
        return scope.Deliver(result<ServiceSnapshot>::err(std::move(response.error())));
    }
    /**
     * 解析只产生候选终态；所有成功/错误统一在 Deliver 与 Immediate 线性化。
     */
    auto outcome = [&response, service]() -> result<ServiceSnapshot> {
        const auto& message = response.value();
        if (auto ok = CheckStatus(message, "health query"); !ok) {
            return result<ServiceSnapshot>::err(std::move(ok.error()));
        }

        const auto index_header = HeaderValue(message.headers, "X-Consul-Index");
        if (!index_header || index_header->empty()) {
            return result<ServiceSnapshot>::err(Protocol("health response missing X-Consul-Index"));
        }
        std::uint64_t revision = 0;
        const auto parsed_index =
            std::from_chars(index_header->data(), index_header->data() + index_header->size(), revision);
        if (parsed_index.ec != std::errc() || parsed_index.ptr != index_header->data() + index_header->size()) {
            return result<ServiceSnapshot>::err(Protocol("health response has invalid X-Consul-Index"));
        }

        boost::system::error_code parse_error;
        const auto document = boost::json::parse(message.body, parse_error);
        if (parse_error) {
            return result<ServiceSnapshot>::err(Protocol("health response is not valid json"));
        }
        if (!document.is_array()) {
            return result<ServiceSnapshot>::err(Protocol("health response must be an array"));
        }

        ServiceSnapshot snapshot;
        snapshot.index = revision;
        for (const auto& entry : document.get_array()) {
            if (!entry.is_object()) {
                return result<ServiceSnapshot>::err(Protocol("health entry must be an object"));
            }
            const auto& object = entry.get_object();
            const auto* service_value = Member(object, "Service");
            if (service_value == nullptr || !service_value->is_object()) {
                return result<ServiceSnapshot>::err(Protocol("health entry missing Service object"));
            }
            const auto& service_object = service_value->get_object();

            /**
             * passing=true 由 agent 过滤；这里再核一遍 checks 状态，避免 agent 忽略参数
             * 时把 critical 实例当成可用目标（判定只是状态串，不重算健康）。
             */
            const auto* checks = Member(object, "Checks");
            if (checks != nullptr) {
                if (!checks->is_array()) {
                    return result<ServiceSnapshot>::err(Protocol("health entry Checks must be an array"));
                }
                bool passing = true;
                for (const auto& check : checks->get_array()) {
                    if (!check.is_object()) {
                        return result<ServiceSnapshot>::err(Protocol("health check must be an object"));
                    }
                    const auto status = StringMember(check.get_object(), "Status");
                    if (!status) {
                        return result<ServiceSnapshot>::err(Protocol("health check missing Status"));
                    }
                    if (*status != "passing") {
                        passing = false;
                        break;
                    }
                }
                if (!passing) {
                    continue;
                }
            }

            const auto id = StringMember(service_object, "ID");
            if (!id || id->empty()) {
                return result<ServiceSnapshot>::err(Protocol("health entry has empty service ID"));
            }
            const auto port = PortMember(service_object, "Port");
            if (!port) {
                return result<ServiceSnapshot>::err(Protocol("health entry has invalid service port"));
            }
            std::string address;
            if (const auto declared = StringMember(service_object, "Address"); declared && !declared->empty()) {
                address = *declared;
            } else {
                const auto* node = Member(object, "Node");
                if (node == nullptr || !node->is_object()) {
                    return result<ServiceSnapshot>::err(Protocol("health entry missing Node address"));
                }
                const auto node_address = StringMember(node->get_object(), "Address");
                if (!node_address || node_address->empty()) {
                    return result<ServiceSnapshot>::err(Protocol("health entry has no usable address"));
                }
                address = *node_address;
            }
            if (!IsNumericIpv4(address)) {
                return result<ServiceSnapshot>::err(Protocol("health entry address is not numeric ipv4"));
            }
            Instance instance;
            instance.id = *id;
            instance.name = StringMember(service_object, "Service").value_or(std::string(service));
            instance.address = std::move(address);
            instance.port = *port;
            snapshot.instances.push_back(std::move(instance));
        }
        return result<ServiceSnapshot>::ok(std::move(snapshot));
    }();
    if (auto pause = TakeHealthDeliveryPause(*this)) {
        pause->reached.store(true, std::memory_order_release);
        bbt::coroutine::sync::CombinedWaitOptions wait;
        wait.deadline = std::chrono::steady_clock::now() + std::chrono::seconds{10};
        (void)pause->resume->Wait(wait);
    }
    return scope.Deliver(std::move(outcome));
}

result<void>
Client::WaitUntil(const std::shared_ptr<State>& state, bbt::coroutine::Deadline deadline, bool terminal) noexcept
{
    const auto satisfied = [&state, terminal] {
        // clang-format off

        /**
         * 调用方持 state->mutex。
         */
        // clang-format on
        return terminal ? state->terminal : state->inflight == 0;
    };
    const auto domain = bbt::infra::detail::CurrentCloseWaitDomain();
    if (domain == bbt::infra::detail::CloseWaitDomain::Invalid) {
        return result<void>::err(
            MakeError(ErrorCode::InvalidContext, "consul: close wait requires coroutine or native host"));
    }
    if (domain == bbt::infra::detail::CloseWaitDomain::NativeHost) {
        std::unique_lock<std::mutex> lock(state->mutex);
        if (!state->idle.wait_until(lock, deadline, satisfied)) {
            return result<void>::err(MakeError(ErrorCode::TimedOut, "consul: close wait deadline expired"));
        }
        return result<void>::ok();
    }

    /**
     * 协程域必须协作挂起：阻塞 Processer 线程会让在途操作无法恢复（单 worker
     * 直接死锁；并发 Close 也能占满多 worker）。每轮绝对截止时间取
     * min(deadline, now+50ms)，不退回 native CV。
     */
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            if (satisfied()) {
                return result<void>::ok();
            }
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return result<void>::err(MakeError(ErrorCode::TimedOut, "consul: close wait deadline expired"));
        }
        auto waiter = bbt::coroutine::sync::CoWaiter::Create();
        if (!waiter) {
            return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable, "consul: close wait signal unavailable"));
        }
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            if (satisfied()) {
                return result<void>::ok();
            }
            state->co_waiters.push_back(waiter);
        }
        auto slice = bbt::infra::detail::WaitCloseSlice(waiter, bbt::infra::detail::NextCloseSlice(deadline));
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            auto& waiters = state->co_waiters;
            waiters.erase(std::remove(waiters.begin(), waiters.end(), waiter), waiters.end());
        }
        if (!slice) {
            return result<void>::err(std::move(slice).error());
        }
    }
}

void Client::_BeginClose(Error reason, bool immediate) noexcept
{
    const auto state = m_state;
    bool close_http = false;
    std::vector<bbt::coroutine::sync::CoWaiter::SPtr> woken;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (!state->reason) {
            state->reason = std::move(reason);
        }
        state->closed.store(true);
        state->immediate = state->immediate || immediate;
        close_http = state->BeginHttpCloseLocked();
        if (!close_http) {
            state->MarkTerminalLocked(&woken);
        }
    }
    if (close_http) {

        /**
         * 已无在途操作：封口专属 HTTP Client（不等待物理退役）。
         */
        state->CompleteHttpClose();
    } else if (!woken.empty()) {
        state->WakeAll(woken);
    }
}

std::optional<Error> Client::CloseReason() const noexcept
{
    const auto state = m_state;
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->reason;
}

result<void> Client::Close(const CloseOptions& options)
{
    auto valid = bbt::infra::detail::ValidateCloseOptions(options);
    if (!valid) {
        return valid;
    }
    const bool waits = options.mode == CloseMode::Graceful || options.wait_release;
    if (waits && bbt::infra::detail::CurrentCloseWaitDomain() == bbt::infra::detail::CloseWaitDomain::Invalid) {
        return result<void>::err(
            MakeError(ErrorCode::InvalidContext, "consul: Close requires coroutine or native host"));
    }
    _BeginClose(
        bbt::infra::detail::SettleCloseError(options.reason, "consul: client closed by owner"),
        options.mode == CloseMode::Immediate);
    if (options.mode == CloseMode::Immediate) {
        return bbt::infra::detail::FinishCloseWithRelease(
            options, [this](bbt::coroutine::Deadline deadline) { return _WaitReleased(deadline); });
    }

    /**
     * 非终态等待：等的是已登记操作离开，不是等本对象真实回收。
     */
    if (auto drained = WaitUntil(m_state, options.deadline, false); !drained) {
        return drained;
    }
    return bbt::infra::detail::FinishCloseWithRelease(
        options, [this](bbt::coroutine::Deadline deadline) { return _WaitReleased(deadline); });
}

result<void> Client::_WaitReleased(bbt::coroutine::Deadline deadline)
{
    const auto state = m_state;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (!state->closed.load()) {
            return result<void>::err(
                MakeError(ErrorCode::InvalidArgument, "consul: Close or RequestClose before WaitStopped"));
        }
    }

    /**
     * 终态等待：调用方要的是真实回收，须等 terminal 而非仅在途归零。
     */
    auto waited = WaitUntil(state, deadline, true);
    if (!waited) {
        return waited;
    }

    /**
     * 专属 HTTP Client 的连接真实退役：同一期限内观察，不与 adapter 终态混同。
     */
    return state->http->Close(bbt::infra::CloseOptions{bbt::infra::CloseMode::Immediate, true, {}, deadline});
}

bool Client::IsClosed() const noexcept
{
    return m_state->closed.load();
}

// clang-format off
}
// clang-format on

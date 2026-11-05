#include <bbt/infra/consul/Client.hpp>

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

// Boost.JSON 以源码方式内联进本模块（仓内唯一一处之外的第二个 TU 会与 MCP 的
// JsonValue.cc 重复定义；顶层 CMake 对 BBT_INFRA_CONSUL + BBT_INFRA_MCP 同开做
// fail-closed 门禁）。不链接 libboost_json，不新增第三方依赖。
#include <boost/json/src.hpp>

#include <arpa/inet.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/syntax/Async.hpp>

namespace bbt::infra::consul {
namespace {

using bbt::infra::Error;
using bbt::infra::ErrorCode;

Error InvalidArgument(std::string message) {
    return MakeError(ErrorCode::InvalidArgument, "consul: " + std::move(message));
}

Error Protocol(std::string message) {
    return MakeError(ErrorCode::ProtocolError, "consul: " + std::move(message));
}

Error ClosedError() {
    return MakeError(ErrorCode::Closed, "consul: client closed");
}

bool EqualsNoCase(std::string_view left, std::string_view right) {
    if (left.size() != right.size()) {
        return false;
    }
    for (std::size_t i = 0; i < left.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(left[i])) !=
            std::tolower(static_cast<unsigned char>(right[i]))) {
            return false;
        }
    }
    return true;
}

// RFC 3986 unreserved 之外的字节一律 %XX：service 名与实例 ID 是用户输入，
// 直接拼进路径会让 '/'、'?'、'#' 改变目标路由。
std::string PercentEncode(std::string_view value) {
    static constexpr char kHex[] = "0123456789ABCDEF";
    std::string out;
    out.reserve(value.size());
    for (const unsigned char c : value) {
        const bool unreserved = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                                (c >= '0' && c <= '9') || c == '-' || c == '_' ||
                                c == '.' || c == '~';
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

bool IsNumericIpv4(std::string_view address) {
    if (address.empty()) {
        return false;
    }
    in_addr parsed{};
    const std::string text(address);
    return ::inet_pton(AF_INET, text.c_str(), &parsed) == 1;
}

bool IsLoopbackIpv4(std::string_view address) {
    in_addr parsed{};
    const std::string text(address);
    if (::inet_pton(AF_INET, text.c_str(), &parsed) != 1) {
        return false;
    }
    return (ntohl(parsed.s_addr) >> 24) == 127;
}

// agent 地址归一为无尾斜杠的 "http://<ipv4>:<port>"：https、主机名、路径、远端
// 地址都在此拒绝——该层只承诺本机 agent，远端注册中心不是它假装支持的形态。
result<void> NormalizeAgentUrl(std::string_view url, std::string* out) {
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
    if (parsed.ec != std::errc() || parsed.ptr != port_text.data() + port_text.size() ||
        port == 0 || port > 65535) {
        return result<void>::err(InvalidArgument("agent_url port must be 1..65535"));
    }
    out->assign("http://" + host + ":" + std::to_string(port));
    return result<void>::ok();
}

result<void> ValidateRegistration(const ServiceRegistration& registration) {
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

// 200/2xx 之外一律 TransportError 并保留状态码：Consul 的拒绝不是传输失败，但
// 对本层调用人是「后端明确拒绝」，与协议错（ProtocolError）、超时（TimedOut）
// 可区分即可；不把响应体拼进消息（可能含注册数据）。
result<void> CheckStatus(const http::Response& response, std::string_view action) {
    if (response.Ok()) {
        return result<void>::ok();
    }
    Error error = MakeError(ErrorCode::TransportError,
                            "consul: " + std::string(action) + " rejected with HTTP " +
                                std::to_string(response.status_code));
    error.backend_category = "consul.http";
    error.backend_code = response.status_code;
    return result<void>::err(std::move(error));
}

std::optional<std::string> HeaderValue(const http::Headers& headers, std::string_view name) {
    for (const auto& header : headers) {
        if (EqualsNoCase(header.first, name)) {
            return header.second;
        }
    }
    return std::nullopt;
}

const boost::json::value* Member(const boost::json::object& object, std::string_view key) {
    return object.if_contains(key);
}

std::optional<std::string> StringMember(const boost::json::object& object, std::string_view key) {
    const auto* value = Member(object, key);
    if (value == nullptr || !value->is_string()) {
        return std::nullopt;
    }
    return std::string(value->get_string());
}

std::optional<std::uint16_t> PortMember(const boost::json::object& object, std::string_view key) {
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

} // namespace

struct Client::State {
    State(std::shared_ptr<bbt::infra::http::Client> client, Options opts)
        : http(std::move(client)), options(std::move(opts)) {}

    // 单次 HTTP 交换：token 头、分阶段超时、总期限在此统一收口，业务路径只给
    // 方法与 target（target 以 '/' 开头的 origin-form）。
    bbt_co_async(result<http::Response>) Send(std::string_view method,
                                              const std::string& target,
                                              std::string body,
                                              std::chrono::milliseconds read_budget) const {
        http::RequestOptions request;
        request.headers.emplace_back("Accept", "application/json");
        if (!options.token.empty()) {
            request.headers.emplace_back("X-Consul-Token", options.token);
        }
        http::Timeout timeout;
        timeout.connect = http::PhaseTimeout::Milliseconds(options.request_timeout);
        timeout.write = http::PhaseTimeout::Milliseconds(options.request_timeout);
        timeout.pool = http::PhaseTimeout::Milliseconds(options.request_timeout);
        timeout.read = http::PhaseTimeout::Milliseconds(read_budget);
        request.timeout = timeout;

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

    // 阻塞查询的读档必须容纳 Consul 允许的 wait/16 附加抖动，否则「阻塞返回」
    // 会先被读档超时打断；非阻塞快照使用整个 request_timeout。
    std::chrono::milliseconds ReadBudgetForQuery(bool blocking) const {
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

    mutable std::mutex mutex;     // 保护 inflight/terminal 与协程等待者登记
    std::condition_variable idle; // 宿主线程 Close 等真实终态
    std::vector<bbt::coroutine::sync::CoWaiter::SPtr> co_waiters; // 协程域 Close
    std::size_t inflight{0};
    std::atomic<bool> closed{false}; // 封口：拒绝新操作（IsClosed 同此语义）
    bool http_closed{false};         // 专属 HTTP Client 已关闭
    bool http_close_started{false};  // 仅一个路径负责关闭专属 HTTP Client
    bool terminal{false};            // 封口 && http_closed && inflight==0，单调不可逆

    // 终态发布（锁内调用）：仅当三条件同时成立才置位，并摘下全部协程等待者。
    // 返回 true 表示本次发布；woken 供调用方在锁外 Notify（不持锁唤醒）。
    bool MarkTerminalLocked(std::vector<bbt::coroutine::sync::CoWaiter::SPtr>* woken) {
        if (terminal || inflight != 0 || !closed.load() || !http_closed) {
            return false;
        }
        terminal = true;
        woken->swap(co_waiters);
        return true;
    }

    // adapter 先封口，待已接纳操作全部离开后再关闭专属 HTTP Client。当前 HTTP
    // Client::Close() 会立即让在途交换失败，因此不能在 adapter Close 的第一步调用。
    bool BeginHttpCloseLocked() {
        if (!closed.load() || inflight != 0 || http_close_started) {
            return false;
        }
        http_close_started = true;
        return true;
    }

    void CompleteHttpClose() noexcept {
        http->Close();
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

    // 锁外唤醒：宿主线程 cv + 协程等待者。Notify 对已决议/已解绑的 waiter 返回
    // -1，无副作用，可安全重复。
    void WakeAll(const std::vector<bbt::coroutine::sync::CoWaiter::SPtr>& woken) noexcept {
        idle.notify_all();
        for (const auto& waiter : woken) {
            waiter->Notify();
        }
    }
};

// 在飞登记：封口后拒绝新操作；析构归还名额，最后一个离开的操作关闭专属 HTTP
// Client 并发布终态、唤醒 Close 的等待者。状态由 shared_ptr 持有，Close 等待期间
// Client 析构也不会悬空。
class Client::OpScope {
public:
    explicit OpScope(std::shared_ptr<State> state) : m_state(std::move(state)) {
        std::lock_guard<std::mutex> lock(m_state->mutex);
        if (m_state->closed.load()) {
            m_error = ClosedError();
            return;
        }
        ++m_state->inflight;
        m_entered = true;
    }
    ~OpScope() {
        if (!m_entered) {
            return;
        }
        std::vector<bbt::coroutine::sync::CoWaiter::SPtr> woken;
        bool published = false;
        bool close_http = false;
        {
            std::lock_guard<std::mutex> lock(m_state->mutex);
            --m_state->inflight;
            close_http = m_state->BeginHttpCloseLocked();
            if (!close_http) {
                published = m_state->MarkTerminalLocked(&woken);
            }
        }
        // 锁外关闭专属 HTTP Client；最后一个离开的操作负责启动它，随后再发布终态。
        if (close_http) {
            m_state->CompleteHttpClose();
        } else if (published) {
            m_state->WakeAll(woken);
        }
    }
    OpScope(const OpScope&) = delete;
    OpScope& operator=(const OpScope&) = delete;

    bool entered() const noexcept { return m_entered; }
    Error error() const { return m_error; }
    const std::shared_ptr<State>& state() const noexcept { return m_state; }

private:
    std::shared_ptr<State> m_state;
    Error m_error;
    bool m_entered{false};
};

result<std::shared_ptr<Client>> Client::Create(
    std::shared_ptr<bbt::infra::http::Client> http, Options options) {
    if (!http) {
        return result<std::shared_ptr<Client>>::err(InvalidArgument("http client required"));
    }
    if (options.request_timeout <= std::chrono::milliseconds{0}) {
        return result<std::shared_ptr<Client>>::err(
            InvalidArgument("request_timeout must be positive"));
    }
    if (options.query_wait < std::chrono::milliseconds{0}) {
        return result<std::shared_ptr<Client>>::err(InvalidArgument("query_wait must not be negative"));
    }
    if (options.query_wait > std::chrono::milliseconds{0}) {
        constexpr auto kQueryMargin = std::chrono::milliseconds{250};
        const auto wait_count = options.query_wait.count();
        const auto jitter = std::chrono::milliseconds{
            wait_count / 16 + (wait_count % 16 == 0 ? 0 : 1)};
        if (options.request_timeout <= kQueryMargin ||
            jitter >= options.request_timeout - kQueryMargin ||
            options.query_wait >= options.request_timeout - kQueryMargin - jitter) {
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

Client::Client(std::shared_ptr<State> state) noexcept : m_state(std::move(state)) {}

Client::~Client() noexcept {
    Close();
}

bbt_co_async(result<void>) Client::Register(ServiceRegistration registration) {
    OpScope scope(m_state);
    if (!scope.entered()) {
        return result<void>::err(scope.error());
    }
    const auto& state = scope.state();
    if (auto valid = ValidateRegistration(registration); !valid) {
        return result<void>::err(std::move(valid.error()));
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
    // TCP check 只证明端口可达，不宣称业务 readiness；DeregisterCriticalServiceAfter
    // 让崩溃进程的登记不会永久残留。
    boost::json::object check;
    check["TCP"] = registration.address + ":" + std::to_string(registration.port);
    check["Interval"] = "1s";
    check["Timeout"] = "500ms";
    check["DeregisterCriticalServiceAfter"] = "1m";
    payload["Check"] = std::move(check);

    auto response = bbt_co_await state->Send("PUT", "/v1/agent/service/register",
                                             boost::json::serialize(payload),
                                             state->options.request_timeout);
    if (!response) {
        return result<void>::err(std::move(response.error()));
    }
    return CheckStatus(response.value(), "register");
}

bbt_co_async(result<void>) Client::Deregister(std::string_view id) {
    OpScope scope(m_state);
    if (!scope.entered()) {
        return result<void>::err(scope.error());
    }
    if (id.empty()) {
        return result<void>::err(InvalidArgument("deregister id must not be empty"));
    }
    const auto& state = scope.state();
    auto response = bbt_co_await state->Send("PUT", "/v1/agent/service/deregister/" + PercentEncode(id),
                                             std::string{}, state->options.request_timeout);
    if (!response) {
        return result<void>::err(std::move(response.error()));
    }
    // agent 对已不存在的 ID 返回 404（实测 1.20.2："Unknown service ID"）。注销的
    // 语义是「结束登记」，只有 agent 明确返回该 body 才视为目标已不存在；其它 404
    // 仍是明确拒绝，避免把错误代理/路径误判为幂等成功。
    if (response.value().status_code == 404 &&
        response.value().body.find("Unknown service ID") != std::string::npos) {
        return result<void>::ok();
    }
    return CheckStatus(response.value(), "deregister");
}

bbt_co_async(result<ServiceSnapshot>) Client::HealthyInstances(std::string_view service,
                                                               std::uint64_t index) {
    OpScope scope(m_state);
    if (!scope.entered()) {
        return result<ServiceSnapshot>::err(scope.error());
    }
    if (service.empty()) {
        return result<ServiceSnapshot>::err(InvalidArgument("service name must not be empty"));
    }
    const auto& state = scope.state();
    http::QueryParams params;
    params.emplace_back("passing", "true");
    if (!state->options.datacenter.empty()) {
        params.emplace_back("dc", state->options.datacenter);
    }
    if (index != 0 && state->options.query_wait > std::chrono::milliseconds{0}) {
        params.emplace_back("index", std::to_string(index));
        // 用 ms 表达 wait：避免秒级取整引入额外 jitter，仍是 Consul 接受的时长。
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
    auto response = bbt_co_await state->Send("GET", target, std::string{},
                                             state->ReadBudgetForQuery(blocking));
    if (!response) {
        return result<ServiceSnapshot>::err(std::move(response.error()));
    }
    const auto& message = response.value();
    if (auto ok = CheckStatus(message, "health query"); !ok) {
        return result<ServiceSnapshot>::err(std::move(ok.error()));
    }

    const auto index_header = HeaderValue(message.headers, "X-Consul-Index");
    if (!index_header || index_header->empty()) {
        return result<ServiceSnapshot>::err(Protocol("health response missing X-Consul-Index"));
    }
    std::uint64_t revision = 0;
    const auto parsed_index = std::from_chars(index_header->data(),
                                              index_header->data() + index_header->size(), revision);
    if (parsed_index.ec != std::errc() ||
        parsed_index.ptr != index_header->data() + index_header->size()) {
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

        // passing=true 由 agent 过滤；这里再核一遍 checks 状态，避免 agent 忽略参数
        // 时把 critical 实例当成可用目标（判定只是状态串，不重算健康）。
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
}

void Client::WaitTerminal(const std::shared_ptr<State>& state) noexcept {
    if (bbt::coroutine::IsInCoroutineContext()) {
        // 协程域必须协作挂起：阻塞 Processer 线程会让在途操作无法恢复（单 worker
        // 直接死锁；并发 Close 也能占满多 worker），因此不用 condition_variable。
        for (;;) {
            bbt::coroutine::sync::CoWaiter::SPtr waiter;
            try {
                waiter = bbt::coroutine::sync::CoWaiter::Create();
            } catch (...) {
                // 等待装配失败（分配等）：退化为宿主线程式阻塞等待同一终态。
                break;
            }
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                if (state->terminal) {
                    return;
                }
                state->co_waiters.push_back(waiter);
            }
            bbt::coroutine::WaitStatus status = bbt::coroutine::WaitStatus::RuntimeUnavailable;
            try {
                status = waiter->WaitWithCallback(
                    bbt::coroutine::WaitOptions{},
                    [state, waiter] {
                        bool done = false;
                        {
                            std::lock_guard<std::mutex> lock(state->mutex);
                            done = state->terminal;
                        }
                        // 登记成功后的复查：终态若落在「入表」与「真正挂起」之间，
                        // 这里同步 Notify 走 PENDING 早到路径，不丢唤醒。
                        if (done) {
                            waiter->Notify();
                        }
                        return true;
                    });
            } catch (...) {
                // 装配失败：先摘掉登记再退化，避免留下不会被等待的孤儿 waiter。
                std::lock_guard<std::mutex> lock(state->mutex);
                state->co_waiters.erase(
                    std::remove(state->co_waiters.begin(), state->co_waiters.end(), waiter),
                    state->co_waiters.end());
                break;
            }
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                auto& waiters = state->co_waiters;
                waiters.erase(std::remove(waiters.begin(), waiters.end(), waiter),
                              waiters.end());
                if (state->terminal) {
                    return;
                }
            }
            if (status == bbt::coroutine::WaitStatus::Cancelled ||
                status == bbt::coroutine::WaitStatus::RuntimeUnavailable) {
                // 预取消或等待设施不可用：CoWaiter 入口即返回，不能挂起；
                // Hook_Sleep/YieldUntilTimeout 同样在取消态立即返回。bbtco_yield
                // 不检查取消位，真实把 worker 还给就绪队列，让在飞操作恢复并推进
                // 终态；仍不提前返回，也不用线程 sleep/自旋占住 worker。
                bbtco_yield;
            }
        }
    }
    std::unique_lock<std::mutex> lock(state->mutex);
    state->idle.wait(lock, [&state] { return state->terminal; });
}

void Client::Close() noexcept {
    const auto state = m_state;
    bool close_http = false;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->closed.exchange(true);
        close_http = state->BeginHttpCloseLocked();
    }
    if (close_http) {
        // HTTP Client::Close() 会立即中止在途交换，所以只在 adapter 已无在途
        // 操作时调用；这样本对象仍能兑现「返回即专属 HTTP Client 已关闭」。
        state->CompleteHttpClose();
    }
    // 首个与并发 Close 都等同一真实终态：返回即已封口、专属 HTTP Client 已关闭、
    // 已登记操作全部离开。每个操作受 request_timeout 绝对期限约束，终态可达；
    // 这里不设提前返回上限（0005 §6.4：不以 drain timeout 放弃等待）。
    WaitTerminal(state);
}

bool Client::IsClosed() const noexcept {
    return m_state->closed.load();
}

} // namespace bbt::infra::consul

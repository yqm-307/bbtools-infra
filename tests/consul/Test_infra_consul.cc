// infra.consul：Consul HTTP adapter 的定向验收。
//
// 覆盖两类证据，全部经真实网络栈（不 mock HTTP、不 mock JSON）：
//   1) 参数/协议/关闭错误面：被拒绝的 agent 地址与注册字段、HTTP 非 2xx、非法
//      JSON、缺失或非法 X-Consul-Index、非法实例端口、critical 实例过滤、
//      封口后拒绝新操作——用自有 loopback 对端返回固定字节（对端只产生协议
//      字节，不替代 Consul 语义）；
//   2) 真后端闭环：以官方固定版 Consul（BBT_TEST_CONSUL_BIN，SHA256 已核）在
//      本进程租用的动态端口上 -dev 启动，走真实注册/健康查询/阻塞查询 index/
//      注销/空视图，并在 agent 停止后验证不可达在有限期限内失败。
//
// 只管理本进程启动的 Consul 与本次租用的 loopback socket；结束/失败均经析构
// 收口（SIGTERM → 有界等待 → SIGKILL 只作用于自己的子进程），不 pkill。
#include "../http_net/TestSupport.hpp"
#include "../net/ProcessRuntimeTestMain.hpp"

#include <bbt/infra/consul/Client.hpp>
#include <bbt/infra/http/Client.hpp>
#include <bbt/infra/net/Engine.hpp>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

extern char** environ;

using namespace http_net_test; // NOLINT：复用 http_net 的 Host/CHECK/Watchdog 夹具
namespace consul = bbt::infra::consul;
namespace http = bbt::infra::http;

namespace {

using std::chrono::milliseconds;
using namespace std::chrono_literals;

// 本套件含真实 Consul 启停与阻塞查询，固定 20s 的 http_net watchdog 不够，
// 这里用同一个「超时仍判失败」的出口，但给出与 ctest TIMEOUT 一致的界。
class BoundedWatchdog {
public:
    explicit BoundedWatchdog(std::chrono::milliseconds budget)
        : m_thread([this, budget] {
              std::unique_lock<std::mutex> lock(m_mutex);
              if (!m_cv.wait_for(lock, budget, [this] { return m_done; })) {
                  std::cerr << "FAIL watchdog: bounded test did not finish\n" << std::flush;
                  std::_Exit(3);
              }
          })
    {
    }
    ~BoundedWatchdog()
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_done = true;
        }
        m_cv.notify_one();
        m_thread.join();
    }

private:
    mutable std::mutex m_mutex;
    std::condition_variable m_cv;
    bool m_done{false};
    std::thread m_thread;
};

// ---------------------------------------------------------------- 协程辅助 ---

// 协程内休眠：用 CoWaiter 的到期唤醒让出 worker，而不是阻塞调度线程。
void CoSleep(milliseconds duration)
{
    auto waiter = bbt::coroutine::sync::CoWaiter::Create();
    CHECK(waiter);
    const auto status = waiter->WaitWithCallback({std::chrono::steady_clock::now() + duration}, [] { return true; });
    CHECK(status == bbt::coroutine::WaitStatus::TimedOut);
}

// 关闭契约的观测时间戳：同一进程内单调可比，只用于比较「谁先返回」。
long long NowMs()
{
    return std::chrono::duration_cast<milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

// 在本进程 Host 的调度器上执行一次 await，可选记录墙钟耗时（阻塞查询证据）。
template <class F> auto Await(Host& host, F fn, long long* elapsed_ms = nullptr) -> decltype(fn())
{
    const auto start = std::chrono::steady_clock::now();
    auto future = host.Submit(std::move(fn));
    auto value = Get(future);
    if (elapsed_ms != nullptr) {
        *elapsed_ms = std::chrono::duration_cast<milliseconds>(std::chrono::steady_clock::now() - start).count();
    }
    return value;
}

// ---------------------------------------------------------------- 端口/监听 ---

int FreeTcpPort()
{
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    CHECK(fd >= 0);
    sockaddr_in local{};
    local.sin_family = AF_INET;
    local.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    local.sin_port = 0;
    CHECK(::bind(fd, reinterpret_cast<sockaddr*>(&local), sizeof(local)) == 0);
    socklen_t size = sizeof(local);
    CHECK(::getsockname(fd, reinterpret_cast<sockaddr*>(&local), &size) == 0);
    const int port = ntohs(local.sin_port);
    ::close(fd);
    return port;
}

// 真实 listener：Consul 的 TCP check 只有真端口才通过，测试据此区分健康实例。
class ListenSocket {
public:
    ListenSocket()
    {
        m_fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        CHECK(m_fd >= 0);
        int one = 1;
        CHECK(::setsockopt(m_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) == 0);
        sockaddr_in local{};
        local.sin_family = AF_INET;
        local.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        CHECK(::bind(m_fd, reinterpret_cast<sockaddr*>(&local), sizeof(local)) == 0);
        CHECK(::listen(m_fd, 16) == 0);
        socklen_t size = sizeof(local);
        CHECK(::getsockname(m_fd, reinterpret_cast<sockaddr*>(&local), &size) == 0);
        m_port = ntohs(local.sin_port);
    }
    ~ListenSocket()
    {
        if (m_fd >= 0) {
            ::close(m_fd);
        }
    }
    ListenSocket(const ListenSocket&) = delete;
    ListenSocket& operator=(const ListenSocket&) = delete;
    std::uint16_t port() const noexcept
    {
        return m_port;
    }

private:
    int m_fd{-1};
    std::uint16_t m_port{0};
};

// ---------------------------------------------------------------- 固定对端 ---

std::string RawResponse(int status, const std::string& reason, std::string body,
                        std::vector<std::pair<std::string, std::string>> headers = {})
{
    std::string out = "HTTP/1.1 " + std::to_string(status) + " " + reason + "\r\n";
    out += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    out += "Content-Type: application/json\r\n";
    out += "Connection: close\r\n";
    for (const auto& header : headers) {
        out += header.first + ": " + header.second + "\r\n";
    }
    out += "\r\n";
    out += body;
    return out;
}

// 固定应答的 loopback HTTP/1.1 对端：只产生协议字节（状态码/头/体），用来验证
// adapter 对非 2xx、坏 JSON、缺 index 的映射；不参与任何 Consul 语义判断。
class CannedPeer {
public:
    struct Recorded {
        std::string method;
        std::string target;
        std::string body;
    };

    explicit CannedPeer(std::string response, bool hold = false) : m_response(std::move(response)), m_hold(hold)
    {
        m_listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        CHECK(m_listener >= 0);
        int one = 1;
        CHECK(::setsockopt(m_listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) == 0);
        sockaddr_in local{};
        local.sin_family = AF_INET;
        local.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        CHECK(::bind(m_listener, reinterpret_cast<sockaddr*>(&local), sizeof(local)) == 0);
        CHECK(::listen(m_listener, 8) == 0);
        socklen_t size = sizeof(local);
        CHECK(::getsockname(m_listener, reinterpret_cast<sockaddr*>(&local), &size) == 0);
        m_port = ntohs(local.sin_port);
        m_thread = std::thread([this] { Loop(); });
    }
    ~CannedPeer()
    {
        Stop();
    }
    CannedPeer(const CannedPeer&) = delete;
    CannedPeer& operator=(const CannedPeer&) = delete;

    std::string base_url() const
    {
        return "http://127.0.0.1:" + std::to_string(m_port);
    }
    std::vector<Recorded> requests() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_requests;
    }
    // hold 模式下：请求已被对端完整读出（在飞阶段成立）才返回 true。这是
    // 「先证明真实操作进入目标阶段，再触发关闭」的观测点，不是 sleep 猜时序。
    bool WaitFirstRequest(milliseconds timeout = 5s)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_cv.wait_for(lock, timeout, [this] { return m_received; });
    }
    // 放行被 hold 的响应；之后的回包时刻由 response_sent_at() 观测。
    void Release()
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_released = true;
        }
        m_cv.notify_all();
    }
    long long response_sent_at() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_response_sent_at;
    }
    void Stop()
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_stopped) {
                return;
            }
            m_stopped = true;
            m_released = true; // hold 中的对端线程必须先被放行才能 join
            if (m_conn >= 0) {
                ::shutdown(m_conn, SHUT_RDWR);
            }
        }
        m_cv.notify_all();
        ::shutdown(m_listener, SHUT_RDWR);
        if (m_thread.joinable()) {
            m_thread.join();
        }
    }

private:
    void Loop()
    {
        while (true) {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (m_stopped) {
                    return;
                }
            }
            pollfd pfd{m_listener, POLLIN, 0};
            if (::poll(&pfd, 1, 50) != 1) {
                continue;
            }
            const int fd = ::accept4(m_listener, nullptr, nullptr, SOCK_CLOEXEC);
            if (fd < 0) {
                continue;
            }
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_conn = fd;
                if (m_stopped) {
                    ::close(fd);
                    m_conn = -1;
                    return;
                }
            }
            Serve(fd);
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (m_conn == fd) {
                    m_conn = -1;
                }
            }
            ::close(fd);
        }
    }

    void Serve(int fd)
    {
        std::string buffer;
        char chunk[2048];
        while (buffer.find("\r\n\r\n") == std::string::npos) {
            const auto n = ::recv(fd, chunk, sizeof(chunk), 0);
            if (n <= 0) {
                return;
            }
            buffer.append(chunk, static_cast<std::size_t>(n));
            if (buffer.size() > 64 * 1024) {
                return;
            }
        }
        const auto head_end = buffer.find("\r\n\r\n");
        Recorded request;
        const std::string head = buffer.substr(0, head_end);
        const auto line_end = head.find("\r\n");
        const std::string request_line = head.substr(0, line_end);
        const auto sp1 = request_line.find(' ');
        const auto sp2 = request_line.find(' ', sp1 == std::string::npos ? 0 : sp1 + 1);
        if (sp1 == std::string::npos || sp2 == std::string::npos) {
            return;
        }
        request.method = request_line.substr(0, sp1);
        request.target = request_line.substr(sp1 + 1, sp2 - sp1 - 1);

        std::size_t content_length = 0;
        std::size_t pos = line_end == std::string::npos ? head.size() : line_end + 2;
        while (pos < head.size()) {
            const auto eol = head.find("\r\n", pos);
            const std::string line = head.substr(pos, eol == std::string::npos ? head.size() - pos : eol - pos);
            const auto colon = line.find(':');
            if (colon != std::string::npos) {
                std::string name = line.substr(0, colon);
                for (auto& c : name) {
                    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
                }
                if (name == "content-length") {
                    content_length = static_cast<std::size_t>(std::atoll(line.c_str() + colon + 1));
                }
            }
            if (eol == std::string::npos) {
                break;
            }
            pos = eol + 2;
        }
        std::string rest = buffer.substr(head_end + 4);
        while (rest.size() < content_length) {
            const auto n = ::recv(fd, chunk, sizeof(chunk), 0);
            if (n <= 0) {
                return;
            }
            rest.append(chunk, static_cast<std::size_t>(n));
        }
        request.body = rest.substr(0, content_length);
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_requests.push_back(request);
            m_received = true;
        }
        m_cv.notify_all();
        if (m_hold) {
            std::unique_lock<std::mutex> lock(m_mutex);
            m_cv.wait(lock, [this] { return m_released || m_stopped; });
            if (m_stopped) {
                return;
            }
        }
        std::string response;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_response_sent_at = NowMs();
            response = m_response;
        }
        std::size_t offset = 0;
        while (offset < response.size()) {
            const auto n = ::send(fd, response.data() + offset, response.size() - offset, MSG_NOSIGNAL);
            if (n <= 0) {
                return;
            }
            offset += static_cast<std::size_t>(n);
        }
    }

    int m_listener{-1};
    int m_port{0};
    int m_conn{-1};
    std::string m_response;
    std::thread m_thread;
    mutable std::mutex m_mutex;
    std::condition_variable m_cv;
    bool m_stopped{false};
    bool m_hold{false};
    bool m_received{false};
    bool m_released{false};
    long long m_response_sent_at{0};
    std::vector<Recorded> m_requests;
};

// ---------------------------------------------------------------- Consul agent --

// 自有 Consul agent 子进程：显式动态端口 + loopback 绑定 + 日志落文件；停止
// 顺序为 SIGTERM → 有界等待 → SIGKILL，绝不触碰其它进程。
class ConsulAgent {
public:
    ConsulAgent(std::string binary, std::filesystem::path data_dir)
        : m_binary(std::move(binary)), m_data_dir(std::move(data_dir))
    {
        m_http_port = FreeTcpPort();
        m_serf_lan = FreeTcpPort();
        m_serf_wan = FreeTcpPort();
        m_server = FreeTcpPort();
    }
    ~ConsulAgent()
    {
        Stop();
    }
    ConsulAgent(const ConsulAgent&) = delete;
    ConsulAgent& operator=(const ConsulAgent&) = delete;

    bool Start()
    {
        std::filesystem::create_directories(m_data_dir);
        m_log_path = m_data_dir / "consul.log";
        std::vector<std::string> args{
            m_binary,
            "agent",
            "-dev",
            "-node=bbt-consul-test",
            "-bind=127.0.0.1",
            "-advertise=127.0.0.1",
            "-client=127.0.0.1",
            "-data-dir=" + m_data_dir.string(),
            "-http-port=" + std::to_string(m_http_port),
            "-serf-lan-port=" + std::to_string(m_serf_lan),
            "-serf-wan-port=" + std::to_string(m_serf_wan),
            "-server-port=" + std::to_string(m_server),
            "-dns-port=0",
            "-grpc-port=0",
            "-log-level=warn",
            "-disable-keyring-file",
            "-ui=false",
        };
        std::vector<char*> argv;
        argv.reserve(args.size() + 1);
        for (auto& arg : args) {
            argv.push_back(arg.data());
        }
        argv.push_back(nullptr);

        posix_spawn_file_actions_t actions;
        CHECK(posix_spawn_file_actions_init(&actions) == 0);
        CHECK(posix_spawn_file_actions_addopen(&actions, 1, m_log_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644)
              == 0);
        CHECK(posix_spawn_file_actions_adddup2(&actions, 1, 2) == 0);
        const int rc = posix_spawn(&m_pid, m_binary.c_str(), &actions, nullptr, argv.data(), environ);
        posix_spawn_file_actions_destroy(&actions);
        if (rc != 0) {
            m_pid = -1;
            return false;
        }
        return true;
    }

    int http_port() const noexcept
    {
        return m_http_port;
    }
    pid_t pid() const noexcept
    {
        return m_pid;
    }

    bool Alive() const
    {
        if (m_pid <= 0) {
            return false;
        }
        int status = 0;
        return ::waitpid(m_pid, &status, WNOHANG) == 0;
    }

    void Stop()
    {
        if (m_pid <= 0) {
            return;
        }
        ::kill(m_pid, SIGTERM);
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        int status = 0;
        while (std::chrono::steady_clock::now() < deadline) {
            const auto waited = ::waitpid(m_pid, &status, WNOHANG);
            if (waited == m_pid) {
                m_pid = -1;
                return;
            }
            if (waited < 0) {
                m_pid = -1;
                return;
            }
            std::this_thread::sleep_for(50ms);
        }
        ::kill(m_pid, SIGKILL);
        ::waitpid(m_pid, &status, 0);
        m_pid = -1;
    }

    std::string LogTail() const
    {
        std::ifstream file(m_log_path);
        std::string out;
        std::string line;
        while (std::getline(file, line)) {
            out += line;
            out.push_back('\n');
            if (out.size() > 4000) {
                out.erase(0, out.size() - 4000);
            }
        }
        return out;
    }

private:
    std::string m_binary;
    std::filesystem::path m_data_dir;
    std::filesystem::path m_log_path;
    pid_t m_pid{-1};
    int m_http_port{0};
    int m_serf_lan{0};
    int m_serf_wan{0};
    int m_server{0};
};

// ---------------------------------------------------------------- 建 Client ---

struct Fixture {
    std::shared_ptr<consul::Client> client;
};

Fixture MakeClient(Host& host, const std::string& agent_url, milliseconds request_timeout = 3s,
                   milliseconds query_wait = 1s, std::string token = {})
{
    http::ClientOptions options;
    options.limits.max_connections = 8;
    options.timeout = http::Timeout::Uniform(http::PhaseTimeout::Milliseconds(5s));
    auto created_http = http::Client::Create(host.engine, std::move(options));
    CHECK(created_http);
    consul::Options consul_options;
    consul_options.agent_url = agent_url;
    consul_options.request_timeout = request_timeout;
    consul_options.query_wait = query_wait;
    consul_options.token = std::move(token);
    auto created = consul::Client::Create(std::move(created_http).value(), std::move(consul_options));
    CHECK(created);
    Fixture fixture;
    fixture.client = std::move(created).value();
    return fixture;
}

consul::ServiceRegistration Catalog(const std::string& id, std::uint16_t port,
                                    const std::string& name = "ecommerce-catalog")
{
    consul::ServiceRegistration registration;
    registration.id = id;
    registration.name = name;
    registration.address = "127.0.0.1";
    registration.port = port;
    registration.tags = {"rpc", "catalog"};
    return registration;
}

std::set<std::string> IdsOf(const consul::ServiceSnapshot& snapshot)
{
    std::set<std::string> ids;
    for (const auto& instance : snapshot.instances) {
        ids.insert(instance.id);
    }
    return ids;
}

// ---------------------------------------------------------------- 用例：参数 ---

void CaseValidation(Host& host)
{
    // agent 地址只接受 http + 数值 loopback。
    for (const std::string url : {"https://127.0.0.1:8500", "http://localhost:8500", "http://10.0.0.1:8500",
                                  "http://127.0.0.1:0", "http://127.0.0.1:8500/v1", "127.0.0.1:8500"}) {
        http::ClientOptions options;
        auto created_http = http::Client::Create(host.engine, std::move(options));
        CHECK(created_http);
        consul::Options consul_options;
        consul_options.agent_url = url;
        auto created = consul::Client::Create(std::move(created_http).value(), consul_options);
        CHECK(!created);
        CHECK(created.error().code == ErrorCode::InvalidArgument);
    }
    // 期限必须为正。
    {
        http::ClientOptions options;
        auto created_http = http::Client::Create(host.engine, std::move(options));
        CHECK(created_http);
        consul::Options consul_options;
        consul_options.agent_url = "http://127.0.0.1:8500";
        consul_options.request_timeout = 0ms;
        auto created = consul::Client::Create(std::move(created_http).value(), consul_options);
        CHECK(!created);
        CHECK(created.error().code == ErrorCode::InvalidArgument);
    }
    // 阻塞查询的 wait 必须给 wait/16 抖动和固定收尾余量留出空间。
    {
        http::ClientOptions options;
        auto created_http = http::Client::Create(host.engine, std::move(options));
        CHECK(created_http);
        consul::Options consul_options;
        consul_options.agent_url = "http://127.0.0.1:8500";
        consul_options.request_timeout = 3s;
        consul_options.query_wait = 3s;
        auto created = consul::Client::Create(std::move(created_http).value(), consul_options);
        CHECK(!created);
        CHECK(created.error().code == ErrorCode::InvalidArgument);
    }
    // 无 http Client 时拒绝。
    {
        consul::Options consul_options;
        consul_options.agent_url = "http://127.0.0.1:8500";
        auto created = consul::Client::Create(nullptr, consul_options);
        CHECK(!created);
        CHECK(created.error().code == ErrorCode::InvalidArgument);
    }
    // 注册字段校验不触网（对端为未使用的 loopback 端口即可）。
    {
        auto fixture = MakeClient(host, "http://127.0.0.1:" + std::to_string(FreeTcpPort()));
        const std::vector<consul::ServiceRegistration> invalid{
            Catalog("", 1234),
            Catalog("id-only", 0),
            Catalog("id-host", 1234, ""),
        };
        for (const auto& registration : invalid) {
            auto outcome = Await(host, [&] { return bbt_co_await fixture.client->Register(registration); });
            CHECK(!outcome);
            CHECK(outcome.error().code == ErrorCode::InvalidArgument);
        }
        consul::ServiceRegistration hostname = Catalog("id-hostname", 1234);
        hostname.address = "localhost";
        auto outcome = Await(host, [&] { return bbt_co_await fixture.client->Register(hostname); });
        CHECK(!outcome);
        CHECK(outcome.error().code == ErrorCode::InvalidArgument);
        auto deregister = Await(host, [&] { return bbt_co_await fixture.client->Deregister(""); });
        CHECK(!deregister);
        CHECK(deregister.error().code == ErrorCode::InvalidArgument);
        auto query = Await(host, [&] { return bbt_co_await fixture.client->HealthyInstances(""); });
        CHECK(!query);
        CHECK(query.error().code == ErrorCode::InvalidArgument);
        fixture.client->Close();
    }
}

// ---------------------------------------------------------------- 用例：不可达 ---

void CaseUnreachable(Host& host)
{
    auto fixture = MakeClient(host, "http://127.0.0.1:" + std::to_string(FreeTcpPort()), 1s, 0ms);
    long long elapsed = 0;
    auto query =
        Await(host, [&] { return bbt_co_await fixture.client->HealthyInstances("ecommerce-catalog"); }, &elapsed);
    CHECK(!query);
    CHECK(query.error().code == ErrorCode::TransportError);
    CHECK(elapsed < 2000);
    std::cout << "unreachable query elapsed_ms=" << elapsed << " code=" << static_cast<int>(query.error().code) << "\n";
    long long register_elapsed = 0;
    auto registered = Await(
        host, [&] { return bbt_co_await fixture.client->Register(Catalog("unreachable-1", 1234)); }, &register_elapsed);
    CHECK(!registered);
    CHECK(registered.error().code == ErrorCode::TransportError);
    CHECK(register_elapsed < 2000);
    fixture.client->Close();
}

// ---------------------------------------------------------------- 用例：协议面 ---

void CaseWireAndErrors(Host& host)
{
    // 1) 注册请求的真实 wire 形态：PUT 目标 + JSON 字段（含 TCP check）。
    {
        CannedPeer peer(RawResponse(200, "OK", ""));
        auto fixture = MakeClient(host, peer.base_url());
        auto registered =
            Await(host, [&] { return bbt_co_await fixture.client->Register(Catalog("catalog-1-pid-a", 18001)); });
        CHECK(registered);
        const auto requests = peer.requests();
        CHECK(requests.size() == 1);
        CHECK(requests[0].method == "PUT");
        CHECK(requests[0].target == "/v1/agent/service/register");
        CHECK(requests[0].body.find("\"ID\":\"catalog-1-pid-a\"") != std::string::npos);
        CHECK(requests[0].body.find("\"Name\":\"ecommerce-catalog\"") != std::string::npos);
        CHECK(requests[0].body.find("\"Port\":18001") != std::string::npos);
        CHECK(requests[0].body.find("\"TCP\":\"127.0.0.1:18001\"") != std::string::npos);
        CHECK(requests[0].body.find("\"DeregisterCriticalServiceAfter\":\"1m\"") != std::string::npos);
        fixture.client->Close();
    }
    // 2) 非 2xx：TransportError + 保留状态码。
    {
        CannedPeer peer(RawResponse(500, "Internal Server Error", "boom"));
        auto fixture = MakeClient(host, peer.base_url());
        auto registered =
            Await(host, [&] { return bbt_co_await fixture.client->Register(Catalog("catalog-1", 18002)); });
        CHECK(!registered);
        CHECK(registered.error().code == ErrorCode::TransportError);
        CHECK(registered.error().backend_code == 500);
        CHECK(registered.error().message.find("500") != std::string::npos);
        fixture.client->Close();
    }
    // 3) 合法健康响应：Service.Address 为空时回落 Node.Address；index 透传。
    {
        const std::string body = "[{\"Node\":{\"Address\":\"127.0.0.1\"},"
                                 "\"Service\":{\"ID\":\"catalog-1\",\"Service\":\"ecommerce-catalog\","
                                 "\"Address\":\"\",\"Port\":19001},"
                                 "\"Checks\":[{\"Status\":\"passing\"}]}]";
        CannedPeer peer(RawResponse(200, "OK", body, {{"X-Consul-Index", "42"}}));
        auto fixture = MakeClient(host, peer.base_url());
        auto snapshot = Await(host, [&] { return bbt_co_await fixture.client->HealthyInstances("ecommerce-catalog"); });
        CHECK(snapshot);
        CHECK(snapshot.value().index == 42);
        CHECK(snapshot.value().instances.size() == 1);
        CHECK(snapshot.value().instances[0].id == "catalog-1");
        CHECK(snapshot.value().instances[0].name == "ecommerce-catalog");
        CHECK(snapshot.value().instances[0].address == "127.0.0.1");
        CHECK(snapshot.value().instances[0].port == 19001);
        // 阻塞查询参数：index 与 wait 都在 wire 上。
        auto blocked =
            Await(host, [&] { return bbt_co_await fixture.client->HealthyInstances("ecommerce-catalog", 42); });
        CHECK(blocked);
        const auto requests = peer.requests();
        CHECK(requests.size() == 2);
        CHECK(requests[0].target == "/v1/health/service/ecommerce-catalog?passing=true");
        CHECK(requests[1].target == "/v1/health/service/ecommerce-catalog?passing=true&index=42&wait=1000ms");
        fixture.client->Close();
    }
    // 4) 缺少 X-Consul-Index / 非法 JSON / 非法端口 / 空 ID → ProtocolError。
    {
        const std::vector<std::string> bad_responses{
            RawResponse(200, "OK", "[]"),
            RawResponse(200, "OK", "not-json", {{"X-Consul-Index", "7"}}),
            RawResponse(200, "OK",
                        "[{\"Node\":{\"Address\":\"127.0.0.1\"},"
                        "\"Service\":{\"ID\":\"c\",\"Service\":\"s\",\"Address\":\"127.0.0.1\","
                        "\"Port\":0}}]",
                        {{"X-Consul-Index", "7"}}),
            RawResponse(200, "OK",
                        "[{\"Node\":{\"Address\":\"127.0.0.1\"},"
                        "\"Service\":{\"ID\":\"\",\"Service\":\"s\",\"Address\":\"127.0.0.1\","
                        "\"Port\":19002}}]",
                        {{"X-Consul-Index", "7"}}),
            RawResponse(200, "OK", "{}", {{"X-Consul-Index", "7"}}),
        };
        for (const auto& response : bad_responses) {
            CannedPeer peer(response);
            auto fixture = MakeClient(host, peer.base_url());
            auto snapshot = Await(host, [&] { return bbt_co_await fixture.client->HealthyInstances("s"); });
            CHECK(!snapshot);
            CHECK(snapshot.error().code == ErrorCode::ProtocolError);
            fixture.client->Close();
        }
    }
    // 5) critical check 过滤：即使 agent 忽略 passing 也不会被当成可用目标。
    {
        const std::string body = "[{\"Node\":{\"Address\":\"127.0.0.1\"},"
                                 "\"Service\":{\"ID\":\"critical-1\",\"Service\":\"ecommerce-catalog\","
                                 "\"Address\":\"127.0.0.1\",\"Port\":19003},"
                                 "\"Checks\":[{\"Status\":\"critical\"}]}]";
        CannedPeer peer(RawResponse(200, "OK", body, {{"X-Consul-Index", "9"}}));
        auto fixture = MakeClient(host, peer.base_url());
        auto snapshot = Await(host, [&] { return bbt_co_await fixture.client->HealthyInstances("ecommerce-catalog"); });
        CHECK(snapshot);
        CHECK(snapshot.value().instances.empty());
        fixture.client->Close();
    }
    // 6) 注销路径：id 必须百分号编码，不能被当成多级路径。
    {
        CannedPeer peer(RawResponse(200, "OK", ""));
        auto fixture = MakeClient(host, peer.base_url());
        auto deregistered = Await(host, [&] { return bbt_co_await fixture.client->Deregister("catalog/1 a+b"); });
        CHECK(deregistered);
        const auto requests = peer.requests();
        CHECK(requests.size() == 1);
        CHECK(requests[0].method == "PUT");
        CHECK(requests[0].target == "/v1/agent/service/deregister/catalog%2F1%20a%2Bb");
        fixture.client->Close();
    }
    // 6b) 注销幂等：agent 对已不存在的 ID 返回 404，也算已注销。
    {
        CannedPeer peer(RawResponse(404, "Not Found", "Unknown service ID"));
        auto fixture = MakeClient(host, peer.base_url());
        auto deregistered = Await(host, [&] { return bbt_co_await fixture.client->Deregister("gone"); });
        CHECK(deregistered);
        fixture.client->Close();
    }
    // 6c) 非 Consul 的 404 不能被误判为幂等注销成功。
    {
        CannedPeer peer(RawResponse(404, "Not Found", "proxy route missing"));
        auto fixture = MakeClient(host, peer.base_url());
        auto deregistered = Await(host, [&] { return bbt_co_await fixture.client->Deregister("gone"); });
        CHECK(!deregistered);
        CHECK(deregistered.error().code == ErrorCode::TransportError);
        fixture.client->Close();
    }
    // 7) 封口后拒绝新操作；Close 幂等。
    {
        CannedPeer peer(RawResponse(200, "OK", "[]", {{"X-Consul-Index", "1"}}));
        auto fixture = MakeClient(host, peer.base_url());
        CHECK(!fixture.client->IsClosed());
        fixture.client->Close();
        CHECK(fixture.client->IsClosed());
        fixture.client->Close(); // 幂等
        auto registered =
            Await(host, [&] { return bbt_co_await fixture.client->Register(Catalog("after-close", 1234)); });
        CHECK(!registered);
        CHECK(registered.error().code == ErrorCode::Closed);
        auto snapshot = Await(host, [&] { return bbt_co_await fixture.client->HealthyInstances("ecommerce-catalog"); });
        CHECK(!snapshot);
        CHECK(snapshot.error().code == ErrorCode::Closed);
        auto deregistered = Await(host, [&] { return bbt_co_await fixture.client->Deregister("after-close"); });
        CHECK(!deregistered);
        CHECK(deregistered.error().code == ErrorCode::Closed);
    }
}

// ---------------------------------------------------------------- 用例：关闭契约 ---

// Close 的同步收口契约：首个/并发/协程域 Close 都必须等同一真实终态（已封口 +
// 专属 HTTP Client 已关闭 + 已登记操作全部离开），不提前返回。每个子用例先让真实
// HTTP 交换停在对端「已完整读出请求、尚未回包」的阶段，再触发 Close：这样
// 「Close 返回时在飞操作是否已离开」可直接观测，不靠 sleep 猜时序。
// 子用例各自记录结论、最后统一断言，便于一次运行同时暴露三类缺陷。
void CaseCloseContract(Host& host)
{
    bool all_ok = true;
    auto expect = [&all_ok](bool condition, const char* what) {
        if (!condition) {
            all_ok = false;
            std::cout << "close_contract FAIL " << what << "\n";
        } else {
            std::cout << "close_contract ok " << what << "\n";
        }
    };
    const auto response = RawResponse(200, "OK", "[]", {{"X-Consul-Index", "7"}});

    // 1) 在途请求关闭：Close 必须等在飞查询离开后才返回。
    {
        CannedPeer peer(response, /*hold=*/true);
        auto fixture = MakeClient(host, peer.base_url(), 3s, 0ms);
        auto& client = *fixture.client;
        std::atomic<long long> op_left_at{0};
        auto op = host.Submit([&] {
            auto snapshot = bbt_co_await client.HealthyInstances("ecommerce-catalog");
            op_left_at.store(NowMs());
            return snapshot;
        });
        CHECK(peer.WaitFirstRequest(5s));
        std::cout << "close_contract inflight request_received\n";

        std::atomic<long long> close_returned_at{0};
        std::thread closer([&] {
            client.Close();
            close_returned_at.store(NowMs());
        });
        std::this_thread::sleep_for(300ms);
        expect(close_returned_at.load() == 0, "inflight: 300ms 时 Close 未返回");
        // 累计 1.2s：越过旧实现 min(request_timeout, 1s) 的提前返回点。
        std::this_thread::sleep_for(900ms);
        expect(close_returned_at.load() == 0, "inflight: 1.2s 时仍在等（旧实现此处已提前返回）");
        peer.Release();
        closer.join();
        auto snapshot = Get(op);
        expect(static_cast<bool>(snapshot), "inflight: 在飞操作正常完成");
        const auto sent_at = peer.response_sent_at();
        expect(sent_at != 0, "inflight: 对端确实回过包");
        expect(close_returned_at.load() >= sent_at, "inflight: Close 不早于回包");
        // 允许调度交接的毫秒级顺序差；旧实现的差距是 1s 等待上限与回包时刻之差。
        expect(close_returned_at.load() + 50 >= op_left_at.load(), "inflight: Close 不早于操作离开");
    }

    // 2) 并发 Close：所有调用者等同一终态，第二个起不能提前返回。
    {
        CannedPeer peer(response, /*hold=*/true);
        auto fixture = MakeClient(host, peer.base_url(), 3s, 0ms);
        auto& client = *fixture.client;
        auto op = host.Submit([&] { return bbt_co_await client.HealthyInstances("ecommerce-catalog"); });
        CHECK(peer.WaitFirstRequest(5s));
        std::cout << "close_contract concurrent request_received\n";

        std::atomic<long long> returned[3];
        for (auto& value : returned) {
            value.store(0);
        }
        std::vector<std::thread> closers;
        closers.reserve(3);
        for (int i = 0; i < 3; ++i) {
            closers.emplace_back([&client, &returned, i] {
                client.Close();
                returned[i].store(NowMs());
            });
        }
        auto none_returned = [&returned] {
            for (const auto& value : returned) {
                if (value.load() != 0) {
                    return false;
                }
            }
            return true;
        };
        std::this_thread::sleep_for(300ms);
        expect(none_returned(), "concurrent: 300ms 时无人返回（旧实现第二个 Close 立即返回）");
        std::this_thread::sleep_for(900ms);
        expect(none_returned(), "concurrent: 1.2s 时无人返回");
        peer.Release();
        for (auto& closer : closers) {
            closer.join();
        }
        auto snapshot = Get(op);
        expect(static_cast<bool>(snapshot), "concurrent: 在飞操作正常完成");
        const auto sent_at = peer.response_sent_at();
        bool all_after_response = true;
        for (const auto& value : returned) {
            all_after_response = all_after_response && value.load() >= sent_at;
        }
        expect(all_after_response, "concurrent: 三个 Close 都不早于回包");
        expect(client.IsClosed(), "concurrent: 收口后 IsClosed");
    }

    // 3) 协程中 Close：必须协作挂起而不是占住 Processer 线程，否则在飞操作没有
    //    worker 可恢复（两个并发 Close 足以占满本套件的 2 个 Processer）。
    {
        CannedPeer peer(response, /*hold=*/true);
        auto fixture = MakeClient(host, peer.base_url(), 3s, 0ms);
        auto& client = *fixture.client;
        std::atomic<long long> op_left_at{0};
        auto op = host.Submit([&] {
            auto snapshot = bbt_co_await client.HealthyInstances("ecommerce-catalog");
            op_left_at.store(NowMs());
            return snapshot;
        });
        CHECK(peer.WaitFirstRequest(5s));
        std::cout << "close_contract coroutine request_received\n";

        std::atomic<bool> stop_ticker{false};
        std::atomic<int> ticks{0};
        auto ticker = host.Submit([&] {
            while (!stop_ticker.load(std::memory_order_relaxed)) {
                CoSleep(5ms);
                ticks.fetch_add(1, std::memory_order_relaxed);
            }
            return true;
        });
        std::atomic<long long> first_close_at{0};
        std::atomic<long long> second_close_at{0};
        std::atomic<bool> close_in_coroutine{false};
        auto first = host.Submit([&] {
            close_in_coroutine.store(bbt::coroutine::IsInCoroutineContext());
            client.Close();
            first_close_at.store(NowMs());
            return true;
        });
        auto second = host.Submit([&] {
            client.Close();
            second_close_at.store(NowMs());
            return true;
        });
        const auto tick_deadline = std::chrono::steady_clock::now() + 2s;
        while (ticks.load() < 20 && std::chrono::steady_clock::now() < tick_deadline) {
            std::this_thread::sleep_for(5ms);
        }
        expect(ticks.load() >= 20, "coroutine: Close 等待期间调度器继续推进其它协程");
        expect(first_close_at.load() == 0 && second_close_at.load() == 0, "coroutine: 两个 Close 均未提前返回");
        peer.Release();
        // Get 有 5s 界：若 Close 在协程里阻塞等待，两个 worker 会被占满，这里
        // 有界失败而不是挂住整个测试进程。
        CHECK(Get(first));
        CHECK(Get(second));
        auto snapshot = Get(op);
        expect(static_cast<bool>(snapshot), "coroutine: 在飞操作正常完成");
        expect(close_in_coroutine.load(), "coroutine: Close 确实在协程上下文内调用");
        const auto sent_at = peer.response_sent_at();
        expect(first_close_at.load() >= sent_at && second_close_at.load() >= sent_at,
               "coroutine: 两个 Close 都不早于回包");
        expect(first_close_at.load() + 50 >= op_left_at.load(), "coroutine: Close 不早于操作离开");
        stop_ticker.store(true);
        CHECK(Get(ticker));
    }

    // 4) 预取消协程中的 Close：取消位已置位时 CoWaiter 入口即返回 Cancelled、
    //    Hook_Sleep 也立即返回，不能靠线程 sleep/自旋等待终态。两个预取消 Close
    //    足以占满本套件的两个 Processer，只有真正让出 worker（bbtco_yield），
    //    ticker 与在飞操作才能推进，Close 也才能等到真实终态。
    {
        CannedPeer peer(response, /*hold=*/true);
        auto fixture = MakeClient(host, peer.base_url(), 3s, 0ms);
        auto& client = *fixture.client;
        std::atomic<long long> op_left_at{0};
        auto op = host.Submit([&] {
            auto snapshot = bbt_co_await client.HealthyInstances("ecommerce-catalog");
            op_left_at.store(NowMs());
            return snapshot;
        });
        CHECK(peer.WaitFirstRequest(5s));
        std::cout << "close_contract cancel request_received\n";

        std::atomic<bool> stop_ticker{false};
        std::atomic<int> ticks{0};
        auto ticker = host.Submit([&] {
            while (!stop_ticker.load(std::memory_order_relaxed)) {
                CoSleep(5ms);
                ticks.fetch_add(1, std::memory_order_relaxed);
            }
            return true;
        });
        std::atomic<long long> first_at{0};
        std::atomic<long long> second_at{0};
        auto first = host.Submit([&] {
            g_bbt_tls_coroutine_co->RequestCancel();
            client.Close();
            first_at.store(NowMs());
            return true;
        });
        auto second = host.Submit([&] {
            g_bbt_tls_coroutine_co->RequestCancel();
            client.Close();
            second_at.store(NowMs());
            return true;
        });
        const auto tick_deadline = std::chrono::steady_clock::now() + 2s;
        while (ticks.load() < 20 && std::chrono::steady_clock::now() < tick_deadline) {
            std::this_thread::sleep_for(5ms);
        }
        expect(ticks.load() >= 20, "cancel: 预取消 Close 等待期间调度器继续推进其它协程");
        expect(first_at.load() == 0 && second_at.load() == 0, "cancel: 两个预取消 Close 均未提前返回");
        peer.Release();
        CHECK(Get(first));
        CHECK(Get(second));
        auto snapshot = Get(op);
        expect(static_cast<bool>(snapshot), "cancel: 在飞操作正常完成");
        const auto sent_at = peer.response_sent_at();
        expect(first_at.load() >= sent_at && second_at.load() >= sent_at, "cancel: 两个预取消 Close 都不早于回包");
        expect(first_at.load() + 50 >= op_left_at.load(), "cancel: Close 不早于操作离开");
        expect(client.IsClosed(), "cancel: 收口后 IsClosed");
        stop_ticker.store(true);
        CHECK(Get(ticker));
    }

    CHECK(all_ok);
}

// ---------------------------------------------------------------- 用例：真后端 ---

bool WaitReady(Host& host, consul::Client& client)
{
    const auto deadline = std::chrono::steady_clock::now() + 25s;
    int attempts = 0;
    while (std::chrono::steady_clock::now() < deadline) {
        auto probe = Await(host, [&] { return bbt_co_await client.HealthyInstances("__bbt_consul_ready__"); });
        if (probe) {
            return true;
        }
        CHECK(probe.error().code == ErrorCode::TransportError || probe.error().code == ErrorCode::TimedOut);
        Await(host, [&] {
            CoSleep(100ms);
            return true;
        });
        ++attempts;
    }
    std::cout << "readiness attempts=" << attempts << "\n";
    return false;
}

// 有限轮询取快照：轮询只是等待 agent 视图收敛，不是用固定 sleep 冒充同步。
consul::ServiceSnapshot WaitForCount(Host& host, consul::Client& client, const std::string& service,
                                     std::size_t expected, milliseconds timeout = 5s)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    consul::ServiceSnapshot last;
    while (std::chrono::steady_clock::now() < deadline) {
        auto snapshot = Await(host, [&] { return bbt_co_await client.HealthyInstances(service, last.index); });
        CHECK(snapshot);
        last = std::move(snapshot.value());
        if (last.instances.size() == expected) {
            return last;
        }
        Await(host, [&] {
            CoSleep(100ms);
            return true;
        });
    }
    CHECK(last.instances.size() == expected);
    return last;
}

void CaseLive(Host& host, const std::string& binary)
{
    const auto root = std::filesystem::temp_directory_path() / ("bbt-consul-live-" + std::to_string(::getpid()));
    struct Cleanup {
        std::filesystem::path path;
        ~Cleanup()
        {
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
        }
    } cleanup{root};

    ConsulAgent agent(binary, root);
    const bool started = agent.Start();
    CHECK(started);
    std::cout << "consul pid=" << agent.pid() << " http_port=" << agent.http_port() << "\n";

    ListenSocket catalog_a;
    ListenSocket catalog_b;
    ListenSocket catalog_c;
    const int dead_port = FreeTcpPort(); // 无监听者 → TCP check 必 critical

    {
        auto fixture = MakeClient(host, "http://127.0.0.1:" + std::to_string(agent.http_port()), 3s, 1s);
        auto& client = *fixture.client;
        const bool ready = WaitReady(host, client);
        if (!ready) {
            std::cerr << "consul did not become ready, log tail:\n" << agent.LogTail() << "\n";
            std::cerr << "binary=" << binary << "\n";
        }
        CHECK(ready);

        // 三实例：两个端口真实可达，一个端口不可达（用于健康过滤证据）。
        CHECK(Await(host, [&] {
            return bbt_co_await client.Register(
                Catalog("catalog-1-" + std::to_string(::getpid()) + "-a", catalog_a.port()));
        }));
        CHECK(Await(host, [&] {
            return bbt_co_await client.Register(
                Catalog("catalog-2-" + std::to_string(::getpid()) + "-b", catalog_b.port()));
        }));
        CHECK(Await(host, [&] {
            return bbt_co_await client.Register(
                Catalog("catalog-3-" + std::to_string(::getpid()) + "-c", static_cast<std::uint16_t>(dead_port)));
        }));

        // 刚注册时 TCP check 还没首探，passing 视图可以合法为空——这次即时查询
        // 是「注册 ≠ 健康」的证据，不拿它做断言（时序取决于 agent 首探）。
        long long immediate_elapsed = 0;
        auto immediate =
            Await(host, [&] { return bbt_co_await client.HealthyInstances("ecommerce-catalog"); }, &immediate_elapsed);
        CHECK(immediate);
        CHECK(immediate.value().index >= 1);
        std::cout << "live immediate_query index=" << immediate.value().index
                  << " instances=" << immediate.value().instances.size() << " elapsed_ms=" << immediate_elapsed << "\n";

        // 等真实 TCP check 通过：只有端口可达的两个实例能进入 passing 视图，
        // 端口不可达的 catalog-3 必须始终缺席（健康过滤证据）。
        auto first = WaitForCount(host, client, "ecommerce-catalog", 2, 10s);
        const std::uint64_t index0 = first.index;
        CHECK(index0 >= immediate.value().index);
        CHECK(IdsOf(first).count("catalog-3-" + std::to_string(::getpid()) + "-c") == 0);
        for (const auto& instance : first.instances) {
            CHECK(instance.name == "ecommerce-catalog");
            CHECK(instance.address == "127.0.0.1");
        }
        std::cout << "live healthy_view index=" << index0 << " instances=" << first.instances.size() << "\n";

        // 阻塞查询：同 index 重问应停住约 wait 而不是立即返回，且视图不变。
        long long blocking_elapsed = 0;
        auto blocked = Await(
            host, [&] { return bbt_co_await client.HealthyInstances("ecommerce-catalog", index0); }, &blocking_elapsed);
        CHECK(blocked);
        CHECK(blocked.value().index >= index0);
        CHECK(blocked.value().instances.size() == 2);
        CHECK(blocking_elapsed >= 700);
        std::cout << "live blocking_query index=" << blocked.value().index << " elapsed_ms=" << blocking_elapsed
                  << "\n";

        // 扩容：新增实例后视图从 2 变 3，index 前进（不是每轮都刷新）。
        const std::uint64_t index1 = blocked.value().index;
        CHECK(Await(host, [&] {
            return bbt_co_await client.Register(
                Catalog("catalog-4-" + std::to_string(::getpid()) + "-d", catalog_c.port()));
        }));
        auto grown = WaitForCount(host, client, "ecommerce-catalog", 3);
        CHECK(grown.index > index1);
        std::cout << "live after_register index=" << grown.index << " instances=" << grown.instances.size() << "\n";

        // 退出：注销一个实例后不再出现在 passing 视图里。
        CHECK(Await(host,
                    [&] { return bbt_co_await client.Deregister("catalog-1-" + std::to_string(::getpid()) + "-a"); }));
        auto shrunk = WaitForCount(host, client, "ecommerce-catalog", 2);
        CHECK(shrunk.index > grown.index);
        CHECK(IdsOf(shrunk).count("catalog-1-" + std::to_string(::getpid()) + "-a") == 0);
        std::cout << "live after_deregister index=" << shrunk.index << " instances=" << shrunk.instances.size() << "\n";

        // 空视图：全部注销后是合法空列表，不是错误也不保留旧实例。
        CHECK(Await(host,
                    [&] { return bbt_co_await client.Deregister("catalog-2-" + std::to_string(::getpid()) + "-b"); }));
        CHECK(Await(host,
                    [&] { return bbt_co_await client.Deregister("catalog-3-" + std::to_string(::getpid()) + "-c"); }));
        CHECK(Await(host,
                    [&] { return bbt_co_await client.Deregister("catalog-4-" + std::to_string(::getpid()) + "-d"); }));
        auto empty = WaitForCount(host, client, "ecommerce-catalog", 0);
        CHECK(empty.index > shrunk.index);
        std::cout << "live empty_view index=" << empty.index << " instances=0\n";

        // 幂等注销 + 未知服务查询。
        CHECK(Await(host,
                    [&] { return bbt_co_await client.Deregister("catalog-2-" + std::to_string(::getpid()) + "-b"); }));
        auto unknown = Await(host, [&] { return bbt_co_await client.HealthyInstances("no-such-service"); });
        CHECK(unknown);
        CHECK(unknown.value().instances.empty());
        CHECK(unknown.value().index >= 1);

        client.Close();
        CHECK(client.IsClosed());
        auto after_close = Await(host, [&] { return bbt_co_await client.Register(Catalog("after-close-live", 1234)); });
        CHECK(!after_close);
        CHECK(after_close.error().code == ErrorCode::Closed);
    }

    // 真实后端退出：有限期限内明确失败，不无限重试。
    agent.Stop();
    CHECK(!agent.Alive());
    {
        auto fixture = MakeClient(host, "http://127.0.0.1:" + std::to_string(agent.http_port()), 1s, 0ms);
        long long elapsed = 0;
        auto query =
            Await(host, [&] { return bbt_co_await fixture.client->HealthyInstances("ecommerce-catalog"); }, &elapsed);
        CHECK(!query);
        CHECK(query.error().code == ErrorCode::TransportError);
        CHECK(elapsed < 2000);
        std::cout << "live after_stop elapsed_ms=" << elapsed << " code=" << static_cast<int>(query.error().code)
                  << "\n";
        fixture.client->Close();
    }
}

// ---------------------------------------------------------------- 入口 ---

int Run(int argc, char** argv)
{
    enum class Mode { all, protocol, live };
    Mode mode = Mode::all;
    if (argc == 2 && std::string(argv[1]) == "--protocol") {
        mode = Mode::protocol;
    } else if (argc == 2 && std::string(argv[1]) == "--live") {
        mode = Mode::live;
    } else if (argc != 1) {
        std::cerr << "usage: Test_infra_consul [--protocol|--live]\n";
        return 2;
    }
    g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
    g_bbt_coroutine_config->m_cfg_stack_size = 192 * 1024;
    g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
    g_scheduler->Start();
    {
        // 延迟初始化后的稳定基线（对齐 http_net/redis_net 夹具的做法）。
        Host warm;
        CHECK(warm.Call([] {
            CoSleep(5ms);
            return true;
        }));
    }
    BoundedWatchdog watchdog(180s);
    {
        Host host;
        if (mode != Mode::live) {
            CaseValidation(host);
            CaseUnreachable(host);
            CaseWireAndErrors(host);
            CaseCloseContract(host);
        }
        if (mode != Mode::protocol) {
            const char* binary = std::getenv("BBT_TEST_CONSUL_BIN");
            if (binary == nullptr || *binary == '\0') {
                std::cout << "skip: BBT_TEST_CONSUL_BIN 未设置，真后端用例未执行\n";
                host.Finish();
                return 77;
            }
            std::error_code ec;
            if (!std::filesystem::is_regular_file(binary, ec) || ::access(binary, X_OK) != 0) {
                std::cerr << "FAIL: BBT_TEST_CONSUL_BIN 必须指向可执行的 Consul 文件\n";
                host.Finish();
                return 1;
            }
            CaseLive(host, binary);
        }
        host.Finish();
    }
    std::cout << "infra_consul_test PASS\n";
    return 0;
}

} // namespace

int main(int argc, char** argv)
{
    return bbt_net_test::RunTestProcessExit([&] {
        try {
            return Run(argc, argv);
        } catch (const std::exception& error) {
            std::cerr << "infra_consul_test exception: " << error.what() << '\n';
            return 1;
        }
    });
}

// 3-3 切片 contract 检查：request 栈外 owner / 唯一终态 / deadline / Cancel / Close /
// listener·fd·timer·queue 同步 teardown 与晚到空壳（specs R10/S21/S22）。
//
//   T1（R10/S17）：已接纳请求恰好一次终态；完成后晚到 cancel 无害（无第二消息）。
//   T2（R10）：已进入 callback 的协作式取消——cancel 命中 → handler 观察 abort 位
//        提前退出 → 提交门只消费空壳（被取消请求不再产生任何消息）。
//   T3（S22）：期限可观察拒绝（-32000）；timeout ≠ 远端未执行（handler 恰执行一次）。
//   T4（R10）：Close 竞争两窗口（callback-start 门之前/之后）；封口后新请求被拒。
//   T5（S4 core 面）：cancel 会话域路由——跨会话 cancel 零影响（P-A 反转：A 的
//        响应不被吞、订阅不被别人收口），同会话 cancel 照常（S17）；同会话同 id
//        重复在途只接纳一次、重复帧 -32600 拒绝（P-B 反转）。
//   T6（S4/S20 网络面）：HTTP 跨连接 cancel 静默 no-op（订阅存活：心跳仍写、名额
//        仍占用）；订阅终止由断连驱动收口（心跳写失败 → 名额归还），流结束帧不出
//        wire（F5 的可测等价断言）。
//   T7（P2.1）：stdio close_drain_timeout < write_deadline 降级专测——reader 在途
//        （分派阻塞于业务 gate）时 Close 超时放弃汇合：O_NONBLOCK 当刻还原（fcntl
//        对象面）、reader_joined=false 快照、晚到完成项不落 stdout（队列空证据）。
//   T8（P2.3/S21）：HTTP 网络级确定性验收——Close 返回当刻 listener fd 已关
//        （/proc/net/tcp{,6} 无 LISTEN 行，对象面，不受 WSL2 端口转发噪声影响）、
//        自有 socket fd 回落基线、并发 Close 均等落定、在途 handler 晚到完成只消费
//        空壳（客户端无后续字节、证据快照不再变化）。
//   T9（P3/S22）：在途容量超限拒绝、不同 server 隔离、业务失败不自动重试、
//        注册复杂度上限。
//
// 确定性纪律：线程交错用显式 gate（mutex+cv）构造；期限/降级路径的真实等待只有
// 有界毫秒级窗口，且断言的是「窗口必然已过」的终态，不是时序运气。资源证据用
// 对象面（fcntl 标志、/proc fd 与 LISTEN 表、证据快照），不用 Sleep/计数归零/
// IsClosed 伪证物理释放。
//
// 无第三方测试框架依赖；经 ${CMAKE_SOURCE_DIR}/src 消费内部实现面（RequestOwner
// 终态计数），与仓内其他 impl 层测试同口径。

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/mcp/JsonValue.hpp>
#include <bbt/infra/mcp/McpHttpTransport.hpp>
#include <bbt/infra/mcp/McpServer.hpp>
#include <bbt/infra/mcp/McpStdioTransport.hpp>
#include <bbt/infra/mcp/McpTypes.hpp>

#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>

#include "mcp/Server.hpp"  // 内部面：RequestOwner 终态计数（仅本目标可见）

namespace {

std::atomic<int> g_checks{0};
std::atomic<int> g_failures{0};

void Check(bool ok, const char* expr, int line) {
    g_checks.fetch_add(1);
    if (!ok) {
        g_failures.fetch_add(1);
        std::fprintf(stderr, "FAIL %s:%d  %s\n", __FILE__, line, expr);
    }
}
#define CHECK(expr) Check(static_cast<bool>(expr), #expr, __LINE__)

using bbt::infra::mcp::CreateSession;
using bbt::infra::mcp::JsonValue;
using bbt::infra::mcp::McpHttpTransport;
using bbt::infra::mcp::McpServer;
using bbt::infra::mcp::McpSession;
using bbt::infra::mcp::McpStdioTransport;
using bbt::infra::mcp::RequestContext;
using bbt::infra::mcp::RequestOwner;
using bbt::infra::mcp::ResourceContents;
using bbt::infra::mcp::ResourceDefinition;
using bbt::infra::mcp::ResourceResult;
using bbt::infra::mcp::ServerOptions;

// ---- 帧构造 ---------------------------------------------------------------------

constexpr const char* kModern = "2026-07-28";

JsonValue ModernMeta() {
    JsonValue meta = JsonValue::Object();
    meta.Set("io.modelcontextprotocol/protocolVersion", JsonValue(kModern));
    meta.Set("io.modelcontextprotocol/clientCapabilities", JsonValue::Object());
    return meta;
}

JsonValue ModernRequest(const std::string& id, const char* method, JsonValue params) {
    params.Set("_meta", ModernMeta());
    JsonValue frame = JsonValue::Object();
    frame.Set("jsonrpc", JsonValue("2.0"));
    frame.Set("id", JsonValue(id));
    frame.Set("method", JsonValue(method));
    frame.Set("params", std::move(params));
    return frame;
}

std::string ModernRead(const std::string& id, const std::string& uri) {
    JsonValue params = JsonValue::Object();
    params.Set("uri", JsonValue(uri));
    return ModernRequest(id, "resources/read", std::move(params)).Serialize();
}

std::string ModernListen(const std::string& id) {
    JsonValue params = JsonValue::Object();
    params.Set("notifications", JsonValue::Object());
    return ModernRequest(id, "subscriptions/listen", std::move(params)).Serialize();
}

std::string CancelFrame(const JsonValue& request_id) {
    JsonValue params = JsonValue::Object();
    params.Set("requestId", request_id);
    JsonValue frame = JsonValue::Object();
    frame.Set("jsonrpc", JsonValue("2.0"));
    frame.Set("method", JsonValue("notifications/cancelled"));
    frame.Set("params", std::move(params));
    return frame.Serialize();
}

// modern 形态 cancel（HTTP modern era 要求所有帧携带 params._meta）。
std::string ModernCancelFrame(const JsonValue& request_id) {
    JsonValue params = JsonValue::Object();
    params.Set("requestId", request_id);
    params.Set("_meta", ModernMeta());
    JsonValue frame = JsonValue::Object();
    frame.Set("jsonrpc", JsonValue("2.0"));
    frame.Set("method", JsonValue("notifications/cancelled"));
    frame.Set("params", std::move(params));
    return frame.Serialize();
}

JsonValue ParseOk(const std::string& text) {
    auto parsed = JsonValue::Parse(text, 1U << 20, 8);
    return parsed ? std::move(parsed).value() : JsonValue();
}

// ---- 同步 gate（确定性交错构造；不用 Sleep 排时序）-------------------------------

struct Gate {
    std::mutex m;
    std::condition_variable cv;
    bool entered = false;
    bool released = false;

    void MarkEntered() {
        { std::lock_guard<std::mutex> lock(m); entered = true; }
        cv.notify_all();
    }
    void WaitEntered() {
        std::unique_lock<std::mutex> lock(m);
        cv.wait(lock, [this] { return entered; });
    }
    void Release() {
        { std::lock_guard<std::mutex> lock(m); released = true; }
        cv.notify_all();
    }
};

// handler 内协作等待：released 或 cancelled（3-3 abort 位）任一为真即返回，
// 返回 true = released。5ms 片段轮询，无忙等。
bool WaitReleasedOrCancelled(Gate& gate, const RequestContext& ctx) {
    for (;;) {
        if (ctx.Cancellation().Cancelled()) return false;
        std::unique_lock<std::mutex> lock(gate.m);
        if (gate.cv.wait_for(lock, std::chrono::milliseconds(5),
                             [&gate] { return gate.released; }))
            return true;
    }
}

// ---- 帧出口记录器（sink；跨线程加锁）---------------------------------------------

struct SinkLog {
    std::mutex m;
    std::vector<std::string> frames;
    void Record(const std::string& frame) {
        std::lock_guard<std::mutex> lock(m);
        frames.push_back(frame);
    }
    std::size_t Size() {
        std::lock_guard<std::mutex> lock(m);
        return frames.size();
    }
    std::string Last() {
        std::lock_guard<std::mutex> lock(m);
        return frames.back();
    }
    bool Contains(const std::string& needle) {
        std::lock_guard<std::mutex> lock(m);
        for (const auto& frame : frames)
            if (frame.find(needle) != std::string::npos) return true;
        return false;
    }
};

// ---- 对象面证据工具（S21：不靠 IsClosed/Sleep 伪证）-------------------------------

// /proc/self/fd 中目标为 socket: 的条目数（不可读返回 -1）。
int CountSocketFds() {
    int count = 0;
    DIR* dir = ::opendir("/proc/self/fd");
    if (!dir) return -1;
    while (dirent* entry = ::readdir(dir)) {
        char path[128];
        std::snprintf(path, sizeof(path), "/proc/self/fd/%s", entry->d_name);
        char target[256];
        const ssize_t n = ::readlink(path, target, sizeof(target) - 1);
        if (n <= 0) continue;
        target[n] = '\0';
        if (std::strncmp(target, "socket:", 7) == 0) ++count;
    }
    ::closedir(dir);
    return count;
}

// /proc/net/tcp{,6} 中是否存在 local port == port 且 state == LISTEN(0A) 的行。
// 行格式：sl local_address rem_address st ...（local_address 为 HEX:PORT）。
bool PortInListenTable(std::uint16_t port) {
    char hex[8];
    std::snprintf(hex, sizeof(hex), "%04X", port);
    const char* files[] = {"/proc/net/tcp", "/proc/net/tcp6"};
    for (const char* file : files) {
        std::ifstream in(file);
        if (!in) continue;
        std::string line;
        std::getline(in, line);  // 表头
        while (std::getline(in, line)) {
            std::istringstream iss(line);
            std::string sl, local, rem, st;
            if (!(iss >> sl >> local >> rem >> st)) continue;
            const auto colon = local.rfind(':');
            if (colon == std::string::npos) continue;
            if (local.substr(colon + 1) == hex && st == "0A") return true;
        }
    }
    return false;
}

// ---- server/会话装配 --------------------------------------------------------------

// handler_calls 用 shared_ptr 持有：handler 闭包的生命周期属于 server 内部登记，
// rig 按值返回后指针必须仍然有效。
struct CoreRig {
    std::shared_ptr<McpServer> server;
    std::shared_ptr<McpSession> session;
    std::shared_ptr<SinkLog> log = std::make_shared<SinkLog>();
    std::shared_ptr<std::atomic<int>> calls = std::make_shared<std::atomic<int>>(0);
    std::shared_ptr<Gate> gate = std::make_shared<Gate>();
    int Calls() const { return calls->load(std::memory_order_relaxed); }
};

// 注册 mem://slow：进入 handler 记数 + 标记，协作等待 released/cancelled。
// gate 传 nullptr 时 handler 直接返回（快速路径）。
CoreRig MakeRig(std::size_t max_inflight = 64,
                std::chrono::milliseconds request_timeout = std::chrono::milliseconds{30000},
                std::size_t max_registrations = 256,
                std::shared_ptr<Gate> gate = std::make_shared<Gate>()) {
    ServerOptions options;
    options.max_inflight_requests = max_inflight;
    options.request_timeout = request_timeout;
    options.max_registrations = max_registrations;
    CoreRig rig;
    rig.gate = gate;
    auto calls = rig.calls;
    auto gate_local = gate;
    rig.server = McpServer::Create(options).value();
    CHECK(rig.server->AddResource(
        ResourceDefinition{"mem://slow", "slow", "", "text/plain"},
        [calls, gate_local](const RequestContext& ctx,
                            const std::string&) -> bbt::infra::result<ResourceResult> {
            calls->fetch_add(1, std::memory_order_relaxed);
            if (gate_local) {
                gate_local->MarkEntered();  // 在途确定性锚点（WaitEntered 的对侧）
                WaitReleasedOrCancelled(*gate_local, ctx);
            }
            ResourceResult result;
            result.contents.push_back(
                ResourceContents::Text("mem://slow", "text/plain", "payload").value());
            return bbt::infra::result<ResourceResult>::ok(std::move(result));
        }));
    CHECK(rig.server->Start());
    rig.session = CreateSession(rig.server);
    rig.session->SetNotificationSink(
        [log = rig.log](const std::string& frame) { log->Record(frame); });
    return rig;
}

long OwnerCount(RequestOwner::Terminal kind) { return RequestOwner::TestCount(kind); }

// ---- 原始 HTTP 客户端（真实 loopback socket）--------------------------------------

class HttpConn {
public:
    bool Connect(std::uint16_t port) {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd_ < 0) return false;
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(port);
        if (::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) != 1) return false;
        return ::connect(fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0;
    }
    bool SendAll(std::string_view data) {
        std::size_t sent = 0;
        while (sent < data.size()) {
            const ssize_t n = ::send(fd_, data.data() + sent, data.size() - sent, 0);
            if (n <= 0) {
                if (errno == EINTR) continue;
                return false;
            }
            sent += static_cast<std::size_t>(n);
        }
        return true;
    }
    // 有界读取一段（deadline 内到达多少收多少；SSE 分段断言用）。
    std::string ReadBounded(int timeout_ms) {
        std::string out;
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
        char buffer[4096];
        while (std::chrono::steady_clock::now() < deadline) {
            pollfd p{fd_, POLLIN, 0};
            const int ready = ::poll(&p, 1, 50);
            if (ready <= 0) continue;
            const ssize_t n = ::recv(fd_, buffer, sizeof(buffer), 0);
            if (n > 0) {
                out.append(buffer, static_cast<std::size_t>(n));
                break;  // 一次到达即返回（分事件断言由调用方续读）
            }
            if (n == 0) { eof_ = true; break; }
            if (n < 0 && errno != EINTR && errno != EAGAIN) break;
        }
        return out;
    }
    bool Eof() const { return eof_; }
    void Close() {
        if (fd_ >= 0) ::close(fd_);
        fd_ = -1;
    }

private:
    int fd_ = -1;
    bool eof_ = false;
};

std::string PostBody(std::uint16_t port, const std::string& body, const char* accept,
                     const char* mcp_method, const char* mcp_name = nullptr) {
    std::string head = "POST /mcp HTTP/1.1\r\nHost: 127.0.0.1:" + std::to_string(port) +
                       "\r\nContent-Type: application/json\r\nAccept: " + accept +
                       "\r\nMCP-Protocol-Version: " + kModern + "\r\n";
    if (mcp_method) {  // modern 请求注解头（S19：缺失 → -32020）
        head += std::string("Mcp-Method: ") + mcp_method + "\r\n";
    }
    // S19：具名方法的注解必须与 body 的 name/uri 精确一致。
    if (mcp_name) head += std::string("Mcp-Name: ") + mcp_name + "\r\n";
    head += "Content-Length: " + std::to_string(body.size()) + "\r\n\r\n";
    return head + body;
}

std::string PostBody(std::uint16_t port, const std::string& body,
                     const char* accept = "application/json") {
    return PostBody(port, body, accept, nullptr);
}

// ---- T1：恰好一次终态 + 晚到 cancel 无害（R10/S17）--------------------------------

void TestUniqueTerminalAndLateCancelHarmless() {
    std::fprintf(stderr, "PROGRESS T1 unique-terminal + late-cancel\n");
    CoreRig rig = MakeRig(64, std::chrono::milliseconds{30000}, 256, nullptr);

    const long c0 = OwnerCount(RequestOwner::Terminal::kCompleted);
    const long x0 = OwnerCount(RequestOwner::Terminal::kCancelled);

    auto outcome = rig.session->HandleFrame(ModernRead("7", "mem://slow"));
    CHECK(outcome && outcome.value().has_response);
    CHECK(ParseOk(outcome.value().response).Find("result"));
    CHECK(OwnerCount(RequestOwner::Terminal::kCompleted) == c0 + 1);  // 恰好一次终态
    CHECK(OwnerCount(RequestOwner::Terminal::kCancelled) == x0);

    // 晚到 cancel（已终态）：静默丢弃，无第二消息、无计数变化。
    auto late = rig.session->HandleFrame(CancelFrame(JsonValue("7")));
    CHECK(late && !late.value().has_response);
    CHECK(OwnerCount(RequestOwner::Terminal::kCompleted) == c0 + 1);
    CHECK(OwnerCount(RequestOwner::Terminal::kCancelled) == x0);

    // 未知 id cancel：同样静默（S17）。
    auto stray = rig.session->HandleFrame(CancelFrame(JsonValue("ghost")));
    CHECK(stray && !stray.value().has_response);
    CHECK(OwnerCount(RequestOwner::Terminal::kCancelled) == x0);
    CHECK(rig.Calls() == 1);
}

// ---- T2：已进入 callback 的协作式取消（cancel 命中 → 空壳提交）----------------------

void TestCooperativeCancelInsideCallback() {
    std::fprintf(stderr, "PROGRESS T2 cooperative cancel in callback\n");
    CoreRig rig = MakeRig();

    const long c0 = OwnerCount(RequestOwner::Terminal::kCompleted);
    const long x0 = OwnerCount(RequestOwner::Terminal::kCancelled);

    // 派发线程：请求 9 在途（handler 阻塞于 gate）。
    std::thread worker([&rig] {
        auto outcome = rig.session->HandleFrame(ModernRead("9", "mem://slow"));
        CHECK(outcome && !outcome.value().has_response);  // 空壳：被取消请求不再有消息
    });
    rig.gate->WaitEntered();  // handler 已进入（确定性交错）

    // 同会话并发 cancel（transport 可并发驱动 pipelined 帧）：会话域路由命中本会话在途 owner。
    auto cancel = rig.session->HandleFrame(CancelFrame(JsonValue("9")));
    CHECK(cancel && !cancel.value().has_response);
    CHECK(OwnerCount(RequestOwner::Terminal::kCancelled) == x0 + 1);  // 恰一次落定
    CHECK(OwnerCount(RequestOwner::Terminal::kCompleted) == c0);      // 未被完成抢注

    rig.gate->Release();  // handler 观察 abort 位协作退出（结果注定被空壳消费）
    worker.join();
    CHECK(rig.Calls() == 1);
}

// ---- T3：期限可观察拒绝（S22；timeout ≠ 未执行）------------------------------------

void TestDeadlineObservableRejection() {
    std::fprintf(stderr, "PROGRESS T3 deadline rejection\n");
    ServerOptions options;
    options.request_timeout = std::chrono::milliseconds{30};
    auto server = McpServer::Create(options).value();
    std::atomic<bool> executed{false};
    CHECK(server->AddResource(
        ResourceDefinition{"mem://deadline", "deadline", "", "text/plain"},
        [&executed](const RequestContext& ctx,
                    const std::string&) -> bbt::infra::result<ResourceResult> {
            executed.store(true, std::memory_order_release);
            // handler 自身可观察期限（协作式退出观察点之一）；这里故意不退，验证
            // 提交门的期限拒绝（结果已产生但被丢弃，timeout ≠ 未执行）。
            static_cast<void>(ctx.Deadline());
            std::this_thread::sleep_for(std::chrono::milliseconds{150});
            ResourceResult result;
            result.contents.push_back(
                ResourceContents::Text("mem://deadline", "text/plain", "late").value());
            return bbt::infra::result<ResourceResult>::ok(std::move(result));
        }));
    CHECK(server->Start());
    auto session = CreateSession(server);

    const long t0 = OwnerCount(RequestOwner::Terminal::kTimedOut);
    const long c0 = OwnerCount(RequestOwner::Terminal::kCompleted);

    auto outcome = session->HandleFrame(ModernRead("d1", "mem://deadline"));
    CHECK(outcome && outcome.value().has_response);
    JsonValue response = ParseOk(outcome.value().response);
    auto error = response.Find("error");
    CHECK(error);  // 结果被期限拒绝替换
    if (error) {
        auto code = error.value().Find("code");
        CHECK(code && code.value().AsInt64() &&
              code.value().AsInt64().value() == -32000);  // RequestDeadlineExceeded
    }
    CHECK(executed.load());  // handler 恰执行一次（副作用已发生，结果被期限丢弃）
    CHECK(OwnerCount(RequestOwner::Terminal::kTimedOut) == t0 + 1);
    CHECK(OwnerCount(RequestOwner::Terminal::kCompleted) == c0);
    // 晚到 cancel：期限终态已落定 → 静默（恰好一次终态）。
    auto late = session->HandleFrame(CancelFrame(JsonValue("d1")));
    CHECK(late && !late.value().has_response);
    CHECK(OwnerCount(RequestOwner::Terminal::kTimedOut) == t0 + 1);
}

// ---- T4：Close 竞争（callback-start 门两侧）+ 封口后拒新（R10）----------------------

void TestCloseRacesAndSealedAdmission() {
    std::fprintf(stderr, "PROGRESS T4 close races\n");
    // (i) callback-start 门之前：Close 先 → 请求不进入回调。
    {
        CoreRig rig = MakeRig();
        std::atomic<bool> hook_reached{false};
        auto hook_gate = std::make_shared<Gate>();
        rig.session->SetTestPreCallbackHook([hook_reached_ptr = &hook_reached,
                                             hook_gate] {
            hook_reached_ptr->store(true, std::memory_order_release);
            std::unique_lock<std::mutex> lock(hook_gate->m);
            hook_gate->cv.wait(lock, [hook_gate] { return hook_gate->released; });
        });
        const long closed0 = OwnerCount(RequestOwner::Terminal::kClosed);
        std::thread worker([&rig] {
            auto outcome = rig.session->HandleFrame(ModernRead("11", "mem://slow"));
            CHECK(!outcome);  // M3 提交门：封口后结果不产出（ClosedCoreError）
        });
        while (!hook_reached.load(std::memory_order_acquire)) std::this_thread::yield();
        rig.server->Close();  // 封口 + 翻转在途 owner（不等业务栈）
        hook_gate->Release();
        worker.join();
        CHECK(rig.Calls() == 0);  // 未进入用户回调
        CHECK(OwnerCount(RequestOwner::Terminal::kClosed) == closed0 + 1);
        rig.session->SetTestPreCallbackHook(nullptr);
        // 封口后新请求被拒（admission 拒绝，无 handler 调用）。
        auto refused = rig.session->HandleFrame(ModernRead("12", "mem://slow"));
        CHECK(!refused);
        CHECK(rig.Calls() == 0);
    }
    // (ii) 已进入 callback：协作退出，输出由提交门抑制。
    {
        CoreRig rig = MakeRig();
        const long closed0 = OwnerCount(RequestOwner::Terminal::kClosed);
        std::thread worker([&rig] {
            auto outcome = rig.session->HandleFrame(ModernRead("13", "mem://slow"));
            CHECK(!outcome);  // 封口后响应被提交门吞掉（不产生消息）
        });
        rig.gate->WaitEntered();  // handler 确已进入回调（确定性交错）
        rig.server->Close();      // 同步返回即封口（不等待 handler）
        rig.gate->Release();      // handler 协作退出（Close 先于完成项返回）
        worker.join();
        CHECK(rig.Calls() == 1);
        CHECK(OwnerCount(RequestOwner::Terminal::kClosed) == closed0 + 1);
    }
}

// ---- T5：cancel 会话域路由 + 同会话在途 id 只接纳一次（S4；P-A/P-B 反转）------------

void TestCancelSessionScopeAndDuplicateAdmission() {
    std::fprintf(stderr, "PROGRESS T5 cancel session scope + duplicate admission\n");

    // ② 跨会话 cancel 零影响（P-A 反转）：A 在途 id="X"（B 从未持有 X），B 发一次
    //    cancel → A 的 owner 不被触碰、A 的响应正常送达（旧全局路由会吞掉该响应）。
    {
        CoreRig rig = MakeRig();
        auto session_b = CreateSession(rig.server);
        const long c0 = OwnerCount(RequestOwner::Terminal::kCompleted);
        const long x0 = OwnerCount(RequestOwner::Terminal::kCancelled);
        std::thread worker([&rig] {
            auto outcome = rig.session->HandleFrame(ModernRead("X", "mem://slow"));
            CHECK(outcome && outcome.value().has_response);  // 响应不被跨会话 cancel 吞掉
            CHECK(ParseOk(outcome.value().response).Find("result"));
        });
        rig.gate->WaitEntered();  // handler 在途（确定性锚点）
        auto cancel_b = session_b->HandleFrame(CancelFrame(JsonValue("X")));
        CHECK(cancel_b && !cancel_b.value().has_response);  // 合法通知永不回包
        CHECK(OwnerCount(RequestOwner::Terminal::kCancelled) == x0);  // B 的 cancel 零命中
        rig.gate->Release();
        worker.join();
        CHECK(rig.Calls() == 1);
        CHECK(OwnerCount(RequestOwner::Terminal::kCompleted) == c0 + 1);
    }

    // ① 同会话 cancel 照常（S17/R10 保持）：取消自己在途 id → 空壳提交、恰一次终态。
    {
        CoreRig rig = MakeRig();
        const long x0 = OwnerCount(RequestOwner::Terminal::kCancelled);
        std::thread worker([&rig] {
            auto outcome = rig.session->HandleFrame(ModernRead("Y", "mem://slow"));
            CHECK(outcome && !outcome.value().has_response);  // 被取消：不再产生消息
        });
        rig.gate->WaitEntered();
        auto cancel_self = rig.session->HandleFrame(CancelFrame(JsonValue("Y")));
        CHECK(cancel_self && !cancel_self.value().has_response);
        CHECK(OwnerCount(RequestOwner::Terminal::kCancelled) == x0 + 1);  // 恰一次命中
        rig.gate->Release();
        worker.join();
        CHECK(rig.Calls() == 1);
    }

    // ③ 同会话同 id 重复在途 → 只接纳一次（P-B 反转）：第二帧 -32600 拒绝、
    //    handler 不双调用（跨会话同 id 共存由 T9(b) 隔离断言继续覆盖）。
    {
        CoreRig rig = MakeRig();
        std::thread worker([&rig] {
            auto outcome = rig.session->HandleFrame(ModernRead("dup", "mem://slow"));
            CHECK(outcome && outcome.value().has_response);
            CHECK(ParseOk(outcome.value().response).Find("result"));
        });
        rig.gate->WaitEntered();
        auto dup = rig.session->HandleFrame(ModernRead("dup", "mem://slow"));
        CHECK(dup && dup.value().has_response);  // 重复帧：恰好一次错误响应
        JsonValue dup_resp = ParseOk(dup.value().response);
        auto dup_err = dup_resp.Find("error");
        CHECK(dup_err);
        if (dup_err) {
            auto code = dup_err.value().Find("code");
            CHECK(code && code.value().AsInt64() &&
                  code.value().AsInt64().value() == -32600);  // InvalidRequest
        }
        CHECK(rig.Calls() == 1);  // handler 恰一次（重复帧不进 handler）
        rig.gate->Release();
        worker.join();
    }

    // 订阅半边：跨会话 cancel 零影响（名额仍占用）；同会话 cancel 结束自己的订阅
    //（流结束帧 + 名额释放）；流结束帧是最后一帧（F5 core 面收口断言）。
    {
        CoreRig rig = MakeRig(1);  // max_inflight=1：订阅独占长请求名额（占用/释放可观察）
        auto listen = rig.session->HandleFrame(ModernListen("sub-1"));
        CHECK(listen && !listen.value().has_response);  // ack 经 sink（长请求无即时响应）
        CHECK(rig.log->Contains("subscriptions/acknowledged"));
        auto session_b = CreateSession(rig.server);
        auto stray = session_b->HandleFrame(CancelFrame(JsonValue("sub-1")));
        CHECK(stray && !stray.value().has_response);
        CHECK(rig.log->Size() == 1);  // 订阅仍活着：无流结束帧
        auto rejected = session_b->HandleFrame(ModernListen("sub-x"));
        CHECK(!rejected);  // 名额仍被 A 的订阅占用（跨会话 cancel 未释放名额）
        auto self_cancel = rig.session->HandleFrame(CancelFrame(JsonValue("sub-1")));
        CHECK(self_cancel && !self_cancel.value().has_response);
        CHECK(rig.log->Size() == 2);  // 流结束帧（id 原值 + subscriptionId meta）
        JsonValue closing = ParseOk(rig.log->Last());
        CHECK(closing.Find("result"));
        auto closing_id = closing.Find("id");
        CHECK(closing_id && closing_id.value().AsString() &&
              closing_id.value().AsString().value() == "sub-1");
        auto relisten = rig.session->HandleFrame(ModernListen("sub-2"));
        CHECK(relisten && !relisten.value().has_response);
        CHECK(rig.log->Size() == 3);  // 只有 sub-2 的 ack：流结束帧后无后续帧（F5）
    }
}

// ---- T6：HTTP 跨连接 cancel 静默 no-op + 断连驱动订阅终止（S4/S20；F5）--------------

void TestHttpCancelNoopAndDisconnectDrivenEnd() {
    std::fprintf(stderr, "PROGRESS T6 http cancel noop + disconnect-driven end\n");
    ServerOptions options;
    options.max_inflight_requests = 1;  // 订阅独占唯一长请求名额（占用/释放可观察）
    auto server = McpServer::Create(options).value();
    CHECK(server->AddResource(
        ResourceDefinition{"mem://a", "a", "", "text/plain"},
        [](const RequestContext&,
           const std::string&) -> bbt::infra::result<ResourceResult> {
            ResourceResult result;
            result.contents.push_back(
                ResourceContents::Text("mem://a", "text/plain", "v").value());
            return bbt::infra::result<ResourceResult>::ok(std::move(result));
        }));
    CHECK(server->Start());
    bbt::infra::mcp::HttpOptions http;
    // 心跳间隔压到 100ms：断连经「写失败」有限确认（SSE 无读侧请求），短周期让
    // 收口延迟确定有界（默认 15s 只适配长稳态）。
    http.sse_heartbeat_interval = std::chrono::milliseconds{100};
    auto transport = McpHttpTransport::Listen(server, http).value();
    const std::uint16_t port = transport->LocalAddress().port;

    // 连接 1：SSE 订阅（真实流）。Accept 必须单头且允许 event-stream（sse_mode 判定）。
    HttpConn subscriber;
    CHECK(subscriber.Connect(port));
    CHECK(subscriber.SendAll(PostBody(port, ModernListen("sub-http"), "text/event-stream",
                                      "subscriptions/listen")));
    std::string stream = subscriber.ReadBounded(2000);
    if (stream.find("event: message") == std::string::npos ||
        stream.find("subscriptions/acknowledged") == std::string::npos) {
        std::fprintf(stderr, "T6-DIAG stream_head=%.300s\n", stream.c_str());
        // 续读一次（ack 与响应头可能分段到达）。
        stream += subscriber.ReadBounded(1000);
    }
    CHECK(stream.find("event: message") != std::string::npos);
    CHECK(stream.find("subscriptions/acknowledged") != std::string::npos);

    // ① 跨连接 cancel（S4）：连接 2 取消连接 1 的订阅 → 合法通知 202，订阅不受影响。
    //    modern era 注解头必需（缺失 → -32020 拒绝，帧根本到不了核心）；状态行必须
    //    全匹配，防止错误码（如 -32020）的子串造成假阳性。
    {
        HttpConn canceller;
        CHECK(canceller.Connect(port));
        CHECK(canceller.SendAll(PostBody(port, ModernCancelFrame(JsonValue("sub-http")),
                                          "application/json", "notifications/cancelled")));
        const std::string notice = canceller.ReadBounded(2000);
        CHECK(notice.find("HTTP/1.1 202") != std::string::npos);  // 合法通知：202 空 body
        canceller.Close();
    }
    // 订阅仍活着（S4 断言）：后续窗口心跳注释行持续到达（正向活性证据），无流结束
    // data 帧、无 EOF。F5：收口改由断连驱动后流结束帧不出 wire，「结束帧后无任何
    // 字节」以「收口前除心跳外无任何后续 data 帧」为可测等价断言。
    {
        const auto alive_deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds{1500};
        bool heartbeat_seen = false;
        bool flow_end_seen = false;
        while (std::chrono::steady_clock::now() < alive_deadline) {
            std::string more = subscriber.ReadBounded(100);
            stream += more;
            if (stream.find("\"result\"") != std::string::npos) flow_end_seen = true;
            if (stream.find("keep-alive") != std::string::npos) {
                heartbeat_seen = true;
                break;
            }
        }
        if (!heartbeat_seen || flow_end_seen)
            std::fprintf(stderr, "T6-DIAG alive tail=%.300s\n",
                         stream.size() > 300 ? stream.substr(stream.size() - 300).c_str()
                                             : stream.c_str());
        CHECK(heartbeat_seen);     // 心跳仍在写：流未被跨连接 cancel 收口
        CHECK(!flow_end_seen);     // 跨连接 cancel 未产生流结束响应帧
        CHECK(!subscriber.Eof());  // 连接未被跨连接 cancel 关闭
    }
    // 名额仍被占用（对象面）：POST resources/read → 503（Overloaded 核心拒绝）。
    {
        HttpConn probe;
        CHECK(probe.Connect(port));
        CHECK(probe.SendAll(PostBody(port, ModernRead("p1", "mem://a"), "application/json",
                                      "resources/read", "mem://a")));
        const std::string busy = probe.ReadBounded(2000);
        CHECK(busy.find("HTTP/1.1 503") != std::string::npos);
        probe.Close();
    }

    // ② 断连驱动收口（S20）：客户端断开 → 服务端心跳写失败确认断连 → 订阅结束
    //    （流结束帧入封队=丢弃、无后续写）、长请求名额归还（503 → 200 翻转可观察）。
    subscriber.Close();
    {
        const auto released_deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds{8000};
        bool released = false;
        while (std::chrono::steady_clock::now() < released_deadline) {
            HttpConn probe;
            if (probe.Connect(port) &&
                probe.SendAll(PostBody(port, ModernRead("p2", "mem://a"), "application/json",
                                       "resources/read", "mem://a"))) {
                const std::string body = probe.ReadBounded(2000);
                if (body.find("HTTP/1.1 200") != std::string::npos) released = true;
            }
            probe.Close();
            if (released) break;
            std::this_thread::sleep_for(std::chrono::milliseconds{50});
        }
        CHECK(released);  // 断连已驱动订阅收口：名额归还（S20）
    }
    transport->Close();
}

// ---- T7：stdio close_drain 降级专测（P2.1）-----------------------------------------

void TestStdioDrainDegradation() {
    std::fprintf(stderr, "PROGRESS T7 stdio drain degradation\n");
    // 独占 pipe 对（transport 不归属 fd；进程寿命内不关闭——在途协程可能仍引用）。
    int in_pipe[2];
    int out_pipe[2];
    CHECK(::pipe(in_pipe) == 0);
    CHECK(::pipe(out_pipe) == 0);
    const int saved_in = ::fcntl(in_pipe[0], F_GETFL);
    const int saved_out = ::fcntl(out_pipe[1], F_GETFL);

    ServerOptions options;
    options.request_timeout = std::chrono::milliseconds{10000};
    auto server = McpServer::Create(options).value();
    auto gate = std::make_shared<Gate>();
    auto calls = std::make_shared<std::atomic<int>>(0);
    CHECK(server->AddResource(
        ResourceDefinition{"mem://slow", "slow", "", "text/plain"},
        [gate, calls](const RequestContext& ctx,
                      const std::string&) -> bbt::infra::result<ResourceResult> {
            calls->fetch_add(1, std::memory_order_relaxed);
            gate->MarkEntered();
            WaitReleasedOrCancelled(*gate, ctx);
            ResourceResult result;
            result.contents.push_back(
                ResourceContents::Text("mem://slow", "text/plain", "x").value());
            return bbt::infra::result<ResourceResult>::ok(std::move(result));
        }));
    CHECK(server->Start());

    // 预置在途请求帧：reader 协程读出后将阻塞在业务 gate（Close 时协程在途的前提）。
    const std::string request = ModernRead("50", "mem://slow") + "\n";
    CHECK(::write(in_pipe[1], request.data(), request.size()) ==
          static_cast<ssize_t>(request.size()));

    bbt::infra::mcp::StdioOptions stdio;
    stdio.stdin_fd = in_pipe[0];
    stdio.stdout_fd = out_pipe[1];
    stdio.close_drain_timeout = std::chrono::milliseconds{150};  // < write_deadline(5s)
    auto transport = McpStdioTransport::Start(server, stdio);
    CHECK(transport);
    gate->WaitEntered();  // reader 协程已把帧分派进 handler（在途确定性成立）

    const long closed0 = OwnerCount(RequestOwner::Terminal::kClosed);
    transport.value()->Close();  // 有界汇合超时 → 降级路径（还原 O_NONBLOCK、快照）
    const auto evidence = transport.value()->LastCloseEvidence();

    // 降级快照：reader 在途（未汇合）、输入输出已封、sink 已卸、非阻塞位已还原。
    CHECK(!evidence.reader_joined);
    CHECK(evidence.input_sealed);
    CHECK(evidence.output_sealed);
    CHECK(!evidence.sink_mounted);
    CHECK(evidence.nonblock_restored);
    // 对象面：fd 标志确已还原为 Start 前值（不经 IsClosed 标签）。
    CHECK(::fcntl(in_pipe[0], F_GETFL) == saved_in);
    CHECK(::fcntl(out_pipe[1], F_GETFL) == saved_out);

    // 晚到完成项只消费空壳：放行 handler → 响应被封队丢弃 → stdout 永无字节。
    gate->Release();
    const auto quiet_deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds{2000};
    bool output_grew = false;
    while (std::chrono::steady_clock::now() < quiet_deadline) {
        pollfd p{out_pipe[0], POLLIN, 0};
        if (::poll(&p, 1, 20) > 0 && (p.revents & POLLIN)) { output_grew = true; break; }
    }
    CHECK(!output_grew);  // 封队后晚到响应不落 wire
    CHECK(calls->load() == 1);
    // transport Close 不碰 core 终态（core 生命周期属业务）。
    CHECK(OwnerCount(RequestOwner::Terminal::kClosed) == closed0);
}

// ---- T8：HTTP 网络级 S21 确定性验收（P2.3）-----------------------------------------

void TestHttpS21NetworkAcceptance() {
    std::fprintf(stderr, "PROGRESS T8 http S21 network acceptance\n");
    ServerOptions options;
    options.request_timeout = std::chrono::milliseconds{10000};
    auto server = McpServer::Create(options).value();
    auto gate = std::make_shared<Gate>();
    auto calls = std::make_shared<std::atomic<int>>(0);
    CHECK(server->AddResource(
        ResourceDefinition{"mem://slow", "slow", "", "text/plain"},
        [gate, calls](const RequestContext&,
                      const std::string&) -> bbt::infra::result<ResourceResult> {
            calls->fetch_add(1, std::memory_order_relaxed);
            gate->MarkEntered();
            for (;;) {
                std::unique_lock<std::mutex> lock(gate->m);
                if (gate->cv.wait_for(lock, std::chrono::milliseconds(5),
                                      [gate] { return gate->released; }))
                    break;
            }
            ResourceResult result;
            result.contents.push_back(
                ResourceContents::Text("mem://slow", "text/plain", "x").value());
            return bbt::infra::result<ResourceResult>::ok(std::move(result));
        }));
    CHECK(server->Start());

    bbt::infra::mcp::HttpOptions http;
    http.close_drain_timeout = std::chrono::milliseconds{300};  // 在途 handler 场景有界放弃
    auto transport = McpHttpTransport::Listen(server, http).value();
    const std::uint16_t port = transport->LocalAddress().port;

    HttpConn client;
    CHECK(client.Connect(port));
    CHECK(client.SendAll(PostBody(port, ModernRead("21", "mem://slow"), "application/json",
                                  "resources/read", "mem://slow")));
    bool entered = false;
    {
        std::unique_lock<std::mutex> lock(gate->m);
        entered = gate->cv.wait_for(lock, std::chrono::milliseconds{2000},
                                    [gate] { return gate->entered; });
    }
    CHECK(entered);  // admission 失败必须报错并清理，不能无限等待未入场的 handler。
    if (!entered) {
        std::fprintf(stderr, "T8-DIAG admission response=%s\n", client.ReadBounded(200).c_str());
        gate->Release();
        transport->Close();
        client.Close();
        return;
    }
    std::fprintf(stderr, "PROGRESS T8.a in-flight established\n");

    // 对象面基线（Close 前）：listener + 已接受连接 + 测试自身客户端的 socket fd。
    const int sockets_before = CountSocketFds();
    CHECK(PortInListenTable(port));  // Start 后 LISTEN 在表（对象面正向证据）

    const long closed0 = OwnerCount(RequestOwner::Terminal::kClosed);
    std::fprintf(stderr, "PROGRESS T8.b closing\n");
    transport->Close();  // 主线程 Close；不等待在途业务栈（有界放弃）
    std::fprintf(stderr, "PROGRESS T8.c close returned\n");
    const auto evidence = transport->LastCloseEvidence();

    // 返回当刻证据（对象面；不依赖 connect 探测，规避 WSL2 端口转发噪声）。
    CHECK(evidence.listener_closed);
    CHECK(evidence.runtime_closed);
    CHECK(evidence.output_sealed);
    CHECK(evidence.connections_closed >= 1);
    CHECK(!evidence.connection_loops_exited);  // 在途 handler 未汇合（降级口径如实落快照）
    CHECK(!PortInListenTable(port));  // 3-2 遗留缺口收口：当刻内核表无 LISTEN 残留
    const int sockets_after = CountSocketFds();
    // 返回当刻：listener fd 必已物理关闭（probe 拒连 + LISTEN 表消失双证）。
    // 连接 fd 由卡住的 loop 退出时归还（CoTCP active 帧安全归还口径），此处断言
    // 「至少少了 listener」并在晚到 loop 退出后断言全额回落。
    CHECK(sockets_after <= sockets_before - 1);
    // 辅助面：connect 不可达（127.0.0.1 本机内核栈，无转发噪声）。
    {
        HttpConn probe;
        CHECK(!probe.Connect(port));
    }
    // 客户端面：连接已被物理关闭，无响应字节（EOF 或 RST 均无数据）。
    CHECK(client.ReadBounded(200).empty());

    // 并发/重复 Close：均等真实落定（幂等，无二次 teardown）。
    {
        std::thread a([&] { transport->Close(); });
        std::thread b([&] { transport->Close(); });
        a.join();
        b.join();
        CHECK(transport->IsClosed());
    }
    CHECK(transport->LastCloseEvidence().connections_closed ==
          evidence.connections_closed);

    // 晚到回调只消费空壳：放行 handler → 晚到写失败于已关连接（Close 唤醒只覆盖
    // 已登记等待者，晚登记的写等待按 write_deadline=5s 有界落定）→ 无字节、证据不变。
    // 注意：证据快照在首次收口当刻冻结（connection_loops_exited 恒为返回当刻值），
    // 晚到 loop 退出的证据用 fd 回落基线（对象面），不用冻结快照。
    gate->Release();
    const auto drain_deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds{8000};
    int sockets_settled = -1;
    while (std::chrono::steady_clock::now() < drain_deadline) {
        sockets_settled = CountSocketFds();
        if (sockets_settled == sockets_before - 2) break;
        std::this_thread::sleep_for(std::chrono::milliseconds{50});
    }
    CHECK(sockets_settled == sockets_before - 2);  // 晚到 loop 已退出并归还 conn fd
    CHECK(client.ReadBounded(300).empty());  // 晚到响应不触任何资源（无字节送达）
    CHECK(calls->load() == 1);
    // core 终态不受 transport Close 影响（core 生命周期属业务）。
    CHECK(OwnerCount(RequestOwner::Terminal::kClosed) == closed0);
    client.Close();
}

// ---- T9：S22 边界（容量/隔离/不重试/注册上限）--------------------------------------

void TestS22Boundaries() {
    std::fprintf(stderr, "PROGRESS T9 S22 boundaries\n");
    // (a) 在途容量：max_inflight=1，占用中第二请求被拒（可观察 Overloaded 错误）。
    {
        CoreRig rig = MakeRig(1);
        std::thread worker([&rig] {
            auto first = rig.session->HandleFrame(ModernRead("a1", "mem://slow"));
            CHECK(first && first.value().has_response);  // 释放后正常完成
        });
        rig.gate->WaitEntered();
        auto second = rig.session->HandleFrame(ModernRead("a2", "mem://slow"));
        CHECK(!second);  // 超在途上限：核心拒绝（HTTP 面 → 503），不排队、不自动重试
        rig.gate->Release();
        worker.join();
        CHECK(rig.Calls() == 1);
    }
    // (b) 不同 server 隔离：A 的 cancel/Close 不波及 B。
    {
        CoreRig rig_a = MakeRig();
        CoreRig rig_b = MakeRig(64, std::chrono::milliseconds{30000}, 256, nullptr);
        std::thread worker([&rig_a] {
            auto outcome = rig_a.session->HandleFrame(ModernRead("iso", "mem://slow"));
            CHECK(outcome && !outcome.value().has_response);  // A：被取消 → 空壳
        });
        rig_a.gate->WaitEntered();
        // B 独立完成同 id 请求（不受 A 在途影响）。
        auto b_outcome = rig_b.session->HandleFrame(ModernRead("iso", "mem://slow"));
        CHECK(b_outcome && b_outcome.value().has_response);
        // 经 A 路由 cancel：只命中 A 的 owner。
        auto cancel = rig_a.session->HandleFrame(CancelFrame(JsonValue("iso")));
        CHECK(cancel && !cancel.value().has_response);
        rig_a.gate->Release();
        worker.join();
        CHECK(rig_a.Calls() == 1);
        CHECK(rig_b.Calls() == 1);
        CHECK(rig_b.server->IsStarted() && !rig_b.server->IsClosed());
        rig_a.server->Close();
        CHECK(!rig_b.server->IsClosed());  // Close 隔离
        auto b_again = rig_b.session->HandleFrame(ModernRead("iso2", "mem://slow"));
        CHECK(b_again && b_again.value().has_response);
    }
    // (c) 业务失败不自动重试：handler 恰调用一次。
    {
        ServerOptions options;
        auto server = McpServer::Create(options).value();
        std::atomic<int> calls{0};
        CHECK(server->AddResource(
            ResourceDefinition{"mem://boom", "boom", "", "text/plain"},
            [&calls](const RequestContext&,
                     const std::string&) -> bbt::infra::result<ResourceResult> {
                calls.fetch_add(1);
                return bbt::infra::result<ResourceResult>::err(
                    bbt::infra::MakeError(bbt::infra::ErrorCode::InternalError, "boom"));
            }));
        CHECK(server->Start());
        auto session = CreateSession(server);
        auto outcome = session->HandleFrame(ModernRead("boom", "mem://boom"));
        CHECK(outcome && outcome.value().has_response);
        CHECK(ParseOk(outcome.value().response).Find("error"));  // 业务失败以错误响应回包
        CHECK(calls.load() == 1);  // 无自动重试
    }
    // (d) 注册复杂度上限：max_registrations=1 时第二条注册被拒。
    {
        ServerOptions options;
        options.max_registrations = 1;
        auto server = McpServer::Create(options).value();
        auto handler = [](const RequestContext&,
                          const std::string&) -> bbt::infra::result<ResourceResult> {
            ResourceResult result;
            result.contents.push_back(
                ResourceContents::Text("mem://x", "text/plain", "x").value());
            return bbt::infra::result<ResourceResult>::ok(std::move(result));
        };
        CHECK(server->AddResource(
            ResourceDefinition{"mem://one", "one", "", "text/plain"}, handler));
        auto second = server->AddResource(
            ResourceDefinition{"mem://two", "two", "", "text/plain"}, handler);
        CHECK(!second);  // Overloaded（可观察拒绝）
    }
}

}  // namespace

int main() {
    // 纯核心面（无协程运行时依赖）。
    TestUniqueTerminalAndLateCancelHarmless();
    TestCooperativeCancelInsideCallback();
    TestDeadlineObservableRejection();
    TestCloseRacesAndSealedAdmission();
    TestCancelSessionScopeAndDuplicateAdmission();

    // 传输面：共享协程运行时（每进程一次）。
    auto* config = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    config->m_cfg_static_thread_num = 4;  // ≥2：跨连接 sink 唤醒需空闲 worker 即时 resume
    config->m_cfg_stack_size = 1024 * 256;  // 分派协程要跑 HandleFrame（大帧解析）
    bbt::coroutine::detail::Scheduler::GetInstance()->Start(
        bbt::coroutine::SCHE_START_OPT_SCHE_THREAD);

    TestHttpCancelNoopAndDisconnectDrivenEnd();
    TestStdioDrainDegradation();
    TestHttpS21NetworkAcceptance();
    TestS22Boundaries();

    std::fprintf(stderr, "SUMMARY checks=%d failures=%d\n", g_checks.load(),
                 g_failures.load());
    return g_failures.load() == 0 ? 0 : 1;
}

// 3-3 切片 contract 检查：request 栈外 owner / 唯一终态 / deadline / Cancel / Close /
// listener·fd·timer·queue 同步 teardown 与晚到空壳（specs R10/S21/S22）。
//
//   T1（R10/S17）：已接纳请求恰好一次终态；完成后晚到 cancel 无害（无第二消息）。
//   T2（R10）：已进入 callback 的协作式取消——cancel 命中 → handler 观察 abort 位
//        提前退出 → 提交门只消费空壳（被取消请求不再产生任何消息）。
//   T3（S22）：期限可观察拒绝（-32603 + data.kind="deadline_exceeded"，
//        architecture §3.3：不向 -32000..-32099 分配新码）；timeout ≠ 远端未执行。
//   T4（R10）：Close 竞争两窗口（callback-start 门之前/之后）；封口后新请求被拒。
//   T5（S4 core 面）：cancel 会话域路由——跨会话 cancel 零影响（P-A 反转：A 的
//        响应不被吞、订阅不被别人收口），同会话 cancel 照常（S17）；同会话同 id
//        重复在途只接纳一次、重复帧 -32600 拒绝（P-B 反转）。
//   T6（G4-a）：stdio Close 返回时间断言——writer 背压阻塞（stdout pipe 满）时
//        Close 在 close_drain_timeout 内有界返回（≤ deadline+ε），返回当刻
//        reader/writer 未汇合按实落快照（不伪造 joined），O_NONBLOCK 已还原。
//   T7（P2.1）：stdio close_drain_timeout < write_deadline 降级专测——reader 在途
//        （分派阻塞于业务 gate）时 Close 超时放弃汇合：O_NONBLOCK 当刻还原（fcntl
//        对象面）、reader_joined=false 快照、晚到完成项不落 stdout（队列空证据）。
//   T8（G4-b）：stdio destroy-while-loop-alive——Close 降级返回（reader 在途
//        未汇合）后立即析构 transport；写端 close + 短窗口后读 pipe 不出现
//        晚到字节（已被 sink 卸载/封队丢弃）。transport 亡后晚到 handler 完成
//        只消费空壳（G1 stdio 形态：读写协程持 shared_ptr Impl、sink 持
//        weak_ptr，晚到不触碰已释放对象）。
//   T9（P3/S22）：在途容量超限拒绝、不同 server 隔离、业务失败不自动重试、
//        注册复杂度上限。
//
// 确定性纪律：线程交错用显式 gate（mutex+cv）构造；期限/降级路径的真实等待只有
// 有界毫秒级窗口，且断言的是「窗口必然已过」的终态，不是时序运气。资源证据用
// 对象面（fcntl 标志、/proc fd 与 LISTEN 表、证据快照），不用 Sleep/计数归零/
// IsClosed 伪证物理释放。
//
// 无第三方测试框架依赖；经 ${PROJECT_SOURCE_DIR}/bbt 消费内部实现面（RequestOwner
// 终态计数），与仓内其他 impl 层测试同口径。

#include <fcntl.h>
#include <poll.h>
#include <sys/stat.h>
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
#include <bbt/infra/mcp/McpServer.hpp>
#include <bbt/infra/mcp/McpStdioTransport.hpp>
#include <bbt/infra/mcp/McpTypes.hpp>

#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>

#include "mcp/detail/Server.hpp"  // 内部面：RequestOwner 终态计数（仅本目标可见）

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

// modern 形态 cancel（供现代 _meta 会话使用；取消本身是通知，无回包）。当前
// stdio 路径不产生 cancel 帧（stdio 无 HTTP 跨连接路由面）——保留为 [[maybe_unused]]
// 已实现的帧构造原语，将来扩展（如经会话喂现代 cancel 帧）复用，不为其重造。
[[maybe_unused]] std::string ModernCancelFrame(const JsonValue& request_id) {
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

// pipe fd 的 flags（fcntl 对象面：Close 返回当刻 O_NONBLOCK 确已还原为 Start 前值）。
int FdFlags(int fd) { return ::fcntl(fd, F_GETFL); }

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
              code.value().AsInt64().value() == -32603);  // InternalError（v1 不用 -32000）
        auto data = error.value().Find("data");
        CHECK(data && data.value().Find("kind") &&
              data.value().Find("kind").value().AsString() &&
              data.value().Find("kind").value().AsString().value() == "deadline_exceeded");
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

// ---- T6：stdio Close 返回时间有界性（G4-a；P1/P5 形态移植）-------------------------
//
// G4(a)：Close 返回时间断言（≤ deadline+ε）。两个场景：
//   (i)  writer 背压阻塞（stdout pipe 满，客户端不读）→ Close 在
//        close_drain_timeout 内返回；返回当刻 reader/writer 可能未汇合，按实落
//        快照（不伪造 joined），O_NONBLOCK 当刻还原、证据快照字段自洽。
//   (ii) 并发/重复 Close：均等首次真实落定（幂等、各自有界返回），证据不二次
//        变化。
// 说明：原 T6 的「跨连接 cancel 静默 no-op + 断连驱动订阅终止」属 HTTP transport
// 面，本切片不携带（HTTP transport 不在本次 SCOPE）；会话域 cancel 路由的 core
// 断言已由 T5 覆盖。
void TestStdioCloseReturnTimeBound() {
    std::fprintf(stderr, "PROGRESS T6 stdio close return-time bound\n");
    int in_pipe[2];
    int out_pipe[2];
    CHECK(::pipe(in_pipe) == 0);
    CHECK(::pipe(out_pipe) == 0);
    // 缩小 pipe 容量加速背压（读端永不读）。
    ::fcntl(out_pipe[1], F_SETPIPE_SZ, 4096);
    const int saved_in = FdFlags(in_pipe[0]);
    const int saved_out = FdFlags(out_pipe[1]);

    ServerOptions options;
    options.request_timeout = std::chrono::milliseconds{10000};
    auto server = McpServer::Create(options).value();
    CHECK(server->AddResource(
        ResourceDefinition{"mem://slow", "slow", "", "text/plain"},
        [](const RequestContext&,
           const std::string&) -> bbt::infra::result<ResourceResult> {
            ResourceResult result;
            result.contents.push_back(
                ResourceContents::Text("mem://slow", "text/plain", "x").value());
            return bbt::infra::result<ResourceResult>::ok(std::move(result));
        }));
    CHECK(server->Start());

    // 预置大量请求帧：每帧一个响应，写端在客户端不读的前提下迅速进入背压。
    std::string reqs;
    for (int i = 0; i < 200; ++i)
        reqs += ModernRead(std::to_string(1000 + i), "mem://slow") + "\n";
    CHECK(::write(in_pipe[1], reqs.data(), reqs.size()) ==
          static_cast<ssize_t>(reqs.size()));

    bbt::infra::mcp::StdioOptions stdio;
    stdio.stdin_fd = in_pipe[0];
    stdio.stdout_fd = out_pipe[1];
    stdio.write_deadline = std::chrono::milliseconds{300};
    stdio.wait_granularity = std::chrono::milliseconds{50};
    stdio.close_drain_timeout = std::chrono::milliseconds{150};
    auto transport = McpStdioTransport::Start(server, stdio);
    CHECK(transport);
    // 给写协程一个进入背压的窗口（不依赖时序运气：进入与否都不影响有界性断言）。
    std::this_thread::sleep_for(std::chrono::milliseconds{300});

    // (i) Close 返回时间 ≤ close_drain_timeout + ε（G4-a）。
    const auto close_begin = std::chrono::steady_clock::now();
    transport.value()->Close();
    const auto close_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - close_begin).count();
    CHECK(close_ms <= 150 + 5000);  // deadline + ε（ε 覆盖 CI 调度噪声，不是无界）
    if (close_ms > 150 + 5000)
        std::fprintf(stderr, "T6-DIAG close_ms=%lld\n",
                     static_cast<long long>(close_ms));

    const auto evidence = transport.value()->LastCloseEvidence();
    // 返回当刻证据：输入/输出已封、sink 已卸、O_NONBLOCK 已还原、在册等待者为空。
    CHECK(evidence.input_sealed);
    CHECK(evidence.output_sealed);
    CHECK(!evidence.sink_mounted);
    CHECK(evidence.nonblock_restored);
    CHECK(evidence.waiters_empty);
    CHECK(FdFlags(in_pipe[0]) == saved_in);   // 对象面：fd 标志确已还原
    CHECK(FdFlags(out_pipe[1]) == saved_out);
    // reader/writer 在背压/在途下可能未汇合——如实断言不伪造（G2 降级口径）。
    if (!evidence.reader_joined || !evidence.writer_joined)
        std::fprintf(stderr, "T6-DIAG degraded close: reader=%d writer=%d\n",
                     evidence.reader_joined ? 1 : 0, evidence.writer_joined ? 1 : 0);

    // (ii) 并发/重复 Close：均等首次真实落定（G4-a 的幂等半边）。
    const auto again_begin = std::chrono::steady_clock::now();
    {
        std::thread a([&] { transport.value()->Close(); });
        std::thread b([&] { transport.value()->Close(); });
        a.join();
        b.join();
    }
    const auto again_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - again_begin).count();
    CHECK(again_ms <= 5000);  // 二次收口立即落定（幂等），不重复走完整 drain
    CHECK(transport.value()->IsClosed());
    CHECK(transport.value()->LastCloseEvidence().abandoned_frames ==
          evidence.abandoned_frames);  // 证据快照不变（首次收口当刻冻结）

    // 排空 out_pipe，防 pipe buffer 满影响本进程后续用例（fd 不属 transport）。
    ::fcntl(out_pipe[0], F_SETFL, O_NONBLOCK);
    char buf[8192];
    while (::read(out_pipe[0], buf, sizeof(buf)) > 0) {}
    ::close(in_pipe[0]); ::close(in_pipe[1]);
    ::close(out_pipe[0]); ::close(out_pipe[1]);
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

// ---- T8：stdio destroy-while-loop-alive（G4-b；G1 stdio 形态回归）------------------
//
// G4(b)：destroy-while-loop-alive 用例。Close 在 handler 在途时降级返回
//（reader_joined=false），随后立即析构 transport；写端 close + 短窗口后
// 读 pipe 验证无任何晚到字节（sink 已卸、封队丢弃）。G1 的 stdio 形态：
// 读写协程持 shared_ptr<Impl>、会话 sink 持 weak_ptr，transport 对象死亡后
// 晚到协程/完成项不再触碰已释放对象（与 HTTP 的裸 this 捕获缺陷对照——该
// 缺陷不随本切片携带）。
void TestStdioDestroyWhileLoopAlive() {
    std::fprintf(stderr, "PROGRESS T8 stdio destroy-while-loop-alive\n");
    int in_pipe[2];
    int out_pipe[2];
    CHECK(::pipe(in_pipe) == 0);
    CHECK(::pipe(out_pipe) == 0);

    auto gate = std::make_shared<Gate>();
    auto calls = std::make_shared<std::atomic<int>>(0);
    ServerOptions options;
    options.request_timeout = std::chrono::milliseconds{10000};
    auto server = McpServer::Create(options).value();
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

    // 预置在途请求：handler 阻塞于 gate（Close 时协程在途的前提）。
    const std::string request = ModernRead("50", "mem://slow") + "\n";
    CHECK(::write(in_pipe[1], request.data(), request.size()) ==
          static_cast<ssize_t>(request.size()));

    {
        bbt::infra::mcp::StdioOptions stdio;
        stdio.stdin_fd = in_pipe[0];
        stdio.stdout_fd = out_pipe[1];
        stdio.close_drain_timeout = std::chrono::milliseconds{150};
        auto transport = McpStdioTransport::Start(server, stdio);
        CHECK(transport);
        gate->WaitEntered();  // reader 协程已把帧分派进 handler（在途确定性成立）

        transport.value()->Close();  // 降级返回：reader 在途未汇合
        const auto evidence = transport.value()->LastCloseEvidence();
        CHECK(!evidence.reader_joined);    // 如实落快照（不伪造汇合）
        CHECK(evidence.output_sealed);
        CHECK(!evidence.sink_mounted);
        CHECK(evidence.nonblock_restored);
        // transport 作用域结束 → ~McpStdioTransport：Impl 由协程侧 shared_ptr
        // 持有（G1：读/写协程 [impl] 捕获、sink [weak] 捕获），对象死亡后晚到
        // 执行不再解引用已释放成员。
    }

    // 放行 handler → 晚到完成项只消费空壳（封队丢弃）；读端观察不到任何字节。
    gate->Release();
    ::close(in_pipe[1]);  // 写端关闭触发读侧 EOF（若 transport 内部仍写则先出字节）
    ::fcntl(out_pipe[0], F_SETFL, O_NONBLOCK);
    const auto quiet_deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds{2000};
    bool output_seen = false;
    while (std::chrono::steady_clock::now() < quiet_deadline) {
        pollfd p{out_pipe[0], POLLIN, 0};
        if (::poll(&p, 1, 20) > 0 && (p.revents & POLLIN)) {
            char buf[256];
            if (::read(out_pipe[0], buf, sizeof(buf)) > 0) { output_seen = true; break; }
        } else if (p.revents & (POLLHUP | POLLERR)) {
            break;  // EOF：写端全部关闭且无残留写者
        }
    }
    CHECK(!output_seen);  // 封队后晚到响应不落 wire
    CHECK(calls->load() == 1);
    ::close(in_pipe[0]);
    ::close(out_pipe[0]);
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

    TestStdioCloseReturnTimeBound();
    TestStdioDrainDegradation();
    TestStdioDestroyWhileLoopAlive();
    TestS22Boundaries();

    std::fprintf(stderr, "SUMMARY checks=%d failures=%d\n", g_checks.load(),
                 g_failures.load());
    return g_failures.load() == 0 ? 0 : 1;
}

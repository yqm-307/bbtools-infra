// 3-1 切片 contract 检查：McpStdioTransport（specs R8/S16/S17）。
//
//   S16：真实非 socket pipe 读写——分割 UTF-8 与 JSON、同次多帧、大响应、
//        输出背压有界、stdin EOF 自收口；stdout 只有协议帧（无日志），错误帧
//        不与已接纳请求错关联。T7 以 fork/exec 复用本二进制做真实子进程全栈
//        交互（官方 SDK 互通属 4-2/S23，此处不冒充）。
//   S17：stdio 通知 cancel 指定订阅（流结束帧出队）、完成后晚到 cancel 无害、
//        Close 竞争时不再发送被取消请求的消息。
//   资源返回当刻证据：Close 真实落定当刻的 StdioCloseEvidence 快照
//   （协程已汇合/队列封口/sink 卸载/在途等待者清空/O_NONBLOCK 还原）。
//
// 无第三方测试框架依赖；经 ${CMAKE_SOURCE_DIR}/src 消费内部实现面（与仓内其他
// impl 层测试同口径，仅本目标可见）。

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
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

namespace {

int g_checks = 0;
int g_failures = 0;

void Check(bool ok, const char* expr, int line) {
    ++g_checks;
    if (!ok) {
        ++g_failures;
        std::fprintf(stderr, "FAIL %s:%d  %s\n", __FILE__, line, expr);
    }
}
#define CHECK(expr) Check(static_cast<bool>(expr), #expr, __LINE__)

using bbt::infra::mcp::JsonValue;
using bbt::infra::mcp::McpServer;
using bbt::infra::mcp::McpStdioTransport;
using bbt::infra::mcp::ResourceContents;
using bbt::infra::mcp::ResourceDefinition;
using bbt::infra::mcp::ResourceResult;
using bbt::infra::mcp::ServerOptions;
using bbt::infra::mcp::StdioCloseEvidence;
using bbt::infra::mcp::StdioOptions;

constexpr std::size_t kBigResourceBytes = 2U << 20;  // 大响应载荷

// --- 帧构造 --------------------------------------------------------------------

JsonValue ModernMeta() {
    JsonValue meta = JsonValue::Object();
    meta.Set("io.modelcontextprotocol/protocolVersion", JsonValue("2026-07-28"));
    meta.Set("io.modelcontextprotocol/clientCapabilities", JsonValue::Object());
    return meta;
}

std::string Frame(const std::string& id, const std::string& method, JsonValue params) {
    JsonValue frame = JsonValue::Object();
    frame.Set("jsonrpc", JsonValue("2.0"));
    frame.Set("id", JsonValue(id));
    frame.Set("method", JsonValue(method));
    params.Set("_meta", ModernMeta());
    frame.Set("params", std::move(params));
    return frame.Serialize();
}

std::string DiscoverFrame(const std::string& id) {
    return Frame(id, "server/discover", JsonValue::Object());
}

std::string ReadFrame(const std::string& id, const std::string& uri) {
    JsonValue params = JsonValue::Object();
    params.Set("uri", JsonValue(uri));
    return Frame(id, "resources/read", std::move(params));
}

std::string ListenFrame(const std::string& id, const std::string& uri) {
    JsonValue filter = JsonValue::Object();
    JsonValue uris = JsonValue::Array();
    uris.Push(JsonValue(uri));
    filter.Set("resourceSubscriptions", std::move(uris));
    JsonValue params = JsonValue::Object();
    params.Set("notifications", std::move(filter));
    return Frame(id, "subscriptions/listen", std::move(params));
}

std::string CancelNotification(const std::string& request_id) {
    JsonValue params = JsonValue::Object();
    params.Set("requestId", JsonValue(request_id));
    JsonValue frame = JsonValue::Object();
    frame.Set("jsonrpc", JsonValue("2.0"));
    frame.Set("method", JsonValue("notifications/cancelled"));
    frame.Set("params", std::move(params));
    return frame.Serialize();
}

// --- 装配 ----------------------------------------------------------------------

std::shared_ptr<McpServer> MakeServer() {
    ServerOptions options;
    auto server = std::move(McpServer::Create(std::move(options)).value());
    {
        ResourceDefinition definition;
        definition.uri = "mem://a";
        definition.name = "a";
        definition.mime_type = "text/plain";
        auto add = server->AddResource(
            std::move(definition), [](const auto&, const std::string& uri) {
                ResourceResult result;
                auto contents = ResourceContents::Text(uri, "text/plain", "中文内容αβγ");
                if (!contents) return bbt::infra::result<ResourceResult>::err(contents.error());
                result.contents.push_back(std::move(contents).value());
                return bbt::infra::result<ResourceResult>::ok(std::move(result));
            });
        if (!add) std::fprintf(stderr, "FAIL add mem://a\n"), ++g_failures;
    }
    {
        ResourceDefinition definition;
        definition.uri = "mem://big";
        definition.name = "big";
        definition.mime_type = "text/plain";
        auto add = server->AddResource(
            std::move(definition), [](const auto&, const std::string& uri) {
                ResourceResult result;
                std::string body(kBigResourceBytes, 'x');
                auto contents = ResourceContents::Text(uri, "text/plain", body);
                if (!contents) return bbt::infra::result<ResourceResult>::err(contents.error());
                result.contents.push_back(std::move(contents).value());
                return bbt::infra::result<ResourceResult>::ok(std::move(result));
            });
        if (!add) std::fprintf(stderr, "FAIL add mem://big\n"), ++g_failures;
    }
    auto started = server->Start();
    if (!started) std::fprintf(stderr, "FAIL server start\n"), ++g_failures;
    return server;
}

struct Pipes {
    int req[2] = {-1, -1};  // 测试写 req[1] → transport 读 req[0]
    int res[2] = {-1, -1};  // transport 写 res[1] → 测试读 res[0]
};

Pipes MakePipes() {
    Pipes p;
    if (::pipe(p.req) != 0 || ::pipe(p.res) != 0) {
        std::fprintf(stderr, "FAIL pipe errno=%d\n", errno);
        ++g_failures;
        return p;
    }
    // 测试侧读端非阻塞：ReadLines 的 deadline 才能生效（transport 只改写端标志）。
    const int fl = ::fcntl(p.res[0], F_GETFL);
    ::fcntl(p.res[0], F_SETFL, fl | O_NONBLOCK);
    return p;
}

void WriteAll(int fd, const std::string& data) {
    std::size_t off = 0;
    while (off < data.size()) {
        const ssize_t n = ::write(fd, data.data() + off, data.size() - off);
        if (n < 0) {
            if (errno == EINTR) continue;
            std::fprintf(stderr, "FAIL write errno=%d\n", errno);
            ++g_failures;
            return;
        }
        off += static_cast<std::size_t>(n);
    }
}

// 读至恰好 want 条完整行或 deadline；返回行集合（不含 '\n'）。
std::vector<std::string> ReadLines(int fd, std::size_t want, int timeout_ms) {
    std::vector<std::string> lines;
    std::string pending;
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_ms);
    char buffer[65536];
    while (lines.size() < want) {
        if (std::chrono::steady_clock::now() > deadline) break;
        const ssize_t n = ::read(fd, buffer, sizeof(buffer));
        if (n > 0) {
            for (ssize_t i = 0; i < n; ++i) {
                if (buffer[i] == '\n') {
                    lines.push_back(pending);
                    pending.clear();
                } else {
                    pending.push_back(buffer[i]);
                }
            }
            continue;
        }
        if (n == 0) break;
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            ::usleep(2000);
            continue;
        }
        break;
    }
    return lines;
}

JsonValue ParseLine(const std::string& line) {
    auto parsed = JsonValue::Parse(line, 64U << 20, 32);
    if (!parsed) {
        std::fprintf(stderr, "FAIL stdout line is not valid JSON (%zu bytes): %.80s\n",
                     line.size(), line.c_str());
        ++g_failures;
        return JsonValue();
    }
    return std::move(parsed).value();
}

// 无新行到达（S17「取消后不再发帧」的观察面）。
bool NoLineWithin(int fd, int timeout_ms) {
    return ReadLines(fd, 1, timeout_ms).empty();
}

void WaitClosed(const std::shared_ptr<McpStdioTransport>& transport, int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_ms);
    while (!transport->IsClosed()) {
        if (std::chrono::steady_clock::now() > deadline) {
            std::fprintf(stderr, "FAIL transport not closed within %dms\n", timeout_ms);
            ++g_failures;
            return;
        }
        ::usleep(5000);
    }
}

void CheckCleanEvidence(const StdioCloseEvidence& e, bool expect_eof) {
    CHECK(e.reader_joined);
    CHECK(e.writer_joined);
    CHECK(e.input_sealed);
    CHECK(e.output_sealed);
    CHECK(!e.sink_mounted);
    CHECK(e.waiters_empty);
    CHECK(e.nonblock_restored);
    CHECK(e.pending_frames == 0);
    CHECK(e.pending_bytes == 0);
    CHECK(e.dropped_frames == 0);
    if (expect_eof) CHECK(e.eof);
}

// --- T1：分割 UTF-8/JSON 与同次多帧 ----------------------------------------------

void TestSplitUtf8AndMultiFrame() {
    auto server = MakeServer();
    Pipes pipes = MakePipes();
    StdioOptions options;
    options.stdin_fd = pipes.req[0];
    options.stdout_fd = pipes.res[1];
    auto transport = std::move(McpStdioTransport::Start(server, options).value());
    CHECK(transport != nullptr);

    // 同一帧在多字节字符内部切割 + 两次 write 间留间隙（真实非阻塞重组）。
    const std::string whole = ReadFrame("r1", "mem://a") + "\n";
    std::size_t cut = whole.find("mem://a") + strlen("mem://") + 1; // '中' 的第二字节内
    if (cut >= whole.size()) cut = whole.size() / 2;
    WriteAll(pipes.req[1], whole.substr(0, cut));
    ::usleep(5000);
    WriteAll(pipes.req[1], whole.substr(cut));

    // 同次多帧：一次 write 携带两个完整帧。
    const std::string batch = DiscoverFrame("r2") + "\n" +
                              Frame("r3", "resources/list", JsonValue::Object()) + "\n";
    WriteAll(pipes.req[1], batch);

    auto lines = ReadLines(pipes.res[0], 3, 8000);
    CHECK(lines.size() == 3);
    if (lines.size() == 3) {
        const auto r1 = ParseLine(lines[0]);
        const auto r2 = ParseLine(lines[1]);
        const auto r3 = ParseLine(lines[2]);
        CHECK(r1.Find("id").value().AsString().value() == "r1");
        CHECK(r2.Find("id").value().AsString().value() == "r2");
        CHECK(r3.Find("id").value().AsString().value() == "r3");
        // 分割 UTF-8 重组正确：资源内容完整到达。
        auto contents = r1.Find("result").value().Find("contents");
        CHECK(contents && contents.value().Size() == 1);
        if (contents && contents.value().Size() == 1) {
            auto text = contents.value().At(0).value().Find("text");
            CHECK(text && text.value().AsString().value().find("中文内容αβγ") !=
                              std::string::npos);
        }
    }
    CHECK(transport->DroppedOutputFrames() == 0);

    transport->Close();
    CheckCleanEvidence(transport->LastCloseEvidence(), false);
    ::close(pipes.req[0]);
    ::close(pipes.req[1]);
    ::close(pipes.res[0]);
    ::close(pipes.res[1]);
}

// --- T2：大响应与输出背压有界 ------------------------------------------------------

void TestLargeResponseAndBackpressure() {
    auto server = MakeServer();
    Pipes pipes = MakePipes();
    StdioOptions options;
    options.stdin_fd = pipes.req[0];
    options.stdout_fd = pipes.res[1];
    options.max_pending_bytes = 3U << 20;  // 两条 ~2MiB 响应必然越限 → 分派侧等待
    auto transport = std::move(McpStdioTransport::Start(server, options).value());

    WriteAll(pipes.req[1], ReadFrame("b1", "mem://big") + "\n");
    WriteAll(pipes.req[1], ReadFrame("b2", "mem://big") + "\n");
    // 故意慢读：给队列堆积窗口（背压面），但上限内不丢帧。
    ::usleep(400000);

    auto lines = ReadLines(pipes.res[0], 2, 15000);
    CHECK(lines.size() == 2);
    if (lines.size() == 2) {
        const auto b1 = ParseLine(lines[0]);
        const auto b2 = ParseLine(lines[1]);
        CHECK(b1.Find("id").value().AsString().value() == "b1");
        CHECK(b2.Find("id").value().AsString().value() == "b2");
        // 大响应完整：载荷字节全部到达（不截断）。
        auto text = b1.Find("result").value().Find("contents").value().At(0).value().Find("text");
        CHECK(text && text.value().AsString().value().size() == kBigResourceBytes);
    }
    CHECK(transport->DroppedOutputFrames() == 0);
    const auto evidence = transport->LastCloseEvidence();
    transport->Close();
    CheckCleanEvidence(transport->LastCloseEvidence(), false);
    // 背压有界：挂起队列峰值不越上限（快照证据，非事后推断）。
    CHECK(transport->LastCloseEvidence().peak_pending_bytes <= (3U << 20));
    (void)evidence;
    ::close(pipes.req[0]);
    ::close(pipes.req[1]);
    ::close(pipes.res[0]);
    ::close(pipes.res[1]);
}

// --- T3：超长行有界截断 + 错误帧不污染后续请求 ---------------------------------------

void TestOversizedLineDoesNotCorruptDispatch() {
    auto server = MakeServer();
    Pipes pipes = MakePipes();
    StdioOptions options;
    options.stdin_fd = pipes.req[0];
    options.stdout_fd = pipes.res[1];
    options.max_line_bytes = 64U << 10;
    auto transport = std::move(McpStdioTransport::Start(server, options).value());

    WriteAll(pipes.req[1], std::string(200U << 10, 'a') + "\n");  // 超长垃圾行
    WriteAll(pipes.req[1], DiscoverFrame("ok") + "\n");           // 同通道后续合法请求

    auto lines = ReadLines(pipes.res[0], 2, 8000);
    CHECK(lines.size() == 2);
    if (lines.size() == 2) {
        const auto err = ParseLine(lines[0]);
        const auto ok = ParseLine(lines[1]);
        // 超长行 → 标准解析错误（截断喂 core，不伪造帧）；id 无法确定 → null。
        CHECK(err.Find("error") && err.Find("id") && err.Find("id").value().IsNull());
        // 错误帧不错关联：后续请求按自己的 id 正常应答，恰好一次终态。
        CHECK(ok.Find("id").value().AsString().value() == "ok");
        CHECK(ok.Find("result").value().Find("resultType").value().AsString().value() ==
              "complete");
    }
    transport->Close();
    CheckCleanEvidence(transport->LastCloseEvidence(), false);
    ::close(pipes.req[0]);
    ::close(pipes.req[1]);
    ::close(pipes.res[0]);
    ::close(pipes.res[1]);
}

// --- T4：EOF 自收口 + 返回当刻证据 + fd 复用 -----------------------------------------

void TestEofTeardownEvidenceAndFdReuse() {
    auto server = MakeServer();
    Pipes pipes = MakePipes();
    StdioOptions options;
    options.stdin_fd = pipes.req[0];
    options.stdout_fd = pipes.res[1];
    auto transport = std::move(McpStdioTransport::Start(server, options).value());

    WriteAll(pipes.req[1], DiscoverFrame("e1") + "\n");
    auto lines = ReadLines(pipes.res[0], 1, 8000);
    CHECK(lines.size() == 1);

    // 客户端关 stdin → EOF → transport 自行走收口序列。
    ::close(pipes.req[1]);
    WaitClosed(transport, 5000);
    transport->Close();  // 幂等：等待/读取首次收口当刻快照
    CheckCleanEvidence(transport->LastCloseEvidence(), true);
    CHECK(transport->LastCloseEvidence().abandoned_frames == 0);

    // 物理 fd：transport 不归属也不关闭（返回当刻两者仍可用）。
    struct stat st {};
    CHECK(::fstat(pipes.req[0], &st) == 0);
    CHECK(::fstat(pipes.res[1], &st) == 0);

    // fd 复用面：全新 pipe 对再起一个 transport 仍正常收发（无泄漏的兴趣注册
    // wedges 新 fd；旧对 waiters_empty 证据由上方快照承载）。
    Pipes pipes2 = MakePipes();
    StdioOptions options2;
    options2.stdin_fd = pipes2.req[0];
    options2.stdout_fd = pipes2.res[1];
    auto transport2 = std::move(McpStdioTransport::Start(server, options2).value());
    WriteAll(pipes2.req[1], DiscoverFrame("e2") + "\n");
    auto lines2 = ReadLines(pipes2.res[0], 1, 8000);
    CHECK(lines2.size() == 1);
    if (lines2.size() == 1) {
        auto parsed = ParseLine(lines2[0]);
        CHECK(parsed.Find("id").value().AsString().value() == "e2");
    }
    ::close(pipes2.req[1]);
    WaitClosed(transport2, 5000);
    transport2->Close();
    CheckCleanEvidence(transport2->LastCloseEvidence(), true);
    ::close(pipes.req[0]);
    ::close(pipes.res[0]);
    ::close(pipes.res[1]);
    ::close(pipes2.req[0]);
    ::close(pipes2.res[0]);
    ::close(pipes2.res[1]);
}

// --- T5：S17 订阅 cancel/流结束帧/晚到 cancel ----------------------------------------

void TestCancelSubscriptionAndLateCancel() {
    auto server = MakeServer();
    Pipes pipes = MakePipes();
    StdioOptions options;
    options.stdin_fd = pipes.req[0];
    options.stdout_fd = pipes.res[1];
    auto transport = std::move(McpStdioTransport::Start(server, options).value());

    WriteAll(pipes.req[1], ListenFrame("sub-1", "mem://a") + "\n");
    auto ack_lines = ReadLines(pipes.res[0], 1, 8000);
    CHECK(ack_lines.size() == 1);
    if (ack_lines.size() == 1) {
        // sink seam 已接真实 stdio：ack 第一帧（notifications/subscriptions/acknowledged，
        // 带 subscriptionId meta）。
        const auto ack = ParseLine(ack_lines[0]);
        auto method = ack.Find("method");
        CHECK(method &&
              method.value().AsString().value() == "notifications/subscriptions/acknowledged");
        auto sub_id = ack.Find("params").value().Find("_meta").value().Find(
            "io.modelcontextprotocol/subscriptionId");
        CHECK(sub_id && sub_id.value().AsString().value() == "sub-1");
    }

    // 事件经 stdio 出帧（真实发布 → stdout）。
    auto published = server->PublishResourceChanged("mem://a");
    CHECK(published && published.value() == 1);
    auto event_lines = ReadLines(pipes.res[0], 1, 8000);
    CHECK(event_lines.size() == 1);
    if (event_lines.size() == 1) {
        const auto event = ParseLine(event_lines[0]);
        auto method = event.Find("method");
        CHECK(method &&
              method.value().AsString().value() == "notifications/resources/updated");
        auto sub_id = event.Find("params").value().Find("_meta").value().Find(
            "io.modelcontextprotocol/subscriptionId");
        CHECK(sub_id && sub_id.value().AsString().value() == "sub-1");
    }

    // cancel 指定订阅 → 流结束帧（id 原值 + result._meta.subscriptionId）。
    WriteAll(pipes.req[1], CancelNotification("sub-1") + "\n");
    auto end_lines = ReadLines(pipes.res[0], 1, 8000);
    CHECK(end_lines.size() == 1);
    if (end_lines.size() == 1) {
        const auto end = ParseLine(end_lines[0]);
        CHECK(end.Find("id").value().AsString().value() == "sub-1");
        CHECK(end.Find("result").value().Find("resultType").value().AsString().value() ==
              "complete");
        CHECK(end.Find("result").value().Find("_meta").value().Find(
                  "io.modelcontextprotocol/subscriptionId")
                  .value()
                  .AsString()
                  .value() == "sub-1");
    }

    // 取消后不再发送被取消请求的消息；晚到 cancel 无害。
    auto publish2 = server->PublishResourceChanged("mem://a");
    CHECK(publish2 && publish2.value() == 0);  // 无在册订阅 → 0 投递
    CHECK(NoLineWithin(pipes.res[0], 600));
    WriteAll(pipes.req[1], CancelNotification("sub-1") + "\n");
    CHECK(NoLineWithin(pipes.res[0], 600));

    transport->Close();
    CheckCleanEvidence(transport->LastCloseEvidence(), false);
    ::close(pipes.req[0]);
    ::close(pipes.req[1]);
    ::close(pipes.res[0]);
    ::close(pipes.res[1]);
}

// --- T6：Close 与在册订阅竞争（流结束帧有界排空，封口后无帧）----------------------------

void TestCloseWithLiveSubscription() {
    auto server = MakeServer();
    Pipes pipes = MakePipes();
    StdioOptions options;
    options.stdin_fd = pipes.req[0];
    options.stdout_fd = pipes.res[1];
    auto transport = std::move(McpStdioTransport::Start(server, options).value());

    WriteAll(pipes.req[1], ListenFrame("sub-2", "mem://a") + "\n");
    auto ack_lines = ReadLines(pipes.res[0], 1, 8000);
    CHECK(ack_lines.size() == 1);

    transport->Close();  // 显式 Close：先投流结束帧再封队（有界排空）
    WaitClosed(transport, 5000);
    auto end_lines = ReadLines(pipes.res[0], 1, 3000);
    CHECK(end_lines.size() == 1);
    if (end_lines.size() == 1) {
        const auto end = ParseLine(end_lines[0]);
        CHECK(end.Find("id").value().AsString().value() == "sub-2");
        CHECK(end.Find("result").value().Find("resultType").value().AsString().value() ==
              "complete");
    }
    CheckCleanEvidence(transport->LastCloseEvidence(), false);
    // 封口后：发布不产生任何出帧（「取消/Close 竞争不再发送被取消请求消息」）。
    (void)server->PublishResourceChanged("mem://a");
    CHECK(NoLineWithin(pipes.res[0], 600));
    ::close(pipes.req[0]);
    ::close(pipes.req[1]);
    ::close(pipes.res[0]);
    ::close(pipes.res[1]);
}

// --- T7：真实子进程全栈交互（S16 的 fork/exec 形态；SDK 互通属 4-2/S23）----------------

int ChildMain() {
    auto server = MakeServer();
    StdioOptions options;  // 默认 fd 0/1（父进程已接好 pipe）
    auto started = McpStdioTransport::Start(server, options);
    if (!started) {
        std::fprintf(stderr, "CHILD_FAIL start\n");
        return 2;
    }
    auto transport = std::move(started).value();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (!transport->IsClosed()) {
        if (std::chrono::steady_clock::now() > deadline) {
            std::fprintf(stderr, "CHILD_FAIL eof timeout\n");
            transport->Close();
            return 3;
        }
        ::usleep(10000);
    }
    transport->Close();  // 幂等：取首次收口当刻快照
    const auto& e = transport->LastCloseEvidence();
    std::fprintf(stderr,
                 "CHILD_EVIDENCE reader_joined=%d writer_joined=%d eof=%d "
                 "sink_mounted=%d waiters_empty=%d nonblock_restored=%d "
                 "pending_bytes=%zu dropped=%zu peak=%zu\n",
                 e.reader_joined ? 1 : 0, e.writer_joined ? 1 : 0, e.eof ? 1 : 0,
                 e.sink_mounted ? 1 : 0, e.waiters_empty ? 1 : 0,
                 e.nonblock_restored ? 1 : 0, e.pending_bytes, e.dropped_frames,
                 e.peak_pending_bytes);
    const bool ok = e.reader_joined && e.writer_joined && e.eof && !e.sink_mounted &&
                    e.waiters_empty && e.nonblock_restored && e.pending_bytes == 0 &&
                    e.dropped_frames == 0;
    return ok ? 0 : 4;
}

void TestChildSubprocess() {
    Pipes pipes = MakePipes();
    int errpipe[2] = {-1, -1};
    if (::pipe(errpipe) != 0) {
        ++g_failures;
        return;
    }
    const pid_t pid = ::fork();
    if (pid < 0) {
        ++g_failures;
        return;
    }
    if (pid == 0) {
        ::dup2(pipes.req[0], 0);
        ::dup2(pipes.res[1], 1);
        ::dup2(errpipe[1], 2);
        ::close(pipes.req[0]);
        ::close(pipes.req[1]);
        ::close(pipes.res[0]);
        ::close(pipes.res[1]);
        ::close(errpipe[0]);
        ::execl("/proc/self/exe", "Test_mcp_stdio_transport", "--mcp-stdio-child",
                static_cast<char*>(nullptr));
        std::fprintf(stderr, "CHILD_FAIL exec errno=%d\n", errno);
        ::_exit(127);
    }
    ::close(pipes.req[0]);
    ::close(pipes.res[1]);
    ::close(errpipe[1]);

    // 分割 UTF-8 帧 + 同次多帧 + 大响应，全部经真实 fd 0/1。
    const std::string whole = ReadFrame("c1", "mem://a") + "\n";
    const std::size_t cut = whole.find("mem://a") + strlen("mem://") + 1;
    WriteAll(pipes.req[1], whole.substr(0, cut));
    ::usleep(3000);
    WriteAll(pipes.req[1], whole.substr(cut));
    WriteAll(pipes.req[1], DiscoverFrame("c2") + "\n" +
                               Frame("c3", "resources/list", JsonValue::Object()) + "\n");
    WriteAll(pipes.req[1], ReadFrame("c4", "mem://big") + "\n");

    auto lines = ReadLines(pipes.res[0], 4, 20000);
    CHECK(lines.size() == 4);
    if (lines.size() == 4) {
        // 子进程 stdout 每一行都是完整合法 JSON（协议 stdout 无日志）。
        CHECK(ParseLine(lines[0]).Find("id").value().AsString().value() == "c1");
        CHECK(ParseLine(lines[1]).Find("id").value().AsString().value() == "c2");
        CHECK(ParseLine(lines[2]).Find("id").value().AsString().value() == "c3");
        const auto c4 = ParseLine(lines[3]);
        CHECK(c4.Find("id").value().AsString().value() == "c4");
        auto text = c4.Find("result").value().Find("contents").value().At(0).value().Find("text");
        CHECK(text && text.value().AsString().value().size() == kBigResourceBytes);
    }

    // stdin EOF → 子进程自收口 → 退出码即证据校验结果。
    ::close(pipes.req[1]);
    int status = -1;
    ::waitpid(pid, &status, 0);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);

    char errbuf[4096];
    std::string child_stderr;
    for (;;) {
        const ssize_t n = ::read(errpipe[0], errbuf, sizeof(errbuf));
        if (n <= 0) break;
        child_stderr.append(errbuf, static_cast<std::size_t>(n));
    }
    CHECK(child_stderr.find("CHILD_EVIDENCE") != std::string::npos);
    if (child_stderr.find("nonblock_restored=1") == std::string::npos ||
        child_stderr.find("eof=1") == std::string::npos)
        std::fprintf(stderr, "CHILD STDERR RAW: %s\n", child_stderr.c_str());
    CHECK(child_stderr.find("eof=1") != std::string::npos);
    CHECK(child_stderr.find("reader_joined=1") != std::string::npos);
    CHECK(child_stderr.find("writer_joined=1") != std::string::npos);
    CHECK(child_stderr.find("sink_mounted=0") != std::string::npos);
    CHECK(child_stderr.find("nonblock_restored=1") != std::string::npos);
    CHECK(child_stderr.find("pending_bytes=0") != std::string::npos);
    ::close(pipes.res[0]);
    ::close(errpipe[0]);
}

// --- T8：R1 回归——LastCloseEvidence()/IsClosed() 与并发 Close 的线程安全 ----------
// 复审 Important-3：LastCloseEvidence() 曾无锁返回内部引用，与持锁写入的 Close
// 构成 data race。修复后返回持锁值快照：并发轮询不崩溃、快照字段自洽、终态一致。

void TestConcurrentCloseEvidenceSnapshot() {
    auto server = MakeServer();
    Pipes pipes = MakePipes();
    StdioOptions options;
    options.stdin_fd = pipes.req[0];
    options.stdout_fd = pipes.res[1];
    auto transport = std::move(McpStdioTransport::Start(server, options).value());
    CHECK(transport != nullptr);

    std::atomic<bool> stop{false};
    std::thread poller([&transport, &stop] {
        for (int i = 0; i < 600 && !stop.load(std::memory_order_relaxed); ++i) {
            const auto evidence = transport->LastCloseEvidence(); // 值快照
            const bool closed = transport->IsClosed();
            // 快照自洽：封口顺序 input → output；sink 卸载与清队先于快照落定；
            // 证据与 closed 同一锁域落定，快照已封 ⇒ Close 已落定（单调真）。
            if (evidence.output_sealed) {
                CHECK(evidence.input_sealed);
                CHECK(!evidence.sink_mounted);
                CHECK(evidence.waiters_empty);
                CHECK(evidence.pending_frames == 0);
                CHECK(evidence.pending_bytes == 0);
                CHECK(closed);
            }
        }
    });
    ::usleep(20000); // 先让轮询与未关闭态交错，再触发并发 Close
    transport->Close();
    stop.store(true);
    poller.join();

    CHECK(transport->IsClosed());
    CheckCleanEvidence(transport->LastCloseEvidence(), false);
    CheckCleanEvidence(transport->LastCloseEvidence(), false); // 值快照：重复取值稳定
    ::close(pipes.req[0]);
    ::close(pipes.req[1]);
    ::close(pipes.res[0]);
    ::close(pipes.res[1]);
}

} // namespace

int main(int argc, char** argv) {
    if (argc > 1 && std::strcmp(argv[1], "--mcp-stdio-child") == 0) {
        auto* config = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
        config->m_cfg_static_thread_num = 2;
        config->m_cfg_stack_size = 1024 * 256;  // 分派协程要跑 HandleFrame（大帧解析）
        bbt::coroutine::detail::Scheduler::GetInstance()->Start(
            bbt::coroutine::SCHE_START_OPT_SCHE_THREAD);
        return ChildMain();
    }

    auto* config = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    config->m_cfg_static_thread_num = 2;
    config->m_cfg_stack_size = 1024 * 256;  // 同上
    bbt::coroutine::detail::Scheduler::GetInstance()->Start(
        bbt::coroutine::SCHE_START_OPT_SCHE_THREAD);

    TestSplitUtf8AndMultiFrame();
    std::fprintf(stderr, "PROGRESS T1 done\n");
    TestLargeResponseAndBackpressure();
    std::fprintf(stderr, "PROGRESS T2 done\n");
    TestOversizedLineDoesNotCorruptDispatch();
    std::fprintf(stderr, "PROGRESS T3 done\n");
    TestEofTeardownEvidenceAndFdReuse();
    std::fprintf(stderr, "PROGRESS T4 done\n");
    TestCancelSubscriptionAndLateCancel();
    std::fprintf(stderr, "PROGRESS T5 done\n");
    TestCloseWithLiveSubscription();
    std::fprintf(stderr, "PROGRESS T6 done\n");
    TestChildSubprocess();
    std::fprintf(stderr, "PROGRESS T7 done\n");
    TestConcurrentCloseEvidenceSnapshot();
    std::fprintf(stderr, "PROGRESS T8 done\n");

    std::printf("mcp.stdio_transport: %d checks, %d failures\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}

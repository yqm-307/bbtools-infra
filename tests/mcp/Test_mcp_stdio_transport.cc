/**
 * contract 检查：McpStdioTransport。
 *
 *   真实非 socket pipe 读写——分割 UTF-8 与 JSON、同次多帧、大响应、
 *        输出背压有界、stdin EOF 自收口；stdout 只有协议帧（无日志），错误帧
 *        不与已接纳请求错关联。T7 以 fork/exec 复用本二进制做真实子进程全栈
 *        交互（官方 SDK 互通属 4-2/S23，此处不冒充）。
 *   stdio 通知 cancel 指定订阅（流结束帧出队）、完成后晚到 cancel 无害、
 *        Close 竞争时不再发送被取消请求的消息。
 *   资源返回当刻证据：Close 真实落定当刻的 StdioCloseEvidence 快照
 *   （协程已汇合/队列封口/sink 卸载/在途等待者清空/O_NONBLOCK 还原）。
 *
 * 无第三方测试框架依赖；经 ${PROJECT_SOURCE_DIR}/bbt 消费内部实现面（与仓内其他
 * impl 层测试同口径，仅本目标可见）。
 */
#include <fcntl.h>
#include <sys/ioctl.h>
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
#include "mcp/detail/StdioTestAccess.hpp"

#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>

namespace {

int g_checks = 0;
int g_failures = 0;

void Check(bool ok, const char* expr, int line)
{
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

/**
 * 大响应载荷
 */
constexpr std::size_t kBigResourceBytes = 2U << 20;

/**
 * 帧构造
 */
JsonValue ModernMeta()
{
    JsonValue meta = JsonValue::Object();
    meta.Set("io.modelcontextprotocol/protocolVersion", JsonValue("2026-07-28"));
    meta.Set("io.modelcontextprotocol/clientCapabilities", JsonValue::Object());
    return meta;
}

std::string Frame(const std::string& id, const std::string& method, JsonValue params)
{
    JsonValue frame = JsonValue::Object();
    frame.Set("jsonrpc", JsonValue("2.0"));
    frame.Set("id", JsonValue(id));
    frame.Set("method", JsonValue(method));
    params.Set("_meta", ModernMeta());
    frame.Set("params", std::move(params));
    return frame.Serialize();
}

std::string DiscoverFrame(const std::string& id)
{
    return Frame(id, "server/discover", JsonValue::Object());
}

std::string ReadFrame(const std::string& id, const std::string& uri)
{
    JsonValue params = JsonValue::Object();
    params.Set("uri", JsonValue(uri));
    return Frame(id, "resources/read", std::move(params));
}

std::string ListenFrame(const std::string& id, const std::string& uri)
{
    JsonValue filter = JsonValue::Object();
    JsonValue uris = JsonValue::Array();
    uris.Push(JsonValue(uri));
    filter.Set("resourceSubscriptions", std::move(uris));
    JsonValue params = JsonValue::Object();
    params.Set("notifications", std::move(filter));
    return Frame(id, "subscriptions/listen", std::move(params));
}

std::string CancelNotification(const std::string& request_id)
{
    JsonValue params = JsonValue::Object();
    params.Set("requestId", JsonValue(request_id));
    JsonValue frame = JsonValue::Object();
    frame.Set("jsonrpc", JsonValue("2.0"));
    frame.Set("method", JsonValue("notifications/cancelled"));
    frame.Set("params", std::move(params));
    return frame.Serialize();
}

/**
 * 装配
 */
std::shared_ptr<McpServer> MakeServer()
{
    ServerOptions options;
    auto server = std::move(McpServer::Create(std::move(options)).value());
    {
        ResourceDefinition definition;
        definition.uri = "mem://a";
        definition.name = "a";
        definition.mime_type = "text/plain";
        auto add = server->AddResource(std::move(definition), [](const auto&, const std::string& uri) {
            ResourceResult result;
            auto contents = ResourceContents::Text(uri, "text/plain", "中文内容αβγ");
            if (!contents)
                return bbt::infra::result<ResourceResult>::err(contents.error());
            result.contents.push_back(std::move(contents).value());
            return bbt::infra::result<ResourceResult>::ok(std::move(result));
        });
        if (!add)
            std::fprintf(stderr, "FAIL add mem://a\n"), ++g_failures;
    }
    {
        ResourceDefinition definition;
        definition.uri = "mem://big";
        definition.name = "big";
        definition.mime_type = "text/plain";
        auto add = server->AddResource(std::move(definition), [](const auto&, const std::string& uri) {
            ResourceResult result;
            std::string body(kBigResourceBytes, 'x');
            auto contents = ResourceContents::Text(uri, "text/plain", body);
            if (!contents)
                return bbt::infra::result<ResourceResult>::err(contents.error());
            result.contents.push_back(std::move(contents).value());
            return bbt::infra::result<ResourceResult>::ok(std::move(result));
        });
        if (!add)
            std::fprintf(stderr, "FAIL add mem://big\n"), ++g_failures;
    }
    auto started = server->Start();
    if (!started)
        std::fprintf(stderr, "FAIL server start\n"), ++g_failures;
    return server;
}

struct Pipes {

    /**
     * 测试写 req[1] → transport 读 req[0]
     */
    int req[2] = {-1, -1};

    /**
     * transport 写 res[1] → 测试读 res[0]
     */
    int res[2] = {-1, -1};
};

Pipes MakePipes()
{
    Pipes p;
    if (::pipe(p.req) != 0 || ::pipe(p.res) != 0) {
        std::fprintf(stderr, "FAIL pipe errno=%d\n", errno);
        ++g_failures;
        return p;
    }

    /**
     * 测试侧读端非阻塞：ReadLines 的 deadline 才能生效（transport 只改写端标志）。
     */
    const int fl = ::fcntl(p.res[0], F_GETFL);
    ::fcntl(p.res[0], F_SETFL, fl | O_NONBLOCK);
    return p;
}

void WriteAll(int fd, const std::string& data)
{
    std::size_t off = 0;
    while (off < data.size()) {
        const ssize_t n = ::write(fd, data.data() + off, data.size() - off);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            std::fprintf(stderr, "FAIL write errno=%d\n", errno);
            ++g_failures;
            return;
        }
        off += static_cast<std::size_t>(n);
    }
}

/**
 * 读端可读字节数（FIONREAD）：只观测 pipe 占用，不消费数据，用于证明 writer
 * 已写满 pipe 并停在背压等待。
 */
int AvailableBytes(int fd)
{
    int bytes = 0;
    if (::ioctl(fd, FIONREAD, &bytes) != 0)
        return -1;
    return bytes;
}

/**
 * 读至恰好 want 条完整行或 deadline；返回行集合（不含 '\n'）。
 */
std::vector<std::string> ReadLines(int fd, std::size_t want, int timeout_ms)
{
    std::vector<std::string> lines;
    std::string pending;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    char buffer[65536];
    while (lines.size() < want) {
        if (std::chrono::steady_clock::now() > deadline)
            break;
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
        if (n == 0)
            break;
        if (errno == EINTR)
            continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            ::usleep(2000);
            continue;
        }
        break;
    }
    return lines;
}

JsonValue ParseLine(const std::string& line)
{
    auto parsed = JsonValue::Parse(line, 64U << 20, 32);
    if (!parsed) {
        std::fprintf(stderr, "FAIL stdout line is not valid JSON (%zu bytes): %.80s\n", line.size(), line.c_str());
        ++g_failures;
        return JsonValue();
    }
    return std::move(parsed).value();
}

/**
 * 无新行到达（「取消后不再发帧」的观察面）。
 */
bool NoLineWithin(int fd, int timeout_ms)
{
    return ReadLines(fd, 1, timeout_ms).empty();
}

void WaitClosed(const std::shared_ptr<McpStdioTransport>& transport, int timeout_ms)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (!transport->IsClosed()) {
        if (std::chrono::steady_clock::now() > deadline) {
            std::fprintf(stderr, "FAIL transport not closed within %dms\n", timeout_ms);
            ++g_failures;
            return;
        }
        ::usleep(5000);
    }
}

void CheckCleanEvidence(const StdioCloseEvidence& e, bool expect_eof)
{
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
    if (expect_eof)
        CHECK(e.eof);
}

/**
 * T1：分割 UTF-8/JSON 与同次多帧
 */
void TestSplitUtf8AndMultiFrame()
{
    auto server = MakeServer();
    Pipes pipes = MakePipes();
    StdioOptions options;
    options.stdin_fd = pipes.req[0];
    options.stdout_fd = pipes.res[1];
    auto transport = std::move(McpStdioTransport::Start(server, options).value());
    CHECK(transport != nullptr);

    /**
     * 同一帧在多字节字符内部切割 + 两次 write 间留间隙（真实非阻塞重组）。
     */
    const std::string whole = ReadFrame("r1", "mem://a") + "\n";

    /**
     * '中' 的第二字节内
     */
    std::size_t cut = whole.find("mem://a") + strlen("mem://") + 1;
    if (cut >= whole.size())
        cut = whole.size() / 2;
    WriteAll(pipes.req[1], whole.substr(0, cut));
    ::usleep(5000);
    WriteAll(pipes.req[1], whole.substr(cut));

    /**
     * 同次多帧：一次 write 携带两个完整帧。
     */
    const std::string batch = DiscoverFrame("r2") + "\n" + Frame("r3", "resources/list", JsonValue::Object()) + "\n";
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

        /**
         * 分割 UTF-8 重组正确：资源内容完整到达。
         */
        auto contents = r1.Find("result").value().Find("contents");
        CHECK(contents && contents.value().Size() == 1);
        if (contents && contents.value().Size() == 1) {
            auto text = contents.value().At(0).value().Find("text");
            CHECK(text && text.value().AsString().value().find("中文内容αβγ") != std::string::npos);
        }
    }
    CHECK(transport->DroppedOutputFrames() == 0);

    CHECK(transport->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Graceful, true, {}, std::chrono::steady_clock::now() + std::chrono::seconds{5}}));
    CheckCleanEvidence(transport->LastCloseEvidence(), false);
    ::close(pipes.req[0]);
    ::close(pipes.req[1]);
    ::close(pipes.res[0]);
    ::close(pipes.res[1]);
}

/**
 * T2：大响应与输出背压有界
 */
void TestLargeResponseAndBackpressure()
{
    auto server = MakeServer();
    Pipes pipes = MakePipes();
    StdioOptions options;
    options.stdin_fd = pipes.req[0];
    options.stdout_fd = pipes.res[1];

    /**
     * 两条 ~2MiB 响应必然越限 → 分派侧等待
     */
    options.max_pending_bytes = 3U << 20;
    auto transport = std::move(McpStdioTransport::Start(server, options).value());

    WriteAll(pipes.req[1], ReadFrame("b1", "mem://big") + "\n");
    WriteAll(pipes.req[1], ReadFrame("b2", "mem://big") + "\n");

    /**
     * 故意慢读：给队列堆积窗口（背压面），但上限内不丢帧。
     */
    ::usleep(400000);

    auto lines = ReadLines(pipes.res[0], 2, 15000);
    CHECK(lines.size() == 2);
    if (lines.size() == 2) {
        const auto b1 = ParseLine(lines[0]);
        const auto b2 = ParseLine(lines[1]);
        CHECK(b1.Find("id").value().AsString().value() == "b1");
        CHECK(b2.Find("id").value().AsString().value() == "b2");

        /**
         * 大响应完整：载荷字节全部到达（不截断）。
         */
        auto text = b1.Find("result").value().Find("contents").value().At(0).value().Find("text");
        CHECK(text && text.value().AsString().value().size() == kBigResourceBytes);
    }
    CHECK(transport->DroppedOutputFrames() == 0);
    const auto evidence = transport->LastCloseEvidence();
    CHECK(transport->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Graceful, true, {}, std::chrono::steady_clock::now() + std::chrono::seconds{5}}));
    CheckCleanEvidence(transport->LastCloseEvidence(), false);

    /**
     * 背压有界：挂起队列峰值不越上限（快照证据，非事后推断）。
     */
    CHECK(transport->LastCloseEvidence().peak_pending_bytes <= (3U << 20));
    (void)evidence;
    ::close(pipes.req[0]);
    ::close(pipes.req[1]);
    ::close(pipes.res[0]);
    ::close(pipes.res[1]);
}

/**
 * T3：超长行有界截断 + 错误帧不污染后续请求
 */
void TestOversizedLineDoesNotCorruptDispatch()
{
    auto server = MakeServer();
    Pipes pipes = MakePipes();
    StdioOptions options;
    options.stdin_fd = pipes.req[0];
    options.stdout_fd = pipes.res[1];
    options.max_line_bytes = 64U << 10;
    auto transport = std::move(McpStdioTransport::Start(server, options).value());

    /**
     * 超长垃圾行
     */
    WriteAll(pipes.req[1], std::string(200U << 10, 'a') + "\n");

    /**
     * 同通道后续合法请求
     */
    WriteAll(pipes.req[1], DiscoverFrame("ok") + "\n");

    auto lines = ReadLines(pipes.res[0], 2, 8000);
    CHECK(lines.size() == 2);
    if (lines.size() == 2) {
        const auto err = ParseLine(lines[0]);
        const auto ok = ParseLine(lines[1]);

        /**
         * 超长行 → 标准解析错误（截断喂 core，不伪造帧）；id 无法确定 → null。
         */
        CHECK(err.Find("error") && err.Find("id") && err.Find("id").value().IsNull());

        /**
         * 错误帧不错关联：后续请求按自己的 id 正常应答，恰好一次终态。
         */
        CHECK(ok.Find("id").value().AsString().value() == "ok");
        CHECK(ok.Find("result").value().Find("resultType").value().AsString().value() == "complete");
    }
    CHECK(transport->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Graceful, true, {}, std::chrono::steady_clock::now() + std::chrono::seconds{5}}));
    CheckCleanEvidence(transport->LastCloseEvidence(), false);
    ::close(pipes.req[0]);
    ::close(pipes.req[1]);
    ::close(pipes.res[0]);
    ::close(pipes.res[1]);
}

/**
 * T4：EOF 自收口 + 返回当刻证据 + fd 复用
 */
void TestEofTeardownEvidenceAndFdReuse()
{
    auto server = MakeServer();
    Pipes pipes = MakePipes();
    StdioOptions options;
    options.stdin_fd = pipes.req[0];
    options.stdout_fd = pipes.res[1];
    auto transport = std::move(McpStdioTransport::Start(server, options).value());

    WriteAll(pipes.req[1], DiscoverFrame("e1") + "\n");
    auto lines = ReadLines(pipes.res[0], 1, 8000);
    CHECK(lines.size() == 1);

    /**
     * 客户端关 stdin → EOF → transport 自行走收口序列。
     */
    ::close(pipes.req[1]);
    WaitClosed(transport, 5000);
    CHECK(transport->Close(
        bbt::infra::CloseOptions{

            /**
             * 幂等：等待/读取首次收口当刻快照
             */
            bbt::infra::CloseMode::Graceful,
            true,
            {},
            std::chrono::steady_clock::now() + std::chrono::seconds{5}}));
    CheckCleanEvidence(transport->LastCloseEvidence(), true);
    CHECK(transport->LastCloseEvidence().abandoned_frames == 0);

    /**
     * 物理 fd：transport 不归属也不关闭（返回当刻两者仍可用）。
     */
    struct stat st{};
    CHECK(::fstat(pipes.req[0], &st) == 0);
    CHECK(::fstat(pipes.res[1], &st) == 0);

    /**
     * fd 复用面：全新 pipe 对再起一个 transport 仍正常收发（无泄漏的兴趣注册
     * wedges 新 fd；旧对 waiters_empty 证据由上方快照承载）。
     */
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
    CHECK(transport2->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Graceful, true, {}, std::chrono::steady_clock::now() + std::chrono::seconds{5}}));
    CheckCleanEvidence(transport2->LastCloseEvidence(), true);
    ::close(pipes.req[0]);
    ::close(pipes.res[0]);
    ::close(pipes.res[1]);
    ::close(pipes2.req[0]);
    ::close(pipes2.res[0]);
    ::close(pipes2.res[1]);
}

/**
 * T5：订阅 cancel/流结束帧/晚到 cancel
 */
void TestCancelSubscriptionAndLateCancel()
{
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

        /**
         * sink seam 已接真实 stdio：ack 第一帧（notifications/subscriptions/acknowledged，
         * 带 subscriptionId meta）。
         */
        const auto ack = ParseLine(ack_lines[0]);
        auto method = ack.Find("method");
        CHECK(method && method.value().AsString().value() == "notifications/subscriptions/acknowledged");
        auto sub_id = ack.Find("params").value().Find("_meta").value().Find("io.modelcontextprotocol/subscriptionId");
        CHECK(sub_id && sub_id.value().AsString().value() == "sub-1");
    }

    /**
     * 事件经 stdio 出帧（真实发布 → stdout）。
     */
    auto published = server->PublishResourceChanged("mem://a");
    CHECK(published && published.value() == 1);
    auto event_lines = ReadLines(pipes.res[0], 1, 8000);
    CHECK(event_lines.size() == 1);
    if (event_lines.size() == 1) {
        const auto event = ParseLine(event_lines[0]);
        auto method = event.Find("method");
        CHECK(method && method.value().AsString().value() == "notifications/resources/updated");
        auto sub_id = event.Find("params").value().Find("_meta").value().Find("io.modelcontextprotocol/subscriptionId");
        CHECK(sub_id && sub_id.value().AsString().value() == "sub-1");
    }

    /**
     * cancel 指定订阅 → 流结束帧（id 原值 + result._meta.subscriptionId）。
     */
    WriteAll(pipes.req[1], CancelNotification("sub-1") + "\n");
    auto end_lines = ReadLines(pipes.res[0], 1, 8000);
    CHECK(end_lines.size() == 1);
    if (end_lines.size() == 1) {
        const auto end = ParseLine(end_lines[0]);
        CHECK(end.Find("id").value().AsString().value() == "sub-1");
        CHECK(end.Find("result").value().Find("resultType").value().AsString().value() == "complete");
        CHECK(
            end.Find("result")
                .value()
                .Find("_meta")
                .value()
                .Find("io.modelcontextprotocol/subscriptionId")
                .value()
                .AsString()
                .value()
            == "sub-1");
    }

    /**
     * 取消后不再发送被取消请求的消息；晚到 cancel 无害。
     */
    auto publish2 = server->PublishResourceChanged("mem://a");

    /**
     * 无在册订阅 → 0 投递
     */
    CHECK(publish2 && publish2.value() == 0);
    CHECK(NoLineWithin(pipes.res[0], 600));
    WriteAll(pipes.req[1], CancelNotification("sub-1") + "\n");
    CHECK(NoLineWithin(pipes.res[0], 600));

    CHECK(transport->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Graceful, true, {}, std::chrono::steady_clock::now() + std::chrono::seconds{5}}));
    CheckCleanEvidence(transport->LastCloseEvidence(), false);
    ::close(pipes.req[0]);
    ::close(pipes.req[1]);
    ::close(pipes.res[0]);
    ::close(pipes.res[1]);
}

/**
 * T6：Close 与在册订阅竞争（流结束帧有界排空，封口后无帧）
 */
void TestCloseWithLiveSubscription()
{
    auto server = MakeServer();
    Pipes pipes = MakePipes();
    StdioOptions options;
    options.stdin_fd = pipes.req[0];
    options.stdout_fd = pipes.res[1];
    auto transport = std::move(McpStdioTransport::Start(server, options).value());

    WriteAll(pipes.req[1], ListenFrame("sub-2", "mem://a") + "\n");
    auto ack_lines = ReadLines(pipes.res[0], 1, 8000);
    CHECK(ack_lines.size() == 1);

    CHECK(transport->Close(
        bbt::infra::CloseOptions{

            /**
             * 显式 Close：先投流结束帧再封队（有界排空）
             */
            bbt::infra::CloseMode::Graceful,
            true,
            {},
            std::chrono::steady_clock::now() + std::chrono::seconds{5}}));
    WaitClosed(transport, 5000);
    auto end_lines = ReadLines(pipes.res[0], 1, 3000);
    CHECK(end_lines.size() == 1);
    if (end_lines.size() == 1) {
        const auto end = ParseLine(end_lines[0]);
        CHECK(end.Find("id").value().AsString().value() == "sub-2");
        CHECK(end.Find("result").value().Find("resultType").value().AsString().value() == "complete");
    }
    CheckCleanEvidence(transport->LastCloseEvidence(), false);

    /**
     * 封口后：发布不产生任何出帧（「取消/Close 竞争不再发送被取消请求消息」）。
     */
    (void)server->PublishResourceChanged("mem://a");
    CHECK(NoLineWithin(pipes.res[0], 600));
    ::close(pipes.req[0]);
    ::close(pipes.req[1]);
    ::close(pipes.res[0]);
    ::close(pipes.res[1]);
}

/**
 * T7：真实子进程全栈交互（fork/exec 形态；SDK 互通不在此列）
 */
int ChildMain()
{
    auto server = MakeServer();

    /**
     * 默认 fd 0/1（父进程已接好 pipe）
     */
    StdioOptions options;
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
            CHECK(transport->Close(
                bbt::infra::CloseOptions{
                    bbt::infra::CloseMode::Graceful,
                    true,
                    {},
                    std::chrono::steady_clock::now() + std::chrono::seconds{5}}));
            return 3;
        }
        ::usleep(10000);
    }
    CHECK(transport->Close(
        bbt::infra::CloseOptions{

            /**
             * 幂等：取首次收口当刻快照
             */
            bbt::infra::CloseMode::Graceful,
            true,
            {},
            std::chrono::steady_clock::now() + std::chrono::seconds{5}}));
    const auto& e = transport->LastCloseEvidence();
    std::fprintf(
        stderr,
        "CHILD_EVIDENCE reader_joined=%d writer_joined=%d eof=%d "
        "sink_mounted=%d waiters_empty=%d nonblock_restored=%d "
        "pending_bytes=%zu dropped=%zu peak=%zu\n",
        e.reader_joined ? 1 : 0,
        e.writer_joined ? 1 : 0,
        e.eof ? 1 : 0,
        e.sink_mounted ? 1 : 0,
        e.waiters_empty ? 1 : 0,
        e.nonblock_restored ? 1 : 0,
        e.pending_bytes,
        e.dropped_frames,
        e.peak_pending_bytes);
    const bool ok = e.reader_joined && e.writer_joined && e.eof && !e.sink_mounted && e.waiters_empty
                    && e.nonblock_restored && e.pending_bytes == 0 && e.dropped_frames == 0;
    return ok ? 0 : 4;
}

void TestChildSubprocess()
{
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
        ::execl("/proc/self/exe", "Test_mcp_stdio_transport", "--mcp-stdio-child", static_cast<char*>(nullptr));
        std::fprintf(stderr, "CHILD_FAIL exec errno=%d\n", errno);
        ::_exit(127);
    }
    ::close(pipes.req[0]);
    ::close(pipes.res[1]);
    ::close(errpipe[1]);

    /**
     * 分割 UTF-8 帧 + 同次多帧 + 大响应，全部经真实 fd 0/1。
     */
    const std::string whole = ReadFrame("c1", "mem://a") + "\n";
    const std::size_t cut = whole.find("mem://a") + strlen("mem://") + 1;
    WriteAll(pipes.req[1], whole.substr(0, cut));
    ::usleep(3000);
    WriteAll(pipes.req[1], whole.substr(cut));
    WriteAll(pipes.req[1], DiscoverFrame("c2") + "\n" + Frame("c3", "resources/list", JsonValue::Object()) + "\n");
    WriteAll(pipes.req[1], ReadFrame("c4", "mem://big") + "\n");

    auto lines = ReadLines(pipes.res[0], 4, 20000);
    CHECK(lines.size() == 4);
    if (lines.size() == 4) {

        /**
         * 子进程 stdout 每一行都是完整合法 JSON（协议 stdout 无日志）。
         */
        CHECK(ParseLine(lines[0]).Find("id").value().AsString().value() == "c1");
        CHECK(ParseLine(lines[1]).Find("id").value().AsString().value() == "c2");
        CHECK(ParseLine(lines[2]).Find("id").value().AsString().value() == "c3");
        const auto c4 = ParseLine(lines[3]);
        CHECK(c4.Find("id").value().AsString().value() == "c4");
        auto text = c4.Find("result").value().Find("contents").value().At(0).value().Find("text");
        CHECK(text && text.value().AsString().value().size() == kBigResourceBytes);
    }

    /**
     * stdin EOF → 子进程自收口 → 退出码即证据校验结果。
     */
    ::close(pipes.req[1]);
    int status = -1;
    ::waitpid(pid, &status, 0);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);

    char errbuf[4096];
    std::string child_stderr;
    for (;;) {
        const ssize_t n = ::read(errpipe[0], errbuf, sizeof(errbuf));
        if (n <= 0)
            break;
        child_stderr.append(errbuf, static_cast<std::size_t>(n));
    }
    CHECK(child_stderr.find("CHILD_EVIDENCE") != std::string::npos);
    if (child_stderr.find("nonblock_restored=1") == std::string::npos
        || child_stderr.find("eof=1") == std::string::npos)
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

/**
 * T8：R1 回归——LastCloseEvidence()/IsClosed() 与并发 Close 的线程安全
 *
 * 复审 Important-3：LastCloseEvidence() 曾无锁返回内部引用，与持锁写入的 Close
 * 构成 data race。修复后返回持锁值快照：并发轮询不崩溃、快照字段自洽、终态一致。
 */
void TestConcurrentCloseEvidenceSnapshot()
{
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

            /**
             * 值快照
             */
            const auto evidence = transport->LastCloseEvidence();
            const bool closed = transport->IsClosed();

            /**
             * 快照自洽：封口顺序 input → output；sink 卸载与清队先于快照落定；
             * 证据与 closed 同一锁域落定，快照已封 ⇒ Close 已落定（单调真）。
             */
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

    /**
     * 先让轮询与未关闭态交错，再触发并发 Close
     */
    ::usleep(20000);
    CHECK(transport->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Graceful, true, {}, std::chrono::steady_clock::now() + std::chrono::seconds{5}}));
    stop.store(true);
    poller.join();

    CHECK(transport->IsClosed());
    CheckCleanEvidence(transport->LastCloseEvidence(), false);

    /**
     * 值快照：重复取值稳定
     */
    CheckCleanEvidence(transport->LastCloseEvidence(), false);
    ::close(pipes.req[0]);
    ::close(pipes.req[1]);
    ::close(pipes.res[0]);
    ::close(pipes.res[1]);
}

/**
 * T9/T10：Close 绝对期限与显式 Immediate 升级（stdout 背压 + 活跃订阅）
 *
 *
 * 夹具不靠 sleep 猜阶段：先经真实 stdout 收到订阅 ack（sink 已挂载），再以
 * 「队列字节数 == 单帧字节数 且 max_pending_bytes == 单帧字节数 + 1」证明
 * 队列已无空间容纳任何后续帧；单帧字节数由同形态 transport 的
 * PendingOutputBytes() 当刻读数校准。
 */
constexpr std::size_t kCalibrationMaxPending = 4U << 20;

std::size_t CalibrateBigFrameBytes()
{
    auto server = MakeServer();
    Pipes pipes = MakePipes();
    ::fcntl(pipes.res[1], F_SETPIPE_SZ, 4096);
    StdioOptions options;
    options.stdin_fd = pipes.req[0];
    options.stdout_fd = pipes.res[1];
    options.max_pending_bytes = kCalibrationMaxPending;
    auto started = McpStdioTransport::Start(server, options);
    CHECK(started);
    if (!started) {
        ::close(pipes.req[0]);
        ::close(pipes.req[1]);
        ::close(pipes.res[0]);
        ::close(pipes.res[1]);
        return 0;
    }
    auto transport = std::move(started).value();

    /**
     * 两条请求：writer 取走第一条后阻塞在 4KiB pipe，第二条留在队列 → 队列字节数
     * 即单帧字节数。id 长度与正式用例一致（响应大小只随 id 长度变化）。
     */
    WriteAll(pipes.req[1], ReadFrame("calib-0", "mem://big") + "\n");
    WriteAll(pipes.req[1], ReadFrame("calib-1", "mem://big") + "\n");
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    std::size_t frame_bytes = 0;
    while (std::chrono::steady_clock::now() < deadline) {
        frame_bytes = transport->PendingOutputBytes();
        if (frame_bytes > 0)
            break;
        ::usleep(2000);
    }
    CHECK(frame_bytes > (1U << 20));
    CHECK(transport->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + std::chrono::seconds{5}}));
    ::close(pipes.req[0]);
    ::close(pipes.req[1]);
    ::close(pipes.res[0]);
    ::close(pipes.res[1]);
    return frame_bytes;
}

struct FilledStdioRig {
    std::shared_ptr<McpServer> server;
    Pipes pipes;
    std::shared_ptr<McpStdioTransport> transport;
    std::size_t frame_bytes = 0;
};

/**
 * 活跃订阅 + 可判定的满队：max_pending = 单帧 + 1，队列驻留恰好一帧，剩余 1 字节
 * 容不下任何订阅结束帧；第三条大响应在 Enqueue 满队等待（reader 协程被证明阻塞）。
 */
FilledStdioRig MakeFilledRig(std::size_t frame_bytes)
{
    FilledStdioRig rig;
    rig.frame_bytes = frame_bytes;
    rig.server = MakeServer();
    rig.pipes = MakePipes();
    ::fcntl(rig.pipes.res[1], F_SETPIPE_SZ, 4096);
    StdioOptions options;
    options.stdin_fd = rig.pipes.req[0];
    options.stdout_fd = rig.pipes.res[1];
    options.max_pending_bytes = frame_bytes + 1;
    options.write_deadline = std::chrono::milliseconds{5000};
    options.wait_granularity = std::chrono::milliseconds{50};
    auto started = McpStdioTransport::Start(rig.server, options);
    CHECK(started);
    if (!started)
        return rig;
    rig.transport = std::move(started).value();

    WriteAll(rig.pipes.req[1], ListenFrame("sub-fill", "mem://a") + "\n");
    auto ack = ReadLines(rig.pipes.res[0], 1, 8000);
    CHECK(ack.size() == 1);
    if (ack.size() == 1) {
        const auto parsed = ParseLine(ack[0]);
        auto method = parsed.Find("method");
        CHECK(method && method.value().AsString().value() == "notifications/subscriptions/acknowledged");
    }

    /**
     * 活跃订阅的第二个证明：事件经 sink 投递计数为 1，且真实到达 stdout。
     */
    auto published = rig.server->PublishResourceChanged("mem://a");
    CHECK(published && published.value() == 1);
    auto event = ReadLines(rig.pipes.res[0], 1, 8000);
    CHECK(event.size() == 1);

    WriteAll(rig.pipes.req[1], ReadFrame("fill-00", "mem://big") + "\n");
    WriteAll(rig.pipes.req[1], ReadFrame("fill-01", "mem://big") + "\n");
    WriteAll(rig.pipes.req[1], ReadFrame("fill-02", "mem://big") + "\n");

    /**
     * 先证明 writer 已写满 stdout pipe 并阻塞（FIONREAD 只观测、不读取），再证明
     * 队列驻留一帧；两个条件连续 20ms 不变才说明驻留的是 writer 无法再取走的第二帧
     * （而不是恰在出队的首帧），此后队列字节数在 Close 期间不再变化。
     */
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    bool filled = false;
    while (std::chrono::steady_clock::now() < deadline && !filled) {
        if (AvailableBytes(rig.pipes.res[0]) >= 4096 && rig.transport->PendingOutputBytes() == rig.frame_bytes) {
            ::usleep(20000);
            filled = AvailableBytes(rig.pipes.res[0]) >= 4096 && rig.transport->PendingOutputBytes() == rig.frame_bytes;
        } else {
            ::usleep(2000);
        }
    }
    CHECK(filled);
    return rig;
}

void WaitEvidenceJoined(const std::shared_ptr<McpStdioTransport>& transport, int timeout_ms)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{timeout_ms};
    while (std::chrono::steady_clock::now() < deadline) {
        const auto evidence = transport->LastCloseEvidence();
        if (evidence.reader_joined && evidence.writer_joined)
            return;
        ::usleep(5000);
    }
}

void DrainPipe(int fd)
{
    const int saved = ::fcntl(fd, F_GETFL);
    ::fcntl(fd, F_SETFL, (saved < 0 ? 0 : saved) | O_NONBLOCK);
    char buffer[65536];
    while (::read(fd, buffer, sizeof(buffer)) > 0) {
    }
    if (saved >= 0)
        ::fcntl(fd, F_SETFL, saved);
}

void CloseRigFds(FilledStdioRig& rig)
{
    ::close(rig.pipes.req[0]);
    ::close(rig.pipes.req[1]);
    ::close(rig.pipes.res[0]);
    ::close(rig.pipes.res[1]);
}

/**
 * Graceful 封口阶段（EndAllSubscriptions → sink → Enqueue 满队等待）必须与
 * Close 共用同一绝对期限；到期只报 TimedOut 并保留队列/owner，不能沿
 * write_deadline 越过 Close 预算。
 */
void TestGracefulSealSharesCloseDeadline()
{
    const std::size_t frame_bytes = CalibrateBigFrameBytes();
    CHECK(frame_bytes > 0);
    if (frame_bytes == 0)
        return;
    FilledStdioRig rig = MakeFilledRig(frame_bytes);
    if (!rig.transport)
        return;

    std::optional<bbt::infra::result<void>> close_result;
    std::atomic_bool close_done{false};
    std::atomic<long long> close_elapsed_ms{0};
    bool registered = false;
    auto transport = rig.transport;
    bbt::coroutine::detail::Scheduler::GetInstance()->RegistCoroutineTask(
        [transport, &close_result, &close_done, &close_elapsed_ms] {
            const auto begin = std::chrono::steady_clock::now();
            close_result.emplace(transport->Close(
                bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::milliseconds{250})));
            close_elapsed_ms.store(
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin).count(),
                std::memory_order_release);
            close_done.store(true, std::memory_order_release);
        },
        registered);
    CHECK(registered);

    const auto wait_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{8};
    while (!close_done.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < wait_deadline)
        ::usleep(5000);
    CHECK(close_done.load(std::memory_order_acquire));
    if (!close_done.load(std::memory_order_acquire)) {
        CloseRigFds(rig);
        return;
    }

    /**
     * 明显小于 write_deadline(5s)：预算是 Close 的 250ms，不是逐帧 5s。
     */
    CHECK(close_elapsed_ms.load(std::memory_order_acquire) < 1500);
    CHECK(close_result && !static_cast<bool>(*close_result));
    if (close_result && !static_cast<bool>(*close_result))
        CHECK(close_result->error().code == bbt::infra::ErrorCode::TimedOut);

    /**
     * TimedOut 即保留 owner/责任、不发布物理收口；封口与首因已生效。
     */
    CHECK(rig.transport->IsClosed());
    CHECK(rig.transport->CloseReason().has_value());

    /**
     * Graceful 超时后显式 Immediate：必须执行强制放弃（封输出、清队计 abandoned）。
     */
    const auto immediate_begin = std::chrono::steady_clock::now();
    auto immediate = rig.transport->Close();
    const auto immediate_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - immediate_begin)
            .count();
    CHECK(immediate);
    CHECK(immediate_ms < 1000);
    CHECK(rig.transport->PendingOutputBytes() == 0);
    WaitEvidenceJoined(rig.transport, 3000);
    const auto evidence = rig.transport->LastCloseEvidence();
    CHECK(evidence.reader_joined);
    CHECK(evidence.writer_joined);
    CHECK(evidence.input_sealed);
    CHECK(evidence.output_sealed);
    CHECK(!evidence.sink_mounted);
    CHECK(evidence.waiters_empty);
    CHECK(evidence.nonblock_restored);
    CHECK(evidence.pending_frames == 0);
    CHECK(evidence.pending_bytes == 0);
    CHECK(evidence.abandoned_frames > 0);

    /**
     * 晚帧不写出：物理收口后 reader 已退出；排空既有字节再写新请求，stdout 无新帧。
     */
    DrainPipe(rig.pipes.res[0]);
    WriteAll(rig.pipes.req[1], DiscoverFrame("late") + "\n");
    CHECK(NoLineWithin(rig.pipes.res[0], 400));
    CloseRigFds(rig);
}

/**
 * 首次订阅终结只做一次，任何时刻的显式 Immediate 都独立执行强制放弃。
 * Graceful 仍在满队等待时另一次 Immediate 必须唤醒它并阻止晚帧继续入队/写出。
 */
void TestImmediateUpgradeWhileGracefulWaits()
{
    const std::size_t frame_bytes = CalibrateBigFrameBytes();
    CHECK(frame_bytes > 0);
    if (frame_bytes == 0)
        return;
    FilledStdioRig rig = MakeFilledRig(frame_bytes);
    if (!rig.transport)
        return;

    std::optional<bbt::infra::result<void>> graceful_result;
    std::atomic_bool graceful_done{false};
    bool registered = false;
    auto transport = rig.transport;
    bbt::coroutine::detail::Scheduler::GetInstance()->RegistCoroutineTask(
        [transport, &graceful_result, &graceful_done] {
            graceful_result.emplace(transport->Close(
                bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::seconds{5})));
            graceful_done.store(true, std::memory_order_release);
        },
        registered);
    CHECK(registered);

    /**
     * 进入目标阶段：已逻辑封口（IsClosed），且队列仍满 ⇒ EndAllSubscriptions 的
     * sink 入队尚未完成（Graceful 正阻塞在满队等待）。
     */
    const auto enter_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{3};
    while (std::chrono::steady_clock::now() < enter_deadline
           && !(transport->IsClosed() && transport->PendingOutputBytes() == frame_bytes))
        ::usleep(2000);
    CHECK(transport->IsClosed());
    CHECK(transport->PendingOutputBytes() == frame_bytes);
    CHECK(!graceful_done.load(std::memory_order_acquire));

    /**
     * 队列满 + IsClosed 已先证明；50ms 后仍未返回即确在等待
     */
    ::usleep(50000);
    CHECK(!graceful_done.load(std::memory_order_acquire));

    const auto upgrade_begin = std::chrono::steady_clock::now();

    /**
     * 默认 Immediate、非等待
     */
    auto immediate = transport->Close();
    const auto upgrade_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - upgrade_begin).count();
    CHECK(immediate);
    CHECK(upgrade_ms < 1000);

    /**
     * 队列被强制放弃，未等 writer
     */
    CHECK(transport->PendingOutputBytes() == 0);

    /**
     * 已有 Graceful 必须被唤醒并在自身预算内结束，而不是继续等到原 5s 队列预算。
     */
    const auto graceful_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{8};
    while (!graceful_done.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < graceful_deadline)
        ::usleep(5000);
    CHECK(graceful_done.load(std::memory_order_acquire));
    const auto graceful_wait_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - upgrade_begin).count();
    CHECK(graceful_wait_ms < 3000);
    CHECK(graceful_result.has_value());

    WaitEvidenceJoined(transport, 3000);
    const auto evidence = transport->LastCloseEvidence();
    CHECK(evidence.reader_joined);
    CHECK(evidence.writer_joined);
    CHECK(evidence.output_sealed);
    CHECK(!evidence.sink_mounted);
    CHECK(evidence.waiters_empty);
    CHECK(evidence.nonblock_restored);
    CHECK(evidence.pending_bytes == 0);

    /**
     * 队列残帧与在途帧明确计入放弃
     */
    CHECK(evidence.abandoned_frames > 0);

    DrainPipe(rig.pipes.res[0]);
    WriteAll(rig.pipes.req[1], DiscoverFrame("late-2") + "\n");
    CHECK(NoLineWithin(rig.pipes.res[0], 400));
    CloseRigFds(rig);
}

/**
 * 只有一帧且已部分写满真实 FIFO：没有队列条目也没有正在分派的请求，
 * Graceful 仍须保留 writer 的责任；解除背压后必须得到完整 JSON 行且无弃帧。
 */
FilledStdioRig MakeWritingRig()
{
    FilledStdioRig rig;
    rig.server = MakeServer();
    rig.pipes = MakePipes();
    CHECK(::fcntl(rig.pipes.res[1], F_SETPIPE_SZ, 4096) == 4096);
    StdioOptions options;
    options.stdin_fd = rig.pipes.req[0];
    options.stdout_fd = rig.pipes.res[1];
    options.max_pending_bytes = kCalibrationMaxPending;
    auto started = McpStdioTransport::Start(rig.server, options);
    CHECK(started);
    if (!started)
        return rig;
    rig.transport = std::move(started).value();
    WriteAll(rig.pipes.req[1], ReadFrame("writing", "mem://big") + "\n");
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (std::chrono::steady_clock::now() < deadline
           && !(
               AvailableBytes(rig.pipes.res[0]) == 4096
               && bbt::infra::mcp::StdioTestAccess::OnlyWritingFrame(*rig.transport)))
        std::this_thread::yield();
    CHECK(AvailableBytes(rig.pipes.res[0]) == 4096);
    CHECK(bbt::infra::mcp::StdioTestAccess::OnlyWritingFrame(*rig.transport));
    CHECK(rig.transport->PendingOutputBytes() == 0);
    return rig;
}

void TestGracefulPreservesWritingFrame()
{
    auto rig = MakeWritingRig();
    if (!rig.transport)
        return;
    auto timed = rig.transport->Close(
        bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::milliseconds{100}));
    CHECK(!timed && timed.error().code == bbt::infra::ErrorCode::TimedOut);
    CHECK(bbt::infra::mcp::StdioTestAccess::OnlyWritingFrame(*rig.transport));
    auto lines = ReadLines(rig.pipes.res[0], 1, 8000);
    CHECK(lines.size() == 1);
    if (lines.size() == 1) {
        auto parsed = ParseLine(lines[0]);
        CHECK(parsed.Find("id").value().AsString().value() == "writing");
        CHECK(lines[0].find(std::string(kBigResourceBytes, 'x')) != std::string::npos);
    }
    auto options = bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::seconds{5});
    options.wait_release = true;
    CHECK(rig.transport->Close(options));
    const auto evidence = rig.transport->LastCloseEvidence();
    CheckCleanEvidence(evidence, false);
    CHECK(evidence.abandoned_frames == 0);
    CloseRigFds(rig);
}

/**
 * 两次 settle 检查之间完成写终态，但扣住 reader/writer 之后的物理收尾。
 * 第二次检查命中不能绕开 wait_release；原期限到时必须报 TimedOut。
 */
void TestCoRecheckWaitsRelease()
{
    using Access = bbt::infra::mcp::StdioTestAccess;
    auto rig = MakeWritingRig();
    if (!rig.transport)
        return;
    auto recheck = Access::Pause(*rig.transport, Access::Stage::CloseRecheck);
    auto release = Access::Pause(*rig.transport, Access::Stage::Release);
    std::atomic<bool> done{false};
    std::optional<bbt::infra::result<void>> closed;
    bool submitted = false;
    bbt::coroutine::detail::Scheduler::GetInstance()->RegistCoroutineTask(
        [&] {
            auto options =
                bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::seconds{5});
            options.wait_release = true;
            closed.emplace(rig.transport->Close(options));
            done.store(true, std::memory_order_release);
        },
        submitted);
    CHECK(submitted);
    auto wait_flag = [](const std::atomic<bool>& flag) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{8};
        while (!flag.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline)
            std::this_thread::yield();
        return flag.load(std::memory_order_acquire);
    };
    CHECK(wait_flag(recheck->reached));
    auto lines = ReadLines(rig.pipes.res[0], 1, 8000);
    CHECK(lines.size() == 1);
    CHECK(wait_flag(release->reached));
    CHECK(!rig.transport->LastCloseEvidence().nonblock_restored);
    recheck->resume->Notify();
    CHECK(wait_flag(done));
    CHECK(closed && !*closed && closed->error().code == bbt::infra::ErrorCode::TimedOut);
    CHECK(!rig.transport->LastCloseEvidence().nonblock_restored);
    release->resume->Notify();
    auto options = bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::seconds{5});
    options.wait_release = true;
    CHECK(rig.transport->Close(options));
    CheckCleanEvidence(rig.transport->LastCloseEvidence(), false);
    CloseRigFds(rig);
}

// clang-format off
}
// clang-format on

int main(int argc, char** argv)
{
    if (argc > 1 && std::strcmp(argv[1], "--mcp-stdio-child") == 0) {
        auto* config = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
        config->m_cfg_static_thread_num = 2;

        /**
         * 分派协程要跑 HandleFrame（大帧解析）
         */
        config->m_cfg_stack_size = 1024 * 256;
        bbt::coroutine::detail::Scheduler::GetInstance()->Start(bbt::coroutine::SCHE_START_OPT_SCHE_THREAD);
        return ChildMain();
    }

    auto* config = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    config->m_cfg_static_thread_num = 2;

    /**
     * 同上
     */
    config->m_cfg_stack_size = 1024 * 256;
    bbt::coroutine::detail::Scheduler::GetInstance()->Start(bbt::coroutine::SCHE_START_OPT_SCHE_THREAD);

    TestGracefulPreservesWritingFrame();
    TestCoRecheckWaitsRelease();
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
    TestGracefulSealSharesCloseDeadline();
    std::fprintf(stderr, "PROGRESS T9 done\n");
    TestImmediateUpgradeWhileGracefulWaits();
    std::fprintf(stderr, "PROGRESS T10 done\n");

    std::printf("mcp.stdio_transport: %d checks, %d failures\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}

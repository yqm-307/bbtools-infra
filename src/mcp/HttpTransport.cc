// MCP Streamable HTTP transport（3-2 切片）：单 POST endpoint、真 SSE 流、
// 头-body 一致性/自定义 header 注解（Base64 sentinel）、Origin、请求级取消。
//
// 形态契约：design「核心与transport」「执行域和关闭」+ specs R9/S18-S20。
//   - fd/配额/Close 组合公开 TransportRuntime/CoTCP：listener 与 accepted TCP
//     全部受管（max_connections 含 listener；op 配额允许并发 accept/reader/
//     writer）；本文件不绕过 runtime 直创建 socket。
//   - framing 复用 Boost.Beast：请求用 http::request_parser 增量 put/put_eof
//     （头/体上限、chunked、截断语义全部由 Beast 承担）；响应序列化为自有
//     小字符串（状态行+头+体），写经受管 CoTCP::WriteAll。公共头不出现任何
//     Beast/Boost 类型。
//   - 不新增 I/O 线程、不建独立 io_context：accept 循环与每连接处理均为共享
//     scheduler 上的受管协程任务；挂起等待用 CoWaiter（fd 等待在 CoTCP 内）。
//   - 单 request/connection、Connection: close（ponytail 天花板：无 keep-alive；
//     升级条件是引入逐连接请求序号 + 幂等重放窗口，属后续切片，不在本包宣称）。
//   - 半关与取消（S20/RFC9112 §9.6）：完整 POST 后读 EOF = 客户端 SHUT_WR，
//     照常响应（不误取消）；正文未完整即 EOF = 截断失败（明确 400，不 part 响应）；
//     SSE 流只以「写失败/流结束响应/Close」收口，读 EOF 不停流；无法立即确认的
//     全关用心跳注释写（有限探测）+ 写期限有界收口。写失败/Close → 订阅取消
//     （EndAllSubscriptions）且封队后不再写任何字节。
//
// 头校验顺序（ADR 0010 决策 12；与 specs「_meta→headers→version→dispatch」的
// 对应关系）：HTTP 门禁（method/path/host/origin/content-type/accept）→ JSON-RPC
// 帧解析/信封 → modern `_meta` 形状 → 注解头结构/一致性 → 分派。
// 不支持的 revision 保留 S7 的 -32022 判定，不改旧 unsupported/version 断言。
// legacy 首次 initialize 可用 body 中受支持的 legacy revision 选择 era；后续仍需版本头。

#include <bbt/infra/mcp/McpHttpTransport.hpp>

#include "mcp/Server.hpp"

#include <bbt/infra/CoTCP.hpp>
#include <bbt/infra/TransportRuntime.hpp>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <boost/asio/buffer.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http.hpp>

#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/sync/WaitTypes.hpp>

namespace bbt::infra::mcp {
namespace {

using bbt::coroutine::Deadline;  // WaitTypes.hpp：steady_clock 时间点
using bbt::coroutine::WaitOptions;
using bbt::coroutine::WaitStatus;
using bbt::coroutine::sync::CoWaiter;

using bbt::infra::tcp::CoTCP;
using bbt::infra::tcp::CoTCPListener;

constexpr std::size_t kReadChunkBytes = 64U << 10;   // 读缓冲（堆上自有，不借 Beast 持久持有）
constexpr std::string_view kMethodListen = "subscriptions/listen";
// 自定义 header 注解的规范 Base64 sentinel（ADR 0010 决策 13）：
// 值形如 `=?base64?<标准 Base64>?=`，编码原值的 UTF-8 字节。
// 原值自身同时含此前缀/后缀时也必须编码，以免被当作 sentinel 解码。
constexpr std::string_view kSentinelPrefix = "=?base64?";
constexpr std::string_view kSentinelSuffix = "?=";

Deadline DeadlineAfter(std::chrono::milliseconds delta) noexcept {
    return std::chrono::steady_clock::now() + delta;
}

std::string ToLowerAscii(std::string_view s) {
    std::string out(s);
    for (auto& c : out) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return out;
}

bool IsLoopbackHost(const std::string& host) {
    return host == "127.0.0.1" || host == "::1" || host == "localhost";
}

// ---- header 注解值校验（缺失/重复之外的取值面：ASCII/sentinel/Base64/CTL）--------

bool IsStandardBase64(std::string_view s) {
    if (s.empty() || s.size() % 4 != 0) return false;
    std::size_t pad = 0;
    for (std::size_t i = 0; i < s.size(); ++i) {
        const char c = s[i];
        const bool alpha = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                           (c >= '0' && c <= '9') || c == '+' || c == '/';
        if (alpha && pad != 0) return false;
        if (!alpha) {
            if (c != '=') return false;
            if (i + 2 < s.size()) return false;  // '=' 只允许作尾部填充
            ++pad;
        }
    }
    return pad <= 2;
}

std::optional<std::string> DecodeSentinel(std::string_view v) {
    // 完整 sentinel 结构 + 合法标准 Base64 + 解码后为合法 UTF-8。
    if (v.size() < kSentinelPrefix.size() + kSentinelSuffix.size() + 1) return std::nullopt;
    if (v.substr(0, kSentinelPrefix.size()) != kSentinelPrefix) return std::nullopt;
    if (v.substr(v.size() - kSentinelSuffix.size()) != kSentinelSuffix) return std::nullopt;
    const std::string_view b64 =
        v.substr(kSentinelPrefix.size(), v.size() - kSentinelPrefix.size() - kSentinelSuffix.size());
    if (!IsStandardBase64(b64)) return std::nullopt;
    std::string out;
    out.reserve(b64.size() / 4 * 3);
    unsigned accum = 0;
    int bits = 0;
    for (const char c : b64) {
        if (c == '=') break;
        int d;
        if (c >= 'A' && c <= 'Z') d = c - 'A';
        else if (c >= 'a' && c <= 'z') d = c - 'a' + 26;
        else if (c >= '0' && c <= '9') d = c - '0' + 52;
        else if (c == '+') d = 62;
        else if (c == '/') d = 63;
        else return std::nullopt;
        accum = (accum << 6) | d;
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out.push_back(static_cast<char>((accum >> bits) & 0xFF));
        }
    }
    if (!bbt::infra::detail::IsValidUtf8(out)) return std::nullopt;
    return out;
}

// 合法注解值：非空；raw 形态必须全部为可打印 ASCII（0x20..0x7E，无 CTL/非 ASCII）；
// 仅同时含规范前缀/后缀的完整 sentinel 模式进行解码（只有前缀仍是普通 ASCII）。
// 返回解码后的原值（raw 原样、sentinel 解码）。
std::optional<std::string> DecodeAnnotationValue(const std::string& raw) {
    if (raw.empty()) return std::nullopt;
    if (raw.size() >= kSentinelPrefix.size() + kSentinelSuffix.size() &&
        raw.compare(0, kSentinelPrefix.size(), kSentinelPrefix.data(), kSentinelPrefix.size()) == 0 &&
        raw.compare(raw.size() - kSentinelSuffix.size(), kSentinelSuffix.size(), kSentinelSuffix) == 0)
        return DecodeSentinel(raw);
    for (const unsigned char c : raw) {
        if (c < 0x20 || c > 0x7E) return std::nullopt;  // CTL/DEL/非 ASCII → 拒
    }
    return raw;
}

// ---- HTTP 响应序列化（自有小串；体内容为库序列化的 JSON/SSE 帧，无注入面）--------

std::string MakeHttpResponse(unsigned status, std::string_view content_type,
                             std::string_view body) {
    const char* reason = "OK";
    switch (status) {
        case 202: reason = "Accepted"; break;
        case 400: reason = "Bad Request"; break;
        case 403: reason = "Forbidden"; break;
        case 404: reason = "Not Found"; break;
        case 405: reason = "Method Not Allowed"; break;
        case 406: reason = "Not Acceptable"; break;
        case 408: reason = "Request Timeout"; break;
        case 415: reason = "Unsupported Media Type"; break;
        case 503: reason = "Service Unavailable"; break;
        default: break;
    }
    std::string out;
    out.reserve(160 + body.size());
    out += "HTTP/1.1 " + std::to_string(status) + " " + reason + "\r\n";
    if (!content_type.empty()) {
        out += "Content-Type: ";
        out += content_type;
        out += "\r\n";
    }
    out += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    out += "Connection: close\r\n";
    if (status == 405) out += "Allow: POST\r\n";
    out += "\r\n";
    out += body;
    return out;
}

// 区分「流结束响应」（JSON-RPC response，含 result/error）与通知/ack。帧由本库
// 序列化器产生，Parse 失败按普通事件处理（不终止流）。
bool IsJsonRpcResponseFrame(const std::string& frame) {
    auto parsed = JsonValue::Parse(frame, 1U << 20, 8);
    if (!parsed || !parsed.value().IsObject()) return false;
    return static_cast<bool>(parsed.value().Find("result")) ||
           static_cast<bool>(parsed.value().Find("error"));
}

// ---- 每连接共享块（sink 出队面与连接协程共用）----------------------------------------

struct HttpConnBlock {
    std::shared_ptr<CoTCP> conn;
    mutable std::mutex mtx;
    std::deque<std::string> queue;        // 完整协议帧（JSON 单行）
    std::size_t bytes = 0;
    std::vector<CoWaiter::SPtr> waiters;  // 空队等待（入队/封口唤醒）
    bool output_sealed = false;
};

} // namespace

struct McpHttpTransport::Impl {
    Impl(std::shared_ptr<McpServer> server, HttpOptions options)
        : m_server(std::move(server)), m_options(options) {}

    // ---- 状态 -------------------------------------------------------------------
    std::shared_ptr<McpServer> m_server;
    HttpOptions m_options;
    std::shared_ptr<TransportRuntime> m_runtime;
    std::shared_ptr<CoTCPListener> m_listener;
    SocketAddress m_local;

    // 连接登记（Close 枚举物理关闭用；loop 退出注销）。
    mutable std::mutex m_conn_mtx;
    std::map<const HttpConnBlock*, std::shared_ptr<HttpConnBlock>> m_conns;

    // 跨连接聚合计数（Close 快照与测试观测面；连接块析构后数值仍有效）。
    std::atomic<std::uint64_t> m_dropped_total{0};
    std::atomic<std::uint64_t> m_abandoned_total{0};
    std::atomic<std::uint64_t> m_closed_conns{0};
    std::atomic<bool> m_truncated_seen{false};
    std::atomic<bool> m_write_error_seen{false};

    // 生命周期域（锁序：m_life_mtx → m_conn_mtx / block->mtx）。
    mutable std::mutex m_life_mtx;
    std::condition_variable m_life_cv;
    bool m_teardown_started = false;
    bool m_closed_done = false;
    bool m_accept_exited = false;
    std::size_t m_live_loops = 0;   // accept + 连接协程在途数
    HttpCloseEvidence m_evidence;

    // ---- 有界输出（订阅 ack/事件/流结束帧出口；形态与 stdio 同口径）--------------
    // 满队：协程上下文内以 write_deadline 为预算等待排空，超限丢弃并计数；
    // 非协程上下文（业务线程 Publish）立即丢弃并计数，绝不阻塞调用方。
    bool Enqueue(const std::shared_ptr<HttpConnBlock>& block, std::string frame) {
        const auto deadline = DeadlineAfter(m_options.write_deadline);
        for (;;) {
            bool pushed = false;
            {
                std::lock_guard<std::mutex> lock(block->mtx);
                if (block->output_sealed) {
                    ++m_abandoned_total;
                    return false;
                }
                if (block->bytes + frame.size() <= m_options.max_pending_bytes) {
                    block->bytes += frame.size();
                    block->queue.push_back(std::move(frame));
                    pushed = true;
                }
            }
            if (pushed) {
                WakeBlock(block);
                return true;
            }
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - std::chrono::steady_clock::now());
            if (remaining <= std::chrono::milliseconds{0}) {
                std::lock_guard<std::mutex> lock(block->mtx);
                if (block->output_sealed) {
                    ++m_abandoned_total;
                    return false;
                }
                ++m_dropped_total;
                return false;
            }
            auto waiter = CoWaiter::Create();
            {
                std::lock_guard<std::mutex> lock(block->mtx);
                if (block->output_sealed) {
                    ++m_abandoned_total;
                    return false;
                }
                block->waiters.push_back(waiter);
            }
            WaitOptions options;
            options.deadline = remaining > std::chrono::milliseconds{0}
                                   ? DeadlineAfter(remaining)
                                   : deadline;
            const auto status = waiter->Wait(options);
            ForgetWaiter(*block, waiter);
            if (status == WaitStatus::InvalidContext) {
                // 非协程上下文：不等待，立即丢弃（业务线程 Publish 从不阻塞）。
                std::lock_guard<std::mutex> lock(block->mtx);
                if (block->output_sealed) { ++m_abandoned_total; return false; }
                ++m_dropped_total;
                return false;
            }
            // Completed/Cancelled/TimedOut 都回循环复查。
        }
    }

    static void ForgetWaiter(HttpConnBlock& block, const CoWaiter::SPtr& waiter) {
        std::lock_guard<std::mutex> lock(block.mtx);
        for (auto it = block.waiters.begin(); it != block.waiters.end(); ++it) {
            if (*it == waiter) {
                block.waiters.erase(it);
                return;
            }
        }
    }

    // 锁内 swap、锁外 Notify（PENDING 路径不丢唤醒）。
    static void WakeBlock(const std::shared_ptr<HttpConnBlock>& block) {
        std::vector<CoWaiter::SPtr> woken;
        {
            std::lock_guard<std::mutex> lock(block->mtx);
            woken.swap(block->waiters);
        }
        for (auto& waiter : woken) waiter->Notify();
    }

    void SealBlockOutput(const std::shared_ptr<HttpConnBlock>& block) {
        {
            std::lock_guard<std::mutex> lock(block->mtx);
            block->output_sealed = true;
            // 封口时队列残帧计入 abandoned（与 stdio 同口径）。
            m_abandoned_total += block->queue.size();
            block->queue.clear();
            block->bytes = 0;
        }
        WakeBlock(block);
    }

    bool BlockSealed(const std::shared_ptr<HttpConnBlock>& block) {
        std::lock_guard<std::mutex> lock(block->mtx);
        return block->output_sealed;
    }

    // 出队：有 deadline（心跳粒度）；空超时返回 nullopt（封口由 BlockSealed 区分）。
    std::optional<std::string> Dequeue(const std::shared_ptr<HttpConnBlock>& block,
                                       std::chrono::milliseconds budget) {
        auto waiter = CoWaiter::Create();
        {
            std::lock_guard<std::mutex> lock(block->mtx);
            if (!block->queue.empty()) {
                auto frame = std::move(block->queue.front());
                block->queue.pop_front();
                block->bytes -= frame.size();
                return frame;
            }
            if (block->output_sealed) return std::nullopt;
            block->waiters.push_back(waiter);
        }
        WaitOptions options;
        options.deadline = DeadlineAfter(budget);
        waiter->Wait(options);
        ForgetWaiter(*block, waiter);
        std::lock_guard<std::mutex> lock(block->mtx);
        if (block->queue.empty()) return std::nullopt;
        auto frame = std::move(block->queue.front());
        block->queue.pop_front();
        block->bytes -= frame.size();
        return frame;
    }

    // ---- 写出（写失败 → m_write_error_seen，调用方据此取消）-----------------------
    bool WriteAllChecked(const std::shared_ptr<CoTCP>& conn, std::string_view data) {
        if (data.empty()) return true;
        CallOptions options;
        options.deadline = DeadlineAfter(m_options.write_deadline);
        const auto written =
            conn->WriteAll(ConstBytes{data.data(), data.size()}, options);
        if (!written) {
            m_write_error_seen = true;
            return false;
        }
        return true;
    }

    // ---- 连接登记 -----------------------------------------------------------------
    void RegisterConn(const std::shared_ptr<HttpConnBlock>& block) {
        std::lock_guard<std::mutex> lock(m_conn_mtx);
        m_conns.emplace(block.get(), block);
    }
    void UnregisterConn(const HttpConnBlock* block) {
        std::lock_guard<std::mutex> lock(m_conn_mtx);
        m_conns.erase(block);
    }
    std::vector<std::shared_ptr<HttpConnBlock>> SnapshotConns() {
        std::vector<std::shared_ptr<HttpConnBlock>> out;
        std::lock_guard<std::mutex> lock(m_conn_mtx);
        out.reserve(m_conns.size());
        for (auto& entry : m_conns) out.push_back(entry.second);
        return out;
    }

    // ---- 协程生命周期 -------------------------------------------------------------
    void LoopEnter() {
        std::lock_guard<std::mutex> lock(m_life_mtx);
        ++m_live_loops;
    }
    void LoopExit() {
        std::lock_guard<std::mutex> lock(m_life_mtx);
        --m_live_loops;
        m_life_cv.notify_all();
    }

    // ---- accept 循环（受管协程；listener 挂起 Accept 由 Close 唤醒）----------------
    void AcceptLoop() {
        LoopEnter();  // 与 LoopExit 配对（Close 的汇合谓词依赖成对计数）
        for (;;) {
            {
                std::lock_guard<std::mutex> lock(m_life_mtx);
                if (m_teardown_started) break;
            }
            CallOptions options;
            options.deadline = DeadlineAfter(std::chrono::milliseconds{250});  // 封口兜底粒度
            auto accepted = m_listener->Accept(options);
            if (!accepted) {
                const auto& error = accepted.error();
                if (error.code == ErrorCode::Closed) break;
                if (error.code == ErrorCode::Overloaded) {
                    // 连接容量满：短暂退避后重试（pending 连接留在内核 backlog，
                    // 容量释放后照常接纳——不 busy loop）。
                    auto waiter = CoWaiter::Create();
                    WaitOptions backoff;
                    backoff.deadline = DeadlineAfter(std::chrono::milliseconds{100});
                    waiter->Wait(backoff);
                    continue;
                }
                if (error.code == ErrorCode::TimedOut) continue;
                std::fprintf(stderr, "bbt.mcp.http: accept failed (%d)\n",
                             static_cast<int>(error.code));
                break;
            }
            auto block = std::make_shared<HttpConnBlock>();
            block->conn = std::move(accepted).value();
            RegisterConn(block);
            bool started = false;
            bbt::coroutine::detail::Scheduler::GetInstance()->RegistCoroutineTask(
                [this, block] { ConnectionLoop(block); }, started);
            if (!started) {
                UnregisterConn(block.get());
                block->conn->Close();
                ++m_closed_conns;
            }
        }
        {
            std::lock_guard<std::mutex> lock(m_life_mtx);
            m_accept_exited = true;
        }
        m_life_cv.notify_all();
        LoopExit();
        TryFinishTeardown(/*blocking=*/false);
    }

    // ---- 单连接处理（受管协程）------------------------------------------------------
    // 返回即连接已物理 Close 并注销。
    void ConnectionLoop(std::shared_ptr<HttpConnBlock> block) {
        LoopEnter();
        auto session = CreateSession(m_server);
        // 会话通知 sink（弱引用：core→transport 无环；块亡 → sink 无操作）。
        std::weak_ptr<HttpConnBlock> weak = block;
        session->SetNotificationSink([this, weak](const std::string& frame) {
            auto self = weak.lock();
            if (!self) return;
            Enqueue(self, frame);
        });
        try {
            ServeOne(block, session);
        } catch (const std::exception& error) {
            // framing/JSON 构造不得把异常漏出协程；按写失败路径收口。
            std::fprintf(stderr, "bbt.mcp.http: serve exception: %s\n", error.what());
            m_write_error_seen = true;
        } catch (...) {
            std::fprintf(stderr, "bbt.mcp.http: serve exception (unknown)\n");
            m_write_error_seen = true;
        }
        // 收口次序（Server.hpp C1 契约）：先结束本会话订阅（流结束帧入封队=丢弃，
        // 长请求名额归还；正常结束路径订阅已被流结束响应移除，此处为空操作），
        // 再物理 close 连接，再注销。
        session->EndAllSubscriptions();
        session->SetNotificationSink(McpSession::NotificationSink{});
        block->conn->Close();
        ++m_closed_conns;
        UnregisterConn(block.get());
        LoopExit();
        TryFinishTeardown(/*blocking=*/false);
    }

    void ServeOne(const std::shared_ptr<HttpConnBlock>& block,
                  const std::shared_ptr<McpSession>& session) {
        const auto& conn = block->conn;
        boost::beast::flat_buffer buffer;
        boost::beast::http::request_parser<boost::beast::http::string_body> parser;
        parser.header_limit(m_options.max_header_bytes);
        parser.body_limit(m_options.max_body_bytes);
        std::vector<char> rbuf(kReadChunkBytes);
        bool header_gated = false;
        bool read_timed_out = false;

        // ---- 读请求（Beast 增量 framing；EOF 语义在此分叉）-------------------------
        // 注意：本 Beast 版本的 put() 在头完成处停住（ec=Success、done=false、
        // 剩余体字节留在 buffer），需要再次 put 才消费体——所以「buffer 非空就
        // 继续喂」必须先于 socket 读取，否则把已到手的体字节饿死在缓冲里。
        for (;;) {
            boost::beast::error_code ec;
            if (buffer.size() != 0) {
                const std::size_t consumed = parser.put(buffer.data(), ec);
                buffer.consume(consumed);
                if (ec == boost::beast::http::error::need_more) {
                    ec = {};
                } else if (ec) {
                    // 帧错误（畸形请求行/头/超上限）：400 后关闭。
                    WriteAllChecked(conn, MakeHttpResponse(400, "text/plain", "bad http framing"));
                    return;
                }
                if (!header_gated && parser.is_header_done()) {
                    header_gated = true;
                    const GateFailure gate = RunHeaderGates(parser.get());
                    if (!gate.ok) {
                        // 失败响应前有界排空已到的 body（避免 close 触发 RST 吞掉响应）。
                        DrainBounded(parser, buffer, conn, rbuf);
                        WriteAllChecked(conn, MakeHttpResponse(gate.status, "text/plain", gate.reason));
                        return;
                    }
                }
                if (parser.is_done()) break;
                if (buffer.size() != 0) continue;  // 残余体字节仍需 put 消费
            }
            CallOptions options;
            options.deadline = DeadlineAfter(m_options.request_read_timeout);
            const auto chunk = conn->ReadSome(MutableBytes{rbuf.data(), rbuf.size()}, options);
            if (!chunk) {
                if (chunk.error().code == ErrorCode::TimedOut) {
                    read_timed_out = true;
                    break;
                }
                return;  // Closed/Cancelled/TransportError：无响应可写
            }
            if (chunk.value().state == IoState::Eof) {
                // 读 EOF：请求已完整 → 客户端 SHUT_WR，照常响应（不取消，S20）；
                // 请求未完整 → 截断失败（明确拒绝，不做部分响应）。
                parser.put_eof(ec);
                if (ec || !parser.is_done()) {
                    m_truncated_seen = true;
                    WriteAllChecked(conn,
                                    MakeHttpResponse(400, "text/plain", "truncated request"));
                    return;
                }
                break;
            }
            if (chunk.value().bytes == 0) continue;
            const auto dst = buffer.prepare(chunk.value().bytes);
            boost::asio::buffer_copy(dst, boost::asio::buffer(rbuf.data(), chunk.value().bytes));
            buffer.commit(chunk.value().bytes);
        }
        if (read_timed_out) {
            WriteAllChecked(conn, MakeHttpResponse(408, "text/plain", "request timeout"));
            return;
        }
        // 门禁兜底：头与体同 chunk 到齐、is_done 先于 is_header_done 检查的路径。
        if (!header_gated) {
            header_gated = true;
            const GateFailure gate = RunHeaderGates(parser.get());
            if (!gate.ok) {
                WriteAllChecked(conn, MakeHttpResponse(gate.status, "text/plain", gate.reason));
                return;
            }
        }
        const auto& request = parser.get();

        // Expect: 100-continue：读体前先放行（客户端在等临时响应）。
        for (const auto& field : request) {
            if (ToLowerAscii(field.name_string()) == "expect" &&
                field.value().find("100-continue") != std::string::npos) {
                WriteAllChecked(conn, "HTTP/1.1 100 Continue\r\n\r\n");
                break;
            }
        }

        // ---- JSON-RPC 管线 ----------------------------------------------------------
        const auto& core_options = m_server->Options();
        const ProtocolLimits limits{core_options.max_message_bytes, core_options.max_depth};
        const auto& advertised = m_server->AdvertisedVersions();

        std::optional<RequestId> detected_id;
        bool notification_recognized = false;
        auto parsed_frame =
            ParseRequestFrame(request.body(), limits, &detected_id, &notification_recognized);
        // result<T> 无 `->`/`*`：解析成功时取 owned 值，便于后续 `frame->` 访问。
        std::optional<JsonRpcRequest> frame;
        if (parsed_frame) frame = parsed_frame.value();

        // modern 形状错误先于注解头错误。已识别 legacy 帧不套现代 _meta 要求。
        const Headers frame_headers = CollectHeaders(request);
        const auto frame_versions = frame_headers.All("MCP-Protocol-Version");
        const bool legacy_header = frame_versions.size() == 1 &&
            (frame_versions[0].second == kProtocolVersion2025_11_25 ||
             frame_versions[0].second == kProtocolVersion2025_06_18);
        // modern 信封判定（与 core Registry.cc HasModernEnvelope 同口径）：仅当
        // `_meta` 含规范保留键 protocolVersion 才套现代校验；合法 legacy `_meta`
        // （progressToken 等）不触发（review M1）。
        const bool modern_envelope =
            frame && frame->meta &&
            static_cast<bool>(frame->meta->Find(kMetaKeyProtocolVersion));
        if (frame && (modern_envelope || (!legacy_header &&
            (frame->method == "server/discover" || frame->method == kMethodListen ||
             (frame_versions.size() == 1 && frame_versions[0].second == kProtocolVersion2026_07_28))))) {
            auto meta = ValidateModernMeta(frame->params, advertised);
            if (auto* failure = std::get_if<ProtocolFailure>(&meta)) {
                if (failure->code == JsonRpcErrorCode::InvalidParams) {
                    RespondJsonRpc(conn, detected_id, *failure);
                    return;
                }
            }
        }

        // 注解头结构（缺失/重复/编码/未知注解）：-32020；未知 revision：-32022。
        // era 判定同点完成（modern/legacy 路由）。
        const HeaderVerdict verdict = ValidateHeaders(request, advertised, frame);
        if (verdict.failure) {
            RespondJsonRpc(conn, detected_id, *verdict.failure);
            return;
        }

        // modern `_meta` 校验 + 头-body 一致（顺序：_meta → 头一致 → dispatch）。
        bool sse_mode = false;
        if (frame && verdict.era == Era::Legacy && frame->method == "initialize" && frame->params) {
            auto body_version = frame->params->Find("protocolVersion");
            if (body_version) {
                auto value = body_version.value().AsString();
                if (value && value.value() != verdict.version_value) {
                    RespondJsonRpc(conn, detected_id,
                                   ProtocolFailure{JsonRpcErrorCode::HeaderMismatch,
                                                   "mcp.http.header.version_mismatch", std::nullopt});
                    return;
                }
            }
        }
        if (frame && verdict.era == Era::Legacy && frame->meta) {
            // legacy 头 + body 带 modern 形态 `_meta.protocolVersion`：头-body 版本
            // 不符 → -32020（明确拒绝，不做静默 era 翻转；「不符→-32020」的可触发面）。
            auto meta_version = frame->meta->Find(kMetaKeyProtocolVersion);
            if (meta_version) {
                auto meta_string = meta_version.value().AsString();
                if (meta_string && meta_string.value() != verdict.version_value) {
                    RespondJsonRpc(conn, detected_id,
                                   ProtocolFailure{JsonRpcErrorCode::HeaderMismatch,
                                                   "mcp.http.header.version_mismatch",
                                                   std::nullopt});
                    return;
                }
            }
        }
        // R9/S23 legacy 无 session profile：官方 legacy 客户端逐 POST 新建 HTTP
        // 连接，每连接会话不承载先前 initialize 状态；受支持 legacy revision 头
        // 已过 ValidateHeaders 门禁，即为 legacy era 门禁。只落定本连接会话的
        // 握手门禁（不回填 clientCapabilities、不伪造 initialize 事件），modern
        // 校验与 stdio 每会话握手语义不变；真实 initialize 已落定时不覆盖。
        if (frame && verdict.era == Era::Legacy && !frame->notification &&
            frame->method != "initialize") {
            session->AdoptLegacyHeaderGate(verdict.version_value);
        }
        if (frame && verdict.era == Era::Modern) {
            // modern era：`params._meta` 必需（core 的 modern 校验同口径固定文本）。
            // 缺失时不得交给 core 分派——core 按 `_meta` 存在性路由，会把无 meta
            // 帧落进 legacy 表（era 翻转），与「头判 era」契约相悖。
            if (!frame->meta) {
                RespondJsonRpc(conn, detected_id,
                               ProtocolFailure{JsonRpcErrorCode::InvalidParams,
                                               "Invalid params: params object with _meta "
                                               "is required",
                                               std::nullopt});
                return;
            }
            auto meta = ValidateModernMeta(frame->params, advertised);
            if (std::holds_alternative<ModernMeta>(meta)) {
                const auto& modern_meta = std::get<ModernMeta>(meta);
                if (auto consistent =
                        CheckHeaderBodyConsistency(request, *frame, modern_meta)) {
                    RespondJsonRpc(conn, detected_id, *consistent);
                    return;
                }
                if (!frame->notification && frame->method == std::string(kMethodListen)) {
                    // SSE 只在客户端声明接受 event-stream 时使用；否则明确 406，
                    // 不把流伪装成 JSON 一次性响应。
                    const Headers headers = CollectHeaders(request);
                    const auto accept = headers.All("Accept");
                    if (accept.size() == 1 &&
                        AcceptAllows(accept[0].second, "text/event-stream")) {
                        sse_mode = true;
                    } else {
                        WriteAllChecked(conn, MakeHttpResponse(
                                                  406, "text/plain",
                                                  "listen requires Accept: text/event-stream"));
                        return;
                    }
                }
            }
            // meta 非法：不在此响应——core HandleFrame 产生同口径 -32602/-32022。
        }

        auto outcome = session->HandleFrame(request.body());
        if (!outcome) {
            // 核心拒绝（未 Start/已 Close/超在途）：503 + 规范 JSON-RPC 错误体。
            RespondJsonRpc(conn, std::nullopt, ToProtocolFailure(outcome.error()));
            return;
        }
        if (!outcome.value().has_response) {
            if (sse_mode) {
                StreamEvents(block, conn);  // ack 已在队列；流结束响应或断连为止
            } else {
                WriteAllChecked(conn, MakeHttpResponse(202, "", ""));  // 合法通知：202 空 body
            }
            return;
        }
        // 普通方法：200 JSON；错误响应按规范码映射 HTTP 状态（-32601→404，
        // 其余 JSON-RPC 错误 →400；核心拒绝已在上一分支映射 503）。
        unsigned status = 200u;
        auto response_value =
            JsonValue::Parse(outcome.value().response, 1U << 20, 8);
        if (response_value && response_value.value().IsObject()) {
            auto error_field = response_value.value().Find("error");
            if (error_field && error_field.value().IsObject()) {
                status = 400u;
                auto error_code = error_field.value().Find("code");
                if (error_code) {
                    auto code_value = error_code.value().AsInt64();
                    if (code_value) {
                        // -32601→404（规范方法不存在）；-32000 请求期限超限→503（服务端
                        // 期限拒绝，非客户端错误）；其余 JSON-RPC 错误 →400。
                        if (code_value.value() == -32601) status = 404u;
                        else if (code_value.value() == -32000) status = 503u;
                    }
                }
            }
        }
        WriteAllChecked(
            conn, MakeHttpResponse(status, "application/json", outcome.value().response));
    }

    // ---- SSE 真流（先 ack/事件并保持连接；流结束响应为最后一个事件）------------------
    void StreamEvents(const std::shared_ptr<HttpConnBlock>& block,
                      const std::shared_ptr<CoTCP>& conn) {
        if (!WriteAllChecked(conn,
                             "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                             "Cache-Control: no-store\r\nConnection: close\r\n\r\n")) {
            return;  // 写失败路径：订阅由 ConnectionLoop 固定收口取消，无后续写
        }
        unsigned event_id = 0;
        for (;;) {
            auto frame = Dequeue(block, m_options.sse_heartbeat_interval);
            if (!frame) {
                if (BlockSealed(block)) break;  // Close 封口：停止流，不再写
                // 心跳注释：不作为 JSON 的有限探测写；失败 → 断连/写错误 → 取消。
                if (!WriteAllChecked(conn, ": keep-alive\n\n")) return;
                continue;
            }
            const bool final_frame = IsJsonRpcResponseFrame(*frame);
            const std::string event = "event: message\r\nid: " + std::to_string(++event_id) +
                                      "\r\ndata: " + *frame + "\n\n";
            if (!WriteAllChecked(conn, event)) return;
            if (final_frame) break;  // 流结束响应（EndSubscription 已移除订阅条目）
        }
    }

    // ---- 门禁与校验 -----------------------------------------------------------------
    struct GateFailure {
        bool ok = true;
        unsigned status = 200;
        std::string reason;
    };

    struct Headers {
        std::string method;
        std::string target;
        std::vector<std::pair<std::string, std::string>> raw;  // 原始序（含重复）
        std::vector<std::pair<std::string, std::string>> All(std::string_view name) const {
            std::vector<std::pair<std::string, std::string>> out;
            const auto lower = ToLowerAscii(name);
            for (const auto& entry : raw)
                if (ToLowerAscii(entry.first) == lower) out.push_back(entry);
            return out;
        }
    };

    static Headers CollectHeaders(
        const boost::beast::http::request<boost::beast::http::string_body>& request) {
        Headers headers;
        headers.method = std::string(request.method_string());
        headers.target = std::string(request.target());
        for (const auto& field : request)
            headers.raw.emplace_back(std::string(field.name_string()), std::string(field.value()));
        return headers;
    }

    GateFailure RunHeaderGates(
        const boost::beast::http::request<boost::beast::http::string_body>& request) {
        // 1. 方法门：现代 GET/DELETE → 405（S18）；其余非 POST 同样 405。
        if (request.method() != boost::beast::http::verb::post)
            return {false, 405, "method not allowed"};
        // 2. 单 POST endpoint 路径：精确匹配（带 query 视为不匹配 → 404）。
        if (std::string(request.target()) != m_options.endpoint_path)
            return {false, 404, "unknown endpoint"};
        const Headers headers = CollectHeaders(request);
        // 3. Host（HTTP/1.1 必需、唯一）。
        const auto host = headers.All("Host");
        if (host.size() != 1) return {false, 400, "missing or duplicate Host"};
        // 4. Origin：存在不合法 → 403（重复/语法非法/不在白名单）。
        const auto origin = headers.All("Origin");
        if (!origin.empty()) {
            if (origin.size() > 1) return {false, 403, "duplicate Origin"};
            if (!OriginAllowed(origin[0].second)) return {false, 403, "Origin not allowed"};
        }
        // 5. Content-Type：POST 必须 application/json（参数如 charset 允许）。
        const auto ct = headers.All("Content-Type");
        if (ct.size() != 1)
            return {false, ct.empty() ? 415u : 400u, "missing or duplicate Content-Type"};
        if (!MediaTypeIs(ct[0].second, "application/json"))
            return {false, 415, "unsupported media type"};
        // 6. Accept：必须声明 application/json 或 text/event-stream（列表任一项）。
        const auto accept = headers.All("Accept");
        if (accept.size() != 1)
            return {false, accept.empty() ? 406u : 400u, "missing or duplicate Accept"};
        if (!AcceptAllows(accept[0].second, "application/json") &&
            !AcceptAllows(accept[0].second, "text/event-stream"))
            return {false, 406, "not acceptable"};
        return {};
    }

    bool OriginAllowed(const std::string& origin) const {
        // 语法：scheme://host[:port]，全可打印 ASCII、无空格/CTL；再对白名单精确匹配。
        if (origin.empty()) return false;
        for (const unsigned char c : origin)
            if (c <= 0x20 || c > 0x7E) return false;
        const auto scheme_end = origin.find("://");
        if (scheme_end == std::string::npos || scheme_end == 0) return false;
        const auto scheme = ToLowerAscii(origin.substr(0, scheme_end));
        if (scheme != "http" && scheme != "https") return false;
        const auto host_port = origin.substr(scheme_end + 3);
        if (host_port.empty() || host_port.find('/') != std::string::npos) return false;
        for (const auto& allowed : m_options.allowed_origins)
            if (origin == allowed) return true;
        return false;
    }

    static bool MediaTypeIs(const std::string& value, std::string_view media) {
        // media type 匹配：大小写不敏感；`;` 参数段忽略。
        std::string type = value.substr(0, value.find(';'));
        while (!type.empty() && (type.back() == ' ' || type.back() == '\t')) type.pop_back();
        while (!type.empty() && (type.front() == ' ' || type.front() == '\t')) type.erase(type.begin());
        return ToLowerAscii(type) == media;
    }

    // Accept 是逗号分隔的媒体类型列表（RFC 9110 §12.5.1）：任一项匹配即允许。
    static bool AcceptAllows(const std::string& value, std::string_view media) {
        std::size_t begin = 0;
        for (;;) {
            const auto comma = value.find(',', begin);
            const std::string item = value.substr(
                begin, comma == std::string::npos ? std::string::npos : comma - begin);
            if (MediaTypeIs(item, media)) return true;
            if (comma == std::string::npos) return false;
            begin = comma + 1;
        }
    }

    // 有界排空已到的 body（门禁失败响应前的收尾读；防 unread-data RST 吞响应）。
    void DrainBounded(boost::beast::http::request_parser<boost::beast::http::string_body>& parser,
                      boost::beast::flat_buffer& buffer, const std::shared_ptr<CoTCP>& conn,
                      std::vector<char>& rbuf) {
        std::size_t drained = 0;
        boost::beast::error_code ec;
        while (!parser.is_done() && drained <= m_options.max_body_bytes) {
            if (buffer.size() != 0) {
                const std::size_t consumed = parser.put(buffer.data(), ec);
                buffer.consume(consumed);
                if (ec == boost::beast::http::error::need_more) ec = {};
                if (ec) return;  // 排空阶段不关心帧错误
                if (parser.is_done()) return;
            }
            CallOptions options;
            options.deadline = DeadlineAfter(std::chrono::milliseconds{1000});
            const auto chunk = conn->ReadSome(MutableBytes{rbuf.data(), rbuf.size()}, options);
            if (!chunk || chunk.value().state == IoState::Eof || chunk.value().bytes == 0) return;
            drained += chunk.value().bytes;
            const auto dst = buffer.prepare(chunk.value().bytes);
            boost::asio::buffer_copy(dst, boost::asio::buffer(rbuf.data(), chunk.value().bytes));
            buffer.commit(chunk.value().bytes);
        }
    }

    // ---- 注解头与 era -----------------------------------------------------------------
    enum class Era { Modern, Legacy };

    struct HeaderVerdict {
        std::optional<ProtocolFailure> failure;
        Era era = Era::Modern;
        std::string version_value;  // MCP-Protocol-Version 原值（一致校验用）
    };

    static std::optional<std::string> InitialLegacyVersion(const std::optional<JsonRpcRequest>& frame) {
        if (!frame || frame->notification || frame->method != "initialize" || !frame->params)
            return std::nullopt;
        // 仅 modern 信封（同 M1 口径）否决无头豁免；合法 legacy `_meta` 不影响豁免
        // （review M2）。
        if (frame->meta && static_cast<bool>(frame->meta->Find(kMetaKeyProtocolVersion)))
            return std::nullopt;
        auto version = frame->params->Find("protocolVersion");
        if (!version) return std::nullopt;
        auto value = version.value().AsString();
        if (!value || (value.value() != kProtocolVersion2025_11_25 &&
                       value.value() != kProtocolVersion2025_06_18)) return std::nullopt;
        return value.value();  // 其余 initialize 形状仍由 core 校验，不绕过生命周期/能力门禁。
    }

    HeaderVerdict ValidateHeaders(
        const boost::beast::http::request<boost::beast::http::string_body>& request,
        const std::vector<std::string>& advertised, const std::optional<JsonRpcRequest>& frame) {
        const Headers headers = CollectHeaders(request);
        // 本 transport 支持的自定义 header 注解全集。Mcp-* 家族里不支持的注解
        // 收到即明确 -32020，绝不静默忽略（「不能 advertise 后不检查」；本
        // transport 不 mint/echo session，故 Mcp-Session-Id 亦按不支持注解拒绝）。
        for (const auto& entry : headers.raw) {
            const auto lower = ToLowerAscii(entry.first);
            const bool mcp_annotated = lower.rfind("mcp-", 0) == 0;
            if (!mcp_annotated) continue;
            const bool supported = lower == "mcp-protocol-version" ||
                                   lower == "mcp-method" || lower == "mcp-name";
            if (!supported)
                return {ProtocolFailure{JsonRpcErrorCode::HeaderMismatch,
                                        "mcp.http.header.unsupported_annotation",
                                        std::nullopt},
                        Era::Modern, {}};
            if (headers.All(lower).size() > 1)
                return {ProtocolFailure{JsonRpcErrorCode::HeaderMismatch,
                                        "mcp.http.header.duplicate", std::nullopt},
                        Era::Modern, {}};
            if (!DecodeAnnotationValue(entry.second))
                return {ProtocolFailure{JsonRpcErrorCode::HeaderMismatch,
                                        "mcp.http.header.value", std::nullopt},
                        Era::Modern, {}};
        }
        // 只有受支持 legacy initialize 能以 body 选择 era；重复头不获得豁免。
        const auto version = headers.All("MCP-Protocol-Version");
        if (version.empty()) {
            if (auto initial = InitialLegacyVersion(frame))
                return {std::nullopt, Era::Legacy, *initial};
        }
        if (version.size() != 1) {
            return {ProtocolFailure{JsonRpcErrorCode::HeaderMismatch,
                                    "mcp.http.header.protocol_version", std::nullopt},
                    Era::Modern, {}};
        }
        const std::string& v = version[0].second;
        const bool modern_era = v == kProtocolVersion2026_07_28;
        if (modern_era) {
            // modern 方法头必需；具名方法的 Mcp-Name 在 body 来源校验后检查。
            if (headers.All("Mcp-Method").size() != 1)
                return {ProtocolFailure{JsonRpcErrorCode::HeaderMismatch,
                                        "mcp.http.header.method", std::nullopt},
                        Era::Modern, v};

            return {std::nullopt, Era::Modern, v};
        }
        if (v == kProtocolVersion2025_11_25 || v == kProtocolVersion2025_06_18) {
            // legacy era 不强制 Mcp-Method/Mcp-Name（legacy 客户端不携带注解头）。
            return {std::nullopt, Era::Legacy, v};
        }
        // 结构合法但 revision 不受支持 → -32022（data: requested/supported）。
        JsonValue data = JsonValue::Object();
        data.Set("requested", JsonValue(v));
        JsonValue supported = JsonValue::Array();
        for (const auto& a : advertised) supported.Push(JsonValue(a));
        data.Set("supported", std::move(supported));
        return {ProtocolFailure{JsonRpcErrorCode::UnsupportedProtocolVersion,
                                "mcp.http.header.unsupported_version", std::move(data)},
                Era::Modern, {}};
    }

    // 头-body 一致（-32020）：MCP-Protocol-Version == `_meta.protocolVersion`；
    // Mcp-Method == JSON-RPC method；适用 Mcp-Name 解码后精确比较 body 来源。
    std::optional<ProtocolFailure> CheckHeaderBodyConsistency(
        const boost::beast::http::request<boost::beast::http::string_body>& request,
        const JsonRpcRequest& frame, const ModernMeta& meta) {
        const Headers headers = CollectHeaders(request);
        const auto version = headers.All("MCP-Protocol-Version");
        if (version.size() == 1) {
            const auto decoded = DecodeAnnotationValue(version[0].second);
            if (!decoded || *decoded != meta.protocol_version)
                return ProtocolFailure{JsonRpcErrorCode::HeaderMismatch,
                                       "mcp.http.header.version_mismatch", std::nullopt};
        }
        const auto method = headers.All("Mcp-Method");
        if (method.size() == 1) {
            const auto decoded = DecodeAnnotationValue(method[0].second);
            if (!decoded || *decoded != frame.method)
                return ProtocolFailure{JsonRpcErrorCode::HeaderMismatch,
                                       "mcp.http.header.method_mismatch", std::nullopt};
        }
        const char* key = frame.method == "resources/read" ? "uri" :
            (frame.method == "tools/call" || frame.method == "prompts/get" ? "name" : nullptr);
        if (key) {
            if (!frame.params)
                return ProtocolFailure{JsonRpcErrorCode::InvalidParams, "Invalid params", std::nullopt};
            auto source = frame.params->Find(key);
            if (!source || !source.value().IsString())
                return ProtocolFailure{JsonRpcErrorCode::InvalidParams, "Invalid params", std::nullopt};
            const auto name = headers.All("Mcp-Name");
            if (name.size() != 1)
                return ProtocolFailure{JsonRpcErrorCode::HeaderMismatch, "mcp.http.header.name", std::nullopt};
            const auto decoded = DecodeAnnotationValue(name[0].second);
            if (!decoded || *decoded != source.value().AsString().value())
                return ProtocolFailure{JsonRpcErrorCode::HeaderMismatch,
                                       "mcp.http.header.name_mismatch", std::nullopt};
        }
        return std::nullopt;
    }

    // ---- JSON-RPC HTTP 响应（规范码 → HTTP 状态映射：-32601→404，核心拒绝→503，其余 400）--
    static unsigned StatusFor(JsonRpcErrorCode code) {
        return code == JsonRpcErrorCode::MethodNotFound ? 404u : 400u;
    }

    void RespondJsonRpc(const std::shared_ptr<CoTCP>& conn, const std::optional<RequestId>& id,
                        const ProtocolFailure& failure) {
        const auto body = MakeErrorResponse(id, failure).Serialize();
        const unsigned status =
            failure.code == JsonRpcErrorCode::InternalError ||
                    failure.code == JsonRpcErrorCode::RequestDeadlineExceeded
                ? 503u
                : StatusFor(failure.code);
        WriteAllChecked(conn, MakeHttpResponse(status, "application/json", body));
    }

    // ---- 收口（显式 Close 与自收口共用）---------------------------------------------
    bool BeginTeardown() {
        {
            std::lock_guard<std::mutex> lock(m_life_mtx);
            if (m_teardown_started) return false;
            m_teardown_started = true;
        }
        // 1) listener 物理关闭（唤醒挂起 Accept；之后 connect 不可达）。
        if (m_listener) m_listener->Close();
        // 2) 封全部连接输出并物理 close（在途 op 得到明确 Closed → loop 自行退出）。
        for (const auto& block : SnapshotConns()) {
            SealBlockOutput(block);
            block->conn->Close();
            ++m_closed_conns;
        }
        return true;
    }

    void TryFinishTeardown(bool blocking) {
        {
            std::lock_guard<std::mutex> life_lock(m_life_mtx);
            if (m_closed_done) return;
        }
        if (blocking) {
            const auto deadline = DeadlineAfter(m_options.close_drain_timeout);
            std::unique_lock<std::mutex> lock(m_life_mtx);
            m_life_cv.wait_until(lock, deadline, [this] { return LoopAllExitedLocked(); });
        } else {
            std::lock_guard<std::mutex> lock(m_life_mtx);
            if (!LoopAllExitedLocked()) return;
        }
        // 3) runtime 收口（账本归还；socket 已关，此处为 owner 状态落定）。
        if (m_runtime) m_runtime->Close();

        std::size_t live = 0;
        {
            std::lock_guard<std::mutex> lock(m_life_mtx);
            live = m_live_loops;
        }
        HttpCloseEvidence snapshot;
        snapshot.listener_closed = m_listener ? m_listener->IsClosed() : false;
        snapshot.runtime_closed = m_runtime ? m_runtime->IsClosed() : false;
        snapshot.accept_loop_exited = m_accept_exited;
        snapshot.connection_loops_exited = live == 0;
        snapshot.output_sealed = true;
        snapshot.connections_closed = static_cast<std::size_t>(m_closed_conns.load());
        snapshot.live_connections = live;
        snapshot.dropped_frames = static_cast<std::size_t>(m_dropped_total.load());
        snapshot.abandoned_frames = static_cast<std::size_t>(m_abandoned_total.load());
        snapshot.truncated_request_seen = m_truncated_seen.load();
        snapshot.write_error_seen = m_write_error_seen.load();
        {
            std::lock_guard<std::mutex> life_lock(m_life_mtx);
            if (!m_closed_done) {
                m_evidence = snapshot;
                m_closed_done = true;
                m_life_cv.notify_all();
            }
        }
    }

    bool LoopAllExitedLocked() const {
        // m_life_mtx held。
        return m_accept_exited && m_live_loops == 0;
    }

    void CloseAndWait() noexcept {
        if (BeginTeardown()) TryFinishTeardown(/*blocking=*/true);
        std::unique_lock<std::mutex> lock(m_life_mtx);
        m_life_cv.wait(lock, [this] { return m_closed_done; });
    }
};

// ---- 公共 API ---------------------------------------------------------------------------

result<std::shared_ptr<McpHttpTransport>> McpHttpTransport::Listen(
    std::shared_ptr<McpServer> server, HttpOptions options) {
    if (!server)
        return result<std::shared_ptr<McpHttpTransport>>::err(
            MakeError(ErrorCode::InvalidArgument, "mcp.http.null_server"));
    if (!server->IsStarted())
        return result<std::shared_ptr<McpHttpTransport>>::err(
            MakeError(ErrorCode::InvalidContext, "mcp.http.server_not_started"));
    if (!IsLoopbackHost(options.listen_host))
        return result<std::shared_ptr<McpHttpTransport>>::err(
            MakeError(ErrorCode::InvalidArgument, "mcp.http.non_loopback_listen"));
    if (options.endpoint_path.empty() || options.endpoint_path[0] != '/' ||
        options.backlog <= 0 || options.max_connections == 0 || options.max_inflight < 3 ||
        options.max_header_bytes == 0 || options.max_body_bytes == 0 ||
        options.max_pending_bytes == 0 ||
        options.request_read_timeout <= std::chrono::milliseconds{0} ||
        options.sse_heartbeat_interval <= std::chrono::milliseconds{0} ||
        options.write_deadline <= std::chrono::milliseconds{0} ||
        options.close_drain_timeout <= std::chrono::milliseconds{0})
        return result<std::shared_ptr<McpHttpTransport>>::err(
            MakeError(ErrorCode::InvalidArgument, "mcp.http.bad_options"));
    if (!bbt::coroutine::detail::Scheduler::GetInstance()->IsInitialized())
        return result<std::shared_ptr<McpHttpTransport>>::err(
            MakeError(ErrorCode::RuntimeUnavailable, "mcp.http.runtime_not_running"));

    auto impl = std::make_shared<Impl>(std::move(server), options);
    const NetworkLimits limits{options.max_connections, options.max_inflight,
                               options.max_header_bytes, options.max_body_bytes,
                               options.request_read_timeout};
    auto runtime = TransportRuntime::Create(limits);
    if (!runtime) return result<std::shared_ptr<McpHttpTransport>>::err(runtime.error());
    impl->m_runtime = std::move(runtime).value();
    if (auto started = impl->m_runtime->Start(); !started)
        return result<std::shared_ptr<McpHttpTransport>>::err(started.error());
    auto listener = impl->m_runtime->ListenTCP(
        SocketAddress{options.listen_host, options.listen_port},
        static_cast<unsigned>(options.backlog));
    if (!listener) return result<std::shared_ptr<McpHttpTransport>>::err(listener.error());
    impl->m_listener = std::move(listener).value();
    impl->m_local = impl->m_listener->LocalAddress();

    bool accept_started = false;
    bbt::coroutine::detail::Scheduler::GetInstance()->RegistCoroutineTask(
        [impl] { impl->AcceptLoop(); }, accept_started);
    if (!accept_started) {
        impl->m_listener->Close();
        impl->m_runtime->Close();
        return result<std::shared_ptr<McpHttpTransport>>::err(
            MakeError(ErrorCode::RuntimeUnavailable, "mcp.http.coroutine_start_failed"));
    }
    return result<std::shared_ptr<McpHttpTransport>>::ok(
        std::shared_ptr<McpHttpTransport>(new McpHttpTransport(std::move(impl))));
}

McpHttpTransport::McpHttpTransport(std::shared_ptr<Impl> impl) noexcept
    : m_impl(std::move(impl)) {}

McpHttpTransport::~McpHttpTransport() {
    if (m_impl) m_impl->CloseAndWait();
}

void McpHttpTransport::Close() noexcept {
    if (m_impl) m_impl->CloseAndWait();
}

bool McpHttpTransport::IsClosed() const noexcept {
    std::lock_guard<std::mutex> lock(m_impl->m_life_mtx);
    return m_impl->m_closed_done;
}

SocketAddress McpHttpTransport::LocalAddress() const noexcept {
    return m_impl->m_local;
}

HttpCloseEvidence McpHttpTransport::LastCloseEvidence() const noexcept {
    std::lock_guard<std::mutex> lock(m_impl->m_life_mtx);
    return m_impl->m_evidence;
}

} // namespace bbt::infra::mcp

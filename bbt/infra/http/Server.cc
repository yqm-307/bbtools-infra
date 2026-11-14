#include <bbt/infra/http/Server.hpp>

#include <algorithm>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/beast/http.hpp>
#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/infra/detail/CloseWait.hpp>
#include <bbt/infra/http/detail/Http11.hpp>
#include <bbt/infra/http/detail/ServerTestAccess.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/ICoConnection.hpp>

namespace bbt::infra::http {
namespace {
namespace wire = boost::beast::http;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

result<void> Failure(ErrorCode code, const char* message)
{
    return result<void>::err(MakeError(code, message));
}

bool Token(std::string_view text)
{
    if (text.empty()) {
        return false;
    }
    for (const unsigned char c : text) {
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
              || std::string_view("!#$%&'*+-.^_`|~").find(c) != std::string_view::npos)) {
            return false;
        }
    }
    return true;
}

bool FieldValue(std::string_view text)
{
    for (const unsigned char c : text) {
        if ((c < 32 && c != '\t') || c == 127) {
            return false;
        }
    }
    return true;
}

bool ValidHost(boost::beast::string_view host)
{
    if (host.empty()) {
        return false;
    }
    boost::beast::string_view port;
    if (host.front() == '[') {
        const auto end = host.find(']');
        if (end == boost::beast::string_view::npos || end <= 1 || host.find('%') != boost::beast::string_view::npos) {
            return false;
        }
        std::error_code error;
        const auto address = net::Address::Ip(std::string(host.substr(1, end - 1)), 0, error);
        if (error || address.Family() != net::AddressFamily::IPv6) {
            return false;
        }
        if (end + 1 < host.size()) {
            if (host[end + 1] != ':') {
                return false;
            }
            port = host.substr(end + 2);
            if (port.empty()) {
                return false;
            }
        }
    } else {
        const auto colon = host.find(':');
        if (colon != boost::beast::string_view::npos) {
            port = host.substr(colon + 1);
            host = host.substr(0, colon);
            if (port.empty()) {
                return false;
            }
        }
        if (host.empty()) {
            return false;
        }

        /**
         * 明确采用 ASCII DNS/数值 host 子集，不接受 userinfo、路径或转义 authority。
         */
        if (host.back() == '.') {
            host.remove_suffix(1);
        }
        if (host.empty()) {
            return false;
        }
        while (!host.empty()) {
            const auto dot = host.find('.');
            const auto label = host.substr(0, dot);
            if (label.empty() || label.size() > 63 || label.front() == '-' || label.back() == '-') {
                return false;
            }
            for (const unsigned char c : label) {
                if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-')) {
                    return false;
                }
            }
            if (dot == boost::beast::string_view::npos) {
                break;
            }
            host.remove_prefix(dot + 1);
            if (host.empty()) {
                return false;
            }
        }
    }
    unsigned number = 0;
    for (const unsigned char c : port) {
        if (c < '0' || c > '9') {
            return false;
        }
        number = number * 10 + (c - '0');
        if (number > 65535) {
            return false;
        }
    }
    return true;
}

bool ValidRequestHead(const wire::request<wire::string_body>& request, std::size_t max_target)
{
    const auto target = request.target();
    if (request.version() != 11 || target.empty() || target.front() != '/' || target.size() > max_target
        || request.method() == wire::verb::connect || request.count(wire::field::host) != 1
        || !ValidHost(request[wire::field::host]) || request.count(wire::field::expect)
        || request.count(wire::field::upgrade)
        || wire::token_list(request[wire::field::connection]).exists("upgrade")) {
        return false;
    }
    const auto hex = [](unsigned char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    };
    for (std::size_t i = 0; i < target.size(); ++i) {
        const unsigned char c = target[i];
        if (c == '%') {
            if (i + 2 >= target.size() || !hex(target[i + 1]) || !hex(target[i + 2])) {
                return false;
            }
            i += 2;
            continue;
        }
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
              || std::string_view("-._~!$&'()*+,;=:@/?").find(c) != std::string_view::npos)) {
            return false;
        }
    }
    return true;
}

result<void> ValidateResponseHeaders(const Headers& headers, std::size_t max_header_bytes)
{
    std::size_t header_bytes = 0;
    for (const auto& field : headers) {
        header_bytes += field.first.size() + field.second.size() + 4;
        if (header_bytes > max_header_bytes || !Token(field.first) || !FieldValue(field.second)
            || boost::beast::iequals(field.first, "Content-Length")
            || boost::beast::iequals(field.first, "Transfer-Encoding")
            || boost::beast::iequals(field.first, "Connection") || boost::beast::iequals(field.first, "Upgrade")
            || boost::beast::iequals(field.first, "Trailer")) {
            return Failure(ErrorCode::InvalidArgument, "invalid server response header");
        }
    }
    return result<void>::ok();
}

/**
 * 已完成响应是否突破本连接的形态/字节上限。发布入队前与写出前用同一判据：
 * 入队前拒绝保证后序超大响应不占队列、不突破内存界；写出前的检查保留为
 * 防御（例如流式头在 Start 时才成形）。
 */
bool ResponseExceedsLimits(const Response& response, const ServerOptions& options)
{
    if (response.status_code < 200 || response.status_code > 599
        || response.body.size() > options.limits.max_body_bytes) {
        return true;
    }
    return !ValidateResponseHeaders(response.headers, options.limits.max_header_bytes);
}

result<std::string> SerializeResponse(Response response, bool head, bool keep_alive, const ServerOptions& options)
{
    if (response.status_code < 200 || response.status_code > 599
        || response.body.size() > options.limits.max_body_bytes) {
        return result<std::string>::err(MakeError(ErrorCode::InvalidArgument, "invalid server response"));
    }
    wire::response<wire::string_body> message{static_cast<wire::status>(response.status_code), 11};
    const auto valid = ValidateResponseHeaders(response.headers, options.limits.max_header_bytes);
    if (!valid) {
        return result<std::string>::err(valid.error());
    }
    for (const auto& field : response.headers) {
        message.insert(field.first, field.second);
    }
    message.keep_alive(keep_alive);
    const bool no_content = response.status_code == 204 || response.status_code == 304;
    if (!no_content) {
        message.content_length(response.body.size());
        if (!head) {
            message.body() = std::move(response.body);
        }
    }
    {
        std::string bytes;
        detail::StringSink sink(bytes);
        boost::system::error_code error;
        wire::write(sink, message, error);
        if (error) {
            return result<std::string>::err(MakeError(ErrorCode::ProtocolError, error.message()));
        }
        return result<std::string>::ok(std::move(bytes));
    }
}

result<std::string> SerializeResponseHead(
    int status_code, const Headers& headers, bool head, bool chunked, bool keep_alive, const ServerOptions& options)
{
    if (status_code < 200 || status_code > 599) {
        return result<std::string>::err(MakeError(ErrorCode::InvalidArgument, "invalid server response"));
    }
    if (chunked && (head || status_code == 204 || status_code == 304)) {
        return result<std::string>::err(MakeError(ErrorCode::InvalidArgument, "chunked reply disallowed"));
    }
    wire::response<wire::empty_body> message{static_cast<wire::status>(status_code), 11};
    const auto valid = ValidateResponseHeaders(headers, options.limits.max_header_bytes);
    if (!valid) {
        return result<std::string>::err(valid.error());
    }
    for (const auto& field : headers) {
        message.insert(field.first, field.second);
    }
    message.keep_alive(keep_alive);
    if (chunked) {
        message.chunked(true);
    } else if (status_code != 204 && status_code != 304) {
        message.content_length(0);
    }
    std::string bytes;
    detail::StringSink sink(bytes);
    wire::response_serializer<wire::empty_body> serializer{message};
    boost::system::error_code error;
    wire::write_header(sink, serializer, error);
    if (error) {
        return result<std::string>::err(MakeError(ErrorCode::ProtocolError, error.message()));
    }
    return result<std::string>::ok(std::move(bytes));
}

/**
 * chunked wire 块手工编码（十六进制长度 + CRLF + 数据 + CRLF）；Boost
 * write_chunk 需要 serializer 状态，手工编码同等正确且不持有 serializer。
 */
std::string EncodeChunk(std::string_view data)
{
    static constexpr char kHex[] = "0123456789ABCDEF";
    char head[24];
    std::size_t size = data.size();
    int digits = 0;
    for (std::size_t v = size; v; v >>= 4) {
        ++digits;
    }
    if (digits == 0) {
        digits = 1;
    }
    for (int i = digits - 1; i >= 0; --i) {
        head[i] = kHex[size & 0xF];
        size >>= 4;
    }
    std::string out(head, head + digits);
    out += "\r\n";
    out += data;
    out += "\r\n";
    return out;
}

// clang-format off
}
// clang-format on

struct Server::State : std::enable_shared_from_this<Server::State> {

    /**
     * 一个已接纳请求的全部拥有型状态。handler 任务只发布结果；writer 是唯一读
     * response/source、唯一写 socket、唯一结算 inflight 的角色。
     */
    struct RequestRecord {
        std::uint64_t seq{0};
        bbt::coroutine::Deadline deadline{};
        bool head{false};
        bool keep_alive{false};

        /**
         * handler 结果已发布，writer 可消费
         */
        bool complete{false};

        /**
         * 该序号已被明确放弃；迟到结果只释放不写
         */
        bool abandoned{false};

        /**
         * inflight 已减一次（唯一减点）
         */
        bool settled{false};

        /**
         * handler 任务仍存活：放弃/关闭时该序号不得提前归还 inflight 额度，
         * 由 HandlerExit 在协程退出时补结算；迟到结果仍只取消流源、不写连接。
         */
        bool handler_active{false};
        bool source_cancelled{false};
        Response response;
        std::shared_ptr<ResponseBody> source;
    };

    struct Session {
        net::Connection connection;

        /**
         * 序号升序；front 是 writer 当前负责的序号。记录只在写终态或连接放弃时
         * 移除，保证「最早未写序号」始终能发布且不会因后续满载而饿死。
         */
        std::deque<std::shared_ptr<RequestRecord>> queue;
        std::uint64_t next_seq{0};
        bool reader_done{false};
        bool writer_done{false};
        bool task_done{false};

        /**
         * 已登记的 reader/writer 数，决定 task_done
         */
        std::size_t live_tasks{0};

        /**
         * 已接纳 Connection: close 或 server 封口
         */
        bool no_more_requests{false};

        /**
         * 连接已逻辑封口/放弃
         */
        bool closed{false};

        /**
         * reader 等名额/封口，writer 等队首完成。CoWaiter 单等待位，故分两个。
         */
        std::shared_ptr<bbt::coroutine::sync::CoWaiter> reader_wait = bbt::coroutine::sync::CoWaiter::Create();
        std::shared_ptr<bbt::coroutine::sync::CoWaiter> writer_wait = bbt::coroutine::sync::CoWaiter::Create();
    };

    /**
     * 每个受管任务一份退出责任；最后一个 callback 捕获销毁时才减任务数。
     */
    struct TaskLease {
        std::shared_ptr<State> state;
        std::shared_ptr<Session> session;
        bool session_life{false};
        bool counted{false};

        ~TaskLease()
        {
            if (!state || !counted) {
                return;
            }
            std::lock_guard<std::mutex> lock(state->mutex);
            if (session_life && session) {
                if (session->live_tasks > 0) {
                    --session->live_tasks;
                }
                state->UpdateTaskDoneLocked(session);
            }
            if (state->tasks > 0) {
                --state->tasks;
            }
            state->NotifyLocked();
        }
    };

    std::shared_ptr<net::Engine> engine;
    ServerHandler handler;

    /**
     * 非空即 streaming server（CreateStreaming）
     */
    StreamingServerHandler streaming_handler;
    ServerOptions options;
    StreamingOptions streaming{};
    mutable std::mutex mutex;
    std::condition_variable cv;
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> close_signal = bbt::coroutine::sync::CoWaiter::Create();

    /**
     * 当前执行本 Server 已接纳任务（reader/writer/handler）的协程 id：
     * Close/WaitStopped 的自等待拒绝依据，不依赖线程 TLS。
     */
    std::unordered_set<bbt::coroutine::detail::CoroutineId> admitted_tasks;
    bool started{false};
    bool stopping{false};
    bool closing{false};

    /**
     * Open → Draining → Closed：draining 只由优雅关闭置位（StopAccepting
     * 不算重复关闭），closing 经 Immediate/RequestClose 表达；两位不共用。
     */
    bool draining{false};

    /**
     * 首个关闭原因固定
     */
    std::optional<Error> close_reason;

    /**
     * 窄私有故障注入，不扩大公开 API
     */
    bool fail_next_task{false};

    /**
     * 窄私有登记窗口注入，只执行一次
     */
    std::function<void()> register_barrier;
    std::size_t accepted{0}, requests{0}, inflight{0}, tasks{0};
    std::vector<std::shared_ptr<Session>> sessions;

    /**
     * 状态变化后统一唤醒 native/Co 关闭等待者，以及每个连接的 reader/writer。
     */
    void NotifyLocked() noexcept
    {
        cv.notify_all();
        if (close_signal) {
            close_signal->Notify();
        }
        for (const auto& session : sessions) {
            if (session->reader_wait) {
                session->reader_wait->Notify();
            }
            if (session->writer_wait) {
                session->writer_wait->Notify();
            }
        }
    }

    /**
     * 已接纳任务标记：以协程 id（而非线程 TLS）登记，任务跨 worker 恢复后
     * 仍能识别自等待；析构在任务退出时移除。
     */
    struct TaskMark {
        std::shared_ptr<State> state;
        bbt::coroutine::detail::CoroutineId id{0};

        explicit TaskMark(std::shared_ptr<State> target)
            : state(std::move(target))
        {
            id = bbt::coroutine::GetLocalCoroutineId();
            if (id != 0) {
                std::lock_guard<std::mutex> lock(state->mutex);
                state->admitted_tasks.insert(id);
            }
        }

        ~TaskMark()
        {
            if (id != 0) {
                std::lock_guard<std::mutex> lock(state->mutex);
                state->admitted_tasks.erase(id);
            }
        }
    };

    void Sweep()
    {
        sessions.erase(
            std::remove_if(
                sessions.begin(),
                sessions.end(),
                [](const auto& session) { return session->task_done && session->connection.Released(); }),
            sessions.end());
    }

    void UpdateTaskDoneLocked(const std::shared_ptr<Session>& session)
    {
        if (session->reader_done && session->writer_done && session->live_tasks == 0) {
            session->task_done = true;
        }
    }

    /**
     * 统一任务登记：Stopping 后不再预约；登记失败由 TaskLease 回滚计数，
     * 调用方负责补齐 session 的 reader_done/writer_done 责任。
     */
    bool RegisterTask(
        std::function<void()> body,
        std::shared_ptr<Session> session = {},
        bool session_life = false,
        bool allow_stopping = false)
    {

        /**
         * 统计前置窗口：测试可在锁外、计数与停止检查之前插入确定性屏障。
         */
        std::function<void()> barrier;
        {
            std::lock_guard<std::mutex> lock(mutex);
            barrier = std::move(register_barrier);
            register_barrier = nullptr;
        }
        if (barrier) {
            barrier();
        }
        auto lease = std::make_shared<TaskLease>();
        {
            std::lock_guard<std::mutex> lock(mutex);

            /**
             * 已接纳的请求任务允许在 Graceful 封口后登记：它们是排空责任的一部分；
             * 新连接/新请求仍由 stopping 拒绝。
             */
            if (stopping && !allow_stopping) {
                return false;
            }
            if (std::exchange(fail_next_task, false)) {

                /**
                 * 未计数，lease 无回滚责任
                 */
                return false;
            }
            ++tasks;
            lease->state = shared_from_this();
            lease->session = std::move(session);
            lease->session_life = session_life;
            lease->counted = true;
            if (session_life) {
                ++lease->session->live_tasks;
            }
        }
        bool registered = false;
        try {
            g_scheduler->RegistCoroutineTask(
                128 * 1024,
                [lease, body = std::move(body)]() mutable {
                    State::TaskMark mark{lease->state};
                    try {
                        body();
                    } catch (...) {
                        if (lease->session) {
                            lease->state->AbandonSession(lease->session);
                        } else {
                            lease->state->Stop(false);
                        }
                    }
                },
                registered);
        } catch (...) {
            registered = false;
        }
        return registered;
    }

    /**
     * 每连接两个独立任务：reader 只读/解析/预约，writer 只按序写。
     */
    bool StartSession(const std::shared_ptr<Session>& session)
    {
        if (!RegisterTask(
                [state = shared_from_this(), session] {
                    try {
                        state->ReaderLoop(session);
                    } catch (...) {
                        state->AbandonSession(session);
                    }
                    state->ReaderExit(session);
                },
                session,
                true)) {
            ReaderExit(session);
            WriterExit(session);
            AbandonSession(session);
            return false;
        }
        if (!RegisterTask(
                [state = shared_from_this(), session] {
                    try {
                        state->WriterLoop(session);
                    } catch (...) {
                        state->AbandonSession(session);
                    }
                    state->WriterExit(session);
                },
                session,
                true)) {

            /**
             * reader 已在跑：封连接让它退出；writer 责任在此显式补齐。
             */
            WriterExit(session);
            AbandonSession(session);
            return false;
        }
        return true;
    }

    void ReaderExit(const std::shared_ptr<Session>& session)
    {
        std::lock_guard<std::mutex> lock(mutex);
        session->reader_done = true;
        UpdateTaskDoneLocked(session);
        NotifyLocked();
    }

    void WriterExit(const std::shared_ptr<Session>& session)
    {
        std::lock_guard<std::mutex> lock(mutex);
        session->writer_done = true;
        UpdateTaskDoneLocked(session);
        NotifyLocked();
    }

    /**
     * inflight 的唯一减点：回复写终态、明确放弃（handler 已退出）或
     * Close(Immediate)（handler 已退出）才结算；handler 返回与任务对象销毁
     * 都不单独减，仍在执行的 handler 继续持有额度直到 HandlerExit。
     */
    void SettleLocked(const std::shared_ptr<RequestRecord>& record)
    {
        if (record->settled) {
            return;
        }
        record->settled = true;
        if (inflight > 0) {
            --inflight;
        }
    }

    /**
     * 放弃/关闭路径的结算：handler 协程仍存活时额度继续由它持有，不能把
     * max_inflight 名额提前放回给新连接/新请求；退出时由 HandlerExit 恰好结算一次。
     */
    void SettleAbandonedLocked(const std::shared_ptr<RequestRecord>& record)
    {
        if (record->handler_active) {
            return;
        }
        SettleLocked(record);
    }

    /**
     * handler 任务退出：清存活标记；若序号已被放弃/连接已关闭而其额度仍在，
     * 此时补结算。迟到结果已在 Publish 中按 abandoned/closed 丢弃，不触碰连接。
     */
    void HandlerExit(const std::shared_ptr<Session>& session, const std::shared_ptr<RequestRecord>& record)
    {
        std::lock_guard<std::mutex> lock(mutex);
        record->handler_active = false;
        if ((record->abandoned || session->closed) && !record->settled) {
            SettleLocked(record);
            NotifyLocked();
        }
    }

    /**
     * 放弃整条连接：所有未结算序号（含已完成未写）明确失败并唤醒，流源恰好
     * Cancel 一次；迟到结果只能释放自身，不再触碰连接。handler 协程仍存活的
     * 序号继续持有 inflight 名额，由 HandlerExit 在其退出时结算。
     */
    void AbandonSession(const std::shared_ptr<Session>& session)
    {
        std::vector<std::shared_ptr<ResponseBody>> cancels;
        {
            std::lock_guard<std::mutex> lock(mutex);
            session->no_more_requests = true;
            if (!session->closed) {
                session->closed = true;
                (void)session->connection.Close();
            }
            for (const auto& record : session->queue) {
                record->abandoned = true;
                if (record->source && !record->source_cancelled) {
                    record->source_cancelled = true;
                    cancels.push_back(record->source);
                }
                SettleAbandonedLocked(record);
            }
            session->queue.clear();
            NotifyLocked();
        }

        /**
         * Cancel 在锁外：实现可能挂起或回调，不能持锁调用未知代码。
         */
        for (const auto& source : cancels) {
            source->Cancel();
        }
    }

    void CloseSessionLocked(const std::shared_ptr<Session>& session)
    {
        session->no_more_requests = true;
        if (!session->closed) {
            session->closed = true;
            (void)session->connection.Close();
        }
    }

    /**
     * 读取/解析新请求前的接纳门：连接内待处理槽与全局 inflight 共用同一预算
     * （一个已接纳请求恰好占一个名额）。名额不足时暂停读 socket 与解析，内核
     * 缓冲形成背压，不丢已读字节、也不越过额度继续解析。
     */
    bool WaitAdmission(const std::shared_ptr<Session>& session, bbt::coroutine::Deadline deadline)
    {
        for (;;) {
            {
                std::lock_guard<std::mutex> lock(mutex);
                if (stopping || session->closed || session->no_more_requests) {
                    return false;
                }
                if (inflight < options.limits.max_inflight) {
                    return true;
                }
            }
            if (Clock::now() >= deadline) {
                return false;
            }
            auto waited =
                bbt::infra::detail::WaitCloseSlice(session->reader_wait, bbt::infra::detail::NextCloseSlice(deadline));
            if (!waited) {
                AbandonSession(session);
                return false;
            }
        }
    }

    bool IsSealed(const std::shared_ptr<Session>& session)
    {
        std::lock_guard<std::mutex> lock(mutex);
        return stopping || session->closed;
    }

    void Accept()
    {
        for (;;) {
            bool full;
            {
                std::lock_guard<std::mutex> lock(mutex);
                Sweep();
                if (stopping) {
                    return;
                }
                full = sessions.size() >= options.limits.max_connections;
            }
            if (full) {
                bbtco_sleep(5);
                continue;
            }

            /**
             * 在取得连接前准备物理记账槽；接纳后不再首次分配责任记录。
             */
            auto session = std::make_shared<Session>();
            const auto accept_result = engine->CoAccept({Clock::now() + 50ms});
            if (accept_result.error == std::errc::timed_out) {
                continue;
            }
            if (accept_result.error) {
                Stop(false);
                return;
            }
            session->connection = accept_result.connection.Raw();

            {
                std::lock_guard<std::mutex> lock(mutex);
                ++accepted;

                /**
                 * 唯一 accept 生产者：物理额度未回收不重用
                 */
                sessions.push_back(session);
                if (stopping) {
                    (void)session->connection.Close();
                    session->reader_done = true;
                    session->writer_done = true;
                    session->task_done = true;
                    return;
                }
            }
            try {
                StartSession(session);
            } catch (...) {
                AbandonSession(session);
            }
            bbtco_yield;
        }
    }

    void Stop(bool force, const Error* reason = nullptr)
    {
        std::vector<std::shared_ptr<ResponseBody>> cancels;
        {
            std::lock_guard<std::mutex> lock(mutex);
            stopping = true;
            closing = closing || force;
            if (reason && !close_reason) {
                close_reason = *reason;
            }
            for (const auto& session : sessions) {
                session->no_more_requests = true;
                if (force) {
                    if (!session->closed) {
                        session->closed = true;
                        (void)session->connection.Close();
                    }
                    for (const auto& record : session->queue) {
                        record->abandoned = true;
                        if (record->source && !record->source_cancelled) {
                            record->source_cancelled = true;
                            cancels.push_back(record->source);
                        }
                        SettleAbandonedLocked(record);
                    }
                    session->queue.clear();
                } else if (session->queue.empty()) {

                    /**
                     * 无已接纳责任的 idle/半包会话立即关闭；有责任的连接留给
                     * writer 按序写完，reader 读到封口后自行退出。
                     */
                    CloseSessionLocked(session);
                }
            }
            NotifyLocked();
        }

        /**
         * Cancel 在锁外：实现可能挂起或回调，不能持锁调用未知代码。
         */
        for (const auto& source : cancels) {
            source->Cancel();
        }
        engine->RequestStopAccepting();
    }

    /**
     * 受管协程里的单次分片等待。判定谓词由调用方在锁内复查，分片只负责让出
     * worker 并保证 50ms 内重新观察 stopping/deadline。
     */
    bool WaitSession(const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& waiter, bbt::coroutine::Deadline deadline)
    {
        auto waited = bbt::infra::detail::WaitCloseSlice(waiter, bbt::infra::detail::NextCloseSlice(deadline));
        return bool(waited);
    }

    bool Write(
        const std::shared_ptr<Session>& session,
        const std::string& bytes,
        bbt::coroutine::Deadline deadline,
        std::chrono::milliseconds per_write)
    {
        deadline = std::min(deadline, Clock::now() + per_write);
        std::size_t offset = 0;
        std::size_t chunk = 16 * 1024;
        while (offset < bytes.size()) {
            {
                std::lock_guard<std::mutex> lock(mutex);
                if (closing) {
                    return false;
                }
            }
            if (detail::CheckCall(deadline)) {
                return false;
            }
            const auto count = std::min(chunk, bytes.size() - offset);
            net::CoTcpConnection connection{session->connection};
            const auto report = connection.Write(bytes.substr(offset, count), {deadline});
            const auto& outcome = report.outcome;

            /**
             * 公开 Rejected + 无 operation = 零提交；有界减半探到 net 可接纳上限。
             * 不在已接纳/可能发送的路径重试，也不假设 Engine 默认写额度。
             */
            if (report.wait_error == std::errc::no_buffer_space && outcome.phase == net::WritePhase::Rejected
                && !report.operation.Valid() && count > 1) {
                chunk = count / 2;
                continue;
            }
            if (report.wait_error || outcome.phase != net::WritePhase::Done || outcome.error) {
                return false;
            }
            offset += count;
            bbtco_yield;
        }
        return true;
    }

    /**
     * 连接级 reader：唯一解析者、唯一序号分配者。名额不足时暂停读取；封口后
     * 停止接纳但不抢写，writer 仍负责已接纳序号。
     */
    void ReaderLoop(const std::shared_ptr<Session>& session)
    {
        std::string pending;
        for (;;) {
            {
                std::lock_guard<std::mutex> lock(mutex);
                if (session->closed || stopping || session->no_more_requests) {
                    return;
                }
            }
            const auto deadline = Clock::now() + options.limits.incoming_timeout;
            if (!WaitAdmission(session, deadline)) {

                /**
                 * 封口保留 writer 已接纳责任；期限到期/取消才放弃整条连接。
                 */
                if (!IsSealed(session)) {
                    AbandonSession(session);
                }
                return;
            }
            wire::request_parser<wire::string_body> parser;
            parser.header_limit(static_cast<std::uint32_t>(options.limits.max_header_bytes));
            parser.body_limit(options.limits.max_body_bytes);
            bool checked_header = false;
            bool parsed = false;

            /**
             * 已向本条请求的 parser 交付过字节
             */
            bool started = false;
            for (;;) {
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    if (session->closed || stopping) {

                        /**
                         * 封口：不发回复，交给 writer 排空已接纳序号
                         */
                        break;
                    }
                }
                if (detail::CheckCall(deadline)) {
                    break;
                }
                if (!pending.empty()) {
                    boost::system::error_code error;
                    const auto consumed = parser.put(boost::asio::buffer(pending), error);
                    pending.erase(0, consumed);
                    started = started || consumed != 0;
                    if (error && error != wire::error::need_more) {

                        /**
                         * 帧损坏：放弃整条连接
                         */
                        break;
                    }
                    if (parser.is_header_done() && !checked_header) {

                        /**
                         * eager=false 先停在 header：Expect 必须在等待 body 前拒绝。
                         */
                        if (!ValidRequestHead(parser.get(), options.max_target_bytes)) {
                            AbandonSession(session);
                            return;
                        }
                        checked_header = true;
                        parser.eager(true);
                    }
                    if (parser.is_done()) {
                        parsed = true;
                        break;
                    }
                    if (consumed) {
                        bbtco_yield;
                        continue;
                    }
                }
                net::CoTcpConnection connection{session->connection};
                auto [read_data, read_error] = connection.Read({deadline});
                if (bool(read_error)) {

                    /**
                     * 干净 EOF：本条请求尚未开始，已接纳序号交给 writer 排空后
                     * 关连接；半包 EOF/读错误才是帧损坏，整条连接明确放弃。
                     */
                    if (!started && pending.empty()) {
                        return;
                    }
                    AbandonSession(session);
                    return;
                }
                read_data.AppendTo(pending);
                bbtco_yield;
            }
            if (!parsed) {

                /**
                 * 封口只让 reader 退出（writer 继续排空）；解析/读取错误整条断开。
                 */
                if (!IsSealed(session)) {
                    AbandonSession(session);
                }
                return;
            }
            if (!ValidRequestHead(parser.get(), options.max_target_bytes)) {
                AbandonSession(session);
                return;
            }
            auto message = parser.release();
            IncomingRequest request;
            request.method = std::string(message.method_string());
            request.target = std::string(message.target());
            request.body = std::move(message.body());
            for (const auto& field : message) {
                request.headers.emplace_back(std::string(field.name_string()), std::string(field.value()));
            }

            /**
             * 解析完成后再占名额：已读字节留在本 reader 的 pending 里，不会因
             * 其它连接抢占额度而丢失，也不会把「正在读」误算成已授权请求。
             */
            std::shared_ptr<RequestRecord> record;
            bool sealed_after_parse = false;
            for (;;) {
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    if (session->closed || stopping) {
                        sealed_after_parse = true;
                        break;
                    }
                    if (inflight < options.limits.max_inflight) {
                        record = std::make_shared<RequestRecord>();
                        record->seq = session->next_seq++;
                        record->deadline = deadline;
                        record->head = message.method() == wire::verb::head;
                        record->keep_alive = message.keep_alive();
                        session->queue.push_back(record);
                        ++inflight;
                        ++requests;
                        if (!record->keep_alive) {
                            session->no_more_requests = true;
                        }
                        break;
                    }
                }
                if (!WaitAdmission(session, deadline)) {
                    if (!IsSealed(session)) {
                        AbandonSession(session);
                    }
                    return;
                }
            }
            if (sealed_after_parse) {

                /**
                 * 封口在解析后到达：丢弃本条，已接纳序号仍由 writer 排空
                 */
                return;
            }
            if (!Dispatch(session, record, std::move(request))) {
                AbandonSession(session);
                return;
            }

            /**
             * hot pipeline 每请求让出，不允许缓存命中无限占 worker
             */
            bbtco_yield;
        }
    }

    bool Dispatch(
        const std::shared_ptr<Session>& session, const std::shared_ptr<RequestRecord>& record, IncomingRequest request)
    {
        auto state = shared_from_this();
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            record->handler_active = true;
        }

        /**
         * handler 协程退出责任：放弃/关闭后由它补结算 inflight，保证额度不早于
         * 真实执行中的 handler 归还。放在任务体内而不是捕获列表里，避免
         * std::function 复制临时闭包时提前触发析构结算。
         */
        struct HandlerExitGuard {
            std::shared_ptr<State> state;
            std::shared_ptr<Session> session;
            std::shared_ptr<RequestRecord> record;

            ~HandlerExitGuard() { state->HandlerExit(session, record); }
        };

        bool registered = false;
        try {
            registered = RegisterTask(
                [state, session, record, request = std::move(request)]() mutable {
                    [[maybe_unused]] HandlerExitGuard exit_guard{state, session, record};
                    Response response;
                    std::shared_ptr<ResponseBody> source;
                    bool produced = false;
                    try {
                        if (state->streaming_handler) {
                            auto reply = state->streaming_handler({record->deadline, {}}, std::move(request));
                            if (reply) {
                                if (auto* buffered = std::get_if<Response>(&reply.value())) {
                                    response = std::move(*buffered);
                                    produced = true;
                                } else {
                                    source = std::get<std::shared_ptr<ResponseBody>>(std::move(reply.value()));
                                    produced = source != nullptr;
                                }
                            }
                        } else {
                            auto result = state->handler({record->deadline, {}}, std::move(request));

                            /**
                             * handler 错误隔离为 500，仍算一个响应；异常同样落地为 500。
                             */
                            response = result ? std::move(result.value()) : Response{500, {}, {}};
                            produced = true;
                        }
                    } catch (...) {
                        produced = false;
                    }
                    if (!produced) {
                        response = Response{500, {}, {}};
                        source.reset();
                    }
                    state->Publish(session, record, std::move(response), std::move(source));
                },
                session,
                false,
                true);
        } catch (...) {
            registered = false;
        }
        if (!registered) {

            /**
             * 任务未登记（闭包已销毁、body 不会执行）：撤销存活标记，让调用方
             * 的放弃路径按普通已结序号处理。
             */
            std::lock_guard<std::mutex> lock(state->mutex);
            record->handler_active = false;
        }
        return registered;
    }

    /**
     * 结果发布：只登记拥有型结果并唤醒 writer；inflight 不在这里减。
     */
    void Publish(
        const std::shared_ptr<Session>& session,
        const std::shared_ptr<RequestRecord>& record,
        Response response,
        std::shared_ptr<ResponseBody> source)
    {

        /**
         * 字节界在发布入队前校验：超限结果不得进入队列（丢弃该结果并按协议放弃
         * 连接），不能只靠队首写出时的检查——后序超大响应会让队列无界增长。
         */
        if (!source && ResponseExceedsLimits(response, options)) {
            AbandonSession(session);
            return;
        }
        bool cancel_now = false;
        {
            std::lock_guard<std::mutex> lock(mutex);
            record->response = std::move(response);
            record->source = source;
            record->complete = true;
            if (record->abandoned || session->closed) {
                if (source && !record->source_cancelled) {
                    record->source_cancelled = true;
                    cancel_now = true;
                }
            }
            NotifyLocked();
        }
        if (cancel_now) {
            source->Cancel();
        }
    }

    /**
     * 单顺序 writer：唯一写 socket、唯一拉取 ResponseBody 的角色。前序未完成时
     * 后续完成记录只在 queue 等待，不抢写。
     */
    void WriterLoop(const std::shared_ptr<Session>& session)
    {
        for (;;) {
            std::shared_ptr<RequestRecord> record;
            {
                std::lock_guard<std::mutex> lock(mutex);
                if (session->closed) {
                    return;
                }
                if (session->queue.empty()) {
                    if (session->reader_done) {
                        CloseSessionLocked(session);
                        NotifyLocked();
                        return;
                    }
                } else {
                    record = session->queue.front();
                }
            }
            if (!record) {
                if (!WaitSession(session->writer_wait, Clock::now() + 50ms)) {
                    AbandonSession(session);
                    return;
                }
                continue;
            }
            bool ready = false;
            bool wait_more = false;
            {
                std::lock_guard<std::mutex> lock(mutex);
                if (session->closed) {
                    return;
                }
                if (!record->abandoned && !closing) {
                    if (record->complete) {

                        /**
                         * 已完成但已过请求总期限：迟到结果不写，整条连接明确放弃。
                         */
                        ready = Clock::now() < record->deadline;
                    } else {
                        wait_more = Clock::now() < record->deadline;
                    }
                }
            }
            if (!ready) {
                if (wait_more) {
                    if (!WaitSession(session->writer_wait, Clock::now() + 50ms)) {
                        AbandonSession(session);
                        return;
                    }
                    continue;
                }

                /**
                 * 队首未完成且已过期限/已放弃：后续序号全部明确失败，不无限等待。
                 */
                AbandonSession(session);
                return;
            }
            const bool served = WriteRecord(session, record);
            std::vector<std::shared_ptr<ResponseBody>> cancels;
            bool close_after = false;
            {
                std::lock_guard<std::mutex> lock(mutex);
                SettleLocked(record);
                record->source.reset();
                auto it = std::find(session->queue.begin(), session->queue.end(), record);
                if (it != session->queue.end()) {
                    session->queue.erase(it);
                }
                close_after = !served || !record->keep_alive || (stopping && session->queue.empty());
                if (close_after) {
                    CloseSessionLocked(session);
                    for (const auto& rest : session->queue) {
                        rest->abandoned = true;
                        if (rest->source && !rest->source_cancelled) {
                            rest->source_cancelled = true;
                            cancels.push_back(rest->source);
                        }
                        SettleAbandonedLocked(rest);
                    }
                    session->queue.clear();
                }
                NotifyLocked();
            }
            for (const auto& source : cancels) {
                source->Cancel();
            }
            if (close_after) {
                return;
            }
        }
    }

    bool WriteRecord(const std::shared_ptr<Session>& session, const std::shared_ptr<RequestRecord>& record)
    {
        bool keep_alive = false;
        {
            std::lock_guard<std::mutex> lock(mutex);
            keep_alive = record->keep_alive && !session->closed;

            /**
             * 封口后仍要把全部已接纳序号写完；只有最后一条才宣告关闭，
             * 前序响应继续 keep-alive，避免客户端把后续 pipeline 回复当成非法。
             */
            if (stopping && session->queue.size() == 1) {
                keep_alive = false;
            }
        }
        if (detail::CheckCall(record->deadline)) {
            return false;
        }
        if (!record->source) {
            auto bytes = SerializeResponse(std::move(record->response), record->head, keep_alive, options);
            if (!bytes) {
                return false;
            }
            return Write(session, bytes.value(), record->deadline, options.write_timeout);
        }
        return WriteStream(session, record, keep_alive);
    }

    /**
     * commit 后切到独立流期限 min(stream_start + max_stream_lifetime, 之后每次
     * 写再取 stream_write_timeout 较早者)。commit 后源错误/超时/peer EOF 取消
     * 源并关闭连接（不重发、不写 trailer），只留本地诊断责任。
     */
    bool
    WriteStream(const std::shared_ptr<Session>& session, const std::shared_ptr<RequestRecord>& record, bool keep_alive)
    {
        bool cancel_now = false;
        {
            std::lock_guard<std::mutex> lock(mutex);

            /**
             * Close 可能已过 Stop() 的取消收集（注册前窗口）：补一次登记检查，
             * 与 Stop() 的「source 非空则取消」合起来保证恰好一次。
             */
            if ((closing || session->closed) && !record->source_cancelled) {
                record->source_cancelled = true;
                cancel_now = true;
            }
        }
        if (cancel_now) {
            record->source->Cancel();
            return false;
        }
        auto started = result<StreamStart>::err(MakeError(ErrorCode::InternalError, "stream start failed"));
        try {
            started = bbt_co_await record->source->Start({record->deadline});
        } catch (...) {
        }
        if (!started) {

            /**
             * 头提交前失败：经正常错误响应路径（500），不当作流错误。
             */
            auto bytes = SerializeResponse({500, {}, {}}, record->head, keep_alive, options);
            return bytes && Write(session, bytes.value(), record->deadline, options.write_timeout);
        }
        StreamStart& start = started.value();
        if (start.mode == StreamStart::Mode::Buffered) {
            Response response{start.status_code, std::move(start.headers), std::move(start.body)};
            auto bytes = SerializeResponse(std::move(response), record->head, keep_alive, options);
            if (!bytes) {
                CancelSource(record);
                return false;
            }
            if (!Write(session, bytes.value(), record->deadline, options.write_timeout)) {
                CancelSource(record);
                return false;
            }
            return true;
        }
        auto head_bytes =
            SerializeResponseHead(start.status_code, start.headers, record->head, true, keep_alive, options);
        if (!head_bytes) {
            CancelSource(record);
            return false;
        }

        /**
         * commit：头发出后进入 Streaming；流期限从此时起算。
         */
        if (!Write(session, head_bytes.value(), record->deadline, options.write_timeout)) {
            CancelSource(record);
            return false;
        }
        const auto stream_deadline = std::min(Clock::now() + streaming.max_stream_lifetime, record->deadline);
        auto write_chunk = [&](std::string_view data) {
            if (data.empty()) {
                return true;
            }
            if (data.size() > streaming.max_chunk_bytes) {
                return false;
            }
            const auto wire = EncodeChunk(data);
            return Write(session, wire, stream_deadline, streaming.stream_write_timeout);
        };
        if (!write_chunk(start.first_chunk)) {
            CancelSource(record);
            return false;
        }
        for (;;) {
            auto next =
                result<std::optional<std::string>>::err(MakeError(ErrorCode::InternalError, "stream next failed"));
            try {
                next = bbt_co_await record->source->Next({stream_deadline});
            } catch (...) {
            }

            /**
             * commit 后源错误：取消源、关连接
             */
            if (!next) {
                CancelSource(record);
                return false;
            }

            /**
             * EOF：0-chunk 收尾
             */
            if (!next.value()) {
                return FinishStream(session);
            }

            if (!write_chunk(next.value().value())) {
                CancelSource(record);
                return false;
            }
        }
    }

    bool FinishStream(const std::shared_ptr<Session>& session)
    {

        /**
         * 正常 EOF：写 0-chunk 终止流。
         */
        return Write(
            session, "0\r\n\r\n", Clock::now() + streaming.stream_write_timeout, streaming.stream_write_timeout);
    }

    void CancelSource(const std::shared_ptr<RequestRecord>& record)
    {
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (record->source_cancelled) {
                return;
            }
            record->source_cancelled = true;
        }
        record->source->Cancel();
    }
};

void ServerTestAccess::FailNextTask(Server& server)
{
    std::lock_guard<std::mutex> lock(server.m_state->mutex);
    server.m_state->fail_next_task = true;
}

void ServerTestAccess::SetRegistrationBarrierOnce(Server& server, std::function<void()> barrier)
{
    std::lock_guard<std::mutex> lock(server.m_state->mutex);
    server.m_state->register_barrier = std::move(barrier);
}

Server::Server(std::shared_ptr<State> state) noexcept
    : m_state(std::move(state))
{
}

Server::~Server() noexcept
{
    _BeginClose();
}

result<std::shared_ptr<Server>>
Server::Create(std::shared_ptr<net::Engine> engine, ServerHandler handler, ServerOptions options)
{
    using R = result<std::shared_ptr<Server>>;
    const auto valid = ValidateNetworkLimits(options.limits);
    if (!engine || !handler || !valid || options.handoff_capacity == 0
        || options.handoff_capacity > kNetworkLimitsMaxConnections || options.max_target_bytes == 0
        || options.max_target_bytes > options.limits.max_header_bytes || options.write_timeout <= 0ms
        || options.write_timeout > kNetworkLimitsMaxIncomingTimeout) {
        return R::err(MakeError(ErrorCode::InvalidArgument, "invalid HTTP Server options"));
    }
    if (engine->Phase() != net::EnginePhase::Idle) {
        return R::err(MakeError(ErrorCode::InvalidContext, "Server::Create requires Idle Engine"));
    }
    auto state = std::make_shared<State>();
    state->engine = std::move(engine);
    state->handler = std::move(handler);
    state->options = options;
    state->sessions.reserve(options.limits.max_connections);
    auto server = std::shared_ptr<Server>(new Server(state));
    if (state->engine->EnableAcceptHandoff(options.handoff_capacity)) {
        return R::err(MakeError(ErrorCode::InvalidContext, "accept handoff configuration failed"));
    }
    return R::ok(std::move(server));
}

result<std::shared_ptr<Server>> Server::CreateStreaming(
    std::shared_ptr<net::Engine> engine,
    StreamingServerHandler handler,
    ServerOptions options,
    StreamingOptions streaming)
{
    using R = result<std::shared_ptr<Server>>;
    const auto valid = ValidateNetworkLimits(options.limits);
    if (!engine || !handler || !valid || options.handoff_capacity == 0
        || options.handoff_capacity > kNetworkLimitsMaxConnections || options.max_target_bytes == 0
        || options.max_target_bytes > options.limits.max_header_bytes || options.write_timeout <= 0ms
        || options.write_timeout > kNetworkLimitsMaxIncomingTimeout || streaming.max_chunk_bytes == 0
        || streaming.max_chunk_bytes > kNetworkLimitsMaxBodyBytes || streaming.max_stream_lifetime <= 0ms
        || streaming.max_stream_lifetime > kNetworkLimitsMaxIncomingTimeout || streaming.stream_write_timeout <= 0ms
        || streaming.stream_write_timeout > kNetworkLimitsMaxIncomingTimeout) {
        return R::err(MakeError(ErrorCode::InvalidArgument, "invalid HTTP Server streaming options"));
    }
    if (engine->Phase() != net::EnginePhase::Idle) {
        return R::err(MakeError(ErrorCode::InvalidContext, "Server::CreateStreaming requires Idle Engine"));
    }
    auto state = std::make_shared<State>();
    state->engine = std::move(engine);
    state->streaming_handler = std::move(handler);
    state->options = options;
    state->streaming = streaming;
    state->sessions.reserve(options.limits.max_connections);
    auto server = std::shared_ptr<Server>(new Server(state));
    if (state->engine->EnableAcceptHandoff(options.handoff_capacity)) {
        return R::err(MakeError(ErrorCode::InvalidContext, "accept handoff configuration failed"));
    }
    return R::ok(std::move(server));
}

result<void> Server::Start()
{
    auto state = m_state;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (state->stopping || state->started) {
            return Failure(ErrorCode::Closed, "Server already started or sealed");
        }
        std::vector<net::Address> addresses;
        if (!g_scheduler->IsInitialized() || state->engine->Phase() != net::EnginePhase::Running
            || state->engine->LocalAddresses(addresses) || addresses.empty()) {
            return Failure(ErrorCode::InvalidContext, "Server::Start requires Running listening Engine");
        }
        state->started = true;
    }
    try {
        if (state->RegisterTask([state] { state->Accept(); })) {
            return result<void>::ok();
        }
    } catch (...) {
    }
    state->Stop(false);
    return Failure(ErrorCode::RuntimeUnavailable, "accept task registration failed");
}

void Server::StopAccepting() noexcept
{
    m_state->Stop(false);
}

result<void> Server::Close(const CloseOptions& options)
{
    auto state = m_state;
    if (const auto valid = bbt::infra::detail::ValidateCloseOptions(options); !valid) {
        return result<void>::err(valid.error());
    }
    const bool waits = options.mode == CloseMode::Graceful || options.wait_release;
    const auto domain = bbt::infra::detail::CurrentCloseWaitDomain();
    if (waits && domain == bbt::infra::detail::CloseWaitDomain::Invalid) {
        return Failure(ErrorCode::InvalidContext, "Server::Close requires coroutine or native host domain");
    }
    const auto self = bbt::coroutine::GetLocalCoroutineId();
    {
        std::lock_guard<std::mutex> lock(state->mutex);

        /**
         * 自等待拒绝先于任何封口副作用：handler/session 任务不能等自己结算。
         */
        if (waits && self != 0 && state->admitted_tasks.count(self) != 0) {
            return Failure(ErrorCode::InvalidContext, "Server::Close rejected from an admitted handler/session task");
        }
    }
    const auto reason = bbt::infra::detail::SettleCloseError(options.reason, "http server: closed by owner");
    if (options.mode == CloseMode::Immediate) {
        state->Stop(true, &reason);
        return bbt::infra::detail::FinishCloseWithRelease(
            options, [this](bbt::coroutine::Deadline deadline) { return _WaitReleased(deadline); });
    }
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->draining = true;
    }
    state->Stop(false, &reason);
    if (domain == bbt::infra::detail::CloseWaitDomain::NativeHost) {
        {
            std::unique_lock<std::mutex> lock(state->mutex);
            for (;;) {

                /**
                 * 已发生 Immediate 时先返回首因：在途责任已被明确放弃，不能再报排空成功。
                 */
                if (state->closing) {
                    return result<void>::err(
                        bbt::infra::detail::SettleCloseError(state->close_reason, "http server: closed"));
                }
                if (state->inflight == 0) {
                    break;
                }
                if (Clock::now() >= options.deadline) {
                    return Failure(ErrorCode::TimedOut, "graceful close deadline expired; responsibility retained");
                }
                state->cv.wait_until(lock, bbt::infra::detail::NextCloseSlice(options.deadline));
            }
        }
        return bbt::infra::detail::FinishCloseWithRelease(
            options, [this](bbt::coroutine::Deadline deadline) { return _WaitReleased(deadline); });
    }
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            if (state->closing) {
                return result<void>::err(
                    bbt::infra::detail::SettleCloseError(state->close_reason, "http server: closed"));
            }
            if (state->inflight == 0) {
                break;
            }
        }
        if (Clock::now() >= options.deadline) {
            return Failure(ErrorCode::TimedOut, "graceful close deadline expired; responsibility retained");
        }
        auto waited = bbt::infra::detail::WaitCloseSlice(
            state->close_signal, bbt::infra::detail::NextCloseSlice(options.deadline));
        if (!waited) {
            return result<void>::err(waited.error());
        }
    }
    return bbt::infra::detail::FinishCloseWithRelease(
        options, [this](bbt::coroutine::Deadline deadline) { return _WaitReleased(deadline); });
}

void Server::_BeginClose(Error reason) noexcept
{
    auto state = m_state;
    try {
        state->Stop(true, &reason);
    } catch (...) {

        /**
         * noexcept 契约：closing/close_reason 已在 Stop 的锁内先行写定，
         * 取消源时的分配失败不放大为 terminate。
         */
    }
}

std::optional<Error> Server::CloseReason() const noexcept
{
    auto state = m_state;
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->close_reason;
}

result<void> Server::_WaitReleased(bbt::coroutine::Deadline deadline)
{
    auto state = m_state;
    const auto domain = bbt::infra::detail::CurrentCloseWaitDomain();
    if (domain == bbt::infra::detail::CloseWaitDomain::Invalid) {
        return Failure(ErrorCode::InvalidContext, "WaitStopped requires coroutine or native host domain");
    }
    const auto self = bbt::coroutine::GetLocalCoroutineId();
    {
        std::lock_guard<std::mutex> lock(state->mutex);

        /**
         * 自等待拒绝先于封口状态判断：已接纳任务不能等自己回收。
         */
        if (self != 0 && state->admitted_tasks.count(self) != 0) {
            return Failure(ErrorCode::InvalidContext, "WaitStopped rejected from an admitted handler/session task");
        }
        if (!state->stopping) {
            return Failure(ErrorCode::InvalidArgument, "close the server before WaitStopped");
        }
    }
    if (domain == bbt::infra::detail::CloseWaitDomain::NativeHost) {
        std::unique_lock<std::mutex> lock(state->mutex);
        for (;;) {
            state->Sweep();
            if (state->tasks == 0 && state->sessions.empty() && state->engine->AcceptingStopped()) {
                return result<void>::ok();
            }
            if (Clock::now() >= deadline) {
                return Failure(ErrorCode::TimedOut, "Server physical stop deadline expired");
            }
            state->cv.wait_until(lock, bbt::infra::detail::NextCloseSlice(deadline));
        }
    }
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->Sweep();
            if (state->tasks == 0 && state->sessions.empty() && state->engine->AcceptingStopped()) {
                return result<void>::ok();
            }
        }
        if (Clock::now() >= deadline) {
            return Failure(ErrorCode::TimedOut, "Server physical stop deadline expired");
        }
        auto waited =
            bbt::infra::detail::WaitCloseSlice(state->close_signal, bbt::infra::detail::NextCloseSlice(deadline));
        if (!waited) {
            return result<void>::err(waited.error());
        }
    }
}

ServerStats Server::Stats() const noexcept
{
    auto state = m_state;
    std::lock_guard<std::mutex> lock(state->mutex);
    state->Sweep();
    std::size_t retiring = 0;
    for (const auto& session : state->sessions) {
        if (session->task_done || session->connection.Closed()) {
            ++retiring;
        }
    }
    return {state->accepted, state->requests, state->sessions.size(), retiring, state->inflight, state->tasks};
}

// clang-format off
}
// clang-format on

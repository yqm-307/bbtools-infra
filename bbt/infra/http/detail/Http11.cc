#include <bbt/infra/http/detail/Http11.hpp>

#include <algorithm>
#include <exception>
#include <utility>

#include <bbt/coroutine/coroutine.hpp>

namespace bbt::infra::http {
namespace detail {
namespace {

using R = result<Http11Connection::ExchangeResult>;

Error CallError(std::error_code cause, const char* message, bool committed) {
    const auto code = cause == std::errc::operation_canceled ? ErrorCode::Cancelled
                    : committed ? ErrorCode::OutcomeUnknown
                    : cause == std::errc::timed_out ? ErrorCode::TimedOut
                    : ErrorCode::TransportError;
    auto error = MakeError(code, std::string("http client: ") + message);
    if (committed) { error.request_phase = RequestPhase::RequestCommitted; }
    return error;
}

Error ProtocolError(const std::string& message) {
    auto error = MakeError(ErrorCode::ProtocolError, "http client: " + message);
    error.request_phase = RequestPhase::RequestCommitted;
    return error;
}

bbt::coroutine::Deadline DeadlineAt(const PhaseTimeout& phase,
                                   bbt::coroutine::Deadline total,
                                   std::chrono::steady_clock::time_point now) {
    const auto finite = phase.Finite();
    if (!finite) { return total; }
    // 先夹住可表示范围，避免调用方给超长毫秒档时溢出单调时钟。
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        bbt::coroutine::Deadline::max() - now);
    return *finite >= remaining ? total : std::min(total, now + *finite);
}

} // namespace

bbt::coroutine::Deadline StageDeadline(const PhaseTimeout& phase,
                                      bbt::coroutine::Deadline total) {
    return DeadlineAt(phase, total, std::chrono::steady_clock::now());
}

std::error_code CheckCall(bbt::coroutine::Deadline deadline) {
    const auto current = g_bbt_tls_coroutine_co;
    if (!current) { return std::make_error_code(std::errc::operation_not_permitted); }
    if (current->IsCancelRequested()) {
        return std::make_error_code(std::errc::operation_canceled);
    }
    if (std::chrono::steady_clock::now() >= deadline) {
        return std::make_error_code(std::errc::timed_out);
    }
    return {};
}

PhaseDeadlines ResolveDeadlines(const Timeout& effective,
                                bbt::coroutine::Deadline total,
                                std::chrono::steady_clock::time_point now) {
    return {effective, total, DeadlineAt(effective.pool, total, now)};
}

result<std::string> SerializeRequest(const PreparedRequest& request) {
    try {
        boost::beast::http::request<boost::beast::http::string_body> message{
            boost::beast::http::verb::unknown, request.target_for_wire, 11u};
        message.method_string(request.method);
        for (const auto& header : request.headers) { message.set(header.first, header.second); }
        message.body() = request.body;
        message.prepare_payload();
        std::string bytes;
        StringSink sink(bytes);
        boost::system::error_code error;
        boost::beast::http::write(sink, message, error);
        if (error) {
            return result<std::string>::err(
                MakeError(ErrorCode::InvalidArgument, "http client: " + error.message()));
        }
        return result<std::string>::ok(std::move(bytes));
    } catch (const std::exception& error) {
        // Beast 的字段/方法验证会抛异常；此时尚未借用连接、没有网络副作用。
        return result<std::string>::err(
            MakeError(ErrorCode::InvalidArgument, "http client: " + std::string(error.what())));
    }
}

result<void> Http11Connection::WriteRequest(const std::string& bytes,
                                            bbt::coroutine::Deadline deadline) {
    if (const auto gate = CheckCall(deadline)) {
        return result<void>::err(CallError(gate, "write rejected before submission", false));
    }
    net::WriteOutcome outcome;
    net::WriteOperation operation;
    const auto error = net::CoWrite(m_connection, bytes, {deadline}, outcome, operation);
    if (!error && outcome.phase == net::WritePhase::Done && !outcome.error) {
        return result<void>::ok();
    }
    const bool committed = outcome.phase == net::WritePhase::Done && !outcome.error;
    auto failure = CallError(error ? error : outcome.error, "write did not complete", committed);
    if (!committed) {
        failure.request_phase = RequestPhase::Writing;
        // 已接纳仍在发送，等待结束不等于撤销；不能诱导调用方当作零提交重试。
        if (operation.Valid() && outcome.phase == net::WritePhase::InFlight &&
            failure.code != ErrorCode::Cancelled) {
            failure.code = ErrorCode::OutcomeUnknown;
        }
    }
    return result<void>::err(std::move(failure));
}

result<Http11Connection::ExchangeResult> Http11Connection::ReadResponse(
    const PreparedRequest& request, const PhaseDeadlines& deadlines) {
    std::string pending;
    // HTTP/1.1 中间响应不交付给用户；有界拒绝无限 1xx 流。
    for (unsigned interim = 0; interim <= 16; ++interim) {
        boost::beast::http::response_parser<boost::beast::http::string_body> parser;
        parser.body_limit(m_max_body_bytes);
        parser.eager(true);
        parser.skip(request.method == "HEAD");
        while (!parser.is_done()) {
            if (const auto gate = CheckCall(deadlines.total)) {
                return R::err(CallError(gate, "response wait ended", true));
            }
            if (!pending.empty()) {
                boost::system::error_code error;
                const auto consumed = parser.put(boost::asio::buffer(pending), error);
                pending.erase(0, consumed);
                if (error && error != boost::beast::http::error::need_more) {
                    return R::err(ProtocolError("malformed response: " + error.message()));
                }
                if (parser.is_done()) { break; }
                // 有进展就先耗尽已有字节，不因任意次数上限等待永远不会到来的新包。
                if (consumed != 0) { continue; }
            }
            if (const auto gate = CheckCall(deadlines.total)) {
                return R::err(CallError(gate, "response read rejected", true));
            }
            std::string bytes;
            const auto error = net::CoRead(
                m_connection, bytes, {StageDeadline(deadlines.timeout.read, deadlines.total)});
            if (error == net::make_error_code(net::NetErrc::Eof)) {
                // Beast put_eof 前置 got_some()：对端一个响应字节都没发就断开
                // 时不能喂空 parser（会 abort）。请求已完整写出，回复丢失，按
                // 未知结果交付（请求可能已生效）。
                if (!parser.got_some()) {
                    return R::err(CallError(error, "peer closed without response", true));
                }
                boost::system::error_code eof;
                parser.put_eof(eof);
                if (eof || !parser.is_done()) {
                    return R::err(CallError(error, "peer closed with incomplete response", true));
                }
                break;
            }
            if (error) { return R::err(CallError(error, "response read failed", true)); }
            pending += bytes;
        }
        const auto status = parser.get().result_int();
        if (status == 101) { return R::err(ProtocolError("protocol upgrade unsupported")); }
        if (status >= 100 && status < 200) { continue; }
        ExchangeResult out;
        auto& message = parser.get();
        out.response.status_code = static_cast<int>(status);
        for (const auto& header : message) {
            out.response.headers.emplace_back(std::string(header.name_string()),
                                              std::string(header.value()));
        }
        out.response.body = std::move(message.body());
        // EOF 定界/尾随未消费数据绝不能回池污染下一请求。
        out.keep_alive = parser.keep_alive() && !parser.need_eof() && pending.empty();
        return R::ok(std::move(out));
    }
    return R::err(ProtocolError("too many informational responses"));
}

bbt_co_async(result<Http11Connection::ExchangeResult>) Http11Connection::Exchange(
    const PreparedRequest& request, const std::string& serialized,
    const PhaseDeadlines& deadlines) {
    if (auto written = WriteRequest(
            serialized, StageDeadline(deadlines.timeout.write, deadlines.total)); !written) {
        return R::err(std::move(written.error()));
    }
    return ReadResponse(request, deadlines);
}

} // namespace detail
} // namespace bbt::infra::http

#include <bbt/infra/redis/CoRedisCli.hpp>

#include <limits>
#include <mutex>
#include <utility>

#include <bbt/coroutine/coroutine.hpp>
#include <hiredis/hiredis.h>

#include <bbt/infra/net/CoConnection.hpp>
#include <bbt/infra/net/Error.hpp>
#include "redis/RedisDetail.hpp"

namespace bbt::infra::redis {
namespace {
using RawReply = redis_detail::RawReply;
using Reader = std::unique_ptr<redisReader, decltype(&redisReaderFree)>;
using Reply = std::unique_ptr<redisReply, decltype(&freeReplyObject)>;

Error ClosedError() { return MakeError(ErrorCode::Closed, "redis: client closed"); }
Error BusyError() { return MakeError(ErrorCode::Overloaded, "redis: operation already active"); }
Error ContextError() { return MakeError(ErrorCode::InvalidContext, "redis: coroutine context required"); }
Error ProtocolError() {
    auto error = MakeError(ErrorCode::ProtocolError, "redis: unexpected RESP2 reply");
    error.backend_category = "hiredis";
    error.backend_code = REDIS_ERR_PROTOCOL;
    return error;
}

const char* CauseName(ErrorCode code) {
    switch (code) {
    case ErrorCode::Closed: return "Closed";
    case ErrorCode::TimedOut: return "TimedOut";
    case ErrorCode::Cancelled: return "Cancelled";
    case ErrorCode::ProtocolError: return "ProtocolError";
    case ErrorCode::InvalidContext: return "InvalidContext";
    case ErrorCode::RuntimeUnavailable: return "RuntimeUnavailable";
    case ErrorCode::Overloaded: return "Overloaded";
    case ErrorCode::TransportError: return "TransportError";
    default: return "InternalError";
    }
}

Error NetError(std::error_code code, bool closed, bool runtime_unavailable = false) {
    ErrorCode mapped = ErrorCode::TransportError;
    if (closed || code == net::NetErrc::Closed) { mapped = ErrorCode::Closed; }
    else if (runtime_unavailable || code == std::errc::operation_not_supported) { mapped = ErrorCode::RuntimeUnavailable; }
    else if (code == std::errc::timed_out) { mapped = ErrorCode::TimedOut; }
    else if (code == std::errc::operation_canceled) { mapped = ErrorCode::Cancelled; }
    else if (code == std::errc::operation_not_permitted) { mapped = ErrorCode::InvalidContext; }
    else if (code == std::errc::invalid_argument) { mapped = ErrorCode::InvalidArgument; }
    else if (code == std::errc::no_buffer_space || code == std::errc::device_or_resource_busy ||
             code == net::NetErrc::Busy || code == net::NetErrc::WriteSettling) { mapped = ErrorCode::Overloaded; }
    auto error = MakeError(mapped, "redis net: " + code.message());
    error.backend_category = "net";
    error.backend_code = code.value();
    return error;
}

bool ExpectedReply(const std::string& command, const RawReply& reply) {
    if (command == "PING") { return reply.type == RawReply::Type::Status && reply.str == "PONG"; }
    if (command == "SET") { return reply.type == RawReply::Type::Status && reply.str == "OK"; }
    if (command == "GET") { return reply.type == RawReply::Type::Bulk || reply.type == RawReply::Type::Nil; }
    if (command == "EXISTS") { return reply.type == RawReply::Type::Integer && (reply.integer == 0 || reply.integer == 1); }
    return command == "DEL" && reply.type == RawReply::Type::Integer && reply.integer >= 0;
}
} // namespace

struct CoRedisCli::State {
    // mutex 保护接纳/发布/封口；活动操作独占 reader，Close 在 busy 时不能释放它。
    // 这一个位置同时串行化 Connect 与五个命令，锁永不跨 CoDial/CoWrite/CoRead。
    std::mutex mutex;
    const std::shared_ptr<net::Engine> engine;
    const net::Address address;
    net::Connection connection;
    Reader reader{redisReaderCreate(), &redisReaderFree};
    bool closed{false};
    bool busy{false};
    bool broken{false};

    State(std::shared_ptr<net::Engine> target, net::Address endpoint)
        : engine(std::move(target)), address(std::move(endpoint)) {}

    struct Active {
        State& state;
        ~Active() {
            std::lock_guard<std::mutex> lock(state.mutex);
            state.busy = false;
            if (state.closed || state.broken) { state.reader.reset(); }
        }
    };

    // 仅活动操作调用；先退役整条流，再发布失败。迟到回复永不进入下一次命令。
    Error Fail(Error cause, bool accepted) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            broken = true;
            connection.RequestClose();
            connection = {};
            reader.reset();
        }
        if (accepted) {
            cause.details.emplace_back("cause", CauseName(cause.code));
            cause.code = ErrorCode::OutcomeUnknown;
        }
        return cause;
    }

    result<void> Connect(CallOptions options) {
        if (bbt::coroutine::GetLocalCoroutineId() == 0) { return result<void>::err(ContextError()); }
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (closed) { return result<void>::err(ClosedError()); }
            // 已健康连接的幂等 Connect 不触碰 reader，也不占活动位置。
            if (!broken && connection.Valid() && !connection.Closed()) { return result<void>::ok(); }
            if (busy) { return result<void>::err(BusyError()); }
            busy = true;
            connection.RequestClose();
            connection = {};
        }
        Active active{*this};
        net::Connection local;
        const auto error = net::CoDial(*engine, address, local, {options.deadline});
        std::lock_guard<std::mutex> lock(mutex);
        if (closed) {
            local.RequestClose();
            return result<void>::err(ClosedError());
        }
        if (error) {
            return result<void>::err(NetError(error, false, engine->Phase() != net::EnginePhase::Running));
        }
        // 只有显式建连成功才恢复流；失败期间不发布任何可用连接。
        if (!reader) { reader.reset(redisReaderCreate()); }
        if (!reader) {
            local.RequestClose();
            return result<void>::err(MakeError(ErrorCode::InternalError, "redis: reader allocation failed"));
        }
        broken = false;
        connection = std::move(local);
        return result<void>::ok();
    }

    result<RawReply> Execute(std::vector<std::string> args, CallOptions options) {
        if (bbt::coroutine::GetLocalCoroutineId() == 0) { return result<RawReply>::err(ContextError()); }
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (closed) { return result<RawReply>::err(ClosedError()); }
        }
        // 参数拥有于本调用，编码后不借用调用者的 key/value/options。
        if ((args.size() == 1 && args[0] == "DEL") || args.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
            return result<RawReply>::err(MakeError(ErrorCode::InvalidArgument, "redis: invalid key list"));
        }
        for (std::size_t i = 1; i < args.size(); ++i) {
            if ((args[0] == "DEL" || i == 1) && args[i].empty()) {
                return result<RawReply>::err(MakeError(ErrorCode::InvalidArgument, "redis: empty key"));
            }
        }
        net::Connection local;
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (closed) { return result<RawReply>::err(ClosedError()); }
            if (busy) { return result<RawReply>::err(BusyError()); }
            if (broken || (connection.Valid() && connection.Closed())) {
                broken = true;
                connection.RequestClose();
                reader.reset();
                return result<RawReply>::err(MakeError(ErrorCode::TransportError, "redis: explicit Connect required"));
            }
            if (!connection.Valid()) {
                return result<RawReply>::err(MakeError(ErrorCode::RuntimeUnavailable, "redis: not connected"));
            }
            busy = true;
            local = connection;
        }
        Active active{*this};
        if (local.ReadableBytes() != 0) { return result<RawReply>::err(Fail(ProtocolError(), false)); }
        std::vector<const char*> argv;
        std::vector<std::size_t> lengths;
        for (const auto& arg : args) { argv.push_back(arg.data()); lengths.push_back(arg.size()); }
        char* formatted = nullptr;
        const auto size = redisFormatCommandArgv(&formatted, static_cast<int>(argv.size()), argv.data(), lengths.data());
        std::unique_ptr<char, decltype(&redisFreeCommand)> allocation(formatted, &redisFreeCommand);
        if (size < 0 || !formatted) {
            return result<RawReply>::err(MakeError(ErrorCode::InternalError, "redis: command allocation failed"));
        }
        const std::string command(formatted, static_cast<std::size_t>(size));
        const bbt::coroutine::WaitOptions wait{options.deadline};
        net::WriteOutcome outcome;
        net::WriteOperation operation;
        auto error = net::CoWrite(local, command, wait, outcome, operation);
        if (error) {
            bool sealed;
            { std::lock_guard<std::mutex> lock(mutex); sealed = closed; }
            auto cause = NetError(error, sealed);
            if (operation.Valid() || outcome.phase != net::WritePhase::Rejected) {
                return result<RawReply>::err(Fail(std::move(cause), true));
            }
            if (cause.code == ErrorCode::Closed || cause.code == ErrorCode::TransportError) {
                cause = Fail(std::move(cause), false);
            }
            return result<RawReply>::err(std::move(cause));
        }

        // CoRead 返回可用字节而非一条 RESP 帧。整个循环复用同一个绝对 deadline。
        for (;;) {
            std::string bytes;
            error = net::CoRead(local, bytes, wait);
            if (error) {
                bool sealed;
                { std::lock_guard<std::mutex> lock(mutex); sealed = closed; }
                return result<RawReply>::err(Fail(NetError(error, sealed), true));
            }
            if (redisReaderFeed(reader.get(), bytes.data(), bytes.size()) != REDIS_OK) {
                return result<RawReply>::err(Fail(redis_detail::ClassifyHiredisError(
                    reader->err, reader->errstr, "redis: reader feed failed"), true));
            }
            void* raw = nullptr;
            const auto parsed = redisReaderGetReply(reader.get(), &raw);
            Reply reply(static_cast<redisReply*>(raw), &freeReplyObject);
            if (parsed != REDIS_OK) {
                return result<RawReply>::err(Fail(redis_detail::ClassifyHiredisError(
                    reader->err, reader->errstr, "redis: reply parse failed"), true));
            }
            if (!reply) { continue; }
            // 无流水线：同批次回复之后的任何额外字节都没有请求归属。
            if (reader->len != reader->pos) { return result<RawReply>::err(Fail(ProtocolError(), true)); }
            auto decoded = redis_detail::DecodeReply(reply.get());
            if (!decoded) { return decoded; } // 完整 Redis ERROR 保持流同步。
            if (!ExpectedReply(args[0], decoded.value())) {
                return result<RawReply>::err(Fail(ProtocolError(), true));
            }
            return decoded;
        }
    }
};

CoRedisCli::CoRedisCli(std::shared_ptr<State> state) noexcept : state_(std::move(state)) {}
CoRedisCli::~CoRedisCli() noexcept { Close(); }

result<std::shared_ptr<CoRedisCli>> CoRedisCli::Create(std::shared_ptr<net::Engine> engine, net::Address address) {
    if (!engine || !address.Valid() ||
        (address.Family() != net::AddressFamily::IPv4 && address.Family() != net::AddressFamily::IPv6)) {
        return result<std::shared_ptr<CoRedisCli>>::err(MakeError(ErrorCode::InvalidArgument, "redis: Engine and IP address required"));
    }
    auto state = std::make_shared<State>(std::move(engine), std::move(address));
    if (!state->reader) {
        return result<std::shared_ptr<CoRedisCli>>::err(MakeError(ErrorCode::InternalError, "redis: reader allocation failed"));
    }
    return result<std::shared_ptr<CoRedisCli>>::ok(std::shared_ptr<CoRedisCli>(new CoRedisCli(std::move(state))));
}

void CoRedisCli::Close() noexcept {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    state->closed = true; // 与接纳和连接发布共用短锁的线性化点。
    state->connection.RequestClose();
    if (!state->busy) { state->reader.reset(); }
}
bool CoRedisCli::IsClosed() const noexcept {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->closed;
}
result<void> CoRedisCli::Connect(const CallOptions& options) {
    const auto state = state_; // 此后包括挂起恢复都不再访问公开对象。
    return state->Connect(options);
}
result<void> CoRedisCli::Ping(const CallOptions& options) {
    const auto state = state_;
    auto value = state->Execute({"PING"}, options);
    return value ? result<void>::ok() : result<void>::err(std::move(value.error()));
}
result<std::optional<std::string>> CoRedisCli::Get(std::string_view key, const CallOptions& options) {
    const auto state = state_;
    auto value = state->Execute({"GET", std::string(key)}, options);
    if (!value) { return result<std::optional<std::string>>::err(std::move(value.error())); }
    if (value.value().type == RawReply::Type::Nil) { return result<std::optional<std::string>>::ok(std::nullopt); }
    return result<std::optional<std::string>>::ok(std::move(value.value().str));
}
result<void> CoRedisCli::Set(std::string_view key, std::string_view value, const CallOptions& options) {
    const auto state = state_;
    auto reply = state->Execute({"SET", std::string(key), std::string(value)}, options);
    return reply ? result<void>::ok() : result<void>::err(std::move(reply.error()));
}
result<bool> CoRedisCli::Exists(std::string_view key, const CallOptions& options) {
    const auto state = state_;
    auto value = state->Execute({"EXISTS", std::string(key)}, options);
    if (!value) { return result<bool>::err(std::move(value.error())); }
    return result<bool>::ok(value.value().integer == 1);
}
result<std::uint64_t> CoRedisCli::Delete(std::vector<std::string> keys, const CallOptions& options) {
    const auto state = state_;
    keys.insert(keys.begin(), "DEL");
    auto value = state->Execute(std::move(keys), options);
    if (!value) { return result<std::uint64_t>::err(std::move(value.error())); }
    return result<std::uint64_t>::ok(static_cast<std::uint64_t>(value.value().integer));
}
} // namespace bbt::infra::redis

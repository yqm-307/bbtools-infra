#include <bbt/infra/redis/CoRedisCli.hpp>

#include <algorithm>
#include <limits>
#include <mutex>
#include <utility>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/syntax/Async.hpp>
#include <hiredis/hiredis.h>

#include <bbt/infra/NetError.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/ICoConnection.hpp>
#include "detail/RedisDetail.hpp"

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

// 底层分类交给公共桥 NetToError；closed/runtime_unavailable 是 Redis
// 客户端自身事实（close 封口、engine 非 Running），桥无从知晓，仍在此覆盖。
Error NetError(std::error_code code, bool closed, bool runtime_unavailable = false) {
    auto error = NetToError(code, "redis net: " + code.message());
    // 优先级与旧映射一致：Closed（含 NetErrc::Closed，桥已给）> RuntimeUnavailable。
    if (closed) { error.code = ErrorCode::Closed; }
    else if (runtime_unavailable && error.code != ErrorCode::Closed) {
        error.code = ErrorCode::RuntimeUnavailable;
    }
    return error;
}

// ExpectedReply 判定「回复形态是否属于该命令」。set_nil_ok 只用于带选项
// 的 SET：NX/XX 条件不满足时服务端回 nil，是合法终态而非协议错误。
bool ExpectedReply(const std::string& command, const RawReply& reply, bool set_nil_ok) {
    if (command == "PING") { return reply.type == RawReply::Type::Status && reply.str == "PONG"; }
    if (command == "SET") {
        if (reply.type == RawReply::Type::Status && reply.str == "OK") { return true; }
        return set_nil_ok && reply.type == RawReply::Type::Nil;
    }
    if (command == "GET" || command == "HGET") {
        return reply.type == RawReply::Type::Bulk || reply.type == RawReply::Type::Nil;
    }
    if (command == "EXISTS" || command == "HEXISTS") {
        return reply.type == RawReply::Type::Integer && (reply.integer == 0 || reply.integer == 1);
    }
    if (command == "DEL" || command == "HDEL") {
        return reply.type == RawReply::Type::Integer && reply.integer >= 0;
    }
    if (command == "PEXPIRE") {
        return reply.type == RawReply::Type::Integer && (reply.integer == 0 || reply.integer == 1);
    }
    if (command == "PTTL" || command == "INCR" || command == "DECR" ||
        command == "INCRBY" || command == "DECRBY" || command == "HSET") {
        return reply.type == RawReply::Type::Integer;
    }
    // HGETALL 契约是 field/value 对：除一维数组外还要求元素数为偶数且
    // 无 nil 元素；奇数或含 nil 的完整数组同样按协议错误退役（fail-close，
    // 不静默丢尾或把 nil 折叠成空串）。
    if (command == "HGETALL") {
        if (reply.type != RawReply::Type::Array) { return false; }
        if (reply.elements.size() % 2 != 0) { return false; }
        return std::none_of(reply.elements.begin(), reply.elements.end(),
                            [](const RawReply::Element& e) { return e.is_nil; });
    }
    if (command == "AUTH" || command == "SELECT") {
        return reply.type == RawReply::Type::Status && reply.str == "OK";
    }
    return false;
}
} // namespace

struct CoRedisCli::State {
    // mutex 保护接纳/发布/封口；活动操作独占 reader，Close 在 busy 时不能释放它。
    // 这一个位置同时串行化 Connect 与五个命令，锁永不跨 CoDial/Write/Read。
    std::mutex mutex;
    const std::shared_ptr<net::Engine> engine;
    const net::Address address;
    const RedisHandshake handshake;
    net::Connection connection;
    Reader reader{redisReaderCreate(), &redisReaderFree};
    bool closed{false};
    bool busy{false};
    bool broken{false};

    State(std::shared_ptr<net::Engine> target, net::Address endpoint,
          RedisHandshake shake)
        : engine(std::move(target)), address(std::move(endpoint)),
          handshake(std::move(shake)) {}

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

    bbt_co_async(result<void>) Connect(CallOptions options) {
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
        auto dialed = engine->CoDial(address, {options.deadline});
        net::Connection local;
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (closed) {
                dialed.connection.RequestClose();
                return result<void>::err(ClosedError());
            }
            if (dialed.error) {
                return result<void>::err(
                    NetError(dialed.error, false, engine->Phase() != net::EnginePhase::Running));
            }
            // 只有显式建连成功才恢复流；失败期间不发布任何可用连接。
            if (!reader) { reader.reset(redisReaderCreate()); }
            if (!reader) {
                dialed.connection.RequestClose();
                return result<void>::err(MakeError(ErrorCode::InternalError, "redis: reader allocation failed"));
            }
            broken = false;
            connection = dialed.connection.Raw();
            // 握手全程用锁内快照的本地句柄：Close 可能与握手 I/O 并发，不在
            // 挂起恢复后读写共享成员 connection；句柄共享同一底层状态，
            // Close 对成员的 RequestClose 仍会封掉这条连接的在途 I/O。
            local = connection;
        }
        // 连接级握手：配置后在每次建连（含显式重连）内按 AUTH 再 SELECT
        // 顺序各等 +OK。仍占本次活动位置与同一绝对 deadline，不流水线；
        // 锁不跨挂起——握手 I/O 在锁外执行。
        // 失败返回原样错误（RemoteError 不回密码），随后标记 broken 使后续
        // 命令按「显式 Connect required」处理；未配置时零动作。
        if (const auto outcome = bbt_co_await RunHandshake(options, local); !outcome) {
            // 握手失败即退役连接：显式置 broken，后续命令按
            // 「explicit Connect required」处理，client 仍可 Close。
            {
                std::lock_guard<std::mutex> lock(mutex);
                broken = true;
                connection.RequestClose();
                connection = {};
                reader.reset();
            }
            return result<void>::err(outcome.error());
        }
        // 握手成功后的线性化复查：Close 在握手期间胜出时 Connect 不得返回
        // 成功——与 Close 共用同一状态锁的线性化点，退役本次已发布连接。
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (closed) {
                broken = true;
                connection.RequestClose();
                connection = {};
                reader.reset();
                return result<void>::err(ClosedError());
            }
        }
        return result<void>::ok();
    }

    // 仅在 Connect 的活动位置内调用；非空结果表示握手失败（错误原样上抛，
    // 随后统一置 broken）。错误消息与 details 不含密码。local 为发布锁内
    // 快照的本地句柄，握手期间不再访问共享成员 connection。
    bbt_co_async(result<void>) RunHandshake(const CallOptions& options,
                                            const net::Connection& local) {
        if (!handshake.password.empty()) {
            std::vector<std::string> args{"AUTH"};
            if (!handshake.username.empty()) { args.push_back(handshake.username); }
            args.push_back(handshake.password);
            auto reply = bbt_co_await RedisRoundtrip(std::move(args), options, false, local);
            if (!reply) { return result<void>::err(std::move(reply.error())); }
        }
        if (handshake.database >= 0) {
            auto reply = bbt_co_await RedisRoundtrip(
                {"SELECT", std::to_string(handshake.database)}, options, false, local);
            if (!reply) { return result<void>::err(std::move(reply.error())); }
        }
        return result<void>::ok();
    }

    // 已接纳的一次命令全量写 + 分帧读一条完整回复：编码、写、读循环、解析
    // 与错误分类集中于此；失败按现有规则退役整条流（Fail）。调用方须已
    // 置 busy 并持本连接 local 句柄（Connect 握手与 Execute 共用本函数）。
    // set_nil_ok 见 ExpectedReply：带选项 SET 的 nil 回复是合法终态。
    bbt_co_async(result<RawReply>) RedisRoundtrip(std::vector<std::string> args,
                                                const CallOptions& options,
                                                bool set_nil_ok,
                                                net::Connection local) {
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
        net::CoTcpConnection co{local};
        const auto report = co.Write(command, wait);
        if (report.wait_error) {
            bool sealed;
            { std::lock_guard<std::mutex> lock(mutex); sealed = closed; }
            auto cause = NetError(report.wait_error, sealed);
            if (report.operation.Valid() ||
                report.outcome.phase != net::WritePhase::Rejected) {
                return result<RawReply>::err(Fail(std::move(cause), true));
            }
            if (cause.code == ErrorCode::Closed || cause.code == ErrorCode::TransportError) {
                cause = Fail(std::move(cause), false);
            }
            return result<RawReply>::err(std::move(cause));
        }

        // Read 交付一次可用字节而非一条 RESP 帧。整个循环复用同一个绝对 deadline。
        for (;;) {
            const auto read = co.Read(wait);
            if (read.state != net::ReadState::Data) {
                bool sealed;
                { std::lock_guard<std::mutex> lock(mutex); sealed = closed; }
                // EOF 与错误统一按流断裂处理：Eof 时 error 为空，补 NetErrc::Eof。
                const auto cause = read.state == net::ReadState::Eof
                    ? net::make_error_code(net::NetErrc::Eof) : read.error;
                return result<RawReply>::err(Fail(NetError(cause, sealed), true));
            }
            const auto bytes = read.data.CopyTo();
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
            if (!decoded) {
                if (decoded.error().code == ErrorCode::RemoteError) { return decoded; } // 完整 Redis ERROR 保持流同步。
                return result<RawReply>::err(Fail(std::move(decoded.error()), true));
            }
            if (!ExpectedReply(args[0], decoded.value(), set_nil_ok)) {
                return result<RawReply>::err(Fail(ProtocolError(), true));
            }
            return decoded;
        }
    }

    // 参数先全量校验再锁内接纳；set_nil_ok 透传给 RedisRoundtrip 的
    // ExpectedReply（见上）。HDEL 与 DEL 一样要求列表非空。
    bbt_co_async(result<RawReply>) Execute(std::vector<std::string> args,
                                         CallOptions options,
                                         bool set_nil_ok = false) {
        if (bbt::coroutine::GetLocalCoroutineId() == 0) { return result<RawReply>::err(ContextError()); }
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (closed) { return result<RawReply>::err(ClosedError()); }
        }
        // 参数拥有于本调用，编码后不借用调用者的 key/value/options。
        // args[0] 是命令名；以下参数（i>=1）才做 key/field 空值校验：
        //   - DEL/HDEL：全部后续参数为 key/field，须非空且列表非空；
        //   - HSET/HGET/HEXISTS：args[1] key 与 args[2] field 都须非空；
        //   - 其余命令：第一个参数（key）须非空，value/delta 等允许空串。
        const bool list_command = args[0] == "DEL" || args[0] == "HDEL";
        if ((args.size() == 1 && list_command) ||
            args.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
            return result<RawReply>::err(MakeError(ErrorCode::InvalidArgument, "redis: invalid key list"));
        }
        const bool hash_field_command =
            args[0] == "HSET" || args[0] == "HGET" || args[0] == "HEXISTS";
        for (std::size_t i = 1; i < args.size(); ++i) {
            if (!args[i].empty()) { continue; }
            if (list_command || i == 1 || (hash_field_command && i == 2)) {
                return result<RawReply>::err(MakeError(ErrorCode::InvalidArgument,
                    "redis: empty key or field"));
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
        return bbt_co_await RedisRoundtrip(std::move(args), options, set_nil_ok, std::move(local));
    }
};

CoRedisCli::CoRedisCli(std::shared_ptr<State> state) noexcept : state_(std::move(state)) {}
CoRedisCli::~CoRedisCli() noexcept { Close(); }

result<std::shared_ptr<CoRedisCli>> CoRedisCli::Create(std::shared_ptr<net::Engine> engine, net::Address address) {
    return Create(std::move(engine), std::move(address), RedisHandshake{});
}

result<std::shared_ptr<CoRedisCli>> CoRedisCli::Create(std::shared_ptr<net::Engine> engine, net::Address address, RedisHandshake handshake) {
    if (!engine || !address.Valid() ||
        (address.Family() != net::AddressFamily::IPv4 && address.Family() != net::AddressFamily::IPv6)) {
        return result<std::shared_ptr<CoRedisCli>>::err(MakeError(ErrorCode::InvalidArgument, "redis: Engine and IP address required"));
    }
    if (handshake.database < -1 || handshake.database > kMaxDatabase) {
        return result<std::shared_ptr<CoRedisCli>>::err(MakeError(ErrorCode::InvalidArgument, "redis: handshake database out of range"));
    }
    auto state = std::make_shared<State>(std::move(engine), std::move(address), std::move(handshake));
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
bool CoRedisCli::StreamHealthy() const noexcept {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    return !state->closed && !state->broken && state->connection.Valid() && !state->connection.Closed();
}
bool CoRedisCli::IsClosed() const noexcept {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->closed;
}
bbt_co_async(result<void>) CoRedisCli::Connect(const CallOptions& options) {
    const auto state = state_; // 此后包括挂起恢复都不再访问公开对象。
    return bbt_co_await state->Connect(options);
}
bbt_co_async(result<void>) CoRedisCli::Ping(const CallOptions& options) {
    const auto state = state_;
    auto value = bbt_co_await state->Execute({"PING"}, options);
    return value ? result<void>::ok() : result<void>::err(std::move(value.error()));
}
bbt_co_async(result<std::optional<std::string>>) CoRedisCli::Get(std::string_view key, const CallOptions& options) {
    const auto state = state_;
    auto value = bbt_co_await state->Execute({"GET", std::string(key)}, options);
    if (!value) { return result<std::optional<std::string>>::err(std::move(value.error())); }
    if (value.value().type == RawReply::Type::Nil) { return result<std::optional<std::string>>::ok(std::nullopt); }
    return result<std::optional<std::string>>::ok(std::move(value.value().str));
}
bbt_co_async(result<void>) CoRedisCli::Set(std::string_view key, std::string_view value, const CallOptions& options) {
    const auto state = state_;
    auto reply = bbt_co_await state->Execute({"SET", std::string(key), std::string(value)}, options);
    return reply ? result<void>::ok() : result<void>::err(std::move(reply.error()));
}
bbt_co_async(result<bool>) CoRedisCli::Exists(std::string_view key, const CallOptions& options) {
    const auto state = state_;
    auto value = bbt_co_await state->Execute({"EXISTS", std::string(key)}, options);
    if (!value) { return result<bool>::err(std::move(value.error())); }
    return result<bool>::ok(value.value().integer == 1);
}
bbt_co_async(result<std::uint64_t>) CoRedisCli::Delete(std::vector<std::string> keys, const CallOptions& options) {
    const auto state = state_;
    keys.insert(keys.begin(), "DEL");
    auto value = bbt_co_await state->Execute(std::move(keys), options);
    if (!value) { return result<std::uint64_t>::err(std::move(value.error())); }
    return result<std::uint64_t>::ok(static_cast<std::uint64_t>(value.value().integer));
}

bbt_co_async(result<bool>) CoRedisCli::Set(std::string_view key, std::string_view value,
                                           const SetOptions& set_options, const CallOptions& options) {
    const auto state = state_;
    // 选项先本地校验再编码：越界零接纳（InvalidArgument），什么都不发。
    if (set_options.expire &&
        (*set_options.expire <= std::chrono::milliseconds{0} ||
         *set_options.expire > kMaxSetExpire)) {
        return result<bool>::err(MakeError(ErrorCode::InvalidArgument, "redis: SET expire out of range"));
    }
    std::vector<std::string> args{"SET", std::string(key), std::string(value)};
    if (set_options.expire) {
        args.emplace_back("PX");
        args.push_back(std::to_string(set_options.expire->count()));
    }
    switch (set_options.condition) {
    case SetOptions::Condition::NX: args.emplace_back("NX"); break;
    case SetOptions::Condition::XX: args.emplace_back("XX"); break;
    case SetOptions::Condition::None: break;
    }
    auto reply = bbt_co_await state->Execute(std::move(args), options, true);
    if (!reply) { return result<bool>::err(std::move(reply.error())); }
    // nil 回复表示条件不满足（NX/XX），是合法终态而非错误。
    return result<bool>::ok(reply.value().type == RawReply::Type::Status);
}

bbt_co_async(result<bool>) CoRedisCli::Expire(std::string_view key, std::chrono::milliseconds ttl,
                                              const CallOptions& options) {
    const auto state = state_;
    if (ttl <= std::chrono::milliseconds{0}) {
        return result<bool>::err(MakeError(ErrorCode::InvalidArgument, "redis: expire must be positive"));
    }
    auto value = bbt_co_await state->Execute(
        {"PEXPIRE", std::string(key), std::to_string(ttl.count())}, options);
    if (!value) { return result<bool>::err(std::move(value.error())); }
    return result<bool>::ok(value.value().integer == 1);
}

bbt_co_async(result<TtlResult>) CoRedisCli::Ttl(std::string_view key, const CallOptions& options) {
    const auto state = state_;
    auto value = bbt_co_await state->Execute({"PTTL", std::string(key)}, options);
    if (!value) { return result<TtlResult>::err(std::move(value.error())); }
    const auto ms = value.value().integer;
    if (ms == -2) { return result<TtlResult>::ok({TtlResult::State::KeyMissing, {}}); }
    if (ms == -1) { return result<TtlResult>::ok({TtlResult::State::NoExpiry, {}}); }
    if (ms < 0) { return result<TtlResult>::err(MakeError(ErrorCode::ProtocolError, "redis: unexpected PTTL value")); }
    return result<TtlResult>::ok({TtlResult::State::Expires, std::chrono::milliseconds{ms}});
}

bbt_co_async(result<std::int64_t>) CoRedisCli::Incr(std::string_view key, const CallOptions& options) {
    const auto state = state_;
    auto value = bbt_co_await state->Execute({"INCR", std::string(key)}, options);
    if (!value) { return result<std::int64_t>::err(std::move(value.error())); }
    return result<std::int64_t>::ok(value.value().integer);
}
bbt_co_async(result<std::int64_t>) CoRedisCli::Decr(std::string_view key, const CallOptions& options) {
    const auto state = state_;
    auto value = bbt_co_await state->Execute({"DECR", std::string(key)}, options);
    if (!value) { return result<std::int64_t>::err(std::move(value.error())); }
    return result<std::int64_t>::ok(value.value().integer);
}
bbt_co_async(result<std::int64_t>) CoRedisCli::IncrBy(std::string_view key, std::int64_t delta, const CallOptions& options) {
    const auto state = state_;
    auto value = bbt_co_await state->Execute({"INCRBY", std::string(key), std::to_string(delta)}, options);
    if (!value) { return result<std::int64_t>::err(std::move(value.error())); }
    return result<std::int64_t>::ok(value.value().integer);
}
bbt_co_async(result<std::int64_t>) CoRedisCli::DecrBy(std::string_view key, std::int64_t delta, const CallOptions& options) {
    const auto state = state_;
    auto value = bbt_co_await state->Execute({"DECRBY", std::string(key), std::to_string(delta)}, options);
    if (!value) { return result<std::int64_t>::err(std::move(value.error())); }
    return result<std::int64_t>::ok(value.value().integer);
}

bbt_co_async(result<bool>) CoRedisCli::HSet(std::string_view key, std::string_view field,
                                            std::string_view value, const CallOptions& options) {
    const auto state = state_;
    auto reply = bbt_co_await state->Execute(
        {"HSET", std::string(key), std::string(field), std::string(value)}, options);
    if (!reply) { return result<bool>::err(std::move(reply.error())); }
    return result<bool>::ok(reply.value().integer == 1);
}
bbt_co_async(result<std::optional<std::string>>) CoRedisCli::HGet(std::string_view key,
                                                                   std::string_view field,
                                                                   const CallOptions& options) {
    const auto state = state_;
    auto value = bbt_co_await state->Execute({"HGET", std::string(key), std::string(field)}, options);
    if (!value) { return result<std::optional<std::string>>::err(std::move(value.error())); }
    if (value.value().type == RawReply::Type::Nil) { return result<std::optional<std::string>>::ok(std::nullopt); }
    return result<std::optional<std::string>>::ok(std::move(value.value().str));
}
bbt_co_async(result<std::uint64_t>) CoRedisCli::HDel(std::string_view key, std::vector<std::string> fields,
                                                     const CallOptions& options) {
    const auto state = state_;
    if (fields.empty()) {
        return result<std::uint64_t>::err(MakeError(ErrorCode::InvalidArgument, "redis: empty field list"));
    }
    std::vector<std::string> args;
    args.reserve(fields.size() + 2);
    args.emplace_back("HDEL");
    args.emplace_back(key);
    for (auto& field : fields) { args.push_back(std::move(field)); }
    auto value = bbt_co_await state->Execute(std::move(args), options);
    if (!value) { return result<std::uint64_t>::err(std::move(value.error())); }
    return result<std::uint64_t>::ok(static_cast<std::uint64_t>(value.value().integer));
}
bbt_co_async(result<bool>) CoRedisCli::HExists(std::string_view key, std::string_view field,
                                               const CallOptions& options) {
    const auto state = state_;
    auto value = bbt_co_await state->Execute({"HEXISTS", std::string(key), std::string(field)}, options);
    if (!value) { return result<bool>::err(std::move(value.error())); }
    return result<bool>::ok(value.value().integer == 1);
}
bbt_co_async(result<HashEntries>) CoRedisCli::HGetAll(std::string_view key, const CallOptions& options) {
    const auto state = state_;
    auto value = bbt_co_await state->Execute({"HGETALL", std::string(key)}, options);
    if (!value) { return result<HashEntries>::err(std::move(value.error())); }
    const auto& elements = value.value().elements;
    HashEntries entries;
    entries.reserve(elements.size() / 2);
    for (std::size_t i = 0; i + 1 < elements.size(); i += 2) {
        entries.emplace_back(elements[i].str, elements[i + 1].str);
    }
    // 奇数/含 nil 的数组已被 ExpectedReply 拒绝并退役连接（不会到达这里）：
    // 能进配对循环的元素必定偶数且非 nil。
    return result<HashEntries>::ok(std::move(entries));
}
} // namespace bbt::infra::redis

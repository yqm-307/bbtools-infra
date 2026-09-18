// N0 公共契约最小验证：result 表面、Error.details 校验、NetworkLimits 校验、
// Codec<void>/Codec<std::string>、ICoObject/ICoNetwork/ICoCloseable 可派生。
// 无第三方测试框架依赖，失败时输出到 stderr 并以非零退出。

#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <bbt/infra/Codec.hpp>
#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/ICoNetwork.hpp>
#include <bbt/infra/ICoObject.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

namespace {

int g_checks   = 0;
int g_failures = 0;

void Check(bool cond, const char* expr, const char* file, int line) {
    ++g_checks;
    if (!cond) {
        ++g_failures;
        std::fprintf(stderr, "FAIL %s:%d: %s\n", file, line, expr);
    }
}

#define CHECK(cond) Check(static_cast<bool>(cond), #cond, __FILE__, __LINE__)

using namespace bbt::infra;

void TestResultOkErrVoid() {
    // ok 分支：&/const&/&& 访问
    auto ok = result<int>::ok(42);
    CHECK(ok);
    CHECK(ok.value() == 42);
    const auto& cok = ok;
    CHECK(cok.value() == 42);
    CHECK(std::move(ok).value() == 42);

    bool threw = false;
    try { (void)cok.error(); }
    catch (const std::logic_error&) { threw = true; }
    CHECK(threw);

    // err 分支
    auto bad = result<int>::err(MakeError(ErrorCode::Unavailable, "nope"));
    CHECK(!bad);
    CHECK(bad.error().code == ErrorCode::Unavailable);
    const auto& cbad = bad;
    CHECK(cbad.error().message == "nope");

    threw = false;
    try { (void)cbad.value(); }
    catch (const std::logic_error&) { threw = true; }
    CHECK(threw);

    // move-only T 支持
    auto ro = result<std::unique_ptr<int>>::ok(std::make_unique<int>(7));
    CHECK(ro);
    CHECK(*std::move(ro).value() == 7);
    static_assert(!std::is_copy_constructible_v<result<std::unique_ptr<int>>>);
    static_assert(std::is_copy_constructible_v<result<int>>);

    // void 特化
    auto vok = result<void>::ok();
    CHECK(vok);
    auto verr = result<void>::err(MakeError(ErrorCode::Closed, "gone"));
    CHECK(!verr);
    CHECK(verr.error().code == ErrorCode::Closed);
    const auto& cverr = verr;
    CHECK(cverr.error().code == ErrorCode::Closed);

    threw = false;
    try { (void)vok.error(); }
    catch (const std::logic_error&) { threw = true; }
    CHECK(threw);
}

Error MakeDomainError(std::string domain, ErrorDetails details) {
    Error e = MakeError(ErrorCode::ProtocolError, "test");
    e.domain  = std::move(domain);
    e.details = std::move(details);
    return e;
}

// 取 error() 需要 result 为 err 分支；调用前已 CHECK(!r)。
ErrorCode ErrCodeOf(result<void>& r) { return r.error().code; }

void TestErrorDetails() {
    // 合法：恰好 16 项，键 64 字节，值 256 字节，含多字节 UTF-8
    ErrorDetails good;
    for (int i = 0; i < 16; ++i)
        good.emplace_back("k" + std::to_string(i), "v");
    good[0].first  = std::string(kMaxKeyBytes, 'k');
    good[0].second = std::string(kMaxValueBytes, 'v');
    good[1]        = {"键", "值"};
    auto e = MakeDomainError(std::string(kErrorDomainInfra), good);
    auto r = ValidateErrorDetails(e);
    CHECK(r);

    // 17 项拒绝
    ErrorDetails too_many = good;
    too_many.emplace_back("extra", "v");
    e.details = too_many;
    r = ValidateErrorDetails(e);
    CHECK(!r);
    if (!r) CHECK(ErrCodeOf(r) == ErrorCode::ProtocolError);

    // 键 65 字节拒绝
    e.details = ErrorDetails{{std::string(kMaxKeyBytes + 1, 'k'), "v"}};
    r = ValidateErrorDetails(e);
    CHECK(!r);
    if (!r) CHECK(ErrCodeOf(r) == ErrorCode::ProtocolError);

    // 值 257 字节拒绝
    e.details = ErrorDetails{{"k", std::string(kMaxValueBytes + 1, 'v')}};
    r = ValidateErrorDetails(e);
    CHECK(!r);
    if (!r) CHECK(ErrCodeOf(r) == ErrorCode::ProtocolError);

    // 重复键拒绝
    e.details = ErrorDetails{{"a", "1"}, {"a", "2"}};
    r = ValidateErrorDetails(e);
    CHECK(!r);
    if (!r) CHECK(ErrCodeOf(r) == ErrorCode::ProtocolError);

    // 非法 UTF-8 拒绝：孤立高位字节与过长编码
    e.details = ErrorDetails{{"k\xff", "v"}};
    r = ValidateErrorDetails(e);
    CHECK(!r);
    if (!r) CHECK(ErrCodeOf(r) == ErrorCode::ProtocolError);

    e.details = ErrorDetails{{"k", "\xC0\xAF"}};
    r = ValidateErrorDetails(e);
    CHECK(!r);
    if (!r) CHECK(ErrCodeOf(r) == ErrorCode::ProtocolError);

    // 保留键 expected_sequence：仅 framework.actor 域可写且为无符号十进制
    e = MakeDomainError(std::string(kErrorDomainFrameworkActor),
                      {{std::string(kErrorDetailExpectedSequence), "12"}});
    r = ValidateErrorDetails(e);
    CHECK(r);

    e = MakeDomainError(std::string(kErrorDomainFrameworkActor),
                      {{std::string(kErrorDetailExpectedSequence), "x12"}});
    r = ValidateErrorDetails(e);
    CHECK(!r);
    if (!r) CHECK(ErrCodeOf(r) == ErrorCode::ProtocolError);

    e = MakeDomainError(std::string(kErrorDomainInfra),
                      {{std::string(kErrorDetailExpectedSequence), "12"}});
    r = ValidateErrorDetails(e);
    CHECK(!r);
    if (!r) CHECK(ErrCodeOf(r) == ErrorCode::ProtocolError);
}

void TestNetworkLimits() {
    const NetworkLimits ok{1024, 256, 64 * 1024, 1024 * 1024,
                           std::chrono::milliseconds{5000}};
    auto r = ValidateNetworkLimits(ok);
    CHECK(r);

    auto expect_invalid = [](const NetworkLimits& l) {
        auto res = ValidateNetworkLimits(l);
        CHECK(!res);
        if (!res) CHECK(ErrCodeOf(res) == ErrorCode::InvalidArgument);
    };

    // 各项为 0 拒绝
    expect_invalid(NetworkLimits{0, ok.max_inflight, ok.max_header_bytes,
                                 ok.max_body_bytes, ok.incoming_timeout});
    expect_invalid(NetworkLimits{ok.max_connections, 0, ok.max_header_bytes,
                                 ok.max_body_bytes, ok.incoming_timeout});
    expect_invalid(NetworkLimits{ok.max_connections, ok.max_inflight, 0,
                                 ok.max_body_bytes, ok.incoming_timeout});
    expect_invalid(NetworkLimits{ok.max_connections, ok.max_inflight,
                                 ok.max_header_bytes, 0, ok.incoming_timeout});
    expect_invalid(NetworkLimits{ok.max_connections, ok.max_inflight,
                                 ok.max_header_bytes, ok.max_body_bytes,
                                 std::chrono::milliseconds{0}});

    // 超过上限拒绝
    expect_invalid(NetworkLimits{kNetworkLimitsMaxConnections + 1,
                                 ok.max_inflight, ok.max_header_bytes,
                                 ok.max_body_bytes, ok.incoming_timeout});
    expect_invalid(NetworkLimits{ok.max_connections,
                                 kNetworkLimitsMaxInflight + 1,
                                 ok.max_header_bytes, ok.max_body_bytes,
                                 ok.incoming_timeout});
    expect_invalid(NetworkLimits{ok.max_connections, ok.max_inflight,
                                 kNetworkLimitsMaxHeaderBytes + 1,
                                 ok.max_body_bytes, ok.incoming_timeout});
    expect_invalid(NetworkLimits{ok.max_connections, ok.max_inflight,
                                 ok.max_header_bytes,
                                 kNetworkLimitsMaxBodyBytes + 1,
                                 ok.incoming_timeout});
    expect_invalid(NetworkLimits{ok.max_connections, ok.max_inflight,
                                 ok.max_header_bytes, ok.max_body_bytes,
                                 kNetworkLimitsMaxIncomingTimeout +
                                     std::chrono::milliseconds{1}});

    // 边界值恰等于上限：接受
    const NetworkLimits at_cap{kNetworkLimitsMaxConnections,
                               kNetworkLimitsMaxInflight,
                               kNetworkLimitsMaxHeaderBytes,
                               kNetworkLimitsMaxBodyBytes,
                               kNetworkLimitsMaxIncomingTimeout};
    r = ValidateNetworkLimits(at_cap);
    CHECK(r);
}

void TestCodec() {
    // Codec<void>
    CHECK(Codec<void>::SchemaId() == "bbt.void/v1");
    auto enc = Codec<void>::Encode();
    CHECK(enc);
    if (enc) CHECK(enc.value().empty());
    auto dv = Codec<void>::Decode({});
    CHECK(dv);
    dv = Codec<void>::Decode({0x00});
    CHECK(!dv);
    if (!dv) CHECK(ErrCodeOf(dv) == ErrorCode::ProtocolError);

    // Codec<std::string>
    CHECK(Codec<std::string>::SchemaId() == "bbt.string.utf8/v1");
    auto se = Codec<std::string>::Encode("hello 世界");
    CHECK(se);
    auto sd = se ? Codec<std::string>::Decode(se.value())
                 : result<std::string>::err(MakeError(ErrorCode::InternalError, "enc"));
    CHECK(sd);
    if (sd) CHECK(sd.value() == "hello 世界");

    sd = Codec<std::string>::Decode({0xFF, 0xFE});
    CHECK(!sd);
    if (!sd) CHECK(sd.error().code == ErrorCode::ProtocolError);

    se = Codec<std::string>::Encode(std::string("a\xff", 2));
    CHECK(!se);
    if (!se) CHECK(se.error().code == ErrorCode::ProtocolError);
}

struct FakeObject : bbt::coroutine::ICoObject {
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override {
        return bbt::coroutine::CoObjectInfo{1, 1, "fake", "fakeobj"};
    }
};

struct FakeNet : ICoNetwork {
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override {
        return bbt::coroutine::CoObjectInfo{2, 1, "net", "fakenet"};
    }
};

struct FakeCloseable : ICoCloseable {
    void RequestClose() noexcept override { closed = true; }
    bool IsClosed() const noexcept override { return closed; }
    CloseStatus WaitClosed(bbt::coroutine::Deadline,
                           bbt::coroutine::CancellationToken) override {
        return CloseStatus::Closed;
    }
    bool closed = false;
};

void TestInterfaces() {
    FakeObject obj;
    CHECK(obj.GetObjectInfo().id == 1);

    FakeNet net;
    bbt::coroutine::ICoObject& base = net;
    CHECK(base.GetObjectInfo().kind == "net");

    FakeCloseable c;
    CHECK(!c.IsClosed());
    c.RequestClose();
    CHECK(c.IsClosed());
    CHECK(c.WaitClosed(bbt::coroutine::Deadline{},
                       bbt::coroutine::CancellationToken{}) ==
          CloseStatus::Closed);

    // CloseStatus 六枚举齐备
    CHECK(CloseStatus::TimedOut != CloseStatus::Closed);
    CHECK(CloseStatus::Cancelled != CloseStatus::InvalidContext);
    CHECK(CloseStatus::AlreadyWaiting != CloseStatus::RuntimeUnavailable);

    // CallOptions / IncomingCallContext 聚合可构造
    CallOptions opts{bbt::coroutine::Deadline{},
                     bbt::coroutine::CancellationToken{}};
    IncomingCallContext ctx{opts.deadline, opts.cancel, "peer"};
    CHECK(ctx.peer_principal == "peer");

    // Handler 别名可用
    HttpHandler hh = [](IncomingCallContext, HttpRequest) {
        return result<HttpResponse>::ok(HttpResponse{200, {}, "ok"});
    };
    auto hr = hh(ctx, HttpRequest{});
    CHECK(hr);
    if (hr) CHECK(hr.value().status == 200);

    RpcHandler rh = [](IncomingCallContext, RpcEnvelope) {
        return result<RpcEnvelope>::ok(RpcEnvelope{});
    };
    auto rr = rh(ctx, RpcEnvelope{});
    CHECK(rr);
}

} // namespace

int main() {
    TestResultOkErrVoid();
    TestErrorDetails();
    TestNetworkLimits();
    TestCodec();
    TestInterfaces();

    std::printf("contract.basics: %d checks, %d failures\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}

// Issue #8：RpcEnvelope wire profile 契约测试。
// 覆盖字段/错误映射、metadata 限制、编解码边界、HTTP body 形状校验。
// 不启动网络；round-trip 由 Test_rpc_wire_roundtrip 覆盖。

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <cstring>
#include <string>
#include <vector>

#include <bbt/infra/rpc/RpcWire.hpp>

using namespace bbt::infra;
using namespace bbt::infra::rpc;

namespace {

RpcWireEnvelope MakeReqEnv() {
    RpcWireEnvelope e;
    e.request_id  = "req-001";
    e.service     = "svc.echo";
    e.method      = "Echo";
    e.remaining_budget_ms = 5000;
    e.request_schema  = "bbt.echo.EchoReq/v1";
    e.response_schema = "bbt.echo.EchoResp/v1";
    e.payload = {'h', 'e', 'l', 'l', 'o'};
    e.metadata = {
        {"route.zone", "a"},
        {"trace.id",   "t-123"},
    };
    e.success = true;
    return e;
}

RpcWireEnvelope MakeRespOkEnv() {
    auto e = MakeReqEnv();
    e.payload = {'w', 'o', 'r', 'l', 'd'};
    return e;
}

RpcWireEnvelope MakeRespErrEnv() {
    auto e = MakeReqEnv();
    e.success = false;
    e.error.code              = ErrorCode::InvalidArgument;
    e.error.domain            = "framework.actor";
    e.error.domain_code       = "seq_gap";
    e.error.message           = "sequence mismatch";
    e.error.backend_category  = "mongo";
    e.error.backend_code      = 11000;
    e.error.transferred_bytes = 256;
    e.error.details = {{"expected", "5"}, {"got", "3"}};
    return e;
}

} // namespace

BOOST_AUTO_TEST_SUITE(rpc_wire_contract)

// --- 编解码基本闭环 ---------------------------------------------------------

BOOST_AUTO_TEST_CASE(t_request_encode_decode_roundtrip) {
    auto env = MakeReqEnv();
    auto bytes = EncodeRpcWireEnvelope(env, /*is_response=*/false);
    BOOST_REQUIRE(bytes);
    BOOST_CHECK(!bytes.value().empty());

    auto back = DecodeRpcWireEnvelope(bytes.value(), /*is_response=*/false);
    BOOST_REQUIRE(back);
    const auto& d = back.value();
    BOOST_CHECK_EQUAL(d.profile_version, kRpcWireProfileVersion);
    BOOST_CHECK_EQUAL(d.request_id,  "req-001");
    BOOST_CHECK_EQUAL(d.service,     "svc.echo");
    BOOST_CHECK_EQUAL(d.method,      "Echo");
    BOOST_CHECK_EQUAL(d.remaining_budget_ms, 5000u);
    BOOST_CHECK_EQUAL(d.request_schema,  "bbt.echo.EchoReq/v1");
    BOOST_CHECK_EQUAL(d.response_schema, "bbt.echo.EchoResp/v1");
    BOOST_CHECK(d.payload == std::vector<std::uint8_t>({'h','e','l','l','o'}));
    BOOST_REQUIRE_EQUAL(d.metadata.size(), 2u);
    BOOST_CHECK_EQUAL(d.metadata[0].first,  "route.zone");
    BOOST_CHECK_EQUAL(d.metadata[0].second, "a");
    BOOST_CHECK_EQUAL(d.metadata[1].first,  "trace.id");
    BOOST_CHECK_EQUAL(d.metadata[1].second, "t-123");
    BOOST_CHECK(d.success);
}

BOOST_AUTO_TEST_CASE(t_response_success_roundtrip) {
    auto env = MakeRespOkEnv();
    auto bytes = EncodeRpcWireEnvelope(env, /*is_response=*/true);
    BOOST_REQUIRE(bytes);
    auto back = DecodeRpcWireEnvelope(bytes.value(), /*is_response=*/true);
    BOOST_REQUIRE(back);
    BOOST_CHECK(back.value().success);
    BOOST_CHECK(back.value().payload ==
                std::vector<std::uint8_t>({'w','o','r','l','d'}));
    // request 关联字段必须保留
    BOOST_CHECK_EQUAL(back.value().request_id, "req-001");
}

BOOST_AUTO_TEST_CASE(t_response_error_roundtrip_lossless) {
    auto env = MakeRespErrEnv();
    auto bytes = EncodeRpcWireEnvelope(env, /*is_response=*/true);
    BOOST_REQUIRE(bytes);
    auto back = DecodeRpcWireEnvelope(bytes.value(), /*is_response=*/true);
    BOOST_REQUIRE(back);
    const auto& d = back.value();
    BOOST_CHECK(!d.success);
    BOOST_CHECK(d.error.code == ErrorCode::InvalidArgument);
    BOOST_CHECK_EQUAL(d.error.domain,            "framework.actor");
    BOOST_CHECK_EQUAL(d.error.domain_code,       "seq_gap");
    BOOST_CHECK_EQUAL(d.error.message,           "sequence mismatch");
    BOOST_CHECK_EQUAL(d.error.backend_category,  "mongo");
    BOOST_CHECK_EQUAL(d.error.backend_code,      11000);
    BOOST_CHECK_EQUAL(d.error.transferred_bytes, 256u);
    BOOST_REQUIRE_EQUAL(d.error.details.size(), 2u);
    BOOST_CHECK_EQUAL(d.error.details[0].first,  "expected");
    BOOST_CHECK_EQUAL(d.error.details[0].second, "5");
}

// --- 拒绝路径 ---------------------------------------------------------------

BOOST_AUTO_TEST_CASE(t_request_with_outcome_rejected) {
    auto env = MakeReqEnv();
    // R1 Issue-1：编码端拒绝 request 携带 success=false（错误分支会被
    // 静默丢弃）。之前允许编码成功、把失败输入伪装成合法 request。
    env.success = false;
    env.error.code = ErrorCode::InternalError;
    env.error.message = "request must not carry failure";
    auto enc = EncodeRpcWireEnvelope(env, /*is_response=*/false);
    BOOST_REQUIRE(!enc);
    BOOST_CHECK(enc.error().code == ErrorCode::InvalidArgument);

    // 解码路径同样钉死：按 response 编码再按 request 解码，必须拒绝。
    auto good = MakeReqEnv();
    auto bytes = EncodeRpcWireEnvelope(good, /*is_response=*/false);
    BOOST_REQUIRE(bytes);
    auto bad = MakeReqEnv();
    bad.success = false;
    bad.error.code = ErrorCode::InternalError;
    bad.error.message = "e";
    auto bad_wire = EncodeRpcWireEnvelope(bad, /*is_response=*/true);
    BOOST_REQUIRE(bad_wire);
    auto r = DecodeRpcWireEnvelope(bad_wire.value(), /*is_response=*/false);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::ProtocolError);
}

BOOST_AUTO_TEST_CASE(t_response_missing_outcome_rejected) {
    auto env = MakeReqEnv();
    auto bytes = EncodeRpcWireEnvelope(env, /*is_response=*/false);
    BOOST_REQUIRE(bytes);
    // 请求编码无 outcome；按 response 解码应拒绝。
    auto r = DecodeRpcWireEnvelope(bytes.value(), /*is_response=*/true);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::ProtocolError);
}

BOOST_AUTO_TEST_CASE(t_truncated_bytes_rejected) {
    auto env = MakeReqEnv();
    auto bytes = EncodeRpcWireEnvelope(env, /*is_response=*/false);
    BOOST_REQUIRE(bytes);
    // 截断为前 N 字节
    auto truncated = std::vector<std::uint8_t>(
        bytes.value().begin(), bytes.value().begin() + bytes.value().size() / 2);
    auto r = DecodeRpcWireEnvelope(truncated, /*is_response=*/false);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::ProtocolError);
}

BOOST_AUTO_TEST_CASE(t_empty_bytes_rejected) {
    std::vector<std::uint8_t> empty;
    auto r = DecodeRpcWireEnvelope(empty, /*is_response=*/false);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::ProtocolError);
}

BOOST_AUTO_TEST_CASE(t_missing_required_field_rejected) {
    auto env = MakeReqEnv();
    env.service.clear();   // 业务必需字段缺失
    auto r = EncodeRpcWireEnvelope(env, /*is_response=*/false);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_CASE(t_unsupported_profile_version_rejected) {
    auto env = MakeReqEnv();
    env.profile_version = 99;
    auto r = EncodeRpcWireEnvelope(env, /*is_response=*/false);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::InvalidArgument);
}

// --- metadata 规则 ----------------------------------------------------------

BOOST_AUTO_TEST_CASE(t_metadata_prefix_whitelist) {
    auto env = MakeReqEnv();
    env.metadata.push_back({"custom.k", "v"});   // 不在白名单
    auto r = EncodeRpcWireEnvelope(env, /*is_response=*/false);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::ProtocolError);
}

BOOST_AUTO_TEST_CASE(t_metadata_duplicate_key_rejected) {
    auto env = MakeReqEnv();
    env.metadata.push_back({"route.zone", "b"});   // 重复 route.zone
    auto r = EncodeRpcWireEnvelope(env, /*is_response=*/false);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::ProtocolError);
}

BOOST_AUTO_TEST_CASE(t_metadata_count_limit) {
    auto env = MakeReqEnv();
    for (std::size_t i = 0; i < kMaxMetadataItems + 1; ++i)
        env.metadata.push_back({"route.k" + std::to_string(i), "v"});
    auto r = EncodeRpcWireEnvelope(env, /*is_response=*/false);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::ProtocolError);
}

BOOST_AUTO_TEST_CASE(t_metadata_key_too_long) {
    auto env = MakeReqEnv();
    env.metadata.clear();
    env.metadata.push_back({"route." + std::string(kMaxMetadataKeyBytes, 'k'), "v"});
    auto r = EncodeRpcWireEnvelope(env, /*is_response=*/false);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::ProtocolError);
}

BOOST_AUTO_TEST_CASE(t_metadata_value_too_long) {
    auto env = MakeReqEnv();
    env.metadata.clear();
    env.metadata.push_back({"route.k", std::string(kMaxMetadataValueBytes + 1, 'v')});
    auto r = EncodeRpcWireEnvelope(env, /*is_response=*/false);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::ProtocolError);
}

// --- remaining_budget --------------------------------------------------------

BOOST_AUTO_TEST_CASE(t_budget_over_limit_rejected) {
    auto env = MakeReqEnv();
    env.remaining_budget_ms = kMaxRemainingBudgetMs + 1;
    auto r = EncodeRpcWireEnvelope(env, /*is_response=*/false);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_CASE(t_budget_boundary_accepted) {
    auto env = MakeReqEnv();
    env.remaining_budget_ms = kMaxRemainingBudgetMs;
    auto r = EncodeRpcWireEnvelope(env, /*is_response=*/false);
    BOOST_REQUIRE(r);
}

// Issue #8：remaining_budget_ms == 0 明确拒绝，不取本地预算兜底。
BOOST_AUTO_TEST_CASE(t_budget_zero_rejected_on_encode) {
    auto env = MakeReqEnv();
    env.remaining_budget_ms = 0;
    auto r = EncodeRpcWireEnvelope(env, /*is_response=*/false);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_CASE(t_budget_zero_rejected_on_decode) {
    // 手工构造 proto 字节流：field 5 (remaining_budget_ms, varint) = 0。
    // proto3 里 0 是默认值、不会被序列化，故等价于「字段缺失」，解码端按
    // 契约视同 0 拒绝。构造：先编码一个合法 envelope，再把 budget 字节段
    // 的 varint 值改为 0。简化：直接在 encoding 前把字段置 0 会过不了
    // encode 校验，改用 proto 级字节——取合法 bytes，找到 field5 tag(0x28)
    // 后的 varint，重写为 0（值仍占 1 字节，总长不变）。
    auto env = MakeReqEnv();
    env.remaining_budget_ms = 7;   // 编码合法，后面 patch 为 0
    auto bytes = EncodeRpcWireEnvelope(env, /*is_response=*/false);
    BOOST_REQUIRE(bytes);
    // field5 = remaining_budget_ms, wire type varint → tag = (5<<3)|0 = 0x28
    // 定位 tag 与值（值是单字节 varint，0..127 直接 1 字节）
    auto& b = bytes.value();
    bool patched = false;
    for (std::size_t i = 0; i + 1 < b.size(); ++i) {
        if (b[i] == 0x28) {          // field 5 varint tag
            b[i + 1] = 0x00;         // 把值改成 0
            patched = true;
            break;
        }
    }
    BOOST_REQUIRE(patched);
    auto r = DecodeRpcWireEnvelope(b, /*is_response=*/false);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::InvalidArgument);
}

// --- Error 分支编码前校验 ---------------------------------------------------

BOOST_AUTO_TEST_CASE(t_error_details_oversized_rejected) {
    auto env = MakeRespErrEnv();
    // details 超 16 条上限：EncodeRpcWireEnvelope 须在序列化前拒绝，
    // 分类保持 ProtocolError（decisions/0002 + Result.hpp 冻结语义，
    // R1 Issue-2 修复后不再改写为 InvalidArgument）。
    env.error.details.clear();
    for (std::size_t i = 0; i < kMaxErrorDetails + 1; ++i)
        env.error.details.push_back({"k" + std::to_string(i), "v"});
    auto r = EncodeRpcWireEnvelope(env, /*is_response=*/true);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::ProtocolError);
}

BOOST_AUTO_TEST_CASE(t_error_details_duplicate_key_rejected) {
    auto env = MakeRespErrEnv();
    env.error.details = {{"a", "1"}, {"a", "2"}};
    auto r = EncodeRpcWireEnvelope(env, /*is_response=*/true);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::ProtocolError);
}

BOOST_AUTO_TEST_CASE(t_error_details_invalid_utf8_rejected) {
    auto env = MakeRespErrEnv();
    env.error.details = {{"k", "\xff\xfe"}};
    auto r = EncodeRpcWireEnvelope(env, /*is_response=*/true);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::ProtocolError);
}

BOOST_AUTO_TEST_CASE(t_error_details_key_too_long_rejected) {
    auto env = MakeRespErrEnv();
    env.error.details = {{std::string(kMaxKeyBytes + 1, 'k'), "v"}};
    auto r = EncodeRpcWireEnvelope(env, /*is_response=*/true);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::ProtocolError);
}

BOOST_AUTO_TEST_CASE(t_error_details_value_too_long_rejected) {
    auto env = MakeRespErrEnv();
    env.error.details = {{"k", std::string(kMaxValueBytes + 1, 'v')}};
    auto r = EncodeRpcWireEnvelope(env, /*is_response=*/true);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::ProtocolError);
}

BOOST_AUTO_TEST_CASE(t_error_message_not_utf8_rejected) {
    auto env = MakeRespErrEnv();
    env.error.message = "\xff\xfe";
    auto r = EncodeRpcWireEnvelope(env, /*is_response=*/true);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_CASE(t_error_domain_too_long_rejected) {
    auto env = MakeRespErrEnv();
    env.error.domain = std::string(kMaxKeyBytes + 1, 'd');
    auto r = EncodeRpcWireEnvelope(env, /*is_response=*/true);
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::InvalidArgument);
}

// transport Error round-trip（ErrorCode::TransportError 双向无损）。
BOOST_AUTO_TEST_CASE(t_transport_error_roundtrip) {
    auto env = MakeRespErrEnv();
    env.error.code = ErrorCode::TransportError;
    env.error.domain = "infra";
    env.error.domain_code = "conn_reset";
    env.error.message = "connection reset by peer";
    env.error.backend_category = "asio";
    env.error.backend_code = 104;
    env.error.transferred_bytes = 128;
    env.error.details = {{"local", "127.0.0.1:1"}};
    auto bytes = EncodeRpcWireEnvelope(env, /*is_response=*/true);
    BOOST_REQUIRE(bytes);
    auto back = DecodeRpcWireEnvelope(bytes.value(), /*is_response=*/true);
    BOOST_REQUIRE(back);
    const auto& d = back.value();
    BOOST_CHECK(!d.success);
    BOOST_CHECK(d.error.code == ErrorCode::TransportError);
    BOOST_CHECK_EQUAL(d.error.domain, "infra");
    BOOST_CHECK_EQUAL(d.error.domain_code, "conn_reset");
    BOOST_CHECK_EQUAL(d.error.message, "connection reset by peer");
    BOOST_CHECK_EQUAL(d.error.backend_category, "asio");
    BOOST_CHECK_EQUAL(d.error.backend_code, 104);
    BOOST_CHECK_EQUAL(d.error.transferred_bytes, 128u);
    BOOST_REQUIRE_EQUAL(d.error.details.size(), 1u);
    BOOST_CHECK_EQUAL(d.error.details[0].first, "local");
}

// --- HTTP body 绑定 ----------------------------------------------------------

BOOST_AUTO_TEST_CASE(t_http_request_shape) {
    auto env = MakeReqEnv();
    auto req = MakeRpcWireHttpRequest(env, "http://127.0.0.1:8080");
    BOOST_REQUIRE(req);
    BOOST_CHECK_EQUAL(req.value().method, "POST");
    BOOST_CHECK_EQUAL(req.value().url, "http://127.0.0.1:8080/rpc");
    bool has_ct = false;
    for (const auto& h : req.value().headers)
        if (h.first == "Content-Type" && h.second == "application/x-protobuf")
            has_ct = true;
    BOOST_CHECK(has_ct);
    // 不允许任何 x-bbt-* 协议头
    for (const auto& h : req.value().headers)
        BOOST_CHECK(h.first.rfind("x-bbt-", 0) != 0 &&
                    h.first.rfind("X-Bbt-", 0) != 0);
    BOOST_CHECK(!req.value().body.empty());

    // body 可解码回 request envelope
    auto back = ParseRpcWireHttpRequest(req.value());
    BOOST_REQUIRE(back);
    BOOST_CHECK_EQUAL(back.value().request_id, "req-001");
}

BOOST_AUTO_TEST_CASE(t_http_request_wrong_method_rejected) {
    auto env = MakeReqEnv();
    auto req = MakeRpcWireHttpRequest(env, "http://127.0.0.1:8080");
    BOOST_REQUIRE(req);
    req.value().method = "GET";
    auto r = ParseRpcWireHttpRequest(req.value());
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_CASE(t_http_request_wrong_path_rejected) {
    auto env = MakeReqEnv();
    auto req = MakeRpcWireHttpRequest(env, "http://127.0.0.1:8080");
    BOOST_REQUIRE(req);
    req.value().url = "http://127.0.0.1:8080/not_rpc";
    auto r = ParseRpcWireHttpRequest(req.value());
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_CASE(t_http_request_missing_content_type_rejected) {
    auto env = MakeReqEnv();
    auto req = MakeRpcWireHttpRequest(env, "http://127.0.0.1:8080");
    BOOST_REQUIRE(req);
    req.value().headers.clear();
    auto r = ParseRpcWireHttpRequest(req.value());
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_CASE(t_http_response_success_roundtrip) {
    auto env = MakeRespOkEnv();
    auto resp = MakeRpcWireHttpResponse(env);
    BOOST_REQUIRE(resp);
    BOOST_CHECK_EQUAL(resp.value().status, 200u);
    auto back = ParseRpcWireHttpResponse(resp.value());
    BOOST_REQUIRE(back);
    BOOST_CHECK(back.value().success);
}

BOOST_AUTO_TEST_CASE(t_http_response_error_roundtrip) {
    auto env = MakeRespErrEnv();
    auto resp = MakeRpcWireHttpResponse(env);
    BOOST_REQUIRE(resp);
    auto back = ParseRpcWireHttpResponse(resp.value());
    BOOST_REQUIRE(back);
    BOOST_CHECK(!back.value().success);
    BOOST_CHECK(back.value().error.code == ErrorCode::InvalidArgument);
}

// --- seam 互转 ---------------------------------------------------------------

BOOST_AUTO_TEST_CASE(t_legacy_envelope_conversion) {
    RpcEnvelope legacy;
    legacy.service         = "svc.echo";
    legacy.method          = "Echo";
    legacy.request_id      = "req-x";
    legacy.request_schema  = "bbt.echo.EchoReq/v1";
    legacy.response_schema = "bbt.echo.EchoResp/v1";
    legacy.payload         = {'p'};
    legacy.metadata        = {{"route.z", "v"}};

    auto w = ToWireEnvelope(legacy, /*remaining_budget_ms=*/3000);
    BOOST_REQUIRE(w);
    BOOST_CHECK_EQUAL(w.value().remaining_budget_ms, 3000u);
    BOOST_CHECK(w.value().success);

    auto back = FromWireEnvelope(w.value());
    BOOST_REQUIRE(back);
    BOOST_CHECK_EQUAL(back.value().service,    "svc.echo");
    BOOST_CHECK_EQUAL(back.value().request_id, "req-x");
    BOOST_CHECK(back.value().payload == std::vector<std::uint8_t>({'p'}));
}

// --- unknown field 兼容 -------------------------------------------------------

BOOST_AUTO_TEST_CASE(t_unknown_fields_ignored) {
    // proto3 语义：构造带未知字段的字节流——手工在末尾追加未知 field tag。
    auto env = MakeReqEnv();
    auto bytes = EncodeRpcWireEnvelope(env, /*is_response=*/false);
    BOOST_REQUIRE(bytes);
    // 追加 field 200 (varint)：tag = (200 << 3) | 0 = 1600 → varint 0xA0 0x0C
    // value = 0x01
    std::vector<std::uint8_t> ext = bytes.value();
    ext.push_back(0xA0);
    ext.push_back(0x0C);
    ext.push_back(0x01);
    auto r = DecodeRpcWireEnvelope(ext, /*is_response=*/false);
    BOOST_REQUIRE(r);   // 未知字段被忽略，不产生错误
    BOOST_CHECK_EQUAL(r.value().request_id, "req-001");
}

BOOST_AUTO_TEST_SUITE_END()

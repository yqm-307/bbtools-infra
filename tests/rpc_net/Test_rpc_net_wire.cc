// rpc_net wire 契约：profile 编解码值层规则与畸形报文拒绝（真实双端之外的
// 纯字节层验证）。字节规则与旧 wire profile 逐位一致，不复制第二套规则。

#include "TestSupport.hpp"

namespace rpc_net_test {
namespace {

using rpc::RpcWireEnvelope;

RpcWireEnvelope ValidRequest() {
    RpcWireEnvelope env;
    env.request_id = "req-1";
    env.service = "echo";
    env.method = "Say";
    env.remaining_budget_ms = 1000;
    env.request_schema = std::string(rpc::kRpcPositionalSchema);
    env.response_schema = std::string(rpc::kRpcPositionalSchema);
    env.payload = Pack("hello", 42);
    env.metadata = {{"trace.id", "t-1"}};
    return env;
}

void Roundtrip() {
    // 请求方向：值类型 → 字节 → 值类型逐字段一致。
    const auto request = ValidRequest();
    auto bytes = rpc::EncodeRpcWireEnvelope(request, /*is_response=*/false);
    CHECK(bytes);
    auto decoded = rpc::DecodeRpcWireEnvelope(bytes.value(), /*is_response=*/false);
    CHECK(decoded);
    CHECK(decoded.value().profile_version == rpc::kRpcWireProfileVersion);
    CHECK(decoded.value().request_id == "req-1");
    CHECK(decoded.value().service == "echo");
    CHECK(decoded.value().method == "Say");
    CHECK(decoded.value().remaining_budget_ms == 1000);
    CHECK(decoded.value().request_schema == std::string(rpc::kRpcPositionalSchema));
    CHECK(decoded.value().response_schema == std::string(rpc::kRpcPositionalSchema));
    CHECK(decoded.value().payload == request.payload);
    CHECK(decoded.value().metadata.size() == 1 &&
          decoded.value().metadata.front().first == "trace.id" &&
          decoded.value().metadata.front().second == "t-1");
    CHECK(decoded.value().success);
    // string 视图入口与 vector 入口等价。
    const std::string text(bytes.value().begin(), bytes.value().end());
    auto from_text = rpc::DecodeRpcWireEnvelope(text, /*is_response=*/false);
    CHECK(from_text && from_text.value().request_id == "req-1");

    // 响应方向：成功与错误分支完整往返（Error 字段不丢失）。
    auto ok_reply = ValidRequest();
    ok_reply.success = true;
    auto ok_bytes = rpc::EncodeRpcWireEnvelope(ok_reply, /*is_response=*/true);
    CHECK(ok_bytes);
    auto ok_back = rpc::DecodeRpcWireEnvelope(ok_bytes.value(), /*is_response=*/true);
    CHECK(ok_back && ok_back.value().success);

    Error failure = MakeError(ErrorCode::NotFound, "service not found");
    failure.domain = "framework.actor";
    failure.domain_code = "svc.missing";
    failure.backend_category = "echo";
    failure.backend_code = 7;
    failure.transferred_bytes = 12;
    failure.details = {{"route.key", "a/b"}};
    auto bad_reply = ValidRequest();
    bad_reply.success = false;
    bad_reply.error = failure;
    auto bad_bytes = rpc::EncodeRpcWireEnvelope(bad_reply, /*is_response=*/true);
    CHECK(bad_bytes);
    auto bad_back = rpc::DecodeRpcWireEnvelope(bad_bytes.value(), /*is_response=*/true);
    CHECK(bad_back && !bad_back.value().success);
    const auto& err = bad_back.value().error;
    CHECK(err.code == ErrorCode::NotFound);
    CHECK(err.domain == "framework.actor");
    CHECK(err.message == "service not found");
    CHECK(err.domain_code == "svc.missing");
    CHECK(err.backend_category == "echo" && err.backend_code == 7);
    CHECK(err.transferred_bytes == 12);
    CHECK(err.details.size() == 1 && err.details.front().first == "route.key" &&
          err.details.front().second == "a/b");

    // seam 互转：RpcEnvelope → wire → RpcEnvelope 保留五关联字段/payload/metadata。
    auto seam = MakeEnvelope("echo", "Say", "seam-1", Pack("x", 1));
    seam.metadata = {{"fw.flag", "1"}};
    auto wire_env = rpc::ToWireEnvelope(seam, 2000);
    CHECK(wire_env && wire_env.value().remaining_budget_ms == 2000 && wire_env.value().success);
    auto back = rpc::FromWireEnvelope(wire_env.value());
    CHECK(back);
    CHECK(back.value().service == "echo" && back.value().method == "Say" &&
          back.value().request_id == "seam-1");
    CHECK(back.value().request_schema == std::string(rpc::kRpcPositionalSchema));
    CHECK(back.value().payload == seam.payload && back.value().metadata.size() == 1);
}

void Reject() {
    const auto valid = ValidRequest();
    // remaining_budget_ms：0 与越界都拒绝。
    {
        auto env = valid;
        env.remaining_budget_ms = 0;
        ExpectCode(rpc::ValidateRpcWireEnvelope(env, false), ErrorCode::InvalidArgument);
        ExpectCode(rpc::EncodeRpcWireEnvelope(env, false), ErrorCode::InvalidArgument);
    }
    {
        auto env = valid;
        env.remaining_budget_ms = rpc::kMaxRemainingBudgetMs + 1;
        ExpectCode(rpc::EncodeRpcWireEnvelope(env, false), ErrorCode::InvalidArgument);
    }
    // 请求方向禁止携带 outcome。
    {
        auto env = valid;
        env.success = false;
        env.error = MakeError(ErrorCode::InternalError, "nope");
        ExpectCode(rpc::EncodeRpcWireEnvelope(env, /*is_response=*/false),
                   ErrorCode::InvalidArgument);
        // 解码路径同样钉死：按 response 编码的信封再按 request 解码必须拒绝。
        auto as_response = rpc::EncodeRpcWireEnvelope(env, /*is_response=*/true);
        CHECK(as_response);
        ExpectCode(rpc::DecodeRpcWireEnvelope(as_response.value(), false),
                   ErrorCode::ProtocolError);
    }
    // 响应缺失 outcome：请求编码（无 outcome 字段）按 response 解码拒绝。
    {
        auto request_bytes = rpc::EncodeRpcWireEnvelope(valid, false);
        CHECK(request_bytes);
        ExpectCode(rpc::DecodeRpcWireEnvelope(request_bytes.value(), true),
                   ErrorCode::ProtocolError);
    }
    // 空关联字段与非法 profile_version。
    {
        auto env = valid;
        env.request_id.clear();
        ExpectCode(rpc::EncodeRpcWireEnvelope(env, false), ErrorCode::InvalidArgument);
    }
    {
        auto env = valid;
        env.profile_version = rpc::kRpcWireProfileVersion + 1;
        ExpectCode(rpc::EncodeRpcWireEnvelope(env, false), ErrorCode::InvalidArgument);
    }
    // metadata：未注册前缀 / 重复键 / 超限。
    {
        auto env = valid;
        env.metadata = {{"user.key", "v"}};
        ExpectCode(rpc::EncodeRpcWireEnvelope(env, false), ErrorCode::ProtocolError);
    }
    {
        auto env = valid;
        env.metadata = {{"trace.id", "a"}, {"trace.id", "b"}};
        ExpectCode(rpc::EncodeRpcWireEnvelope(env, false), ErrorCode::ProtocolError);
    }
    {
        auto env = valid;
        env.metadata = {{"trace.id", std::string(rpc::kMaxMetadataValueBytes + 1, 'x')}};
        ExpectCode(rpc::EncodeRpcWireEnvelope(env, false), ErrorCode::ProtocolError);
    }
    {
        auto env = valid;
        for (std::size_t i = 0; i <= rpc::kMaxMetadataItems; ++i) {
            env.metadata.push_back({"route.k" + std::to_string(i), "v"});
        }
        ExpectCode(rpc::EncodeRpcWireEnvelope(env, false), ErrorCode::ProtocolError);
    }
    {
        auto env = valid;
        env.metadata = {{"route." + std::string(rpc::kMaxMetadataKeyBytes, 'k'), "v"}};
        ExpectCode(rpc::EncodeRpcWireEnvelope(env, false), ErrorCode::ProtocolError);
    }
    // Error.details：重复键 / 非法 UTF-8 / 键值长度 / 条数上限（结构违规保持
    // ProtocolError，不改写分类）。
    {
        auto env = valid;
        env.success = false;
        env.error = MakeError(ErrorCode::InternalError, "details");
        {
            auto broken = env;
            broken.error.details = {{"a", "1"}, {"a", "2"}};
            ExpectCode(rpc::EncodeRpcWireEnvelope(broken, true), ErrorCode::ProtocolError);
        }
        {
            auto broken = env;
            broken.error.details = {{"k", "\xff\xfe"}};
            ExpectCode(rpc::EncodeRpcWireEnvelope(broken, true), ErrorCode::ProtocolError);
        }
        {
            auto broken = env;
            broken.error.details = {{std::string(kMaxKeyBytes + 1, 'k'), "v"}};
            ExpectCode(rpc::EncodeRpcWireEnvelope(broken, true), ErrorCode::ProtocolError);
        }
        {
            auto broken = env;
            broken.error.details = {{"k", std::string(kMaxValueBytes + 1, 'v')}};
            ExpectCode(rpc::EncodeRpcWireEnvelope(broken, true), ErrorCode::ProtocolError);
        }
        {
            auto broken = env;
            for (std::size_t i = 0; i <= kMaxErrorDetails; ++i) {
                broken.error.details.push_back({"k" + std::to_string(i), "v"});
            }
            ExpectCode(rpc::EncodeRpcWireEnvelope(broken, true), ErrorCode::ProtocolError);
        }
        // Error 字符串界限：非法 UTF-8 与超长 message/domain 按 InvalidArgument。
        {
            auto broken = env;
            broken.error.message = "\xff\xfe";
            ExpectCode(rpc::EncodeRpcWireEnvelope(broken, true), ErrorCode::InvalidArgument);
        }
        {
            auto broken = env;
            broken.error.message = std::string(kMaxValueBytes + 1, 'm');
            ExpectCode(rpc::EncodeRpcWireEnvelope(broken, true), ErrorCode::InvalidArgument);
        }
        {
            auto broken = env;
            broken.error.domain = std::string(kMaxKeyBytes + 1, 'd');
            ExpectCode(rpc::EncodeRpcWireEnvelope(broken, true), ErrorCode::InvalidArgument);
        }
    }
    // 解码侧同规则：手工匠改字节流（字段 4 varint 0 追加在合法信封尾部）使
    // remaining_budget_ms=0，必须按 InvalidArgument 拒绝——不依赖标准编码器
    // 能产生 0 值（proto3 不序列化零值）。
    {
        auto bytes = rpc::EncodeRpcWireEnvelope(valid, true);
        CHECK(bytes);
        bytes.value().push_back(0x28);  // field 5 (remaining_budget_ms), varint
        bytes.value().push_back(0x00);  // value 0
        ExpectCode(rpc::DecodeRpcWireEnvelope(bytes.value(), true),
                   ErrorCode::InvalidArgument);
    }
}

void Malformed() {
    // 空字节与垃圾字节：ProtocolError，不产生部分解码。
    ExpectCode(rpc::DecodeRpcWireEnvelope(std::vector<std::uint8_t>{}, false),
               ErrorCode::ProtocolError);
    {
        std::vector<std::uint8_t> garbage{0xff, 0xff, 0xff, 0xff, 0x0a};
        ExpectCode(rpc::DecodeRpcWireEnvelope(garbage, false), ErrorCode::ProtocolError);
    }
    // 合法信封截断：不再能整体解出（截断/字段断裂按 ProtocolError 或
    // 缺字段拒绝，绝不当成功）。
    {
        auto bytes = rpc::EncodeRpcWireEnvelope(ValidRequest(), false);
        CHECK(bytes && bytes.value().size() > 4);
        bytes.value().pop_back();
        bytes.value().pop_back();
        auto decoded = rpc::DecodeRpcWireEnvelope(bytes.value(), false);
        CHECK(!decoded);  // method/request_id 等必填字段缺失或解析失败
    }

    // HTTP 绑定形状：POST /rpc + application/x-protobuf + 非空 body。
    http::IncomingRequest good;
    good.method = "POST";
    good.target = rpc::kRpcWireHttpPath;
    good.headers = {{rpc::kRpcWireContentTypeKey, rpc::kRpcWireContentType}};
    good.body = RequestBody(ValidRequest());
    auto parsed = rpc::ParseRpcWireHttpRequest(good);
    CHECK(parsed && parsed.value().request_id == "req-1");
    {
        auto bad = good; bad.method = "GET";
        ExpectCode(rpc::ParseRpcWireHttpRequest(bad), ErrorCode::InvalidArgument);
    }
    {
        auto bad = good; bad.target = "/rpc/extra";
        ExpectCode(rpc::ParseRpcWireHttpRequest(bad), ErrorCode::InvalidArgument);
    }
    {
        auto bad = good;
        bad.headers = {{rpc::kRpcWireContentTypeKey, "text/plain"}};
        ExpectCode(rpc::ParseRpcWireHttpRequest(bad), ErrorCode::InvalidArgument);
    }
    {
        auto bad = good; bad.body.clear();
        ExpectCode(rpc::ParseRpcWireHttpRequest(bad), ErrorCode::ProtocolError);
    }

    // 响应绑定形状：200 + Content-Type + 非空 body。
    auto reply = ValidRequest();
    auto made = rpc::MakeRpcWireHttpResponse(reply);
    CHECK(made && made.value().status_code == 200);
    auto parsed_reply = rpc::ParseRpcWireHttpResponse(made.value());
    CHECK(parsed_reply && parsed_reply.value().service == "echo");
    {
        auto bad = made.value(); bad.status_code = 500;
        ExpectCode(rpc::ParseRpcWireHttpResponse(bad), ErrorCode::InvalidArgument);
    }
    {
        auto bad = made.value(); bad.headers.clear();
        ExpectCode(rpc::ParseRpcWireHttpResponse(bad), ErrorCode::InvalidArgument);
    }
    {
        auto bad = made.value(); bad.body.clear();
        ExpectCode(rpc::ParseRpcWireHttpResponse(bad), ErrorCode::ProtocolError);
    }

    // 响应显式 success=false 且无 error 分支：手工改写 field 10 的 bool 值后
    // 不得被静默升级为成功（proto3 不序列化显式 false，只能字节级构造）。
    {
        auto bytes = rpc::EncodeRpcWireEnvelope(reply, /*is_response=*/true);
        CHECK(bytes);
        bool patched = false;
        for (std::size_t i = 0; i + 1 < bytes.value().size(); ++i) {
            if (bytes.value()[i] == 0x50) {
                bytes.value()[i + 1] = 0x00;
                patched = true;
                break;
            }
        }
        CHECK(patched);
        ExpectCode(rpc::DecodeRpcWireEnvelope(bytes.value(), true),
                   ErrorCode::ProtocolError);
    }
    // 未知字段忽略：末尾追加 field 200（varint tag 0xA0 0x0C + value 0x01）后
    // 解码仍成功，关联字段保持。
    {
        auto bytes = rpc::EncodeRpcWireEnvelope(ValidRequest(), false);
        CHECK(bytes);
        bytes.value().push_back(0xA0);
        bytes.value().push_back(0x0C);
        bytes.value().push_back(0x01);
        auto decoded = rpc::DecodeRpcWireEnvelope(bytes.value(), false);
        CHECK(decoded && decoded.value().request_id == "req-1" &&
              decoded.value().service == "echo");
    }
}

} // namespace

int RunCase(const std::string& mode) {
    if (mode == "wire_roundtrip") { Roundtrip(); }
    else if (mode == "wire_reject") { Reject(); }
    else if (mode == "wire_malformed") { Malformed(); }
    else { CHECK(false); }
    std::cout << "rpc wire " << mode << " PASS\n";
    return 0;
}

} // namespace rpc_net_test

#include "../net/ProcessRuntimeTestMain.hpp"

int main(int argc, char** argv) {
    return bbt_net_test::RunTestProcessExit([&] {
        rpc_net_test::Watchdog watchdog;
        try {
            CHECK(argc == 2);
            return rpc_net_test::RunCase(argv[1]);
        } catch (const std::exception& error) {
            std::cerr << "FAIL " << error.what() << '\n';
            return 1;
        }
    });
}

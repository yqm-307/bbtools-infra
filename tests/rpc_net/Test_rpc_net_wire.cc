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
        ExpectCode(rpc::ParseRpcWireHttpResponse(bad), ErrorCode::ProtocolError);
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

void PhaseWire() {
    // Error.request_phase 带过 wire（decisions/0008 的升级路径）：optional
    // 字段保留存在位——缺席 → nullopt，绝不补成 NotStarted；UNSPECIFIED 与
    // 未识别值按 ProtocolError 拒绝；远端 Error 阶段只作远端事实透传，本地
    // HTTP 层的提交程度由传输错误路径携带，永不被远端回包改写。
    const auto valid = ValidRequest();
    auto failing = valid;
    failing.success = false;
    failing.error = MakeError(ErrorCode::OutcomeUnknown, "reply lost after commit");

    // 各合法阶段完整往返（本地枚举序 0..4 ↔ wire 1..5，不裸转数值）。
    // wire 存在性用确定性 diff 证明：带阶段的编码 = 缺字段编码在 error
    // 子消息尾部追加 {0x48, wire_value}（field 9 varint），且 error LEN
    // 前缀恰大 2——不靠字节扫描（payload 内容可能撞 tag 值）。
    const RequestPhase phases[] = {
        RequestPhase::NotStarted, RequestPhase::Connecting,
        RequestPhase::Writing, RequestPhase::RequestCommitted,
        RequestPhase::ReplyTerminal};
    for (const auto phase : phases) {
        auto with = failing;
        with.error.request_phase = phase;
        auto without = failing;  // request_phase 缺席
        auto with_bytes = rpc::EncodeRpcWireEnvelope(with, /*is_response=*/true);
        auto without_bytes = rpc::EncodeRpcWireEnvelope(without, true);
        CHECK(with_bytes && without_bytes);
        const auto& a = with_bytes.value();
        const auto& b = without_bytes.value();
        const int wire_value = static_cast<int>(phase) + 1;
        CHECK(a.size() == b.size() + 2);
        CHECK(a[a.size() - 2] == 0x48 && a.back() == wire_value);
        std::size_t i = 0;
        while (i < b.size() && a[i] == b[i]) { ++i; }  // error LEN 前缀处
        CHECK(i < b.size() && a[i] == b[i] + 2);
        CHECK(std::equal(b.begin() + i + 1, b.end(), a.begin() + i + 1));
        auto back = rpc::DecodeRpcWireEnvelope(a, /*is_response=*/true);
        CHECK(back && !back.value().success);
        CHECK(back.value().error.request_phase.has_value());
        CHECK(*back.value().error.request_phase == phase);
        CHECK(IsRequestCommitted(*back.value().error.request_phase) ==
              IsRequestCommitted(phase));
    }

    // nullopt 与旧 peer（缺字段）：编码端不落 field 9，解码端回 nullopt——
    // 不补 NotStarted、不推断未提交。
    {
        auto env = failing;  // request_phase 默认 nullopt
        auto bytes = rpc::EncodeRpcWireEnvelope(env, /*is_response=*/true);
        CHECK(bytes);
        auto back = rpc::DecodeRpcWireEnvelope(bytes.value(), true);
        CHECK(back && !back.value().success);
        CHECK(back.value().error.code == ErrorCode::OutcomeUnknown);
        CHECK(!back.value().error.request_phase.has_value());
    }

    // 远端声称的阶段原样透传：远端回包写 NotStarted（哪怕本地这条请求明明
    // 已提交且拿到了完整 200 回复），解码端保留远端的事实声明；本地提交
    // 程度只由本地传输错误路径携带（未知结果/写中断用例另行钉死），
    // 不被此字段改写。
    {
        auto env = failing;
        env.error.request_phase = RequestPhase::NotStarted;
        auto bytes = rpc::EncodeRpcWireEnvelope(env, /*is_response=*/true);
        CHECK(bytes);
        auto back = rpc::DecodeRpcWireEnvelope(bytes.value(), true);
        CHECK(back && back.value().error.request_phase.has_value());
        CHECK(*back.value().error.request_phase == RequestPhase::NotStarted);
    }

    // 非法 wire 值拒绝：UNSPECIFIED=0 与未知枚举（7 / 99）都 ProtocolError。
    // 手工构造嵌套 RpcError（field 1 code + field 9 非法阶段）装进
    // envelope outcome 分支。
    auto base = rpc::EncodeRpcWireEnvelope(valid, /*is_response=*/false);
    CHECK(base);
    for (const std::uint8_t bad : {0x00, 0x07, 0x63}) {
        std::vector<std::uint8_t> bytes = base.value();
        // outcome=error：field 11 LEN tag 0x5A；子消息 {0x08,code,0x48,bad}。
        bytes.push_back(0x5A);
        bytes.push_back(0x04);
        bytes.push_back(0x08);
        bytes.push_back(0x0B);   // code=NOT_FOUND，合法
        bytes.push_back(0x48);
        bytes.push_back(bad);    // request_phase 非法
        ExpectCode(rpc::DecodeRpcWireEnvelope(bytes, /*is_response=*/true),
                   ErrorCode::ProtocolError);
    }

    // 成功响应不产生 error 分支，也无 request_phase 可丢。
    {
        auto ok = rpc::EncodeRpcWireEnvelope(valid, /*is_response=*/true);
        CHECK(ok);
        auto back = rpc::DecodeRpcWireEnvelope(ok.value(), true);
        CHECK(back && back.value().success);
    }
}

// 修复后契约断言（原复现探针已翻转）：调用方把 Error.request_phase
// 设为本地枚举范围之外的值时，Validate 与 Encode 都按 ProtocolError 拒绝，
// 不再产出 present-UNSPECIFIED（接收端必拒）的 wire。
void PhaseWireInvalidLocalProbe() {
    auto env = ValidRequest();
    env.success = false;
    env.error = MakeError(ErrorCode::OutcomeUnknown, "reply lost after commit");

    const RequestPhase invalids[] = {
        static_cast<RequestPhase>(-1),
        static_cast<RequestPhase>(5),   // 合法序 0..4 之外
        static_cast<RequestPhase>(99)};
    for (const auto bad : invalids) {
        env.error.request_phase = bad;
        const int v = static_cast<int>(bad);

        auto validated = rpc::ValidateRpcWireEnvelope(env, /*is_response=*/true);
        std::cout << "invalid_local_phase value=" << v
                  << " validate_ok=" << (validated ? 1 : 0)
                  << " validate_error="
                  << (validated ? -1 : static_cast<int>(validated.error().code))
                  << '\n';
        // 修复后契约：Validate 必须先拒非法本地阶段。
        CHECK(!validated);
        if (!validated) {
            CHECK(validated.error().code == ErrorCode::ProtocolError);
        }

        auto bytes = rpc::EncodeRpcWireEnvelope(env, /*is_response=*/true);
        std::cout << "invalid_local_phase value=" << v
                  << " encode_ok=" << (bytes ? 1 : 0)
                  << " encode_error="
                  << (bytes ? -1 : static_cast<int>(bytes.error().code))
                  << '\n';
        // 修复后契约：Encode 同样拒绝，不再产出接收端必拒的 wire。
        CHECK(!bytes);
        if (!bytes) {
            CHECK(bytes.error().code == ErrorCode::ProtocolError);
        }
    }
    // 合法 phase 仍正常编码往返。
    env.error.request_phase = RequestPhase::RequestCommitted;
    auto good = rpc::EncodeRpcWireEnvelope(env, /*is_response=*/true);
    CHECK(good);
    auto back = rpc::DecodeRpcWireEnvelope(good.value(), /*is_response=*/true);
    CHECK(back);
    if (back) {
        CHECK(back.value().error.request_phase.has_value() &&
              *back.value().error.request_phase == RequestPhase::RequestCommitted);
    }
}

} // namespace

int RunCase(const std::string& mode) {
    if (mode == "wire_roundtrip") { Roundtrip(); }
    else if (mode == "wire_reject") { Reject(); }
    else if (mode == "wire_malformed") { Malformed(); }
    else if (mode == "wire_request_phase") { PhaseWire(); }
    else if (mode == "wire_phase_invalid_local_probe") { PhaseWireInvalidLocalProbe(); }
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

// rpc v2 wire 契约：SessionFrame profile 2 编解码、kind 字段存在性矩阵、
// acceptance_requested presence、错误阶段、metadata/budget 上限、u32be 分帧
// 上限与畸形报文拒绝（纯字节层验证，不连网络）。

#include <bbt/infra/rpc/Codec.hpp>
#include <bbt/infra/rpc/Options.hpp>
#include <bbt/infra/rpc/Stats.hpp>

#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "TestSupport.hpp"

namespace rpc_v2_test {
namespace {

namespace rpc = bbt::infra::rpc;
using namespace bbt::infra;             // ErrorCode / MakeError / RequestPhase / result
using rpc_net_test::ExpectCode;
using rpc::SessionFrame;
using rpc::FrameKind;
using rpc::TrafficClass;
using rpc::ReplyMode;

// 标准合法帧模板：Send（带 message_id 与显式 acceptance_requested）。
SessionFrame ValidSend() {
    SessionFrame f;
    f.kind                = FrameKind::Send;
    f.traffic             = TrafficClass::Actor;
    f.session_epoch       = 7;
    f.message_id          = "msg-1";
    f.service             = "echo";
    f.method              = "Tell";
    f.reply               = ReplyMode::None;
    f.remaining_budget_ms = 500;
    f.payload             = {1, 2, 3};
    f.metadata            = {{"trace.id", "t-9"}};
    f.acceptance_requested = true;
    return f;
}

SessionFrame ValidCall() {
    SessionFrame f;
    f.kind                = FrameKind::Call;
    f.traffic             = TrafficClass::Service;
    f.session_epoch       = 7;
    f.request_id          = "req-1";
    f.service             = "echo";
    f.method              = "Say";
    f.reply               = ReplyMode::Required;
    f.remaining_budget_ms = 1000;
    f.request_schema      = "bbt.rpc.positional/v1";
    f.response_schema     = "bbt.rpc.positional/v1";
    f.payload             = {4, 5, 6};
    return f;
}

SessionFrame ValidResponse() {
    auto f = ValidCall();
    f.kind = FrameKind::Response;
    f.has_outcome = true;
    f.success = true;
    return f;
}

SessionFrame ValidHello() {
    SessionFrame f;
    f.kind             = FrameKind::Hello;
    f.traffic          = TrafficClass::System;
    f.peer_identity    = "app-a";
    f.session_epoch    = 1;
    f.connection_nonce = 42;
    return f;
}

// ---- roundtrip --------------------------------------------------------------

void Roundtrip() {
    // Send：acceptance_requested=true 显式保留 presence 与值。
    {
        const auto send = ValidSend();
        auto bytes = rpc::EncodeSessionFrame(send);
        CHECK(bytes);
        auto back = rpc::DecodeSessionFrame(bytes.value());
        CHECK(back);
        const auto& d = back.value();
        CHECK(d.kind == FrameKind::Send);
        CHECK(d.traffic == TrafficClass::Actor);
        CHECK(d.session_epoch == 7 && d.message_id == "msg-1");
        CHECK(d.service == "echo" && d.method == "Tell");
        CHECK(d.reply == ReplyMode::None && d.remaining_budget_ms == 500);
        CHECK(d.payload == send.payload);
        CHECK(d.metadata.size() == 1 && d.metadata[0].first == "trace.id");
        CHECK(d.acceptance_requested.has_value());
        CHECK(*d.acceptance_requested == true);
        CHECK(!d.has_outcome);
    }
    // Send acceptance_requested=false：presence 显式 false ≠ 缺席。
    {
        auto send = ValidSend();
        send.acceptance_requested = false;
        auto bytes = rpc::EncodeSessionFrame(send);
        CHECK(bytes);
        auto back = rpc::DecodeSessionFrame(bytes.value());
        CHECK(back);
        CHECK(back.value().acceptance_requested.has_value());
        CHECK(*back.value().acceptance_requested == false);
    }
    // Send 无 acceptance_requested：presence 缺席保留 nullopt。
    {
        auto send = ValidSend();
        send.acceptance_requested = std::nullopt;
        auto bytes = rpc::EncodeSessionFrame(send);
        CHECK(bytes);
        auto back = rpc::DecodeSessionFrame(bytes.value());
        CHECK(back && !back.value().acceptance_requested.has_value());
    }
    // Call 往返：reply Required、schema/budget/ordering 保留。
    {
        auto call = ValidCall();
        call.sequence = 3;
        call.ordering_key = "actor-42";
        auto bytes = rpc::EncodeSessionFrame(call);
        CHECK(bytes);
        auto back = rpc::DecodeSessionFrame(bytes.value());
        CHECK(back);
        const auto& d = back.value();
        CHECK(d.kind == FrameKind::Call && d.request_id == "req-1");
        CHECK(d.reply == ReplyMode::Required && d.sequence == 3);
        CHECK(d.ordering_key == "actor-42");
        CHECK(d.request_schema == "bbt.rpc.positional/v1");
        CHECK(d.remaining_budget_ms == 1000);
    }
    // Response 成功分支：outcome=success=true 保留。
    {
        auto resp = ValidResponse();
        auto bytes = rpc::EncodeSessionFrame(resp);
        CHECK(bytes);
        auto back = rpc::DecodeSessionFrame(bytes.value());
        CHECK(back && back.value().has_outcome && back.value().success);
    }
    // Response 错误分支：Error 全字段 + request_phase 保留。
    {
        auto resp = ValidResponse();
        resp.success = false;
        resp.error = MakeError(ErrorCode::NotFound, "missing");
        resp.error.domain = "framework.actor";
        resp.error.domain_code = "svc.x";
        resp.error.backend_category = "echo";
        resp.error.backend_code = 7;
        resp.error.transferred_bytes = 9;
        resp.error.details = {{"route.key", "a/b"}};
        resp.error.request_phase = RequestPhase::RequestCommitted;
        auto bytes = rpc::EncodeSessionFrame(resp);
        CHECK(bytes);
        auto back = rpc::DecodeSessionFrame(bytes.value());
        CHECK(back && back.value().has_outcome && !back.value().success);
        const auto& e = back.value().error;
        CHECK(e.code == ErrorCode::NotFound && e.domain == "framework.actor");
        CHECK(e.domain_code == "svc.x" && e.message == "missing");
        CHECK(e.backend_category == "echo" && e.backend_code == 7);
        CHECK(e.transferred_bytes == 9);
        CHECK(e.details.size() == 1 && e.details[0].first == "route.key");
        CHECK(e.request_phase.has_value() &&
              *e.request_phase == RequestPhase::RequestCommitted);
    }
    // Response 无 request_phase：nullopt 保留，不补成未提交。
    {
        auto resp = ValidResponse();
        resp.success = false;
        resp.error = MakeError(ErrorCode::OutcomeUnknown, "lost");
        auto bytes = rpc::EncodeSessionFrame(resp);
        CHECK(bytes);
        auto back = rpc::DecodeSessionFrame(bytes.value());
        CHECK(back && !back.value().error.request_phase.has_value());
    }
    // Hello：peer_identity/connection_nonce 保留。
    {
        auto hello = ValidHello();
        auto bytes = rpc::EncodeSessionFrame(hello);
        CHECK(bytes);
        auto back = rpc::DecodeSessionFrame(bytes.value());
        CHECK(back);
        CHECK(back.value().peer_identity == "app-a" &&
              back.value().connection_nonce == 42 &&
              back.value().session_epoch == 1);
    }
    // SendAccepted：outcome + message_id；无 request_id。
    {
        SessionFrame sa;
        sa.kind = FrameKind::SendAccepted;
        sa.traffic = TrafficClass::System;
        sa.session_epoch = 7;
        sa.message_id = "msg-1";
        sa.has_outcome = true;
        sa.success = true;
        auto bytes = rpc::EncodeSessionFrame(sa);
        CHECK(bytes);
        auto back = rpc::DecodeSessionFrame(bytes.value());
        CHECK(back && back.value().has_outcome && back.value().success);
        CHECK(back.value().message_id == "msg-1");
    }
    // Request/Response 值层互转：Call → frame → Request 字段不丢失，
    // RequestContext 经 detail 接缝填充后读回。
    {
        rpc::Request req;
        req.SetService("echo").SetMethod("Say").SetRequestId("r-1")
           .SetRequestSchema("s").SetResponseSchema("s")
           .SetPayload({9, 8}).SetTraffic(TrafficClass::Service)
           .SetReply(ReplyMode::Required).SetOrderingKey("k").SetSequence(5);
        auto f = rpc::ToSessionFrame(req, FrameKind::Call, 11, 250);
        CHECK(f);
        rpc::RequestContext ctx;
        ctx.peer_identity = "peer-b";
        ctx.peer_principal = "svc@cluster";
        auto back = rpc::RequestFromSessionFrame(f.value(), ctx);
        CHECK(back);
        CHECK(back.value().RequestId() == "r-1" &&
              back.value().Sequence() == 5 &&
              back.value().Context().peer_identity == "peer-b" &&
              back.value().Context().peer_principal == "svc@cluster");
        // Response 互转。
        rpc::Response resp;
        resp.SetRequestId("r-1").SetService("echo").SetMethod("Say")
            .SetRequestSchema("s").SetResponseSchema("s").SetPayload({3});
        auto rf = rpc::ResponseToSessionFrame(resp, 11);
        CHECK(rf && rf.value().kind == FrameKind::Response);
        auto rback = rpc::ResponseFromSessionFrame(rf.value());
        CHECK(rback && rback.value().RequestId() == "r-1");
    }
}

// ---- 非法值（编码端） ---------------------------------------------------------

void Reject() {
    const auto valid = ValidCall();
    // frame_kind：Unspecified/未知本地值。
    {
        auto f = valid; f.kind = FrameKind::Unspecified;
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::ProtocolError);
        f.kind = static_cast<FrameKind>(99);
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::ProtocolError);
    }
    // traffic：数据帧用 System / 控制帧用非 System。
    {
        auto f = valid; f.traffic = TrafficClass::System;
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::InvalidArgument);
        auto h = ValidHello(); h.traffic = TrafficClass::Service;
        ExpectCode(rpc::EncodeSessionFrame(h), ErrorCode::InvalidArgument);
    }
    // reply_mode：Call 非 Required / Send 非 None。
    {
        auto f = valid; f.reply = ReplyMode::None;
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::InvalidArgument);
        auto s = ValidSend(); s.reply = ReplyMode::Required;
        ExpectCode(rpc::EncodeSessionFrame(s), ErrorCode::InvalidArgument);
    }
    // acceptance_requested 出现在非 Send。
    {
        auto f = valid; f.acceptance_requested = true;
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::InvalidArgument);
        auto r = ValidResponse(); r.acceptance_requested = false;
        ExpectCode(rpc::EncodeSessionFrame(r), ErrorCode::InvalidArgument);
        auto h = ValidHello(); h.acceptance_requested = true;
        ExpectCode(rpc::EncodeSessionFrame(h), ErrorCode::InvalidArgument);
    }
    // outcome：请求帧携带 / 响应帧缺席。
    {
        auto f = valid; f.has_outcome = true; f.success = true;
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::InvalidArgument);
        auto r = ValidResponse(); r.has_outcome = false;
        ExpectCode(rpc::EncodeSessionFrame(r), ErrorCode::InvalidArgument);
    }
    // peer_identity / connection_nonce：非握手帧携带、握手帧缺失。
    {
        auto f = valid; f.peer_identity = "x";
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::InvalidArgument);
        auto h = ValidHello(); h.peer_identity.clear();
        ExpectCode(rpc::EncodeSessionFrame(h), ErrorCode::InvalidArgument);
        h = ValidHello(); h.connection_nonce = 0;
        ExpectCode(rpc::EncodeSessionFrame(h), ErrorCode::InvalidArgument);
        f = valid; f.connection_nonce = 1;
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::InvalidArgument);
    }
    // session_epoch：数据帧/握手帧 0。
    {
        auto f = valid; f.session_epoch = 0;
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::InvalidArgument);
        auto h = ValidHello(); h.session_epoch = 0;
        ExpectCode(rpc::EncodeSessionFrame(h), ErrorCode::InvalidArgument);
    }
    // message_id：Send 缺、超长；非 Send 携带。
    {
        auto s = ValidSend(); s.message_id.clear();
        ExpectCode(rpc::EncodeSessionFrame(s), ErrorCode::InvalidArgument);
        s = ValidSend(); s.message_id = std::string(rpc::kRpcV2MaxMessageIdBytes + 1, 'm');
        ExpectCode(rpc::EncodeSessionFrame(s), ErrorCode::InvalidArgument);
        auto f = valid; f.message_id = "m";
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::InvalidArgument);
    }
    // request_id：Call 缺失。
    {
        auto f = valid; f.request_id.clear();
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::InvalidArgument);
    }
    // budget：Call/Send 0 或越界；控制帧非零。
    {
        auto f = valid; f.remaining_budget_ms = 0;
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::InvalidArgument);
        f = valid; f.remaining_budget_ms = rpc::kRpcV2MaxRemainingBudgetMs + 1;
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::InvalidArgument);
        auto h = ValidHello(); h.remaining_budget_ms = 1;
        ExpectCode(rpc::EncodeSessionFrame(h), ErrorCode::InvalidArgument);
    }
    // ordering：seq>0 无 key / seq=0 带 key / key 超长。
    {
        auto f = valid; f.sequence = 9; f.ordering_key.clear();
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::InvalidArgument);
        f = valid; f.sequence = 0; f.ordering_key = "k";
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::InvalidArgument);
        f = valid; f.sequence = 1;
        f.ordering_key = std::string(rpc::kRpcV2MaxOrderingKeyBytes + 1, 'k');
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::InvalidArgument);
    }
    // schema：Call 缺失。
    {
        auto f = valid; f.request_schema.clear();
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::InvalidArgument);
        f = valid; f.response_schema.clear();
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::InvalidArgument);
    }
    // metadata：未注册前缀 / 重复键 / 条数超限。
    {
        auto f = valid; f.metadata = {{"user.k", "v"}};
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::ProtocolError);
        f = valid; f.metadata = {{"trace.a", "1"}, {"trace.a", "2"}};
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::ProtocolError);
        f = valid;
        for (std::size_t i = 0; i <= 32; ++i)
            f.metadata.push_back({"route.k" + std::to_string(i), "v"});
        ExpectCode(rpc::EncodeSessionFrame(f), ErrorCode::ProtocolError);
    }
    // error.details 结构违规保持 ProtocolError。
    {
        auto r = ValidResponse();
        r.success = false;
        r.error = MakeError(ErrorCode::InternalError, "x");
        r.error.details = {{"a", "1"}, {"a", "2"}};
        ExpectCode(rpc::EncodeSessionFrame(r), ErrorCode::ProtocolError);
    }
    // 非法本地 request_phase：编码端 ProtocolError。
    {
        auto r = ValidResponse();
        r.success = false;
        r.error = MakeError(ErrorCode::OutcomeUnknown, "lost");
        r.error.request_phase = static_cast<RequestPhase>(99);
        ExpectCode(rpc::EncodeSessionFrame(r), ErrorCode::ProtocolError);
    }
}

// ---- 解码端负用例 -------------------------------------------------------------

void Malformed() {
    const auto valid = ValidCall();
    auto good_bytes = rpc::EncodeSessionFrame(valid);
    CHECK(good_bytes);

    // 空字节/垃圾字节：ProtocolError。
    ExpectCode(rpc::DecodeSessionFrame(std::vector<std::uint8_t>{}),
               ErrorCode::ProtocolError);
    {
        std::vector<std::uint8_t> garbage{0xff, 0xff, 0x0a};
        ExpectCode(rpc::DecodeSessionFrame(garbage), ErrorCode::ProtocolError);
    }
    // 截断帧：不再整体解出。
    {
        auto bytes = good_bytes.value();
        bytes.pop_back(); bytes.pop_back();
        CHECK(!rpc::DecodeSessionFrame(bytes));
    }
    // profile_version 非 2：field1 varint 3（tag 0x08 值 0x03）。
    {
        auto bytes = good_bytes.value();
        bool patched = false;
        for (std::size_t i = 0; i + 1 < bytes.size(); ++i) {
            if (bytes[i] == 0x08 && bytes[i + 1] == 0x02) {
                bytes[i + 1] = 0x03; patched = true; break;
            }
        }
        CHECK(patched);
        ExpectCode(rpc::DecodeSessionFrame(bytes), ErrorCode::ProtocolError);
    }
    // 未知 frame_kind：field2 varint 99（tag 0x10 值 0x63）。
    {
        auto bytes = good_bytes.value();
        bool patched = false;
        for (std::size_t i = 0; i + 1 < bytes.size(); ++i) {
            if (bytes[i] == 0x10 && bytes[i + 1] == 0x03) {
                bytes[i + 1] = 0x63; patched = true; break;
            }
        }
        CHECK(patched);
        ExpectCode(rpc::DecodeSessionFrame(bytes), ErrorCode::ProtocolError);
    }
    // acceptance_requested 出现在 Call：field21 varint 1（tag 0xA8 0x01 值 0x01）。
    {
        auto bytes = good_bytes.value();
        bytes.push_back(0xA8); bytes.push_back(0x01); bytes.push_back(0x01);
        ExpectCode(rpc::DecodeSessionFrame(bytes), ErrorCode::ProtocolError);
    }
    // Call 无 outcome（合法帧按 Call 解码无 outcome）——改成 Response kind
    // 后必须拒绝（Response 要求 outcome）：field2 值 3→4。
    {
        auto bytes = good_bytes.value();
        for (std::size_t i = 0; i + 1 < bytes.size(); ++i) {
            if (bytes[i] == 0x10 && bytes[i + 1] == 0x03) {
                bytes[i + 1] = 0x04; break;
            }
        }
        ExpectCode(rpc::DecodeSessionFrame(bytes), ErrorCode::ProtocolError);
    }
    // 未知 request_phase：嵌套 error{code=NOT_FOUND, request_phase=99} 装在
    // Response 上（field19 LEN 子消息），按枚举矩阵拒绝。
    {
        std::vector<std::uint8_t> bad = {
            0x08, 0x02, 0x10, 0x04, 0x18, 0x01, 0x28, 0x07,
            0x3A, 0x01, 'r', 0x42, 0x01, 's', 0x4A, 0x01, 'm',
            0x50, 0x01, 0x6A, 0x01, 'q', 0x72, 0x01, 'q',
            0x9A, 0x01, 0x04,           // error LEN=4
            0x08, 0x0B, 0x48, 0x63,     // code=NOT_FOUND, request_phase=99
        };
        ExpectCode(rpc::DecodeSessionFrame(bad), ErrorCode::ProtocolError);
    }
    // 未知字段忽略：末尾追加 field 200 varint。
    {
        auto bytes = good_bytes.value();
        bytes.push_back(0xC0); bytes.push_back(0x0C); bytes.push_back(0x01);
        auto decoded = rpc::DecodeSessionFrame(bytes);
        CHECK(decoded && decoded.value().request_id == "req-1");
    }
}

// ---- 分帧（u32be） ------------------------------------------------------------

void Framing() {
    const auto valid = ValidSend();
    // 前缀编解码：u32be 与逐字节移位一致。
    {
        std::uint8_t pre[4];
        rpc::EncodeFramePrefix(0x01020304, pre);
        CHECK(pre[0] == 0x01 && pre[1] == 0x02 && pre[2] == 0x03 && pre[3] == 0x04);
        CHECK(rpc::DecodeFramePrefix(pre) == 0x01020304);
    }
    // EncodeFramedSessionFrame：前缀==body 长度，decoder 一轮取回。
    {
        auto framed = rpc::EncodeFramedSessionFrame(valid);
        CHECK(framed && framed.value().size() > 4);
        CHECK(rpc::DecodeFramePrefix(framed.value().data()) ==
              framed.value().size() - 4);
        rpc::SessionFrameDecoder dec;
        CHECK(dec.Feed(framed.value()));
        auto f = dec.Next();
        CHECK(f && f.value().has_value());
        CHECK(f.value()->message_id == "msg-1");
    }
    // 分片到达：前缀/body 任意切分后仍按完整帧解出（含 1 字节片）。
    {
        auto framed = rpc::EncodeFramedSessionFrame(valid);
        CHECK(framed);
        rpc::SessionFrameDecoder dec;
        std::optional<SessionFrame> got;
        for (const auto byte : framed.value()) {
            CHECK(dec.Feed(&byte, 1));
            auto f = dec.Next();
            CHECK(f);
            if (f.value().has_value()) got = std::move(f.value()).value();
        }
        CHECK(got.has_value() && got->message_id == "msg-1");
    }
    // 一次喂入多帧：按序取出。
    {
        auto a = rpc::EncodeFramedSessionFrame(ValidHello());
        auto b = rpc::EncodeFramedSessionFrame(valid);
        CHECK(a && b);
        rpc::SessionFrameDecoder dec;
        std::vector<std::uint8_t> stream(a.value());
        stream.insert(stream.end(), b.value().begin(), b.value().end());
        CHECK(dec.Feed(stream));
        auto f1 = dec.Next();
        auto f2 = dec.Next();
        auto f3 = dec.Next();
        CHECK(f1 && f1.value().has_value() && f1.value()->kind == FrameKind::Hello);
        CHECK(f2 && f2.value().has_value() && f2.value()->kind == FrameKind::Send);
        CHECK(f3 && !f3.value().has_value());
    }
    // 前缀 0：立即 ProtocolError，进入 terminal。
    {
        rpc::SessionFrameDecoder dec;
        const std::uint8_t zero[4] = {0, 0, 0, 0};
        CHECK(dec.Feed(zero, 4));
        ExpectCode(dec.Next(), ErrorCode::ProtocolError);
        CHECK(dec.IsTerminal());
        // terminal 后后续 Feed/Next 保持失败。
        ExpectCode(dec.Feed(zero, 4), ErrorCode::ProtocolError);
        ExpectCode(dec.Next(), ErrorCode::ProtocolError);
    }
    // 前缀超硬上限：Feed 读到完整前缀即拒绝，不必喂满 body。
    {
        rpc::SessionFrameDecoder dec;
        std::uint8_t pre[4];
        rpc::EncodeFramePrefix(
            static_cast<std::uint32_t>(0xFFFFFFFFu), pre);  // > 1GiB 硬上限
        ExpectCode(dec.Feed(pre, 4), ErrorCode::ProtocolError);
        CHECK(dec.IsTerminal());
        ExpectCode(dec.Next(), ErrorCode::ProtocolError);
    }
    // Hello 超 64KiB 握手上限：编码端 EncodeFramed 对超 kind 上限拒绝；
    // 解码端握手态（默认 hello_phase）在**前缀阶段**即拒，不等 body 喂满
    // （ADR §5.2：检查长度后才允许分配 body——F2）。
    {
        rpc::SessionFrame big = ValidHello();
        big.payload.assign(70000, 0xAB);   // 合法 Hello 字段，仅尺寸超限
        ExpectCode(rpc::EncodeFramedSessionFrame(big, rpc::kRpcV2DefaultMaxFrameBytes),
                   ErrorCode::InvalidArgument);
        // 解码端：EncodeSessionFrame（无尺寸上限）拿到可解析 body，手工
        // u32be 前缀喂给 hello_phase decoder——声明 70000 > min(8MiB,64KiB)
        // → Feed 前缀即 ProtocolError 终态，无需喂 body。
        auto body = rpc::EncodeSessionFrame(big);
        CHECK(body && body.value().size() > rpc::kRpcV2HelloMaxFrameBytes);
        rpc::SessionFrameDecoder dec;
        std::uint8_t pre[4];
        rpc::EncodeFramePrefix(static_cast<std::uint32_t>(body.value().size()), pre);
        ExpectCode(dec.Feed(pre, 4), ErrorCode::ProtocolError);
        CHECK(dec.IsTerminal());
        ExpectCode(dec.Next(), ErrorCode::ProtocolError);
    }
    // 握手态前缀限额小额度即时拒绝：max=1024 → min(max,64KiB)=1024；
    // 声明 2048 的帧前缀一到即拒（先于任何 body 复制），terminal 保持。
    {
        rpc::SessionFrameDecoder dec(/*max_frame_bytes=*/1024);
        std::uint8_t pre[4];
        rpc::EncodeFramePrefix(2048, pre);
        ExpectCode(dec.Feed(pre, 4), ErrorCode::ProtocolError);
        CHECK(dec.IsTerminal());
        ExpectCode(dec.Next(), ErrorCode::ProtocolError);
    }
    // F2：同批输入「4 字节前缀 + 超限 body」——body 字节不得先复制进
    // 缓冲（ADR §5.2：检查长度后才允许分配 body）；拒绝后 PendingBytes
    // 只剩合法前缀本身，terminal 保持。
    // 总输入 2044 ≤ pending 上限 2052，故不会先撞 pending 总界——唯一能
    // 区分「复制 body 前校验」与「整个 Feed 后校验」的路径。
    {
        rpc::SessionFrameDecoder dec(/*max_frame_bytes=*/1024);
        std::vector<std::uint8_t> input(4 + 2040, 0xEE);
        rpc::EncodeFramePrefix(2048, input.data());   // 声明 2048 > 1024
        ExpectCode(dec.Feed(input.data(), input.size()),
                   ErrorCode::ProtocolError);
        CHECK(dec.IsTerminal());
        // body 未复制：缓冲只剩 4 字节前缀，不是 2044。
        CHECK(dec.PendingBytes() == 4);
        ExpectCode(dec.Next(), ErrorCode::ProtocolError);
    }
    // F2：前缀拆 3+1 且第 4 字节与超限 body 同批——拼齐前缀即拒，
    // body 仍不复制（PendingBytes 最终 4，不含同批 body）。
    {
        rpc::SessionFrameDecoder dec(/*max_frame_bytes=*/1024);
        std::uint8_t pre[4];
        rpc::EncodeFramePrefix(2048, pre);
        CHECK(dec.Feed(pre, 3));                      // 先喂 3 字节
        CHECK(dec.PendingBytes() == 3);
        std::vector<std::uint8_t> tail(1 + 2040, 0xDD);
        tail[0] = pre[3];                             // 第 4 字节 + body
        ExpectCode(dec.Feed(tail.data(), tail.size()),
                   ErrorCode::ProtocolError);
        CHECK(dec.IsTerminal());
        CHECK(dec.PendingBytes() == 4);               // body 未入缓冲
        ExpectCode(dec.Next(), ErrorCode::ProtocolError);
    }
    // F2 流水线：合法完整首帧尚未 Next 出队时，同批第二帧「前缀+超限 body」
    // 同样先校验声明长度再复制 body——不依赖「不完整帧在缓冲头」之外假设；
    // PendingBytes 只保留合法首帧 + 4B 第二帧前缀，body 字节不入缓冲。
    // 数据阶段（SetHelloPhase(false)）帧限统一，尾随帧前缀可即时校验；
    // 握手阶段完整帧后的余字节属未验证 stash，不在本用例范围。
    {
        auto first = rpc::EncodeFramedSessionFrame(valid, 4096);
        CHECK(first);
        rpc::SessionFrameDecoder dec(/*max_frame_bytes=*/4096);
        dec.SetHelloPhase(false);              // 数据态：limit=4096
        std::vector<std::uint8_t> stream(first.value());
        const std::size_t base = stream.size();      // 首帧全长（前缀+body）
        stream.resize(base + 4 + 2040, 0xEE);
        rpc::EncodeFramePrefix(8192, stream.data() + base);  // 声明 > 4096
        ExpectCode(dec.Feed(stream.data(), stream.size()),
                   ErrorCode::ProtocolError);
        CHECK(dec.IsTerminal());
        // 第二帧 body 未复制：缓冲 = 完整首帧 + 4B 前缀。
        CHECK(dec.PendingBytes() == base + 4);
        ExpectCode(dec.Next(), ErrorCode::ProtocolError);
    }
    // F2 流水线：第二帧前缀跨 Feed 分批——首帧完整在缓冲、第二帧前缀
    // 先喂 3B，第 4 字节与超限 body 同批到达；拼齐前缀即拒，body 不复制。
    {
        auto first = rpc::EncodeFramedSessionFrame(valid, 4096);
        CHECK(first);
        rpc::SessionFrameDecoder dec(/*max_frame_bytes=*/4096);
        dec.SetHelloPhase(false);              // 数据态：尾随帧前缀即时校验
        std::vector<std::uint8_t> stream(first.value());
        const std::size_t base = stream.size();
        std::uint8_t pre[4];
        rpc::EncodeFramePrefix(8192, pre);           // 声明 > 4096
        stream.insert(stream.end(), pre, pre + 3);   // 第二帧前缀前 3 字节
        CHECK(dec.Feed(stream.data(), stream.size()));
        CHECK(!dec.IsTerminal());
        CHECK(dec.PendingBytes() == base + 3);
        std::vector<std::uint8_t> tail(1 + 2040, 0xDD);
        tail[0] = pre[3];                            // 第 4 字节 + body
        ExpectCode(dec.Feed(tail.data(), tail.size()),
                   ErrorCode::ProtocolError);
        CHECK(dec.IsTerminal());
        CHECK(dec.PendingBytes() == base + 4);       // 只多留 1B 前缀
        ExpectCode(dec.Next(), ErrorCode::ProtocolError);
    }
    // F2 流水线：合法两帧同批且首帧不 Next 出队——第二帧前缀照常被
    // 校验但不误算 PendingBytes；喂第三帧超限前缀即拒，缓冲保留
    // 「首帧+第二帧完整+4B 前缀」，不误计 body。
    {
        auto f1 = rpc::EncodeFramedSessionFrame(valid, 4096);
        auto f2 = rpc::EncodeFramedSessionFrame(valid, 4096);
        CHECK(f1 && f2);
        rpc::SessionFrameDecoder dec(/*max_frame_bytes=*/4096);
        dec.SetHelloPhase(false);              // 数据态：尾随帧前缀即时校验
        std::vector<std::uint8_t> stream(f1.value());
        stream.insert(stream.end(), f2.value().begin(), f2.value().end());
        const std::size_t pipelined = stream.size(); // 两帧总长
        std::uint8_t pre[4];
        rpc::EncodeFramePrefix(8192, pre);           // 第三帧声明 > 4096
        stream.insert(stream.end(), pre, pre + 4);
        stream.resize(stream.size() + 100, 0xCC);    // 第三帧 body 片段
        ExpectCode(dec.Feed(stream.data(), stream.size()),
                   ErrorCode::ProtocolError);
        CHECK(dec.IsTerminal());
        CHECK(dec.PendingBytes() == pipelined + 4);  // 前缀保留，body 不入
        ExpectCode(dec.Next(), ErrorCode::ProtocolError);
    }
    // 跃迁后合法合包不误拒：hello_phase 下 Hello(≤64KiB)+Call 同缓冲，两帧
    // 按序解出（第二帧声明 ≤64KiB 故握手态前缀门不拦，kind 复核通过）；
    // 转数据态后声明 >64KiB 的数据帧同样接纳。
    {
        auto hello = rpc::EncodeFramedSessionFrame(ValidHello());
        auto call = rpc::EncodeFramedSessionFrame(ValidCall());
        CHECK(hello && call);
        rpc::SessionFrameDecoder dec;
        std::vector<std::uint8_t> stream(hello.value());
        stream.insert(stream.end(), call.value().begin(), call.value().end());
        CHECK(dec.Feed(stream));
        auto f1 = dec.Next();
        auto f2 = dec.Next();
        CHECK(f1 && f1.value().has_value() &&
              f1.value()->kind == FrameKind::Hello);
        CHECK(f2 && f2.value().has_value() &&
              f2.value()->kind == FrameKind::Call);
        dec.SetHelloPhase(false);   // 握手完成 → 数据态
        rpc::SessionFrame big_send = ValidSend();
        big_send.payload.assign(80000, 0x5A);  // >64KiB ≤8MiB：握手态会拒
        auto big = rpc::EncodeFramedSessionFrame(big_send);
        CHECK(big);
        CHECK(dec.Feed(big.value()));
        auto f3 = dec.Next();
        CHECK(f3 && f3.value().has_value() &&
              f3.value()->kind == FrameKind::Send);
        CHECK(!dec.IsTerminal());
    }
    // 余帧移交：PendingBytes/TakePending 把未耗尽字节原样转给下一解码器。
    {
        auto hello = rpc::EncodeFramedSessionFrame(ValidHello());
        auto call = rpc::EncodeFramedSessionFrame(ValidCall());
        rpc::SessionFrameDecoder a;
        std::vector<std::uint8_t> stream(hello.value());
        stream.insert(stream.end(), call.value().begin(), call.value().end());
        CHECK(a.Feed(stream));
        auto f1 = a.Next();
        CHECK(f1 && f1.value().has_value());
        CHECK(a.PendingBytes() == call.value().size());
        auto rest = a.TakePending();
        CHECK(a.PendingBytes() == 0);
        rpc::SessionFrameDecoder b;
        CHECK(b.Feed(rest));
        auto f2 = b.Next();
        CHECK(f2 && f2.value().has_value() &&
              f2.value()->kind == FrameKind::Call);
    }
    // payload 超数据帧上限（max_frame_bytes 可配）：EncodeFramed 拒绝。
    {
        rpc::SessionFrame big = ValidSend();
        big.payload.assign(1025, 0xCD);
        auto framed = rpc::EncodeFramedSessionFrame(big, /*max_frame_bytes=*/1024);
        ExpectCode(framed, ErrorCode::InvalidArgument);
    }
    // 未决缓冲硬上限（分配/复制前检查，不实际分配大额）：小额 max_frame
    // 下，前缀声明恰好界内但 body 未喂满时连续 Feed 到达上限即终态。
    // 字节流必须是合法 wire：喂过帧界的字节属于下一帧，其 4B 前缀照常
    // 参与校验——用合法小帧流水线把 pending 精确推到 2052 再越界。
    {
        rpc::SessionFrameDecoder dec(/*max_frame_bytes=*/1024);
        // 上限 = 4 + 2*1024 = 2052。喂入一个 1024B 声明前缀 + 不足 body 的
        // 合法开头（buffer=4+512），未超上限，不拒绝。
        std::uint8_t pre[4];
        rpc::EncodeFramePrefix(1024, pre);
        CHECK(dec.Feed(pre, 4));
        const std::vector<std::uint8_t> partial(512, 0xEE);
        CHECK(dec.Feed(partial.data(), partial.size()));
        // 补满首帧剩余 512B body 后再接第二帧前缀——帧界对齐，声明 1020
        // ≤1024 合法且占满 pending：4+1024+4+1020=2052 ≤ 2052。
        std::vector<std::uint8_t> more(512, 0xDD);
        rpc::EncodeFramePrefix(1020, pre);
        more.insert(more.end(), pre, pre + 4);
        more.resize(more.size() + 1020, 0xBB);       // 第二帧 body 1020B
        CHECK(dec.Feed(more.data(), more.size()));
        CHECK(dec.PendingBytes() == 2052);
        // 再喂 1B → 2053 > 2052 → 分配前拒绝并进 terminal。
        const std::uint8_t one = 0xAA;
        ExpectCode(dec.Feed(&one, 1), ErrorCode::ProtocolError);
        CHECK(dec.IsTerminal());
        ExpectCode(dec.Feed(&one, 1), ErrorCode::ProtocolError);
        ExpectCode(dec.Next(), ErrorCode::ProtocolError);
    }
    {
        // 合法流水线不误拒：两个完整 Send 帧（均 ≤1024）同一 Feed 到达。
        auto f1 = rpc::EncodeFramedSessionFrame(valid, 1024);
        auto f2 = rpc::EncodeFramedSessionFrame(valid, 1024);
        CHECK(f1 && f2);
        rpc::SessionFrameDecoder dec(/*max_frame_bytes=*/1024);
        std::vector<std::uint8_t> stream(f1.value());
        stream.insert(stream.end(), f2.value().begin(), f2.value().end());
        CHECK(dec.Feed(stream));
        auto a = dec.Next();
        auto b = dec.Next();
        CHECK(a && a.value().has_value());
        CHECK(b && b.value().has_value());
        CHECK(!dec.IsTerminal());
    }
    // max_frame_bytes 非法：0 与超硬上限拒绝。
    {
        ExpectCode(rpc::EncodeFramedSessionFrame(valid, 0),
                   ErrorCode::InvalidArgument);
        ExpectCode(rpc::EncodeFramedSessionFrame(valid, rpc::kRpcV2MaxFrameBytes + 1),
                   ErrorCode::InvalidArgument);
    }
}

// ---- Options/Stats 值层 ---------------------------------------------------------

void Values() {
    // ClientOptions Validate：合法默认通过；逐项越界拒绝。
    {
        rpc::ClientOptions o;
        o.SetLocalIdentity("app-a").SetTrustedNetworkPlaintext(true);
        CHECK(o.Validate());
        o.SetLocalIdentity("");
        ExpectCode(o.Validate(), ErrorCode::InvalidArgument);
        o.SetLocalIdentity(std::string(300, 'x'));
        ExpectCode(o.Validate(), ErrorCode::InvalidArgument);
    }
    {
        rpc::ClientOptions o;
        o.SetLocalIdentity("a").SetMaxSessions(0);
        ExpectCode(o.Validate(), ErrorCode::InvalidArgument);
        o.SetMaxSessions(4097);
        ExpectCode(o.Validate(), ErrorCode::InvalidArgument);
        o.SetMaxSessions(64).SetMaxFrameBytes(rpc::kMaxFrameBytes + 1);
        ExpectCode(o.Validate(), ErrorCode::InvalidArgument);
        o.SetMaxFrameBytes(1024).SetServiceWeight(0);
        ExpectCode(o.Validate(), ErrorCode::InvalidArgument);
    }
    // SendOptions：deadline 必须有限且未过期。
    {
        rpc::SendOptions o;
        ExpectCode(o.Validate(), ErrorCode::InvalidArgument);
        o.SetDeadline(std::chrono::steady_clock::now() - std::chrono::seconds{1});
        ExpectCode(o.Validate(), ErrorCode::InvalidArgument);
        o.SetDeadline(std::chrono::steady_clock::now() + std::chrono::seconds{5});
        CHECK(o.Validate());
        CHECK(!o.RequiresRemoteAcceptance());
        o.RequireRemoteAcceptance();
        CHECK(o.RequiresRemoteAcceptance());
    }
    // ServerOptions：listen 必填、各项上限。
    {
        rpc::ServerOptions o;
        ExpectCode(o.Validate(), ErrorCode::InvalidArgument);  // 无 listen
        std::error_code ec;
        o.SetListenAddress(bbt::infra::net::Address::Ip("127.0.0.1", 0, ec));
        CHECK(!ec);
        CHECK(o.Validate());
        o.SetIncomingTimeout(std::chrono::milliseconds{0});
        ExpectCode(o.Validate(), ErrorCode::InvalidArgument);
        o.SetIncomingTimeout(std::chrono::milliseconds{25000 * 3600});
        ExpectCode(o.Validate(), ErrorCode::InvalidArgument);
    }
    // Stats 值类型可聚合。
    {
        rpc::ClientStats cs;
        cs.sessions = 2; cs.links = 1; cs.service.inflight = 3;
        rpc::ServerStats ss;
        ss.accepted_connections = 5; ss.actor.queued = 9;
        CHECK(cs.sessions == 2 && cs.service.inflight == 3);
        CHECK(ss.accepted_connections == 5 && ss.actor.queued == 9);
    }
    // RequestContext 默认空值；Request 默认 traffic=Service reply=None。
    {
        rpc::Request r;
        CHECK(r.Traffic() == TrafficClass::Service);
        CHECK(r.Reply() == ReplyMode::None);
        CHECK(r.Context().peer_identity.empty());
        CHECK(r.Context().deadline == bbt::coroutine::Deadline::max());
    }
}

} // namespace

int RunCase(const std::string& mode) {
    if (mode == "v2_roundtrip") { Roundtrip(); }
    else if (mode == "v2_reject") { Reject(); }
    else if (mode == "v2_malformed") { Malformed(); }
    else if (mode == "v2_framing") { Framing(); }
    else if (mode == "v2_values") { Values(); }
    else { CHECK(false); }
    std::cout << "rpc v2 wire " << mode << " PASS\n";
    return 0;
}

} // namespace rpc_v2_test

int main(int argc, char** argv) {
    try {
        CHECK(argc == 2);
        return rpc_v2_test::RunCase(argv[1]);
    } catch (const std::exception& error) {
        std::cerr << "FAIL " << error.what() << '\n';
        return 1;
    }
}

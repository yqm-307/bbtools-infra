/**
 * V2 公共面正对照：只用公开头使用保留符号（唯一 RPC 消费面）。
 * 由 consumers/expect_rpc_v2_surface.cmake 以 -fsyntax-only 编译；
 * 该 TU 通过是全部负对照成立的前提。
 */
#include <bbt/infra/rpc/Admission.hpp>
#include <bbt/infra/rpc/CoRpcClient.hpp>
#include <bbt/infra/rpc/CoRpcServer.hpp>
#include <bbt/infra/rpc/Hooks.hpp>
#include <bbt/infra/rpc/Options.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>
#include <bbt/infra/rpc/SessionCodec.hpp>
#include <bbt/infra/rpc/Stats.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace {

/**
 * 唯一入站 handler：admission 转移 owning item（同步接纳，不挂起）。
 */
bbt::infra::result<void> Admit(bbt::infra::rpc::AdmissionItem&& item)
{
    (void)item;
    return bbt::infra::result<void>::ok();
}

/**
 * 唯一注册路由 handler 别名（V2 保留面，与旧 bbt::infra::RpcHandler 不同名）。
 */
bbt::infra::rpc::CoRpcServer::RpcHandler Route()
{
    return [](bbt::infra::rpc::Request request) -> bbt_co_async(bbt::infra::result<bbt::infra::rpc::Response>) {
        return bbt::infra::result<bbt::infra::rpc::Response>::ok(bbt::infra::rpc::Response::FromRequest(request));
    };
}

void UseAll()
{
    using namespace bbt::infra::rpc;
    Request request("svc", "method");
    request.SetRequestId("id")
        .SetReply(ReplyMode::Required)
        .SetTraffic(TrafficClass::Service)
        .SetRequestSchema("req")
        .SetResponseSchema("resp");
    request.SetPayload({std::uint8_t{1}});
    (void)request.Service();
    (void)request.Method();
    (void)request.RequestId();
    (void)request.RequestSchema();
    (void)request.ResponseSchema();
    (void)request.Payload();
    (void)request.Metadata();
    (void)request.Context();

    auto response = Response::FromRequest(request);
    (void)response.Payload();
    (void)response.RequestId();

    ClientOptions client_options;
    client_options.SetLocalIdentity("identity").SetTrustedNetworkPlaintext(true);
    (void)client_options.LocalIdentity();

    ServerOptions server_options;
    (void)server_options.MaxConnections();
    (void)server_options.MaxInflight();

    SendOptions send_options;
    send_options.SetDeadline(bbt::coroutine::Deadline::max());
    (void)send_options.Deadline();

    AdmissionHandler handler = Admit;
    (void)handler;

    SessionFrame frame;
    frame.kind = FrameKind::Hello;
    frame.traffic = TrafficClass::System;
    frame.peer_identity = "peer";
    frame.session_epoch = 1;
    frame.connection_nonce = 1;
    auto framed = EncodeFramedSessionFrame(frame);
    if (framed) {
        std::uint8_t prefix[4];
        EncodeFramePrefix(static_cast<std::uint32_t>(framed.value().size()), prefix);
        (void)DecodeFramePrefix(prefix);
        (void)DecodeSessionFrame(framed.value().data(), framed.value().size());
    }
    (void)ValidateSessionFrame(frame);
    (void)Route();

    /**
     * 统一关闭面：CloseOptions 是唯一保留的关闭输入。
     */
    bbt::infra::rpc::CoRpcClient* client_owner = nullptr;
    bbt::infra::rpc::CoRpcServer* server_owner = nullptr;
    (void)client_owner;
    (void)server_owner;
    bbt::infra::rpc::CoRpcServer::RouteOptions route_limit;
    route_limit.burst_tokens = 1;
    route_limit.refill_per_second = 1.0;
    (void)route_limit;
    const auto immediate = bbt::infra::CloseOptions::Immediate();
    const auto graceful =
        bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::milliseconds(100));
    (void)immediate;
    (void)graceful;
}

// clang-format off
}
// clang-format on

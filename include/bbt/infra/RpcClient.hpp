#pragma once
// co-network/v1 N2：无服务治理的 RPC 出站客户端公共面。
// HTTP/Protobuf 仅是当前 infra backend；不泄漏 Beast/protobuf 类型。

#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/ICoNetwork.hpp>
#include <bbt/infra/NetworkTypes.hpp>

namespace bbt::infra {

class RpcClient : public ICoNetwork, public ICoCloseable {
public:
    // 具体 endpoint 已由 framework 解析；infra 不读取服务发现或路由表。
    // 仅在受管 coroutine 内调用，期限由 options.deadline 表达。
    virtual result<RpcEnvelope> Call(const RpcAddress& address,
                                     RpcEnvelope request,
                                     const CallOptions& options) = 0;
};

} // namespace bbt::infra

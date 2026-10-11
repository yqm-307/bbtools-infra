#pragma once
// co-network/v1 N2：无服务治理的 RPC 入站服务端公共面。
// handler 由 framework 适配器提供；infra 只负责 wire、传输与资源生命周期。

#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/ICoNetwork.hpp>
#include <bbt/infra/NetworkTypes.hpp>

namespace bbt::infra {

class RpcServer : public ICoNetwork, public ICoCloseable {
public:
    virtual ListenAddress LocalAddress() const = 0;

    // 只停止新连接/新请求接纳；不取消已接纳 handler。
    // handler 可能在多个受管 coroutine 中并发调用；实现必须自行保证
    // 共享状态同步，且返回 envelope 的关联字段必须保持请求值。
    virtual void StopAccepting() noexcept = 0;
};

} // namespace bbt::infra

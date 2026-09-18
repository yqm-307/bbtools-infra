#pragma once
// co-network/v1 N1b-1：HTTP/1.1 服务端公共面。
// 业务 handler 只在 infra 提供的协程执行环境调用；I/O 回调不直接跑业务。

#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/ICoNetwork.hpp>
#include <bbt/infra/NetworkTypes.hpp>

namespace bbt::infra {

// HttpServer：入站 listener 组件。ListenHttp 成功后即可接纳；
// 已接纳请求在受管协程中调用 HttpHandler，异常由边界捕获转 InternalError。
class HttpServer : public ICoNetwork, public ICoCloseable {
public:
    // 实际绑定地址（动态端口时为内核分配结果）。
    virtual ListenAddress LocalAddress() const = 0;

    // 幂等、可从控制线程调用：停止新连接及既有连接上的新请求接纳；
    // 不取消已接纳 handler、不关闭其回复路径；不等价于 RequestClose。
    virtual void StopAccepting() noexcept = 0;
};

} // namespace bbt::infra

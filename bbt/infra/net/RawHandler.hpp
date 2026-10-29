#pragma once

// 原始短回调接口（R2）：在 Reactor owner 的非业务协程边界执行，必须短、
// 非阻塞、不可挂起；同连接不重入，不递归触发另一用户回调。不同 owner 可
// 并发调用同一个共享 handler。
//
// 用户回调异常由 Engine 在调用点就地隔离：记录到引擎错误状态，不逃逸到
// driver 层，不吞成成功，也不自动关闭策略以外的连接（关闭仍由 owner 收口）。

#include <cstddef>
#include <cstdint>
#include <system_error>

#include <bbt/infra/net/Connection.hpp>

namespace bbt::infra::net {

// 单个已接纳写操作的终态：sent 是该操作实际交内核的字节数（真实前缀），
// error 为该操作的真实错误（无错误表示本地完成）。
struct WriteResult {
    std::uint64_t operation{0};
    std::size_t sent{0};
    std::error_code error;
};

class RawHandler {
public:
    virtual ~RawHandler() = default;

    // 连接已由 owner 接纳并登记后交付稳定句柄；不自动创建业务协程。
    virtual void OnAccept(Connection connection) { (void)connection; }

    // 非阻塞建连终态（成功或失败恰一次）：失败时 connection 无效、error 非零；
    // operation 是 AsyncConnect 返回的同一次操作身份，成功/失败都可关联。
    virtual void OnConnect(Connection connection, std::error_code error,
                           std::uint64_t operation) {
        (void)connection;
        (void)error;
        (void)operation;
    }

    // 只是通知：提示复查同一接收 ring 与持久读状态；可合并，不是数据快照。
    virtual void OnRead(Connection connection) { (void)connection; }

    // 每个已接纳操作唯一终态；在状态发布后、锁外、非递归调用。
    virtual void OnWrite(Connection connection, WriteResult result) {
        (void)connection;
        (void)result;
    }

    // 物理 fd 已关闭后由 owner 通知；同连接最多一次。reason 是 owner 发布
    // 的关闭原因。本回调只证明 fd 已关——不等于本连接全部责任已清：raw
    // 回调/Co 等待者可能仍在进行；物理出账事实以 Connection::Released()
    // （或带句柄 CoDial 的 ReclaimReceipt::Released()）为准，不得在本回调
    // 内把它当成已真。driver Failed 路径冻结收口，不伪调本回调。
    virtual void OnClose(Connection connection, std::error_code reason) {
        (void)connection;
        (void)reason;
    }
};

} // namespace bbt::infra::net

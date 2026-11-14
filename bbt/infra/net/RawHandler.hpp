#pragma once
#include <bbt/infra/net/ReclaimReceipt.hpp>

/**
 * @file RawHandler.hpp
 * @brief 原始回调接口。回调在 Reactor owner 的非业务协程边界执行，必须短、非阻塞、不可挂起。
 *
 * 同一连接的回调不重入，也不递归触发另一个用户回调。不同 owner 可以并发调用同一个共享 handler。
 *
 * 用户回调抛出的异常由 Engine 在调用点就地隔离，并累计到 HandlerErrorCount。异常不会逃逸到
 * driver 层，也不会被吞掉并当作成功，同时不会自动关闭任何连接。关闭仍由 owner 收口。
 */
#include <cstddef>
#include <cstdint>
#include <system_error>

#include <bbt/infra/net/Connection.hpp>

namespace bbt::infra::net {

/**
 * @brief 单个已接纳写操作的终态。
 *
 * sent 为该操作实际交给内核的字节数，即真实前缀。error 为该操作的真实错误，无错误表示本地完成。
 */
struct WriteResult {

    /**
     * owner 分配的写操作身份，用于终态诊断；AsyncWrite 不返回票据
     */
    std::uint64_t operation{0};

    /**
     * 实际交给内核的字节前缀
     */
    std::size_t sent{0};

    /**
     * 该操作的真实错误；空表示本地完成
     */
    std::error_code error;
};

/**
 * @brief 原始消费面回调接口，由 Engine owner 在 driver 边界调用。
 *
 * 所有回调都默认空实现。回调内不可挂起、不可长时间阻塞，也不应递归触发用户回调。
 */
class RawHandler {
public:
    virtual ~RawHandler() = default;

    /**
     * @brief 连接被 owner 接纳并登记后交付稳定句柄；不会自动创建业务协程。
     */
    virtual void OnAccept(Connection connection) { (void)connection; }

    /**
     * @brief 非阻塞建连的终态，成功或失败恰好交付一次。
     *
     * 失败时 connection 无效、error 非零。operation 与 AsyncConnect 返回的操作身份相同，
     * 成功和失败都可据此关联。
     */
    virtual void OnConnect(Connection connection, std::error_code error, std::uint64_t operation)
    {
        (void)connection;
        (void)error;
        (void)operation;
    }

    /**
     * @brief 只是通知，提示复查同一接收 ring 与持久读状态。多次通知可以合并，它不是数据快照。
     */
    virtual void OnRead(Connection connection) { (void)connection; }

    /**
     * @brief 每个已接纳操作恰好交付一次终态。在状态发布之后、锁外、非递归地调用。
     */
    virtual void OnWrite(Connection connection, WriteResult result)
    {
        (void)connection;
        (void)result;
    }

    /**
     * @brief 物理 fd 关闭后由 owner 通知，同一连接最多一次。
     *
     * reason 是 owner 发布的关闭原因。本回调只证明 fd 已关闭，并不代表本连接的全部责任已清理：
     * raw 回调或 Co 等待者可能仍在进行。物理出账的事实以 Connection::Released() 为准；
     * 带句柄 CoDial 则以 ReclaimReceipt::Released() 为准。不得在本回调内把它当作已真。
     * driver 进入 Failed 时冻结收口，不调用本回调。
     */
    virtual void OnClose(Connection connection, std::error_code reason)
    {
        (void)connection;
        (void)reason;
    }
};

// clang-format off
}
// clang-format on

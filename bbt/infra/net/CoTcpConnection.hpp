#pragma once

/**
 * @file CoTcpConnection.hpp
 * @brief TCP 连接的 ICoConnection 实现。纯值包装，不复制状态。
 *
 * 本对象只持有一个 Connection 值句柄（内部是 shared_ptr<ConnectionState>），不复制 ring、
 * 不缓存状态投影、不建第二队列。所有 ICoConnection 成员直接转发到底层 Connection/ConnectionState
 * 的同一真源。拷贝本对象只是再持一份同一连接的视图，销毁视图不影响连接寿命；关闭经 RequestClose
 * 发起，由 owner 完成物理收口。
 *
 * 构造责任：只有 Engine::CoAccept/CoDial 与测试接缝以 Connection 构造本对象；消费方不能凭裸状态
 * 构造。Connection 的私有构造约束原样继承。
 *
 * 实现位置：Read、Write、WaitClosed、AsyncWrite 与 Status 的函数体在 CoTcpConnection.cc，
 * 等待逻辑依赖 ConnectionState 与 detail 部件，不进入公共头。RequestClose 与 Raw 为纯转发，
 * 留在头内。
 */

#include <cstdint>
#include <string>
#include <system_error>

#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/net/Buffer.hpp>
#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/ICoConnection.hpp>
#include <bbt/infra/net/Types.hpp>
#include <bbt/infra/net/WriteOperation.hpp>

namespace bbt::infra::net {

/// @brief ICoConnection 的 TCP 实现：持有 Connection 值句柄的纯转发视图。
class CoTcpConnection final : public ICoConnection {
public:
    /// @brief 构造无效视图；不持 socket，不承担资源责任。
    CoTcpConnection() = default;

    /// @brief 以既有连接句柄构造视图，共享同一状态而不复制；explicit 防隐式转换。
    explicit CoTcpConnection(Connection connection) noexcept : m_conn(std::move(connection))
    {
    }
    /// @brief 析构不隐式 RequestClose，也不等物理回收。
    ~CoTcpConnection() override = default;

    // 以下每个 override 完整继承 ICoConnection 的域/挂起/所有权/错误契约。
    using ICoConnection::Read;
    using ICoConnection::TryRead;
    /// @brief 非等待 owning 读取；契约见 ICoConnection::TryRead。
    std::error_code TryRead(Buffer& out) override
    {
        return m_conn.TryRead(out);
    }
    /// @brief 协程等待 owning 读取；契约见 ICoConnection::Read。
    std::error_code Read(Buffer& out, const bbt::coroutine::WaitOptions& options = {}) override;
    /// @brief 非挂起提交写；契约见 ICoConnection::AsyncWrite。
    WriteOperation AsyncWrite(std::string data) override;
    /// @brief 协程内组合等待写；契约见 ICoConnection::Write。
    WriteReport Write(std::string data, const bbt::coroutine::WaitOptions& options = {}) override;
    /// @brief 等待逻辑关闭终态；契约见 ICoConnection::WaitClosed。
    std::error_code WaitClosed(const bbt::coroutine::WaitOptions& options = {}) override;
    /// @brief 转发到底层 Connection::RequestClose。
    void RequestClose(std::error_code reason = {}) override;
    /// @brief 返回状态投影副本；契约见 ICoConnection::Status。
    ConnectionStatus Status() const override;

    /**
     * @brief 返回底层 Connection 的 const 引用，指向同一共享状态，不是副本。
     *
     * 它只能用于只读查询（例如 Valid、Id、Closed、Released）。要调用 AsyncWrite、TryRead、
     * TryAccess 等非 const 方法，需要持有一个 Connection 副本。
     */
    const Connection& Raw() const noexcept
    {
        return m_conn;
    }

private:
    Connection m_conn; // 唯一的共享值句柄，没有第二数据队列或关闭槽
};

} // namespace bbt::infra::net

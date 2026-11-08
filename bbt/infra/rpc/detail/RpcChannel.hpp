#pragma once
// RPC Channel 接缝（bbt/infra/rpc 实现 TU 专用，非公开头）。
//
// Channel 是无状态字节转发：ReadBytes 交付底层已到达字节（不解析、不分帧），
// WriteBytes 把一条连续字节交给底层，RequestClose/IsClosed/IsReleased 表达
// 物理关闭。RPC 帧缓存、拆包游标、pending、顺序、握手进度、路由与关闭状态
// 一律归 RpcSession 一侧；Channel 自己只保留底层句柄与物理关闭状态。
//
// Channel 不提供独立的 WaitClosed 生命周期接口：关闭与等待统一从 RPC 会话
// 的 Close 入口表达。本接缝也不是 transport 插件平台——新传输按同一最小
// 契约加 adapter（如后续 HttpChannel），不引入 registry/ABI。

#include <string>
#include <system_error>
#include <tuple>
#include <utility>

#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/infra/net/Buffer.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/WriteOperation.hpp>

namespace bbt::infra::rpc {
namespace detail {

/**
 * @brief 协议无关的单 link 字节通道。
 *
 * 只表达读、写与物理关闭；不承诺分帧、关联、重发或对端回包。Read/Write
 * 的域与等待语义沿用 net::ICoConnection 既有契约（受管协程内可挂起）。
 */
class ICoChannel {
public:
    virtual ~ICoChannel() = default;

    /// @brief 等待并把底层已到达字节复制到 out 后消费；不解析帧。
    /// 仅受管协程内可调用；options 只约束本次等待。
    virtual std::error_code ReadBytes(net::Buffer& out, const bbt::coroutine::WaitOptions& options = {}) = 0;
    /// @brief 非等待读取；失败时 out 不变、不消费数据。
    virtual std::error_code TryReadBytes(net::Buffer& out) = 0;
    /// @brief 读并以 tuple 返回缓冲区和结果；失败时缓冲区为空。
    std::tuple<net::Buffer, std::error_code> ReadBytes(const bbt::coroutine::WaitOptions& options = {})
    {
        net::Buffer out;
        const auto error = ReadBytes(out, options);
        if (error) {
            return {net::Buffer{}, error};
        }
        return {std::move(out), std::error_code{}};
    }

    /// @brief 非挂起全量接纳或零接纳；不等待容量与本地发送完成。
    virtual std::error_code WriteBytes(std::string bytes) = 0;
    /// @brief 同一绝对 deadline 依次等容量、接纳与本地完成；不等待业务回包。
    virtual net::WriteReport WriteBytes(std::string bytes, const bbt::coroutine::WaitOptions& options) = 0;

    /// @brief 请求物理封口：任意域、不挂起、幂等；首次 reason 固定。
    /// 不保证冲刷未发完的写；等待排空与收口归 RpcSession 的 Close 语义。
    virtual void RequestClose(std::error_code reason = {}) = 0;
    /// @brief 逻辑关闭终态（任意域、不挂起）。
    virtual bool IsClosed() const = 0;
    /// @brief 物理资源是否已回收（fd 关 + owner 本连接责任清）。
    virtual bool IsReleased() const = 0;
    /// @brief 句柄是否持有效共享状态。
    virtual bool Valid() const = 0;
};

/**
 * @brief TCP Channel adapter：持有一条 net::CoTcpConnection 值视图。
 *
 * 只转发字节与关闭请求，自身不维护 RPC 接收/发送缓存——那属于 RpcSession。
 */
class TcpChannel final : public ICoChannel {
public:
    TcpChannel() = default;
    explicit TcpChannel(net::CoTcpConnection conn) noexcept : m_conn(std::move(conn))
    {
    }
    ~TcpChannel() override = default;
    TcpChannel(const TcpChannel&) = delete;
    TcpChannel& operator=(const TcpChannel&) = delete;
    TcpChannel(TcpChannel&&) noexcept = default;
    TcpChannel& operator=(TcpChannel&&) noexcept = default;

    /// @brief 底层连接视图（只读访问权；握手台账取 Raw()/Id()）。
    const net::CoTcpConnection& Connection() const noexcept
    {
        return m_conn;
    }
    /// @brief 底层共享句柄（连接台账与 RequestClose 透传点）。
    const net::Connection& Raw() const noexcept
    {
        return m_conn.Raw();
    }

    // 基类的 tuple 便捷重载不被本类同名 override 遮蔽。
    using ICoChannel::ReadBytes;

    std::error_code ReadBytes(net::Buffer& out, const bbt::coroutine::WaitOptions& options = {}) override
    {
        return m_conn.Read(out, options);
    }
    std::error_code TryReadBytes(net::Buffer& out) override
    {
        return m_conn.TryRead(out);
    }
    std::error_code WriteBytes(std::string bytes) override
    {
        return m_conn.AsyncWrite(std::move(bytes));
    }
    net::WriteReport WriteBytes(std::string bytes, const bbt::coroutine::WaitOptions& options) override
    {
        return m_conn.Write(std::move(bytes), options);
    }
    void RequestClose(std::error_code reason = {}) override
    {
        m_conn.RequestClose(reason);
    }
    bool IsClosed() const override
    {
        return m_conn.IsClosed();
    }
    bool IsReleased() const override
    {
        return m_conn.IsReleased();
    }
    bool Valid() const override
    {
        return m_conn.Raw().Valid();
    }

private:
    net::CoTcpConnection m_conn;
};

} // namespace detail
} // namespace bbt::infra::rpc

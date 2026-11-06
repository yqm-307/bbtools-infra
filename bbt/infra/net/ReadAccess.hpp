#pragma once

/**
 * @file ReadAccess.hpp
 * @brief 公共读取守卫：在一次短保护内组合 Peek 与 Consume。
 *
 * 寿命：守卫私有持有连接状态的强引用。因此，即使 Engine 已收口、调用方已释放最后一个 Connection，
 * 守卫仍可安全调用 Size、Peek、Consume 并析构。关闭后已收的尾部数据仍可读。
 * 保活不等于长时间持锁。内部的解锁与通知一定早于最后一个状态引用的释放。移动之后，源对象不再持锁，
 * 也不再承担通知责任。
 *
 * 只允许移动。移动赋值先安全释放旧守卫（解锁、通知、释放旧状态），再接管新守卫。
 * 构造不做额外的堆分配，shared_ptr 与 unique_lock 都是值语义。
 *
 * 有效期：Peek 返回的视图只在守卫有效，且底层内容未被 Consume 或 ConsumeInto 改变时有效。
 * 不得跨越挂起点，不得调用未知代码，也不得外借。Consume 或 ConsumeInto 之后，旧视图失效。
 */

#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <system_error>

#include <bbt/infra/net/Types.hpp>

namespace bbt::infra::net {

class ConnectionState;
class ReceiveRing;

/**
 * @brief 一次短保护的读取守卫，持有连接状态强引用与 ring 锁。
 *
 * 无效守卫（未取得保护）的 Size 为 0、Peek 为空、Consume 与 ConsumeInto 返回拒绝。
 * 只可移动；析构时完成解锁与通知。
 */
class ReadAccess {
public:
    /// @brief 构造无效守卫，不持锁也不持有连接状态。
    ReadAccess() noexcept = default;
    /// @brief 移动构造：接管锁与状态引用，源对象不再持锁。
    ReadAccess(ReadAccess&& other) noexcept;
    /// @brief 移动赋值：先释放旧守卫（解锁、通知、释放旧状态），再接管新守卫。
    ReadAccess& operator=(ReadAccess&& other) noexcept;
    /// @brief 拷贝已删除：锁与通知责任不能有两份副本。
    ReadAccess(const ReadAccess&) = delete;
    /// @brief 拷贝赋值已删除，原因同拷贝构造。
    ReadAccess& operator=(const ReadAccess&) = delete;
    /// @brief 释放守卫：解锁并通知等待者。
    ~ReadAccess();

    /// @brief 是否已取得保护并可以访问数据。
    explicit operator bool() const noexcept
    {
        return m_status == ReadStatus::Ok;
    }
    /// @brief 是否与其它路径竞争失败，本次零消费。
    bool Busy() const noexcept
    {
        return m_status == ReadStatus::Busy;
    }
    /// @brief 是否暂无数据且尚未到达终态。
    bool WouldBlock() const noexcept
    {
        return m_status == ReadStatus::WouldBlock;
    }
    /// @brief 是否到达持久 EOF，且尾部数据已耗尽。
    bool Eof() const noexcept
    {
        return m_status == ReadStatus::Eof;
    }
    /// @brief 是否处于持久错误。
    bool Error() const noexcept
    {
        return m_status == ReadStatus::Error;
    }
    /// @brief 持久错误原因；非 Error 状态时不携带错误。
    std::error_code ErrorCode() const noexcept
    {
        return m_error;
    }

    // 以下访问仅在 operator bool 为真时有意义。守卫无效时，Size 返回 0，Peek 返回空，Consume 与 ConsumeInto 返回拒绝。
    /// @brief 当前保护内可读的字节数；无效守卫为 0。
    std::size_t Size() const;
    /// @brief 返回可读内容的视图，不复制；有效期见文件说明。
    std::string_view Peek() const;
    /// @brief 消费前 count 字节；失败时不改变 ring 状态。
    std::error_code Consume(std::size_t count);
    /// @brief 复制全部数据但不消费。异常安全，不移动读取游标，跨绕环完整。
    std::error_code CopyAll(std::string& out) const;
    /// @brief 复制并消费，提供强异常保证：out 追加失败时 ring 不消费。
    std::error_code ConsumeInto(std::string& out);

private:
    friend class Connection;
    friend class ConnectionState;
    friend class ReceiveRing;
    friend class ConnectionTestAccess;
    ReadAccess(std::shared_ptr<ConnectionState> state, std::unique_lock<std::mutex> lock, ReadStatus status,
               std::error_code error);

    void Release() noexcept;

    // 成员顺序即释放顺序：Release 在析构时完成解锁与通知，m_state 最后释放。
    std::shared_ptr<ConnectionState> m_state;
    std::unique_lock<std::mutex> m_lock;
    ReadStatus m_status{ReadStatus::WouldBlock};
    std::error_code m_error;
    bool m_changed{false};
};

} // namespace bbt::infra::net

#pragma once

// 公共读取守卫（D1/D2）：一次短保护内的 Peek+Consume 组合视图。
//
// 寿命（D2）：守卫私有持连接状态强引用，因此 Engine 收口、调用者释放最后一个
// Connection 之后守卫仍可安全 Size/Peek/Consume/析构；关闭后的已收尾部仍可读。
// 保活不等于授予长时间持锁权限：内部解锁与 Notify 一定早于最后一个状态引用
// 释放，移动后源对象不再持锁、不再承担通知责任。
//
// 只允许移动；移动赋值先安全释放旧守卫（解锁 → 通知 → 释放旧状态），再接管
// 新守卫。构造不额外堆分配（shared_ptr/unique_lock 均为值语义拷贝）。
//
// 有效期：Peek 视图只在守卫有效且底层内容未被 Consume/ConsumeInto 改变时有效；
// 不得跨挂起、不得调用未知代码、不得外借；Consume/ConsumeInto 之后旧视图失效。

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

class ReadAccess {
public:
    ReadAccess() noexcept = default;
    ReadAccess(ReadAccess&& other) noexcept;
    ReadAccess& operator=(ReadAccess&& other) noexcept;
    ReadAccess(const ReadAccess&) = delete;
    ReadAccess& operator=(const ReadAccess&) = delete;
    ~ReadAccess();

    explicit operator bool() const noexcept { return m_status == ReadStatus::Ok; }
    bool Busy() const noexcept { return m_status == ReadStatus::Busy; }
    bool WouldBlock() const noexcept { return m_status == ReadStatus::WouldBlock; }
    bool Eof() const noexcept { return m_status == ReadStatus::Eof; }
    bool Error() const noexcept { return m_status == ReadStatus::Error; }
    std::error_code ErrorCode() const noexcept { return m_error; }

    // 以下访问仅在 operator bool 为真时有意义；无保护时返回 0/空/拒绝。
    std::size_t Size() const;
    std::string_view Peek() const;
    std::error_code Consume(std::size_t count);
    // 复制全部但不消费（异常安全，不改游标；跨绕环完整）。
    std::error_code CopyAll(std::string& out) const;
    // 强保证复制+消费：out 追加失败时 ring 不消费。
    std::error_code ConsumeInto(std::string& out);

private:
    friend class Connection;
    friend class ConnectionState;
    friend class ReceiveRing;
    friend class ConnectionTestAccess;
    ReadAccess(std::shared_ptr<ConnectionState> state, std::unique_lock<std::mutex> lock,
               ReadStatus status, std::error_code error);

    void Release() noexcept;

    // 成员顺序即寿命顺序：Release 在析构体内完成解锁/Notify，m_state 最后释放。
    std::shared_ptr<ConnectionState> m_state;
    std::unique_lock<std::mutex> m_lock;
    ReadStatus m_status{ReadStatus::WouldBlock};
    std::error_code m_error;
    bool m_changed{false};
};

} // namespace bbt::infra::net

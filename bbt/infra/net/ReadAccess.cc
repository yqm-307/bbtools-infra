#include <bbt/infra/net/ReadAccess.hpp>

#include <utility>

#include <bbt/infra/net/detail/ConnectionState.hpp>

// 实现放 .cc：守卫要在解锁/Notify 之后才释放最后一个状态引用，顺序必须与
// ConnectionState 的完整定义一起编译，同时避免把 ring/state 布局经公共头传出。

namespace bbt::infra::net {

ReadAccess::ReadAccess(std::shared_ptr<ConnectionState> state, std::unique_lock<std::mutex> lock,
                       ReadStatus status, std::error_code error)
    : m_state(std::move(state)),
      m_lock(std::move(lock)),
      m_status(status),
      m_error(error) {}

ReadAccess::ReadAccess(ReadAccess&& other) noexcept
    : m_state(std::move(other.m_state)),
      m_lock(std::move(other.m_lock)),
      m_status(other.m_status),
      m_error(other.m_error),
      m_changed(other.m_changed) {
    // 源对象不再拥有锁与通知责任（unique_lock/state 都已移出）。
    other.m_state.reset();
    other.m_status = ReadStatus::WouldBlock;
    other.m_error = {};
    other.m_changed = false;
}

ReadAccess& ReadAccess::operator=(ReadAccess&& other) noexcept {
    if (this != &other) {
        Release();  // 先释放旧守卫（解锁 → Notify → 释放旧 state），再接管新守卫
        m_state = std::move(other.m_state);
        m_lock = std::move(other.m_lock);
        m_status = other.m_status;
        m_error = other.m_error;
        m_changed = other.m_changed;
        other.m_state.reset();
        other.m_status = ReadStatus::WouldBlock;
        other.m_error = {};
        other.m_changed = false;
    }
    return *this;
}

ReadAccess::~ReadAccess() { Release(); }

void ReadAccess::Release() noexcept {
    if (m_lock.owns_lock()) {
        m_lock.unlock();
        if (m_state) {
            // 通知必须早于最后一个 state 引用释放：AfterProtectionRelease 只
            // 触达 wake/data hub，不延长 state 寿命。
            m_state->ring.AfterProtectionRelease(m_changed);
        }
    }
    m_state.reset();
    m_changed = false;
}

std::size_t ReadAccess::Size() const {
    return m_lock.owns_lock() && m_state ? m_state->ring.LockedSize() : 0;
}

std::string_view ReadAccess::Peek() const {
    return m_lock.owns_lock() && m_state ? m_state->ring.LockedPeek() : std::string_view{};
}

std::error_code ReadAccess::Consume(std::size_t count) {
    if (!m_lock.owns_lock() || !m_state) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    const auto error = m_state->ring.LockedDiscard(count);
    if (!error) {
        m_changed = true;
    }
    return error;
}

std::error_code ReadAccess::CopyAll(std::string& out) const {
    if (!m_lock.owns_lock() || !m_state) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    return m_state->ring.LockedCopyAll(out);
}

std::error_code ReadAccess::ConsumeInto(std::string& out) {
    if (!m_lock.owns_lock() || !m_state) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    const auto error = m_state->ring.LockedConsumeInto(out);
    if (!error) {
        m_changed = true;
    }
    return error;
}

} // namespace bbt::infra::net

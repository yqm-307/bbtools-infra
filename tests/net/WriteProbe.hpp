#pragma once

#include <memory>
#include <bbt/infra/net/detail/ConnectionState.hpp>

/**
 * 既有账本测试借用的等待槽；观察与撤销均经 state 写锁，槽不进入公共 API。
 */
class WriteProbe {
public:
    std::error_code Submit(bbt::infra::net::ConnectionState& state, std::string_view data)
    {
        auto slot = std::make_unique<bbt::infra::net::WriteWait>();
        std::uint64_t id = 0;
        const auto error = state.SubmitWrite(data, id, slot.get());
        if (!error) {
            m_state = &state;
            m_id = id;
            m_slot = std::move(slot);
        }
        return error;
    }

    bbt::infra::net::WriteWait Query() const { return m_state->QueryWrite(*m_slot); }

    std::uint64_t Id() const { return m_id; }

    ~WriteProbe()
    {
        if (m_slot)
            m_state->CancelWrite(m_id, *m_slot, std::make_error_code(std::errc::operation_canceled));
    }

private:
    bbt::infra::net::ConnectionState* m_state{nullptr};
    std::uint64_t m_id{0};
    std::unique_ptr<bbt::infra::net::WriteWait> m_slot;
};

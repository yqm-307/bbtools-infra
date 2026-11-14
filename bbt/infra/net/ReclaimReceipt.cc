#include <bbt/infra/net/ReclaimReceipt.hpp>
#include <bbt/infra/net/detail/ReclaimRecord.hpp>

namespace bbt::infra::net {

/**
 * ReclaimReceipt：无效句柄没有已提交资源责任（Released=true）；有效句柄的
 * 释放状态在共享记录上，终态在句柄寿命内永久可查，观察不改变回收本身。
 */
bool ReclaimReceipt::Valid() const noexcept
{
    return m_record != nullptr;
}

bool ReclaimReceipt::Released() const noexcept
{
    return !m_record || m_record->released.load(std::memory_order_acquire);
}

ReclaimReceipt::ReclaimReceipt(std::shared_ptr<ReclaimRecord> record)
    : m_record(std::move(record))
{
}

// clang-format off
}
// clang-format on

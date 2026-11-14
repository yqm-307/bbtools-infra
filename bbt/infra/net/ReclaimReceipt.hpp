#pragma once

/**
 * @brief 连接物理回收凭据。
 */
#include <memory>

namespace bbt::infra::net {
struct ReclaimRecord;
struct CoAccess;

/**
 * @brief 一次带句柄 CoDial 的物理回收观察句柄。
 *
 * 值语义，默认无效。
 *   - 无效句柄：Released() 为 true。没有已提交的资源责任，即默认构造，或调用在上下文或提交前被拒绝。
 *   - 有效句柄：Released() 为 false 时，本次建连与物理 socket 责任仍存活，包括排队、建连中、
 *     已连接但未交付，以及已交付但尚未物理收口的连接。
 *   - 有效句柄：Released() 为 true 时，本次 fd 已真实关闭，owner 资源责任已清理。未交付的失败由
 *     owner 回收收口；成功交付则与该连接的 Released 同为终态。
 *
 * 句柄只做观察，不等待、不取消、不收回。它不延长 socket 或 Engine 的寿命，也不与 Engine 成环。
 * 持有句柄不阻碍 Engine 回收物理资源。它不是 Future，也不是等待机制。
 */
class ReclaimReceipt {
public:
    ReclaimReceipt() = default;

    /**
     * @brief 是否来自一次已提交的建连；默认构造与提交前被拒绝的调用为 false。
     */
    bool Valid() const noexcept;

    /**
     * @brief 本次 fd 是否已真实关闭且 owner 责任已清理；无效句柄恒为 true。
     */
    bool Released() const noexcept;

private:
    /**
     * 内部构造：只有带句柄 CoDial 凭本次提交的共享记录构造有效句柄。
     */
    friend struct CoAccess;
    explicit ReclaimReceipt(std::shared_ptr<ReclaimRecord> record);

    std::shared_ptr<ReclaimRecord> m_record;
};

// clang-format off
}
// clang-format on

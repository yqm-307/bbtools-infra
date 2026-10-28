#pragma once

// 物理回收共享释放记录（detail 内部部件）：一次 dial 提交前预分配的原子标记，
// 是 ReclaimReceipt 与 Connection::Released() 的同一真源。released=true 只表示
// 「该次责任已无存活物理 socket」——fd 创建前失败即时收口，或 fd 在真实 close
// 点（NativeSocket/FdHandle 显式 close、ConnCore 后端 close、native 对象析构
// 兜底）关闭且 owner 责任清理。记录不指向 socket，不延长其寿命。

#include <atomic>

namespace bbt::infra::net {

struct ReclaimRecord {
    explicit ReclaimRecord(bool initial_released) noexcept
        : released(initial_released) {}

    std::atomic<bool> released;
};

} // namespace bbt::infra::net

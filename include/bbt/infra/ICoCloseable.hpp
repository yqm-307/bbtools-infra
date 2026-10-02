#pragma once
// co-network/v1 N0：关闭契约（进程寿命运行时修订）。
//
// 关闭时机由资源 owner/上层（framework manager）决定，不绑定 coroutine
// runtime：Close() 同步完成物理释放并返回——返回即本对象拥有的物理资源
// 已释放、后端不会再访问它们；未发送数据直接丢弃，不做 flush；不重开。
// IsClosed() 只读查询当前状态。
//
// 已删除（不保留兼容壳）：RequestClose / WaitClosed / ReleaseClosed 与
// CloseStatus 等待结果枚举；coroutine Stop、运行时代际、取消令牌、
// CompletionSignal 都不再参与资源收口。需要保序投递的上层应在 Close()
// 前自行确认已写完。

namespace bbt::infra {

class ICoCloseable {
public:
    virtual ~ICoCloseable() = default;

    // 幂等、可在任意线程调用（各实现显式声明其 owner 域与并发口径）。
    // 返回即物理资源已释放、后端不再访问；不承诺已发送请求的后端结果。
    virtual void Close() noexcept = 0;

    virtual bool IsClosed() const noexcept = 0;
};

} // namespace bbt::infra

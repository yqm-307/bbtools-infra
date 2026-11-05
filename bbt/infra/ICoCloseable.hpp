/** @file ICoCloseable.hpp
 * @brief 协程网络对象的关闭契约：同步释放物理资源，不依赖协程运行时。
 *
 * 关闭时机由资源 owner 或上层（framework manager）决定，本接口不绑定 coroutine runtime。
 * Close() 同步完成物理释放并返回：返回即本对象拥有的物理资源已释放、后端不会再访问它们；
 * 未发送的数据直接丢弃，不做 flush；关闭后不重新打开。IsClosed() 只读查询当前状态。
 *
 * 本接口不提供 RequestClose / WaitClosed / ReleaseClosed 等异步关闭接口；coroutine Stop、
 * 运行时代际、取消令牌与 CompletionSignal 都不参与资源收口。需要保序投递的上层，应在 Close() 之前
 * 自行确认数据已写完。
 */
#pragma once

namespace bbt::infra {

/** @brief 可同步关闭的资源接口：Close() 返回即物理资源已释放。 */
class ICoCloseable {
public:
    /// @brief 虚析构；本接口不提供额外的析构语义。
    virtual ~ICoCloseable() = default;

    /**
     * @brief 同步释放物理资源。幂等，可在任意线程调用；各实现须显式声明其 owner 域与并发口径。
     *
     * 返回即物理资源已释放、后端不再访问；不承诺已发送请求的后端结果。
     */
    virtual void Close() noexcept = 0;

    /// @brief 只读查询是否已关闭。
    virtual bool IsClosed() const noexcept = 0;
};

} // namespace bbt::infra

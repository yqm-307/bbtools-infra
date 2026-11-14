/**
 * 单集合客户端的 owner 与句柄装配。
 */
#pragma once

#include <bbt/infra/mongo/detail/MongoCollImpl.hpp>

namespace bbt::infra::mongo_detail {

/**
 * CoMongoCliImpl：单集合 client 的实现，内部持有一个独占 runtime 与一个
 * 集合句柄。worker、队列与 pool 资源归属在 MongoRuntime，本类只做状态转发。
 */
class CoMongoCliImpl {
public:
    CoMongoCliImpl(MongoClientConfig config, bbt::coroutine::CoObjectInfo info);
    ~CoMongoCliImpl();

    result<void> Start();

    result<void> InsertOne(const MongoDocument& doc, const CallOptions& options);
    result<std::optional<MongoDocument>> FindOne(const MongoDocument& filter, const CallOptions& options);
    result<MongoUpdateResult>
    UpdateOne(const MongoDocument& filter, const MongoDocument& update, const CallOptions& options);
    result<std::uint64_t> DeleteOne(const MongoDocument& filter, const CallOptions& options);

    /**
     * client 关闭 = 句柄接纳封口 + 独占 runtime 同步收口：返回即 worker
     * 全退、ops 清空（未 Start 时无 runtime，本对象信号即刻落定）。
     * Close 与在途 Start 并发时先封口、再等该次 Start 完成（发布则同步
     * 收口，被抢先封口则 Start 自清不发布），不会出现「Close 已返回、
     * Start 事后发布资源」。
     */
    void Close() noexcept;

    /**
     * 语义：runtime 物理收口（ops 清空 + worker 全退）才视为 Closed；
     * 未 Start（m_runtime 为空）时看本对象 close 信号。
     */
    bool IsClosed() const noexcept;
    bbt::coroutine::CoObjectInfo GetObjectInfo() const;

    /**
     * @brief 测试观测保留外层句柄寿命，不单独拥有内部实现。
     */
    static std::shared_ptr<CoMongoCliImpl> ForTest(const std::shared_ptr<CoMongoCli>& client);

    /**
     * 真实生产状态观测：委托 runtime 的存活 worker 数（Release 下同样可用；
     * Start 未成功建立 runtime 时为 0）。
     */
    std::size_t LiveWorkersForTest() const noexcept;

    /**
     * 真实生产状态：委托 runtime 的接纳队列深度（读 m_queue，Release 下
     * 同样可用；Start 未建立 runtime 时为 0）。测试用它做确定性同步。
     */
    std::size_t QueuedOpsForTest() const noexcept;

    /**
     * 真实生产状态：委托 runtime 的执行槽占用数（读 m_ops + phase==kRunning，
     * Release 下同样可用；Start 未建立 runtime 时为 0）。
     */
    std::size_t DriverCallsInFlightForTest() const noexcept;
#ifdef BBT_INFRA_STRINGENT_DEBUG

    /**
     * 兼容 debug 观测钩子（委托 runtime；Start 未建立 runtime 时为 0）。
     * Release 下不存在。
     */
    std::size_t RunningDriverCallsForTest() const noexcept;
    std::size_t PeakDriverCallsForTest() const noexcept;
#endif

private:
    /**
     * 稳定快照：m_coll/m_runtime 由 Start 在 m_lifecycle_mtx 内发布、由
     * Close 在同一锁内取走并收口。所有直接消费者先取快照再使用，绝不
     * 持锁跨协程 Submit／回调，避免与 Close 配对制造死锁。
     */
    std::shared_ptr<MongoCollImpl> CollSnapshot() const noexcept;
    std::shared_ptr<MongoRuntime> RuntimeSnapshot() const noexcept;

    /**
     * 「Start 在途」标志的收尾：必须在资源自清完成后才清标志并唤醒
     * Close，保证 Close 返回当刻资源已收口。
     */
    void FinishStarting() noexcept;

    /**
     * Start 全部出口（成功/失败/被 Close 抢先封口）的作用域收尾。
     */
    struct StartingScope {
        CoMongoCliImpl* cli;

        explicit StartingScope(CoMongoCliImpl* c) noexcept
            : cli(c)
        {
        }

        StartingScope(const StartingScope&) = delete;
        StartingScope& operator=(const StartingScope&) = delete;
        ~StartingScope() noexcept;
    };

private:
    MongoClientConfig m_config;
    bbt::coroutine::CoObjectInfo m_info;
    ManagedCloseState m_close;

    /**
     * 发布/封口配对
     */
    mutable std::mutex m_lifecycle_mtx;

    /**
     * Start 完成唤醒 Close
     */
    std::condition_variable m_lifecycle_cv;

    /**
     * Start 在途
     */
    bool m_starting = false;

    /**
     * Start 成功才建立
     */
    std::shared_ptr<MongoRuntime> m_runtime;

    /**
     * 与 runtime 同建
     */
    std::shared_ptr<MongoCollImpl> m_coll;
};

// clang-format off
}
// clang-format on

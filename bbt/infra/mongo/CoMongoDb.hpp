/**
 * @brief Mongo worker/队列 owner 接口。
 */
#pragma once

#include <bbt/infra/mongo/CoMongoColl.hpp>

namespace bbt::infra::mongo {

/**
 * @brief Mongo worker/队列 owner，集合句柄共享其背压和 driver 资源。
 *
 * Close 默认 Immediate 任意线程非等待封口；Graceful 等已提交 op 的逻辑结果发布。
 * wait_release=true 在同一有限 deadline 等 native finalizer 退出 worker、join 并归还 lease；
 * Co 调用只挂起等待信号，不自己 join。超时保留真实责任；IsClosed 只在物理回收完成时为 true。
 * Collection 只拥有目标集合和 runtime 引用，关闭句柄不影响兄弟句柄。
 */
class CoMongoDb : public ICoNetwork {
public:
    /**
     * @brief 校验 owner 装配参数并取对象身份，不建立资源；资源由 Start 建立。
     * @param config owner 装配参数，校验规则见 ValidateMongoRuntimeConfig。
     * @return 成功返回 owner 句柄；参数非法返回 InvalidArgument。
     */
    static result<std::shared_ptr<CoMongoDb>> Create(MongoRuntimeConfig config);

    /**
     * @brief 同一 owner 只成功启动一次；Start 要求协程运行时已初始化，
     * 建立 worker 线程组并接入进程级 pool；关闭后不重开。
     * @return 协程运行时未初始化或 pool 获取失败返回 RuntimeUnavailable；已启动返回
     *         InvalidArgument；关闭中或已关闭返回 Closed。
     */
    virtual result<void> Start() = 0;

    /**
     * @brief 创建集合句柄：owner 必须 Running；target.database/.collection
     * 空则 InvalidArgument。
     *
     * 句柄共享 owner 的 worker/队列/pool，不新建线程。owner 关闭后不再接纳新句柄。
     * @param target 目标 database 与 collection。
     */
    virtual result<std::shared_ptr<CoMongoColl>> Collection(MongoTarget target) = 0;

    /**
     * @brief 默认非等待；等待组合须有限绝对期限，Co 域以 50ms 上限分片挂起。
     */
    virtual result<void> Close(const CloseOptions& options = {}) = 0;
    virtual bool IsClosed() const noexcept = 0;

    /**
     * @brief 首个关闭原因；封口前为空，之后不覆盖。
     */
    virtual std::optional<Error> CloseReason() const noexcept = 0;
};

// clang-format off
}
// clang-format on

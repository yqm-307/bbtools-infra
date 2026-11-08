/** @file CoRedisPool.hpp
 * @brief 固定大小 Redis 连接池：业务侧无手动 Acquire，命令内部借还连接。
 *
 * 只持有 CoRedisCli 句柄，不关共享 Engine；排队、借用与关闭语义见 CoRedisPool 类说明。
 */
#pragma once

#include <bbt/infra/redis/CoRedisCli.hpp>
#include <bbt/infra/redis/interface/IRedisCli.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/coroutine/syntax/Async.hpp>

namespace bbt::infra::redis {

/**
 * @brief 固定大小连接池：命令内部借连接执行归还，业务无需手动 Acquire。
 *
 * Create 只装配不联网；Connect 显式把全池建好才 Ready，部分失败回滚本次建连；
 * 重复 Connect 已就绪幂等，坏 slot 由显式 Connect 重建，无自动重连/命令重发。
 *
 * 池满时有界 FIFO 排队：max_waiters=0 拒绝排队返回 Overloaded；借与命令执行共用同一绝对
 * deadline；超时/取消/Close 的等待者不遗失，借用 RAII 归还。
 *
 * Close 并发安全幂等不可重开：立即逻辑封口并唤醒等待者，关闭持有 client，不关共享 Engine，
 * 不假装物理排空；in-flight 命令按底层单连接语义保留 OutcomeUnknown。
 */
class CoRedisPool final : public IRedisCli {
private:
    friend struct PoolTestAccess;
    struct State;

public:
    /**
     * @brief 建池参数。
     */
    struct PoolOptions {
        std::size_t connections; ///< 固定连接数，Create 校验大于零。
        std::size_t max_waiters; ///< 排队上限；0 表示池满立即 Overloaded。
        /// 连接级握手配置：对池开的每条连接（含修复/重建）在 Connect 内生效；
        /// 非法配置在建连时由 CoRedisCli::Create 拒绝并按 rollback 归还预算。
        RedisHandshake handshake{};
    };

    /**
     * @brief 一次借用的 RAII guard：析构按实际流健康状态归还。
     *
     * 坏流 Retire 关闭连接并退役归属 slot（坏流不回池），等待显式 Connect 修复。
     */
    class Borrow {
    public:
        /// @brief 转移借用句柄（默认成员语义，不触碰池内状态）。
        Borrow(Borrow&&) noexcept = default;
        /// @brief 转移借用句柄（默认成员语义，不触碰池内状态）。
        Borrow& operator=(Borrow&&) noexcept = default;
        /// @brief 按流健康状态归还连接，见类说明。
        ~Borrow();

        std::shared_ptr<CoRedisCli> client; ///< 借到的连接；Retire 后不再可用。
        /// @brief 声明本次借用发现坏流：关闭连接并退役 slot，等显式 Connect 修复。
        void Retire();

    private:
        friend class CoRedisPool;
        Borrow(std::shared_ptr<State> state, std::shared_ptr<CoRedisCli> client, std::size_t index);
        void GiveBack();

        std::shared_ptr<State> state_;
        std::size_t index_{0};
        bool finished_{false};
    };

    /**
     * @brief 装配连接池，只建句柄不联网。
     * @param engine 宿主拥有并推进的 Engine。
     * @param address 每条连接的目标地址，须为数值 IPv4/IPv6。
     * @param options 建池参数，见 PoolOptions。
     * @return 成功返回池句柄；参数非法返回 InvalidArgument。
     */
    static result<std::shared_ptr<CoRedisPool>> Create(std::shared_ptr<net::Engine> engine, net::Address address,
                                                       PoolOptions options);
    /// @brief 只调用 Close()，不等在途命令退出。
    ~CoRedisPool() noexcept override;
    /// @brief 不可拷贝：句柄独占池槽位与等待者队列。
    CoRedisPool(const CoRedisPool&) = delete;
    /// @brief 不可拷贝赋值。
    CoRedisPool& operator=(const CoRedisPool&) = delete;

    /**
     * @brief 显式建连/修复：全池就绪才 ok；部分失败回滚本次建连；已就绪且无坏
     * slot 时幂等零动作。并发 Connect 竞争返回 Overloaded。
     * @param options 绝对期限选项，同时约束建连各步。
     */
    bbt_co_async(result<void>) Connect(const CallOptions& options) override;

    /**
     * @brief 命令方法：内部借连接执行归还，业务无需手动 Acquire；与单连接 CoRedisCli
     * 同命令方法，调用方用 bbt_co_await。语义与单连接一致，排队与借用期限共用同一 deadline。
     */
    bbt_co_async(result<void>) Ping(const CallOptions& options) override;
    /// @brief GET：键缺失返回 ok(nullopt)。
    bbt_co_async(result<std::optional<std::string>>) Get(std::string_view key, const CallOptions& options) override;
    /// @brief 无条件 SET：写入成功返回 ok。
    bbt_co_async(result<void>) Set(std::string_view key, std::string_view value, const CallOptions& options) override;
    /// @brief 带选项的 SET：条件不满足（nil 回复）返回 ok(false)，不是错误。
    bbt_co_async(result<bool>) Set(std::string_view key, std::string_view value, const SetOptions& set_options,
                                   const CallOptions& options) override;
    /// @brief EXISTS：键存在为 true。
    bbt_co_async(result<bool>) Exists(std::string_view key, const CallOptions& options) override;
    /// @brief DEL：返回服务端报告的实际删除数量。
    bbt_co_async(result<std::uint64_t>) Delete(std::vector<std::string> keys, const CallOptions& options) override;
    /// @brief PEXPIRE：ok(true) 已设置过期；key 不存在 ok(false)。
    bbt_co_async(result<bool>)
        Expire(std::string_view key, std::chrono::milliseconds ttl, const CallOptions& options) override;
    /// @brief PTTL：KeyMissing / NoExpiry / Expires 三态。
    bbt_co_async(result<TtlResult>) Ttl(std::string_view key, const CallOptions& options) override;
    /// @brief INCR：返回服务端整数值；错误语义同 CoRedisCli::Incr。
    bbt_co_async(result<std::int64_t>) Incr(std::string_view key, const CallOptions& options) override;
    /// @brief DECR 一步。
    bbt_co_async(result<std::int64_t>) Decr(std::string_view key, const CallOptions& options) override;
    /// @brief INCRBY 指定增量。
    bbt_co_async(result<std::int64_t>)
        IncrBy(std::string_view key, std::int64_t delta, const CallOptions& options) override;
    /// @brief DECRBY 指定减量。
    bbt_co_async(result<std::int64_t>)
        DecrBy(std::string_view key, std::int64_t delta, const CallOptions& options) override;
    /// @brief HSET：新建字段 ok(true)，覆盖已有字段 ok(false)。
    bbt_co_async(result<bool>)
        HSet(std::string_view key, std::string_view field, std::string_view value, const CallOptions& options) override;
    /// @brief HGET：字段缺失或 key 不存在为 ok(nullopt)。
    bbt_co_async(result<std::optional<std::string>>)
        HGet(std::string_view key, std::string_view field, const CallOptions& options) override;
    /// @brief HDEL：返回删除的字段数量。
    bbt_co_async(result<std::uint64_t>)
        HDel(std::string_view key, std::vector<std::string> fields, const CallOptions& options) override;
    /// @brief HEXISTS：字段存在为 true。
    bbt_co_async(result<bool>)
        HExists(std::string_view key, std::string_view field, const CallOptions& options) override;
    /// @brief HGETALL：field/value 对按服务端顺序，协议异常时退役连接（见 CoRedisCli::HGetAll）。
    bbt_co_async(result<HashEntries>) HGetAll(std::string_view key, const CallOptions& options) override;

    /// @brief 立即逻辑封口并唤醒全部等待者；幂等不可重开，不关共享 Engine。
    void Close() noexcept override;
    /// @brief 只读查询逻辑封口：Close() 已生效为 true。
    bool IsClosed() const noexcept override;

private:
    /// @brief 池内借出一条连接（快路径或 FIFO 等待 handoff），命令包装共用。
    bbt_co_async(result<Borrow>) Acquire(const CallOptions& options);
    /// 命令包装共用样板（定义在 .cc，仅本翻译单元实例化）：借还语义与 Acquire/
    /// Borrow RAII 一致，把借到的 client 交给 invoke 并原样返回其终态。
    template <typename R, typename Invoke> bbt_co_async(R) RunBorrowed(const CallOptions& options, Invoke&& invoke);

    explicit CoRedisPool(std::shared_ptr<State> state) noexcept;
    std::shared_ptr<State> state_;
};

} // namespace bbt::infra::redis

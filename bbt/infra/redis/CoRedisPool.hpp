#pragma once

#include <bbt/infra/redis/CoRedisCli.hpp>

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

// 固定大小连接池：命令内部借连接执行归还，业务无需手动 Acquire。
// Create 只装配不联网；Connect 显式把全池建好才 Ready，部分失败回滚本次建连；
// 重复 Connect 已就绪幂等，坏 slot 由显式 Connect 重建，无自动重连/命令重发。
// 池满时有界 FIFO 排队：max_waiters=0 拒绝排队返回 Overloaded；借与命令执行
// 共用同一绝对 deadline；超时/取消/Close 的等待者不遗失，借用 RAII 归还。
// Close 并发安全幂等不可重开：立即逻辑封口并唤醒等待者，关闭持有 client，
// 不关共享 Engine，不假装物理排空；in-flight 命令按底层单连接语义保留
// OutcomeUnknown。
class CoRedisPool final {
private:
    friend struct PoolTestAccess;
    struct State;

public:
    struct PoolOptions {
        std::size_t connections; // 固定连接数，Create 校验大于零。
        std::size_t max_waiters; // 排队上限；0 表示池满立即 Overloaded。
        // 连接级握手配置：对池开的每条连接（含修复/重建）在 Connect 内生效；
        // 非法配置在建连时由 CoRedisCli::Create 拒绝并按 rollback 归还预算。
        RedisHandshake handshake{};
    };

    // 一次借用的 RAII guard：析构按实际流健康状态归还；坏流 Retire 关闭连接并
    // 退役归属 slot（坏流不回池），等待显式 Connect 修复。
    class Borrow {
    public:
        Borrow(Borrow&&) noexcept = default;
        Borrow& operator=(Borrow&&) noexcept = default;
        ~Borrow();

        std::shared_ptr<CoRedisCli> client;
        void            Retire();

    private:
        friend class CoRedisPool;
        Borrow(std::shared_ptr<State> state, std::shared_ptr<CoRedisCli> client, std::size_t index);
        void            GiveBack();

        std::shared_ptr<State> state_;
        std::size_t index_{0};
        bool finished_{false};
    };

    static result<std::shared_ptr<CoRedisPool>> Create(
        std::shared_ptr<net::Engine> engine,
        net::Address address,
        PoolOptions options);
    ~CoRedisPool() noexcept;
    CoRedisPool(const CoRedisPool&) = delete;
    CoRedisPool& operator=(const CoRedisPool&) = delete;

    // 显式建连/修复：全池就绪才 ok；部分失败回滚本次建连；已就绪且无坏
    // slot 时幂等零动作。并发 Connect 竞争返回 Overloaded。
    bbt_co_async(result<void>)    Connect(const CallOptions& options);

    // 命令方法：内部借连接执行归还，业务无需手动 Acquire；与单连接 CoRedisCli
    // 同命令方法，调用方用 bbt_co_await。
    bbt_co_async(result<void>)    Ping(const CallOptions& options);
    bbt_co_async(result<std::optional<std::string>>) Get(std::string_view key, const CallOptions& options);
    bbt_co_async(result<void>)    Set(std::string_view key, std::string_view value, const CallOptions& options);
    bbt_co_async(result<bool>)    Set(std::string_view key, std::string_view value, const SetOptions& set_options, const CallOptions& options);
    bbt_co_async(result<bool>)    Exists(std::string_view key, const CallOptions& options);
    bbt_co_async(result<std::uint64_t>) Delete(std::vector<std::string> keys, const CallOptions& options);
    bbt_co_async(result<bool>)    Expire(std::string_view key, std::chrono::milliseconds ttl, const CallOptions& options);
    bbt_co_async(result<TtlResult>) Ttl(std::string_view key, const CallOptions& options);
    bbt_co_async(result<std::int64_t>) Incr(std::string_view key, const CallOptions& options);
    bbt_co_async(result<std::int64_t>) Decr(std::string_view key, const CallOptions& options);
    bbt_co_async(result<std::int64_t>) IncrBy(std::string_view key, std::int64_t delta, const CallOptions& options);
    bbt_co_async(result<std::int64_t>) DecrBy(std::string_view key, std::int64_t delta, const CallOptions& options);
    bbt_co_async(result<bool>)    HSet(std::string_view key, std::string_view field, std::string_view value, const CallOptions& options);
    bbt_co_async(result<std::optional<std::string>>) HGet(std::string_view key, std::string_view field, const CallOptions& options);
    bbt_co_async(result<std::uint64_t>) HDel(std::string_view key, std::vector<std::string> fields, const CallOptions& options);
    bbt_co_async(result<bool>)    HExists(std::string_view key, std::string_view field, const CallOptions& options);
    bbt_co_async(result<HashEntries>) HGetAll(std::string_view key, const CallOptions& options);

    // 立即逻辑封口并唤醒全部等待者；幂等不可重开。不关共享 Engine。
    void            Close() noexcept;
    bool            IsClosed() const noexcept;

private:
    // 池内借出一条连接（快路径或 FIFO 等待 handoff）。命令包装共用。
    bbt_co_async(result<Borrow>) Acquire(const CallOptions& options);
    // 命令包装共用样板（定义在 .cc，仅本翻译单元实例化）：借还语义与 Acquire/
    // Borrow RAII 一致，把借到的 client 交给 invoke 并原样返回其终态。
    template <typename R, typename Invoke>
    bbt_co_async(R) RunBorrowed(const CallOptions& options, Invoke&& invoke);

    explicit        CoRedisPool(std::shared_ptr<State> state) noexcept;
    std::shared_ptr<State> state_;
};

} // namespace bbt::infra::redis

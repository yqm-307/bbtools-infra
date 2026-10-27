#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/coroutine/syntax/Async.hpp>

namespace bbt::infra::redis {

// 单连接、至多一个活动操作，无后台队列或隐式重连。所有等待入口要求协程域。
// 宿主拥有并推进无 RawHandler 的共享 Engine；地址只接受数值 IPv4/IPv6。
class CoRedisCli final {
public:
    static result<std::shared_ptr<CoRedisCli>> Create(std::shared_ptr<net::Engine> engine, net::Address address);
    ~CoRedisCli() noexcept;
    CoRedisCli(const CoRedisCli&) = delete;
    CoRedisCli& operator=(const CoRedisCli&) = delete;

    // 已连接时幂等；故障后须显式 Connect，竞争活动操作返回 Overloaded。
    // 可能挂起的等待入口用 async/await 语法糖标记；调用方经 bbt_co_await 消费。
    bbt_co_async(result<void>)    Connect(const CallOptions& options);
    bbt_co_async(result<void>)    Ping(const CallOptions& options);
    bbt_co_async(result<std::optional<std::string>>) Get(std::string_view key, const CallOptions& options);
    bbt_co_async(result<void>)    Set(std::string_view key, std::string_view value, const CallOptions& options);
    bbt_co_async(result<bool>)    Exists(std::string_view key, const CallOptions& options);
    bbt_co_async(result<std::uint64_t>) Delete(std::vector<std::string> keys, const CallOptions& options);

    // 立即逻辑封口并请求关闭已发布连接；不等 fd、在途拨号或 API 任务退出。
    // 宿主最终须 Close 全部 client、RequestClose Engine、join Run 且核 Stopped、
    // 等登记的全部 API 任务完成并释放捕获。不可直接充当旧 ICoCloseable 同步资源。
    void            Close() noexcept;
    bool            IsClosed() const noexcept;

private:
    friend class CoRedisPool;
    bool StreamHealthy() const noexcept;
    struct State;
    explicit        CoRedisCli(std::shared_ptr<State> state) noexcept;
    std::shared_ptr<State> state_;
};

} // namespace bbt::infra::redis

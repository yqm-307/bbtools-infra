/** @file Client.hpp
 * @brief Consul agent HTTP API 的最小注册/健康发现适配（bbt::infra::consul）。
 *
 * 职责：把「注册本实例」「注销本实例」「按服务名取 passing 实例快照」映射到
 * Consul agent HTTP API，并给出有限超时、稳定错误码与同步关闭收口。
 */
#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/http/Client.hpp>

namespace bbt::infra::consul {

/**
 * @brief agent 连接与查询选项。
 */
struct Options {
    /// 例 "http://127.0.0.1:8500"；仅 http + 数值 loopback，可带结尾 '/'。
    std::string agent_url;
    /// 单次操作的总期限（含连接、写出、读取、阻塞等待）。
    std::chrono::milliseconds request_timeout{3000};
    /// 阻塞查询的 wait；0 表示不附加 index/wait（只取一次权威快照）。
    std::chrono::milliseconds query_wait{2000};
    /// 空表示使用 agent 默认 datacenter。
    std::string datacenter;
    /// 空表示无 ACL；只走 X-Consul-Token 头，不落日志。
    std::string token;
};

/**
 * @brief 一次服务注册的本实例描述。
 */
struct ServiceRegistration {
    std::string id;                ///< 每进程 incarnation 唯一，不能用可复用地址充当。
    std::string name;              ///< 逻辑服务名。
    std::string address;           ///< 数值地址（不接受主机名）。
    std::uint16_t port{0};         ///< 服务端口。
    std::vector<std::string> tags; ///< 服务标签，原样交给 agent。
};

/// @brief 健康发现返回的单个实例（agent 视图里的一条 service 记录）。
struct Instance {
    std::string id;        ///< 实例 id。
    std::string name;      ///< 逻辑服务名。
    std::string address;   ///< 数值地址。
    std::uint16_t port{0}; ///< 端口。
};

/**
 * @brief 一次健康发现的快照。
 *
 * index 是 Consul 的 X-Consul-Index（agent 视图修订号）：只在同一 agent 内可比；有效
 * header 为 0 时表示 agent 未提供有效修订（调用方下一轮应以 index=1 重问，不能判等）。
 * 缺失、非数字或越界 header 是 ProtocolError。
 */
struct ServiceSnapshot {
    std::uint64_t index{0};          ///< agent 视图修订号。
    std::vector<Instance> instances; ///< passing 实例，顺序即 agent 返回顺序。
};

/**
 * @brief Consul agent 适配器：注册、注销与 passing 实例查询，含同步关闭收口。
 *
 * 不承担（由上层决定，不能在本层假装）：
 *   - 路由/负载均衡/重试策略：注册、注销、查询都有副作用或视图语义，本层不自动
 *     重试；调用方自己决定是否重发；
 *   - 阻塞查询循环与陈旧视图边界：本层只做一次有界查询并回报 X-Consul-Index；
 *   - TTL/业务健康判定：TCP check 只表达端口可达，不等于 RPC readiness；
 *   - 多 datacenter 选路、Enterprise namespace、ACL 管理（token 只透传）。
 *
 * 边界：
 *   - 只经 bbt::infra::http::Client 出网；不引入 Consul SDK、不自建 Engine/线程。
 *     该 HTTP Client 由调用方创建并专属本对象：Client 对它承担关闭责任，禁止
 *     与其它业务共用。
 *   - agent 地址只接受 http:// 数值 loopback（127.0.0.0/8）；https、主机名、
 *     远端地址在 Create 时 fail-closed 拒绝（DNS/远端注册中心不在此层承诺）。
 *   - 每次调用总耗时受 Options::request_timeout 绝对 deadline 限制；该 deadline
 *     同时交给 http::Client::CallOptions，与分阶段超时取更紧者。
 *   - token 只以 X-Consul-Token 头发送；错误消息与日志不含 token、完整请求头或
 *     注册/响应 payload。
 *   - 所有操作只在协程上下文调用（bbt_co_await）；协程外调用返回 InvalidContext，
 *     不抛异常、不静默降级。
 */
class Client : public bbt::infra::ICoCloseable {
public:
    /**
     * @brief 创建适配器并接管专属 HTTP Client。
     *
     * http 必须非空；成功后由本对象独占并负责关闭。失败不接管 http 的所有权语义
     * 之外的东西（调用方仍持有 shared_ptr，可按需自行关闭）。
     * @param http 专属 HTTP 客户端，不得为空，也不得与其它业务共用。
     * @param options agent 地址、期限与查询选项。
     * @return 成功返回适配器句柄；参数非法（含非 loopback/非 http 地址）返回 InvalidArgument。
     */
    static result<std::shared_ptr<Client>> Create(std::shared_ptr<bbt::infra::http::Client> http, Options options);

    /// @brief 只调用 Close()。
    ~Client() noexcept override;
    /// @brief 不可拷贝：适配器独占专属 HTTP Client 的关闭责任。
    Client(const Client&) = delete;
    /// @brief 不可拷贝赋值。
    Client& operator=(const Client&) = delete;

    /**
     * @brief PUT /v1/agent/service/register：仅 2xx 视为已登记。
     *
     * 非 2xx 为 TransportError 并保留状态码。同一 id 重复注册是 Consul 的更新语义，
     * 本层不判重。
     * @param registration 本实例描述。
     */
    bbt_co_async(result<void>) Register(ServiceRegistration registration);

    /**
     * @brief PUT /v1/agent/service/deregister/\<id\>：幂等。
     *
     * agent 对已不存在的 id 返回的 404 视为成功（目标已不存在即已注销），
     * 其它非 2xx 仍是 TransportError。
     * @param id 注册时使用的实例 id。
     */
    bbt_co_async(result<void>) Deregister(std::string_view id);

    /**
     * @brief GET /v1/health/service/\<name\>?passing=true[&dc=..][&index=N&wait=..]。
     *
     * index!=0 且 query_wait>0 时走阻塞查询。返回的实例顺序即 agent 返回顺序，
     * 不做排序或去重（上层视图替换时自行判定）。
     * @param service 逻辑服务名。
     * @param index 上一轮的 X-Consul-Index；0 表示不阻塞，只取一次权威快照。
     */
    bbt_co_async(result<ServiceSnapshot>) HealthyInstances(std::string_view service, std::uint64_t index = 0);

    /**
     * @brief 同步物理收口：拒绝新操作 → 等待已接纳交换完成或失败 → 关闭专属 HTTP
     * Client → 等待全部已登记操作离开本对象状态后返回。
     *
     * 首个与并发 Close 都等同一终态，返回即「已封口、专属 HTTP Client 已关闭、在飞操作
     * 已离开」；不设 1s 之类的提前返回上限——每个已登记操作受 Options::request_timeout
     * 绝对期限约束，终态可达：不以 drain timeout 放弃等待（见
     * docs/decisions/0005-co-io-adapter-contract.md）。协程上下文内以协作挂起等待，
     * 不阻塞 Processer 线程。幂等、可在任意线程调用；须在调度器/Engine 仍能推进在途操作时
     * 调用（框架在 handlers drain 后、网络关闭前调用）。
     */
    void Close() noexcept override;
    /// @brief 只读查询是否已封口。
    bool IsClosed() const noexcept override;

private:
    struct State;
    class OpScope;
    explicit Client(std::shared_ptr<State> state) noexcept;
    // Close 的等待半段：宿主线程阻塞等待、协程域协作挂起等待，都等同一终态。
    static void WaitTerminal(const std::shared_ptr<State>& state) noexcept;

    std::shared_ptr<State> m_state;
};

} // namespace bbt::infra::consul

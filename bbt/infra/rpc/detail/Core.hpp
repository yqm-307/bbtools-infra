#pragma once
// RPC core 模块内部共享助手（bbt::infra::rpc 实现 TU 专用，非公开头）。
// Client.cc / Server.cc 各自组合这些纯函数，不重复第二份实现。

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <string>
#include <utility>

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/rpc/RpcWire.hpp>

namespace bbt::infra::rpc {
namespace detail {

// deadline → wire remaining_budget_ms。已过期/未设置（epoch）在调用前显式
// 失败（确定 TimedOut，不发请求）；合法值 clamp 到 [1, kMaxRemainingBudgetMs]。
inline std::uint32_t RemainingBudgetMs(const bbt::coroutine::Deadline& deadline,
                                       result<void>& status) {
    const auto now = std::chrono::steady_clock::now();
    if (deadline <= now) {
        status = result<void>::err(
            MakeError(ErrorCode::TimedOut, "rpc deadline already expired"));
        return 0;
    }
    auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        deadline - now).count();
    if (remaining < 1)
        remaining = 1;
    if (remaining > static_cast<long long>(kMaxRemainingBudgetMs))
        remaining = static_cast<long long>(kMaxRemainingBudgetMs);
    status = result<void>::ok();
    return static_cast<std::uint32_t>(remaining);
}

// 服务端错误回复信封：success=false + 调用方 error；关联字段缺失时用
// 确定占位值（unparsed/unknown）保持 wire 可解析，预算取最小合法值 1。
inline RpcWireEnvelope ErrorEnvelope(const Error& error,
                                     std::string request_id,
                                     std::string service,
                                     std::string method,
                                     std::string request_schema,
                                     std::string response_schema) {
    RpcWireEnvelope out;
    out.profile_version = kRpcWireProfileVersion;
    out.request_id = request_id.empty() ? "unparsed" : std::move(request_id);
    out.service = service.empty() ? "unknown" : std::move(service);
    out.method = method.empty() ? "unknown" : std::move(method);
    out.request_schema = std::move(request_schema);
    out.response_schema = std::move(response_schema);
    out.remaining_budget_ms = 1;
    out.success = false;
    out.error = error;
    return out;
}

} // namespace detail
} // namespace bbt::infra::rpc

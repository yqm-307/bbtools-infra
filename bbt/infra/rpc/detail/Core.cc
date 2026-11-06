#include <bbt/infra/rpc/detail/Core.hpp>

namespace bbt::infra::rpc {
namespace detail {

std::uint32_t RemainingBudgetMs(const bbt::coroutine::Deadline& deadline, result<void>& status)
{
    const auto now = std::chrono::steady_clock::now();
    if (deadline <= now) {
        status = result<void>::err(MakeError(ErrorCode::TimedOut, "rpc deadline already expired"));
        return 0;
    }
    auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
    if (remaining < 1)
        remaining = 1;
    if (remaining > static_cast<long long>(kMaxRemainingBudgetMs))
        remaining = static_cast<long long>(kMaxRemainingBudgetMs);
    status = result<void>::ok();
    return static_cast<std::uint32_t>(remaining);
}

RpcWireEnvelope ErrorEnvelope(const Error& error, std::string request_id, std::string service, std::string method,
                              std::string request_schema, std::string response_schema)
{
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

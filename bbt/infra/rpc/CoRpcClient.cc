/**
 * 会话客户端生命周期；状态与网络责任统一由 session owner 持有。
 */
#include <bbt/infra/rpc/CoRpcClient.hpp>

#include <utility>

#include <bbt/infra/rpc/detail/RpcSessionState.hpp>
#include <bbt/infra/rpc/detail/Session.hpp>

namespace bbt::infra::rpc {

CoRpcClient::CoRpcClient(std::shared_ptr<SessionOwnerState> owner) noexcept
    : m_owner(std::move(owner))
{
}

result<std::shared_ptr<CoRpcClient>> CoRpcClient::Create(std::shared_ptr<net::Engine> engine, ClientOptions options)
{
    using R = result<std::shared_ptr<CoRpcClient>>;
    if (!engine) {
        return R::err(Err(ErrorCode::InvalidArgument, "engine required"));
    }
    if (auto valid = options.Validate(); !valid) {
        return R::err(valid.error());
    }
    if (!options.TrustedNetworkPlaintext()) {
        return R::err(Err(ErrorCode::UnsupportedRoute, "no verifier: trusted-network plaintext required"));
    }
    auto owner = std::make_shared<SessionOwnerState>();
    owner->engine = std::move(engine);
    owner->options = std::move(options);
    return R::ok(
        bbt::coroutine::MakeCoObject<CoRpcClient>("infra.rpc.CoRpcClient", owner->options.LocalIdentity(), [&] {
            return std::shared_ptr<CoRpcClient>(new CoRpcClient(std::move(owner)));
        }));
}

bbt_co_async(result<Response>) CoRpcClient::Call(std::string_view target, Request request, const CallOptions& options)
{
    return detail::SessionOwnerCall(m_owner, target, std::move(request), options);
}

bbt_co_async(result<void>) CoRpcClient::Send(std::string_view target, Request message, const SendOptions& options)
{
    return detail::SessionOwnerSend(m_owner, target, std::move(message), options);
}

std::shared_ptr<SessionHooks> CoRpcClient::Hooks() const noexcept
{
    return m_owner->hooks;
}

ClientStats CoRpcClient::Stats() const noexcept
{
    return detail::SessionOwnerStats(m_owner);
}

CoRpcClient::~CoRpcClient() noexcept
{
    detail::SessionOwnerSeal(m_owner, SettleCloseReason(Error{}, "rpc client: closed by owner"));
}

bbt_co_async(result<void>) CoRpcClient::Close(const CloseOptions& options)
{
    return detail::SessionOwnerClose(m_owner, options);
}

bool CoRpcClient::IsClosed() const noexcept
{
    return detail::SessionOwnerSealed(*m_owner);
}

std::optional<Error> CoRpcClient::CloseReason() const noexcept
{
    return detail::SessionOwnerCloseReason(*m_owner);
}

// clang-format off
}
// clang-format on

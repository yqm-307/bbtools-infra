#include <bbt/infra/rpc/Transport.hpp>

namespace bbt::infra::rpc {

void IRpcTransport::Close(Error reason) noexcept
{
    (void)reason;
    Close();
}

result<void> IRpcTransport::Shutdown(bbt::coroutine::Deadline deadline)
{
    (void)deadline;
    return result<void>::err(
        MakeError(ErrorCode::UnsupportedRoute, "rpc transport: Shutdown not implemented by this adapter"));
}

void IRpcInboundTransport::Close(Error reason) noexcept
{
    (void)reason;
    Close();
}

result<void> IRpcInboundTransport::Shutdown(bbt::coroutine::Deadline deadline)
{
    StopAccepting();
    return Drain(deadline);
}

} // namespace bbt::infra::rpc

#include <bbt/infra/net/ICoConnection.hpp>

namespace bbt::infra::net {

std::tuple<Buffer, std::error_code> ICoConnection::Read(const bbt::coroutine::WaitOptions& options)
{
    Buffer out;
    auto error = Read(out, options);
    return {std::move(out), error};
}

std::tuple<Buffer, std::error_code> ICoConnection::TryRead()
{
    Buffer out;
    auto error = TryRead(out);
    return {std::move(out), error};
}

} // namespace bbt::infra::net

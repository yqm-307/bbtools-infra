#pragma once

namespace bbt::infra::net {

inline std::tuple<Buffer, std::error_code> ICoConnection::Read(const bbt::coroutine::WaitOptions& options)
{
    Buffer out;
    auto error = Read(out, options);
    return {std::move(out), error};
}

inline std::tuple<Buffer, std::error_code> ICoConnection::TryRead()
{
    Buffer out;
    auto error = TryRead(out);
    return {std::move(out), error};
}

} // namespace bbt::infra::net

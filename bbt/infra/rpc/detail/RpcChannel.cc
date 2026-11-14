/**
 * Channel 接缝的非内联实现：tuple 便捷重载返回出参版同一份结果，
 * 不改变 Read/Write 的域与等待语义。
 */
#include <bbt/infra/rpc/detail/RpcChannel.hpp>

namespace bbt::infra::rpc {
namespace detail {

std::tuple<net::Buffer, std::error_code> ICoChannel::ReadBytes(const bbt::coroutine::WaitOptions& options)
{
    net::Buffer out;
    const auto error = ReadBytes(out, options);
    if (error) {
        return {net::Buffer{}, error};
    }
    return {std::move(out), std::error_code{}};
}

// clang-format off
}
// clang-format on
// clang-format off
}
// clang-format on

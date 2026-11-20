#pragma once

#include <cstddef>
#include <string>

#include <bbt/infra/rpc/SessionCodec.hpp>

namespace bbt::infra::rpc::detail {

// 直接编码到写队列使用的拥有型缓冲（含 u32be 前缀）。
result<std::string> EncodeFramedSessionBytes(const SessionFrame& frame, std::size_t max_frame_bytes);

} // namespace bbt::infra::rpc::detail
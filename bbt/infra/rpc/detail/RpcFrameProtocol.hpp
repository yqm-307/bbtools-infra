#pragma once

// 仅供既有 wire 用例；生产直接走 SessionCodec.hpp。
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/rpc/SessionCodec.hpp>

namespace bbt::infra::rpc {
namespace detail {

class RpcFrameProtocol {
public:
    /**
     * @brief 帧值校验（kind 相关的字段存在性矩阵）。
     */
    static result<void> Validate(const SessionFrame& frame) { return ValidateSessionFrame(frame); }

    /**
     * @brief 完整编码一帧：u32be 前缀 + protobuf body；超 kind 上限即拒绝。
     */
    static result<std::vector<std::uint8_t>>
    Encode(const SessionFrame& frame, std::size_t max_frame_bytes = kRpcV2DefaultMaxFrameBytes)
    {
        return EncodeFramedSessionFrame(frame, max_frame_bytes);
    }

    /**
     * @brief 解码一帧 protobuf body（不含前缀）。
     */
    static result<SessionFrame> Decode(const std::uint8_t* data, std::size_t size)
    {
        return DecodeSessionFrame(data, size);
    }

    /**
     * @brief 向 session 持有的 decoder 追加到达字节（任意分片/合并）。
     */
    static result<void> Feed(SessionFrameDecoder& decoder, const std::uint8_t* data, std::size_t size)
    {
        return decoder.Feed(data, size);
    }

    /**
     * @brief 取一帧：无完整帧返回 ok nullopt；协议违规进入永久错误态。
     */
    static result<std::optional<SessionFrame>> Next(SessionFrameDecoder& decoder) { return decoder.Next(); }
};

// clang-format off
}
// clang-format on
// clang-format off
}
// clang-format on

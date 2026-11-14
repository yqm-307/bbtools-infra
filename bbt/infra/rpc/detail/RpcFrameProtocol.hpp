#pragma once

/**
 * RPC 帧协议接缝（bbt/infra/rpc 实现 TU 专用，非公开头）。
 *
 * 协议层是无状态的纯帧编解码：profile_version/帧类型/长度上限/关联字段与
 * 字段存在性规则都由常量与函数承载（wire 无 magic，只有 u32be 前缀与
 * profile_version）。连接级状态——握手进度、拆包缓冲、pending、发送队列、
 * 入站顺序台账——一律归 RpcSession；本类不持有其中任何一项。
 *
 * 增量拆包所需的接收缓存由调用方以 SessionFrameDecoder 持有（session 的
 * 接收缓存，不是协议层状态）。wire 真源仍是 SessionCodec.hpp（profile 2 +
 * u32be 分帧），本类把「协议外壳」显式命名为一条接缝，字节规则不复制。
 */
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/rpc/SessionCodec.hpp>

namespace bbt::infra::rpc {
namespace detail {

/**
 * @brief profile 2 无状态帧协议：校验、编码、解码与增量拆包入口。
 *
 * 全部方法为纯函数；解码端字段级违规归 ProtocolError，编码端不可编码值归
 * InvalidArgument（与 SessionCodec 同一口径）。不含 JSON/XML/Protobuf 业务
 * 类型，payload 始终是不透明字节。
 */
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

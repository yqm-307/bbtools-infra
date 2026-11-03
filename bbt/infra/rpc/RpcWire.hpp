/**
 * @file RpcWire.hpp
 * @brief 版本化 Protobuf RpcEnvelope wire profile（RPC core，不依赖物理 transport 栈）。
 *
 * wire 单一真源仍是 proto/bbt/infra/rpc/v1/rpc_envelope.proto；字节规则、
 * 值类型校验、错误域映射与 metadata 约束自旧 include/bbt/infra/rpc/RpcWire.hpp
 * 原样保留（复用，不改 profile）。随 transport seam 切分（architecture §5）：
 *   * 本头只承载值类型 + protobuf 编解码 + RpcEnvelope seam 互转，不含任何
 *     HTTP/net 类型；HTTP body 绑定助手迁入 HTTP adapter
 *     （bbt/infra/rpc/HttpTransport.hpp）；
 *   * protobuf 生成物仍不出现在公共头（adapter 边界，AGENTS.md §4）；
 *     实现经 PIMPL 隔在本模块 .cc 内。
 *
 * protoc/runtime 同源锁定见 cmake/RpcNet.cmake 的接入注释（BBT_INFRA_RPC_NET）。
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra::rpc {

// wire profile 版本；与 proto 中 profile_version 对应，当前固定为 1。
// proto package 保留 bbt.infra.rpc.v1 表达 wire 版本；公共 C++ 命名空间
// 收敛到 bbt::infra::rpc（AGENTS.md「namespace 不超过三层」）。
inline constexpr std::uint32_t kRpcWireProfileVersion = 1;

// HTTP 绑定：固定 POST path 与 Content-Type。headers 只承担标准传输语义，
// 禁止 x-bbt-* 成为协议字段（wire Profile 约束，见 docs/decisions/0009-rpc-runtime.md）。
inline constexpr char kRpcWireHttpPath[]        = "/rpc";
inline constexpr char kRpcWireContentType[]     = "application/x-protobuf";
inline constexpr char kRpcWireContentTypeKey[]  = "Content-Type";

// metadata 前缀白名单（docs/decisions/0002-co-network-contract.md 契约）：仅这三组注册前缀可出现在 envelope.metadata。
inline constexpr char kMetaPrefixFw[]    = "fw.";
inline constexpr char kMetaPrefixRoute[] = "route.";
inline constexpr char kMetaPrefixTrace[] = "trace.";
// 每项上限与条数上限沿用该契约：最多 32 项，键 ≤64 UTF-8 字节，值 ≤256 UTF-8 字节。
inline constexpr std::size_t kMaxMetadataItems = 32;
inline constexpr std::size_t kMaxMetadataKeyBytes   = 64;
inline constexpr std::size_t kMaxMetadataValueBytes = 256;

// remaining_budget_ms：有上限的无符号毫秒，wire 上合法范围
// 1..kMaxRemainingBudgetMs。0 明确拒绝（wire 契约），越界同样拒绝；
// 发送端由本地 deadline 换算、至少 1ms；接收端按本地 handler 预算取 min，
// clamp 发生在上层（rpc::Server），本层只钉死 wire 合法范围。
// 上限沿用 NetworkLimits::incoming_timeout 的语义边界（不超过 24h）。
inline constexpr std::uint32_t kMaxRemainingBudgetMs =
    static_cast<std::uint32_t>(
        std::chrono::milliseconds{kNetworkLimitsMaxIncomingTimeout}.count());

/**
 * @brief wire profile 的值类型视图。
 *
 * 与 NetworkTypes.hpp 中既有 RpcEnvelope（C++ 结构 seam）的关系：
 *   RpcEnvelope 是 framework 消费的公共数据形态；本结构是 wire profile 的
 *   显式版本化形态，多了 profile_version / remaining_budget_ms / outcome。
 *   两者经 ToWireEnvelope / FromWireEnvelope 显式互转。
 */
struct RpcWireEnvelope {
    std::uint32_t profile_version = kRpcWireProfileVersion;  ///< wire profile 版本，当前固定 1
    std::string   request_id;    ///< 请求唯一标识，五字段关联校验之一
    std::string   service;       ///< 目标服务名
    std::string   method;        ///< 目标方法名
    std::uint32_t remaining_budget_ms = 0;  ///< 剩余预算毫秒，合法范围 1..kMaxRemainingBudgetMs
    std::string   request_schema;  ///< 请求 payload 的 schema 标识
    std::string   response_schema;  ///< 响应 payload 的 schema 标识
    std::vector<std::uint8_t> payload;  ///< 业务负载
    /// metadata 键值对，顺序保留；编码/解码均不允许重复键。
    std::vector<std::pair<std::string, std::string>> metadata;

    /// 请求方向与成功响应：true 时 error 为空。错误响应：false 且 error 填充完整字段。
    bool  success = true;
    Error error;
};

// --- 值类型校验（不触碰 wire） ----------------------------------------------

/**
 * @brief 校验 envelope 的可编码前提。
 *
 * 检查 profile_version 支持、request_id/service/method 非空 UTF-8、
 * metadata 前缀/条数/键值长度/重复键、remaining_budget 上限、错误分支
 * 完整性；request 方向禁止携带 outcome（success=false 拒绝）。违规返回
 * 对应 Error，不产生部分编码。
 *
 * @param env         待校验的 envelope。
 * @param is_response true 表示按响应方向校验（允许 success=false）。
 * @return 成功为 ok；违规返回 InvalidArgument 或 ProtocolError。
 */
result<void> ValidateRpcWireEnvelope(const RpcWireEnvelope& env,
                                     bool is_response);

// --- 编解码（无 protobuf 类型出现在签名） ------------------------------------

/**
 * @brief 编码为 protobuf wire 字节。
 *
 * @param env         待编码的 envelope（须先通过 ValidateRpcWireEnvelope）。
 * @param is_response true 表示按响应方向编码（允许 success=false）。
 * @return 成功返回 wire 字节；失败返回 InvalidArgument 或 ProtocolError。
 */
result<std::vector<std::uint8_t>> EncodeRpcWireEnvelope(
    const RpcWireEnvelope& env, bool is_response);

/**
 * @brief 从 protobuf wire 字节解码。
 *
 * 截断/非法字节返回 ProtocolError；未知字段按 proto3 忽略；字段级约束
 * 违规返回 InvalidArgument。
 *
 * @param bytes       wire 字节。
 * @param is_response true 表示按响应方向解码。
 * @return 成功返回 RpcWireEnvelope；失败返回 ProtocolError 或 InvalidArgument。
 */
result<RpcWireEnvelope> DecodeRpcWireEnvelope(
    const std::vector<std::uint8_t>& bytes, bool is_response);

/**
 * @brief 同 DecodeRpcWireEnvelope(vector)，接受 string 视图以减少一次拷贝。
 *
 * HTTP body 已是 std::string，直接传入避免额外的 vector 构造。
 */
result<RpcWireEnvelope> DecodeRpcWireEnvelope(
    const std::string& bytes, bool is_response);

// --- 与既有 RpcEnvelope seam 的互转 -------------------------------------------

/**
 * @brief 公共数据形态（framework 消费面）→ wire profile。
 *
 * 不填 outcome；success 由调用方按响应方向另行设置。
 *
 * @param env                 源 RpcEnvelope。
 * @param remaining_budget_ms  剩余预算毫秒，写入 wire 字段。
 * @return 成功返回 RpcWireEnvelope；失败返回 InvalidArgument。
 */
result<RpcWireEnvelope> ToWireEnvelope(const RpcEnvelope& env,
                                       std::uint32_t remaining_budget_ms);

/**
 * @brief wire profile → 公共数据形态。
 *
 * 丢弃 outcome 分支与 profile_version；上层经其他路径获知成功/失败。
 *
 * @param env  源 RpcWireEnvelope。
 * @return 成功返回 RpcEnvelope；失败返回 InvalidArgument 或 ProtocolError。
 */
result<RpcEnvelope> FromWireEnvelope(const RpcWireEnvelope& env);

} // namespace bbt::infra::rpc

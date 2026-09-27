#pragma once
// bbtools-infra Issue #8：版本化 Protobuf RpcEnvelope wire profile（HTTP body）。
//
// 本头是 wire profile 的稳定公开契约：值类型 + 编解码 + HTTP body 绑定。
// 第三方 protobuf 生成物不出现在公共头（adapter 边界，AGENTS.md §4）；
// 实现经 PIMPL 隔在 src/rpc/ 内。
//
// wire 单一真源：proto/bbt/infra/rpc/v1/rpc_envelope.proto（proto package
// 保留 bbt.infra.rpc.v1；公共 C++ API 收敛到 bbt::infra::rpc，
// AGENTS.md「namespace 不超过三层」）。
// protoc/runtime 同源锁定见 src/CMakeLists.txt 中 bbt_infra_rpc 的接入注释。

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
// 禁止 x-bbt-* 成为协议字段（Issue #8 Profile 约束）。
inline constexpr char kRpcWireHttpPath[]        = "/rpc";
inline constexpr char kRpcWireContentType[]     = "application/x-protobuf";
inline constexpr char kRpcWireContentTypeKey[]  = "Content-Type";

// metadata 前缀白名单（#2 契约）：仅这三组注册前缀可出现在 envelope.metadata。
inline constexpr char kMetaPrefixFw[]    = "fw.";
inline constexpr char kMetaPrefixRoute[] = "route.";
inline constexpr char kMetaPrefixTrace[] = "trace.";
// 每项上限与条数上限沿用 #2：最多 32 项，键 ≤64 UTF-8 字节，值 ≤256 UTF-8 字节。
inline constexpr std::size_t kMaxMetadataItems = 32;
inline constexpr std::size_t kMaxMetadataKeyBytes   = 64;
inline constexpr std::size_t kMaxMetadataValueBytes = 256;

// remaining_budget_ms：有上限的无符号毫秒，wire 上合法范围
// 1..kMaxRemainingBudgetMs。0 明确拒绝（Issue #8），越界同样拒绝；
// 发送端由本地 deadline 换算、至少 1ms；接收端按本地 listener 预算取 min，
// clamp 发生在上层，本层只钉死 wire 合法范围。
// 上限沿用 NetworkLimits::incoming_timeout 的语义边界（不超过 24h）。
inline constexpr std::uint32_t kMaxRemainingBudgetMs =
    static_cast<std::uint32_t>(
        std::chrono::milliseconds{kNetworkLimitsMaxIncomingTimeout}.count());

// RpcWireEnvelope：wire profile 的值类型视图。
// 与 NetworkTypes.hpp 中既有 RpcEnvelope（C++ 结构 seam）的关系：
//   RpcEnvelope 是 framework 消费的公共数据形态；本结构是 wire profile 的
//   显式版本化形态，多了 profile_version / remaining_budget_ms / outcome。
//   两者经 ToLegacyEnvelope / FromLegacyEnvelope 显式互转。
struct RpcWireEnvelope {
    std::uint32_t profile_version = kRpcWireProfileVersion;
    std::string   request_id;
    std::string   service;
    std::string   method;
    std::uint32_t remaining_budget_ms = 0;
    std::string   request_schema;
    std::string   response_schema;
    std::vector<std::uint8_t> payload;
    // 顺序保留；编码/解码均不允许重复键。
    std::vector<std::pair<std::string, std::string>> metadata;

    // 请求方向与成功响应：success=true, error 为空。
    // 错误响应：success=false, error 填充完整字段。
    bool  success = true;
    Error error;
};

// --- 值类型校验（不触碰 wire） ----------------------------------------------

// 校验 envelope 的可编码前提：profile_version 支持、request_id/service/method
// 非空 UTF-8、metadata 前缀/条数/键值长度/重复键、remaining_budget 上限、
// 错误分支完整性；request 方向禁止携带 outcome（success=false 拒绝）。
// 违规返回对应 Error，不产生部分编码。
result<void> ValidateRpcWireEnvelope(const RpcWireEnvelope& env,
                                     bool is_response);

// --- 编解码（无 protobuf 类型出现在签名） ------------------------------------

// 编码为 protobuf wire 字节。失败返回 Error（InvalidArgument/ProtocolError）。
result<std::vector<std::uint8_t>> EncodeRpcWireEnvelope(
    const RpcWireEnvelope& env, bool is_response);

// 从 protobuf wire 字节解码。截断/非法字节返回 ProtocolError；
// 未知字段按 proto3 忽略；字段级约束违规返回 InvalidArgument。
result<RpcWireEnvelope> DecodeRpcWireEnvelope(
    const std::vector<std::uint8_t>& bytes, bool is_response);
// 同上，接受 string 视图以减少一次拷贝（HTTP body 已是 std::string）。
result<RpcWireEnvelope> DecodeRpcWireEnvelope(
    const std::string& bytes, bool is_response);

// --- HTTP body 绑定 -----------------------------------------------------------

// 把 envelope 编码并装进 HttpRequest/HttpResponse body。
// 请求方向：method=POST、url.path=kRpcWireHttpPath、
//           Content-Type: application/x-protobuf、body=wire bytes。
// 响应方向：status=200、Content-Type: application/x-protobuf、body=wire bytes。
// 仅填标准传输字段；不设置任何 x-bbt-* 头。
result<HttpRequest>  MakeRpcWireHttpRequest(
    const RpcWireEnvelope& env, const std::string& base_url);
result<HttpResponse> MakeRpcWireHttpResponse(
    const RpcWireEnvelope& env);

// 从 HTTP body 还原 envelope。
// 非 POST / 非 /rpc path / 非 protobuf Content-Type / 空 body 返回
// InvalidArgument 或 ProtocolError；不读取任何 x-bbt-* 字段。
result<RpcWireEnvelope> ParseRpcWireHttpRequest(const HttpRequest& req);
result<RpcWireEnvelope> ParseRpcWireHttpResponse(const HttpResponse& resp);

// --- 与既有 RpcEnvelope seam 的互转 -------------------------------------------

// 公共数据形态（framework 消费面）→ wire profile。
// 不填 outcome；success 由调用方按响应方向另行设置。
result<RpcWireEnvelope> ToWireEnvelope(const RpcEnvelope& env,
                                       std::uint32_t remaining_budget_ms);
// wire profile → 公共数据形态。丢弃 outcome 分支与 profile_version；
// 上层经其他路径获知成功/失败。
result<RpcEnvelope> FromWireEnvelope(const RpcWireEnvelope& env);

} // namespace bbt::infra::rpc

#pragma once
// bbtools-infra v2：SessionFrame wire codec 与 u32be 分帧（RPC core，
// 不依赖任何物理 transport 栈）。
//
// wire 单一真源：proto/bbt/infra/rpc/v2/session_frame.proto（profile 2，
// 字段号冻结见 docs/decisions/0014-app-session-v2.md §5.1）；protobuf 生成物
// 只出现在 Codec.cc，公共头无 pb 类型泄漏（adapter 边界，AGENTS.md §4）。
//
// SessionFrame 值类型按 ADR §5.1 字段表逐字段承载，含：
//   - acceptance_requested 的 presence 语义（std::optional<bool>：缺失≠显式
//     false；仅 Send 可出现，其他 kind 必须缺失）；
//   - outcome 存在位与 success/error 二选一（Error 内含 request_phase
//     存在位——nullopt 不补成未提交，UNSPECIFIED/未知值解码即拒）；
//   - frame kind 相关的字段存在性规则（peer_identity/connection_nonce 仅握手、
//     message_id 仅 Send/SendAccepted、outcome 仅 Response/SendAccepted/
//     SessionError、reply_mode/budget/ordering 按 kind 校验）。
//
// u32be 分帧（ADR §5.2）：u32be payload_length | payload[payload_length]。
//   - payload_length == 0 立即拒绝（当前 link 退役归 session 层）；
//   - Hello/HelloAck payload ≤ min(max_frame_bytes, 64KiB)；
//   - 数据帧默认上限 kRpcV2DefaultMaxFrameBytes（8MiB），硬上限
//     kNetworkLimitsMaxBodyBytes（1GiB）；前缀检查后才允许分配 body。
//
// 解码端（Decode/Next）把字段级违规归为 ProtocolError；编码端（Validate/
// Encode）把不可编码值归为 InvalidArgument、把「会产出接收端必拒 wire」的
// 本地非法枚举归为 ProtocolError——与 v1 RpcWire 同一错误映射约定。

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>

namespace bbt::infra::rpc {

// v2 wire profile 版本：所有 frame 的 profile_version 固定为 2。
inline constexpr std::uint32_t kRpcV2ProfileVersion = 2;

// 分帧上限（ADR §5.2）。
inline constexpr std::size_t kRpcV2DefaultMaxFrameBytes = 8 * 1024 * 1024;
inline constexpr std::size_t kRpcV2MaxFrameBytes =
    kNetworkLimitsMaxBodyBytes;  // 1 GiB 硬上限
inline constexpr std::size_t kRpcV2HelloMaxFrameBytes = 64 * 1024;

// 字段级上限（ADR §5.1 字段约束表）。
inline constexpr std::size_t kRpcV2MaxPeerIdentityBytes = 256;
inline constexpr std::size_t kRpcV2MaxMessageIdBytes    = 128;
inline constexpr std::size_t kRpcV2MaxOrderingKeyBytes  = 256;
// service/method/request_id/schema 只做值与长度检查（不解析业务地址）。
inline constexpr std::size_t kRpcV2MaxFieldBytes        = 256;
// remaining_budget_ms 合法范围 1..kRpcV2MaxRemainingBudgetMs（沿用 ≤24h）。
inline constexpr std::uint32_t kRpcV2MaxRemainingBudgetMs =
    static_cast<std::uint32_t>(
        std::chrono::milliseconds{kNetworkLimitsMaxIncomingTimeout}.count());

// wire FrameKind 对应枚举（序号固定；Unspecified 在解码端视为 ProtocolError）。
enum class FrameKind : std::uint8_t {
    Unspecified  = 0,
    Hello        = 1,
    HelloAck     = 2,
    Call         = 3,
    Response     = 4,
    Send         = 5,
    SendAccepted = 6,
    SessionError = 7,
    Close        = 8,
};

// SessionFrame：wire profile 2 的值类型视图。
// outcome 语义：has_outcome=false 表示 outcome oneof 缺席；has_outcome=true
// 时 success/error 二选一（success=false → error 承载完整分支）。
// acceptance_requested 保留 presence：nullopt=字段缺席；true/false=显式值。
struct SessionFrame {
    FrameKind    kind = FrameKind::Unspecified;
    TrafficClass traffic = TrafficClass::System;
    std::string  peer_identity;
    std::uint64_t session_epoch = 0;
    std::string  message_id;
    std::string  request_id;
    std::string  service;
    std::string  method;
    ReplyMode    reply = ReplyMode::None;
    std::uint64_t sequence = 0;
    std::string  ordering_key;   // opaque bytes
    std::uint32_t remaining_budget_ms = 0;
    std::string  request_schema;
    std::string  response_schema;
    std::vector<std::uint8_t> payload;
    Metadata     metadata;

    bool  has_outcome = false;
    bool  success = false;
    Error error;

    std::uint64_t connection_nonce = 0;
    std::optional<bool> acceptance_requested;
};

// --- 值类型校验 --------------------------------------------------------------
// 按 kind 的字段存在性矩阵校验可编码前提；违规 InvalidArgument（值问题）或
// ProtocolError（本地枚举非法会产出接收端必拒 wire）。不产生部分编码。
result<void> ValidateSessionFrame(const SessionFrame& frame);

// --- 编解码（无 protobuf 类型出现在签名） -------------------------------------
result<std::vector<std::uint8_t>> EncodeSessionFrame(const SessionFrame& frame);
result<SessionFrame> DecodeSessionFrame(const std::uint8_t* data,
                                        std::size_t size);
result<SessionFrame> DecodeSessionFrame(const std::vector<std::uint8_t>& bytes);
result<SessionFrame> DecodeSessionFrame(const std::string& bytes);

// --- Request/Response 与 SessionFrame 数据帧互转 ------------------------------
// CALL/SEND → SessionFrame：traffic/reply 真源是 Request 成员；message_id 取
// Request::MessageId()（Send 必填）；acceptance_requested 由调用方（Send 的
// RequireRemoteAcceptance）显式给出——Codec 不从 Request 猜 presence。
result<SessionFrame> ToSessionFrame(const Request& request, FrameKind kind,
                                    std::uint64_t session_epoch,
                                    std::uint32_t remaining_budget_ms,
                                    std::optional<bool> acceptance_requested = std::nullopt);
// CALL/SEND → Request：入站方向，携带 deadline/peer_identity/peer_principal
// 经 detail::RequestContextAccess 填充；剩余预算换算归 session 层（本函数
// 只搬字段，不做 clamp）。
result<Request> RequestFromSessionFrame(const SessionFrame& frame,
                                        RequestContext context = {});
// RESPONSE → SessionFrame：关联字段由调用方保证与 Request 一致（Server 边界
// 另做相等校验）；outcome 由 success/error 显式给出。
result<SessionFrame> ResponseToSessionFrame(const Response& response,
                                            std::uint64_t session_epoch);
// RESPONSE → Response：入站方向；不读 outcome（终态错误走 error 分支）。
result<Response> ResponseFromSessionFrame(const SessionFrame& frame);

// --- u32be 分帧 ---------------------------------------------------------------
// 前缀写读（无状态；length==0 在解码端拒绝）。
void EncodeFramePrefix(std::uint32_t payload_length, std::uint8_t out[4]) noexcept;
std::uint32_t DecodeFramePrefix(const std::uint8_t in[4]) noexcept;

// 完整编码一帧：u32be 前缀 + protobuf body。body 长度必须先经 kind 上限校验
// （Hello/HelloAck ≤ min(max_frame_bytes,64KiB)；数据帧 ≤ max_frame_bytes），
// 超限返回 InvalidArgument 且不产出部分字节。
result<std::vector<std::uint8_t>> EncodeFramedSessionFrame(
    const SessionFrame& frame,
    std::size_t max_frame_bytes = kRpcV2DefaultMaxFrameBytes);

// 增量分帧解码器：按 ADR §5.2「reader 必须读满一帧后再解码，不能把一次 recv
// 当作一帧」。Feed 累积字节流；Next 在完整一帧就绪时解出，否则返回 ok nullopt。
// 协议违规（前缀 0、超 kind 上限、body 不可解码）返回 ProtocolError 并进入
// 永久错误态——调用方据此退役当前 link（会话层职责，本类不管理 socket）。
// max_frame_bytes 为数据帧上限；握手阶段（SetHelloPhase(true)，默认开）前缀
// 提前按 min(max,64KiB) 拒绝（ADR §5.2：检查长度后才允许分配 body），跃迁到
// 数据阶段后合法首数据合包不误拒。
class SessionFrameDecoder {
public:
    explicit SessionFrameDecoder(
        std::size_t max_frame_bytes = kRpcV2DefaultMaxFrameBytes);
    ~SessionFrameDecoder();
    SessionFrameDecoder(const SessionFrameDecoder&) = delete;
    SessionFrameDecoder& operator=(const SessionFrameDecoder&) = delete;

    // move 构造保留未决缓冲与错误态：握手 decoder 的余帧可整体移交 live
    // reader，不外拷字节（pending_limit 随之转移）。
    SessionFrameDecoder(SessionFrameDecoder&&) noexcept;
    SessionFrameDecoder& operator=(SessionFrameDecoder&&) noexcept;

    // 追加到达字节（任意分片/合并）。错误态下继续 Feed 立即返回同一错误。
    result<void> Feed(const std::uint8_t* data, std::size_t size);
    result<void> Feed(const std::vector<std::uint8_t>& bytes);
    result<void> Feed(const std::string& bytes);
    // 取一帧：无完整帧返回 ok nullopt。成功/空都不消耗错误态；
    // 一旦返回 error，流进入退役态，后续调用保持失败。
    result<std::optional<SessionFrame>> Next();
    bool IsTerminal() const noexcept;

    // 握手阶段边界（默认 true）：前缀按 min(max_frame_bytes,64KiB) 提前拒绝，
    // 未决缓冲限 4+2*min(max,64KiB)；转 false 后恢复数据帧边界。
    // 已知余帧移交时另见 PendingBytes/TakePending。
    void SetHelloPhase(bool hello_phase) noexcept;
    // 诊断/移交握手余帧：只读视图与整块取走（进入数据阶段前调用方保证
    // decoder 已按序吐净完整帧，余字节只能是未决分片或合包首数据）。
    std::size_t PendingBytes() const noexcept;
    std::vector<std::uint8_t> TakePending();

private:
    struct Impl;
    std::unique_ptr<Impl> m_impl;  // 不透明内部状态（缓冲/前缀/错误/阶段）
};

} // namespace bbt::infra::rpc

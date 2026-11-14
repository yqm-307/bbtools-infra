#pragma once

/**
 * @file SessionCodec.hpp
 * @brief SessionFrame wire codec 与 u32be 分帧（RPC core，不依赖任何物理
 *        transport 栈）。
 *
 * 本头只服务 RPC session（wire profile 2），不是 bbt::infra::Codec<T> 公共
 * 类型编解码扩展点；通用值层不反向依赖本头。
 *
 * wire 单一真源：proto/bbt/infra/rpc/v2/session_frame.proto（profile 2，
 * 字段号冻结见 docs/decisions/0014-app-session.md §5.1）；protobuf 生成物
 * 只出现在 SessionCodec.cc，公共头无 pb 类型泄漏（adapter 边界）。
 *
 * SessionFrame 值类型按 ADR 0014 §5.1 字段表逐字段承载，含：
 *   - acceptance_requested 的 presence 语义（std::optional<bool>：缺失≠显式
 *     false；仅 Send 可出现，其他 kind 必须缺失）；
 *   - outcome 存在位与 success/error 二选一（Error 内含 request_phase
 *     存在位——nullopt 不补成未提交，UNSPECIFIED/未知值解码即拒）；
 *   - frame kind 相关的字段存在性规则（peer_identity/connection_nonce 仅握手、
 *     message_id 仅 Send/SendAccepted、outcome 仅 Response/SendAccepted/
 *     SessionError、reply_mode/budget/ordering 按 kind 校验）。
 *
 * u32be 分帧（ADR 0014 §5.2）：u32be payload_length | payload[payload_length]。
 *   - payload_length == 0 立即拒绝（当前 link 退役归 session 层）；
 *   - Hello/HelloAck payload ≤ min(max_frame_bytes, 64KiB)；
 *   - 数据帧默认上限 kRpcV2DefaultMaxFrameBytes（8MiB），硬上限
 *     kNetworkLimitsMaxBodyBytes（1GiB）；前缀检查后才允许分配 body。
 *
 * 解码端（Decode/Next）把字段级违规归为 ProtocolError；编码端（Validate/
 * Encode）把不可编码值归为 InvalidArgument、把「会产出接收端必拒 wire」的
 * 本地非法枚举归为 ProtocolError。
 */
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

/**
 * wire profile 版本：所有 frame 的 profile_version 固定为 2。
 */
inline constexpr std::uint32_t kRpcV2ProfileVersion = 2;

/**
 * 分帧上限（ADR 0014 §5.2）。
 *
 * 数据帧默认上限
 */
inline constexpr std::size_t kRpcV2DefaultMaxFrameBytes = 8 * 1024 * 1024;

/**
 * 帧长硬上限（1 GiB）
 */
inline constexpr std::size_t kRpcV2MaxFrameBytes = kNetworkLimitsMaxBodyBytes;

/**
 * Hello/HelloAck 上限
 */
inline constexpr std::size_t kRpcV2HelloMaxFrameBytes = 64 * 1024;

/**
 * 字段级上限（ADR 0014 §5.1 字段约束表）。
 *
 * peer_identity 字节上限
 */
inline constexpr std::size_t kRpcV2MaxPeerIdentityBytes = 256;

/**
 * message_id 字节上限
 */
inline constexpr std::size_t kRpcV2MaxMessageIdBytes = 128;

/**
 * ordering_key 字节上限
 */
inline constexpr std::size_t kRpcV2MaxOrderingKeyBytes = 256;

/**
 * service/method/request_id/schema 只做值与长度检查（不解析业务地址）。
 */
inline constexpr std::size_t kRpcV2MaxFieldBytes = 256;

/**
 * remaining_budget_ms 合法范围 1..kRpcV2MaxRemainingBudgetMs（沿用 ≤24h）。
 */
inline constexpr std::uint32_t kRpcV2MaxRemainingBudgetMs =
    static_cast<std::uint32_t>(std::chrono::milliseconds{kNetworkLimitsMaxIncomingTimeout}.count());

/**
 * @brief wire FrameKind 对应枚举。
 *
 * 序号固定；Unspecified 在解码端视为 ProtocolError。
 */
enum class FrameKind : std::uint8_t {
    Unspecified = 0,
    Hello = 1,
    HelloAck = 2,
    Call = 3,
    Response = 4,
    Send = 5,
    SendAccepted = 6,
    SessionError = 7,
    Close = 8,
};

/**
 * @brief SessionFrame：wire profile 2 的值类型视图。
 *
 * outcome 语义：has_outcome=false 表示 outcome oneof 缺席；has_outcome=true
 * 时 success/error 二选一（success=false → error 承载完整分支）。
 * acceptance_requested 保留 presence：nullopt=字段缺席；true/false=显式值。
 */
struct SessionFrame {

    /**
     * 帧类型
     */
    FrameKind kind = FrameKind::Unspecified;

    /**
     * 流量类别（lane 路由依据）
     */
    TrafficClass traffic = TrafficClass::System;

    /**
     * 握手自报身份，≤kRpcV2MaxPeerIdentityBytes
     */
    std::string peer_identity;

    /**
     * 会话代号
     */
    std::uint64_t session_epoch = 0;

    /**
     * Send 关联 id，≤kRpcV2MaxMessageIdBytes
     */
    std::string message_id;

    /**
     * 请求 id，≤kRpcV2MaxFieldBytes
     */
    std::string request_id;

    /**
     * 服务名，≤kRpcV2MaxFieldBytes
     */
    std::string service;

    /**
     * 方法名，≤kRpcV2MaxFieldBytes
     */
    std::string method;

    /**
     * 回复模式（仅 Call 为 Required）
     */
    ReplyMode reply = ReplyMode::None;

    /**
     * 会话内序号
     */
    std::uint64_t sequence = 0;

    /**
     * opaque bytes，≤kRpcV2MaxOrderingKeyBytes
     */
    std::string ordering_key;

    /**
     * 剩余预算毫秒，1..kRpcV2MaxRemainingBudgetMs
     */
    std::uint32_t remaining_budget_ms = 0;

    /**
     * 请求 schema 标识
     */
    std::string request_schema;

    /**
     * 响应 schema 标识
     */
    std::string response_schema;

    /**
     * 数据帧 payload
     */
    std::vector<std::uint8_t> payload;

    /**
     * envelope metadata，顺序保留
     */
    Metadata metadata;

    /**
     * outcome oneof 是否在场
     */
    bool has_outcome = false;

    /**
     * has_outcome 时 true=成功分支，false=error 分支
     */
    bool success = false;

    /**
     * 失败分支的完整错误
     */
    Error error;

    /**
     * 握手 nonce
     */
    std::uint64_t connection_nonce = 0;

    /**
     * 远端接纳确认的 presence 语义
     */
    std::optional<bool> acceptance_requested;
};

/**
 * @brief 值类型校验。
 *
 * 按 kind 的字段存在性矩阵校验可编码前提；违规 InvalidArgument（值问题）或
 * ProtocolError（本地枚举非法会产出接收端必拒 wire）。不产生部分编码。
 * @param frame 待校验帧
 * @return 违规时为对应错误
 */
result<void> ValidateSessionFrame(const SessionFrame& frame);

/**
 * @name SessionFrame 编解码
 * @brief body 级编解码：编码产出 protobuf 字节，解码还原帧值。
 *
 * 解码端字段级违规归 ProtocolError，不产出部分解码。
 * @{
 */

/**
 * @brief 编码一帧为 protobuf body（不含 u32be 前缀）。
 * @param frame 已校验的帧
 * @return body 字节或编码错误
 */
result<std::vector<std::uint8_t>> EncodeSessionFrame(const SessionFrame& frame);

/**
 * @param data body 起始地址
 * @param size body 字节数
 * @return 解出的帧；字段级违规为 ProtocolError
 */
result<SessionFrame> DecodeSessionFrame(const std::uint8_t* data, std::size_t size);

/**
 * @param bytes 同上，输入为字节数组
 */
result<SessionFrame> DecodeSessionFrame(const std::vector<std::uint8_t>& bytes);

/**
 * @param bytes 同上，输入为字符串视图（HTTP body 场景减少一次拷贝）
 */
result<SessionFrame> DecodeSessionFrame(const std::string& bytes);

/**
 * @}
 * @brief CALL/SEND → SessionFrame。
 *
 * traffic/reply 真源是 Request 成员；message_id 取 Request::MessageId()
 * （Send 必填）；acceptance_requested 由调用方（Send 的 RequireRemoteAcceptance）
 * 显式给出——Codec 不从 Request 猜 presence。
 * @param request 出站请求
 * @param kind Call 或 Send
 * @param session_epoch 当前会话代号
 * @param remaining_budget_ms 剩余预算毫秒
 * @param acceptance_requested 远端接纳确认的显式 presence
 * @return 数据帧或编码错误
 */
result<SessionFrame> ToSessionFrame(
    const Request& request,
    FrameKind kind,
    std::uint64_t session_epoch,
    std::uint32_t remaining_budget_ms,
    std::optional<bool> acceptance_requested = std::nullopt);

/**
 * @brief CALL/SEND → Request（入站方向）。
 *
 * deadline/peer_identity/peer_principal 经 detail::RequestContextAccess 填充；
 * 剩余预算换算归 session 层（本函数只搬字段，不做 clamp）。
 * @param frame 入站数据帧
 * @param context 入站期限与对端身份
 * @return 请求或字段错误
 */
result<Request> RequestFromSessionFrame(const SessionFrame& frame, RequestContext context = {});

/**
 * @brief RESPONSE → SessionFrame。
 *
 * 关联字段由调用方保证与 Request 一致（Server 边界另做相等校验）；outcome 由
 * success/error 显式给出。
 * @param response 出站响应
 * @param session_epoch 当前会话代号
 * @return 响应帧或编码错误
 */
result<SessionFrame> ResponseToSessionFrame(const Response& response, std::uint64_t session_epoch);

/**
 * @brief RESPONSE → Response（入站方向）。
 *
 * 不读 outcome（终态错误走 error 分支）。
 * @param frame 入站响应帧
 * @return 响应或字段错误
 */
result<Response> ResponseFromSessionFrame(const SessionFrame& frame);

/**
 * @brief 写 4 字节大端长度前缀（无状态）。
 */
void EncodeFramePrefix(std::uint32_t payload_length, std::uint8_t out[4]) noexcept;

/**
 * @brief 读 4 字节大端长度前缀（无状态；length==0 在解码端拒绝）。
 */
std::uint32_t DecodeFramePrefix(const std::uint8_t in[4]) noexcept;

/**
 * @brief 完整编码一帧：u32be 前缀 + protobuf body。
 *
 * body 长度必须先经 kind 上限校验（Hello/HelloAck ≤ min(max_frame_bytes,64KiB)；
 * 数据帧 ≤ max_frame_bytes），超限返回 InvalidArgument 且不产出部分字节。
 * @param frame 待编码帧
 * @param max_frame_bytes 数据帧上限
 * @return 完整帧字节或错误
 */
result<std::vector<std::uint8_t>>
EncodeFramedSessionFrame(const SessionFrame& frame, std::size_t max_frame_bytes = kRpcV2DefaultMaxFrameBytes);

/**
 * @brief 增量分帧解码器。
 *
 * 按 ADR 0014 §5.2「reader 必须读满一帧后再解码，不能把一次 recv 当作一帧」：
 * Feed 累积字节流；Next 在完整一帧就绪时解出，否则返回 ok nullopt。协议违规
 * （前缀 0、超 kind 上限、body 不可解码）返回 ProtocolError 并进入永久错误态
 * ——调用方据此退役当前 link（会话层职责，本类不管理 socket）。
 *
 * max_frame_bytes 为数据帧上限；握手阶段（SetHelloPhase(true)，默认开）前缀
 * 提前按 min(max,64KiB) 拒绝（ADR 0014 §5.2：检查长度后才允许分配 body），
 * 跃迁到数据阶段后合法首数据合包不误拒。
 */
class SessionFrameDecoder {
public:
    /**
     * @param max_frame_bytes 数据帧上限
     */
    explicit SessionFrameDecoder(std::size_t max_frame_bytes = kRpcV2DefaultMaxFrameBytes);
    ~SessionFrameDecoder();
    SessionFrameDecoder(const SessionFrameDecoder&) = delete;
    SessionFrameDecoder& operator=(const SessionFrameDecoder&) = delete;

    /**
     * move 构造保留未决缓冲与错误态：握手 decoder 的余帧可整体移交 live
     * reader，不外拷字节（pending_limit 随之转移）。
     * move 构造：未决缓冲、前缀状态与错误态随之转移。
     */
    SessionFrameDecoder(SessionFrameDecoder&&) noexcept;

    /**
     * move 赋值：同上，原对象回到可析构的初始态。
     */
    SessionFrameDecoder& operator=(SessionFrameDecoder&&) noexcept;

    /**
     * @name Feed 系
     * @brief 追加到达字节（任意分片/合并）。
     *
     * 错误态下继续 Feed 立即返回同一错误；缓冲溢出等违规返回错误并进入
     * 退役态。三种输入形态语义相同。
     * @{
     */

    /**
     * @param data 字节起始地址
     * @param size 字节数
     */
    result<void> Feed(const std::uint8_t* data, std::size_t size);

    /**
     * @param bytes 同上，输入为字节数组
     */
    result<void> Feed(const std::vector<std::uint8_t>& bytes);

    /**
     * @param bytes 同上，输入为字符串视图
     */
    result<void> Feed(const std::string& bytes);

    /**
     * @}
     * @brief 取一帧：无完整帧返回 ok nullopt。成功/空都不消耗错误态；
     * 一旦返回 error，流进入退役态，后续调用保持失败。
     * @return 解出的帧、空或终止性错误
     */
    result<std::optional<SessionFrame>> Next();

    /**
     * @brief 是否已进入永久错误态（调用方应退役该 link）。
     */
    bool IsTerminal() const noexcept;

    /**
     * @brief 握手阶段边界（默认 true）：前缀按 min(max_frame_bytes,64KiB) 提前拒绝，
     * 未决缓冲限 4+2*min(max,64KiB)；转 false 后恢复数据帧边界。
     * 已知余帧移交时另见 PendingBytes/TakePending。
     * @param hello_phase 是否仍处握手阶段
     */
    void SetHelloPhase(bool hello_phase) noexcept;

    /**
     * @brief 诊断/移交握手余帧：未决字节数。只读视图；进入数据阶段前调用方保证
     * decoder 已按序吐净完整帧，余字节只能是未决分片或合包首数据。
     */
    std::size_t PendingBytes() const noexcept;

    /**
     * @brief 整块取走未决字节（移交 live reader 用），取走后缓冲清空。
     */
    std::vector<std::uint8_t> TakePending();

private:
    struct Impl;

    /**
     * 不透明内部状态（缓冲/前缀/错误/阶段）
     */
    std::unique_ptr<Impl> m_impl;
};

// clang-format off
}
// clang-format on

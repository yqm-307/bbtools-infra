#pragma once
// bbtools-infra v2：RPC 值层 Request 与关联值类型。
// 契约来源：docs/decisions/0014-app-session-v2.md §2.1 稳定签名。
//
// 本头只承载值类型：标准库 + bbt::infra::Result + bbt::coroutine::Deadline；
// 不引用 framework、HTTP/net transport 或 protobuf 生成物（AGENTS.md §4）。
// wire 编解码见 bbt/infra/rpc/Codec.hpp（SessionFrame profile 2）。
//
// 语义要点（ADR §2.1）：
//   - Request::Traffic() 是 traffic_class 的唯一真源；SendOptions 不再重复
//     携带 traffic class。
//   - Request::Reply() 是 wire reply_mode 的唯一真源：Call 要求 Required，
//     Send 要求 None（远端确认走独立 SendAccepted 控制帧）。
//   - System traffic 只能由 infra 内部生成；公共 setter 不阻止设置，由
//     session/adapter 边界按契约拒绝业务伪造（值层不装守卫语义）。
//   - Request::Context() 无公开 setter：入站期限/peer_identity/peer_principal
//     由 infra 在交付 admission 前经 detail 接缝填充，业务不能伪造。
//
// 注意：访问器 Metadata() 与别名 rpc::Metadata 同名；类内所有别名使用均以
// rpc:: 限定，避免成员函数声明隐藏类型名（GCC 「changes meaning」硬错误）。

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra::rpc {

// wire TrafficClass 对应枚举（ADR §5.1 enum TrafficClass，序号固定）。
enum class TrafficClass : std::uint8_t {
    System  = 0,
    Service = 1,
    Actor   = 2,
};

// wire ReplyMode 对应枚举（序号固定）。
enum class ReplyMode : std::uint8_t {
    None     = 0,
    Required = 1,
};

// envelope metadata：顺序保留；前缀白名单/条数/键值上限约束见 Codec.hpp。
using Metadata = std::vector<std::pair<std::string, std::string>>;

// 入站请求上下文：handler 读取入站期限、远端 opaque identity 与已认证主体
// 的唯一入口。peer_identity 不是认证凭据（trusted-network 自报）；
// peer_principal 来自可信 transport/adapter，trusted-network 模式恒为空。
// 无公开 setter——只能经 detail::RequestContextAccess 由 infra 填充。
struct RequestContext {
    // 入站时由 infra 按 min(wire remaining budget, 本地 handler 预算) 设置。
    bbt::coroutine::Deadline deadline{bbt::coroutine::Deadline::max()};
    std::string              peer_identity;
    std::string              peer_principal;
};

namespace detail { struct RequestContextAccess; }

class Request {
public:
    Request() = default;
    Request(std::string service, std::string method)
        : m_service(std::move(service)), m_method(std::move(method)) {}

    const std::string& Service() const noexcept        { return m_service; }
    const std::string& Method() const noexcept         { return m_method; }
    const std::string& RequestId() const noexcept      { return m_request_id; }
    const std::string& MessageId() const noexcept      { return m_message_id; }
    const std::string& RequestSchema() const noexcept  { return m_request_schema; }
    const std::string& ResponseSchema() const noexcept { return m_response_schema; }
    const std::vector<std::uint8_t>& Payload() const noexcept { return m_payload; }
    const rpc::Metadata& Metadata() const noexcept     { return m_metadata; }
    TrafficClass       Traffic() const noexcept        { return m_traffic; }
    ReplyMode          Reply() const noexcept          { return m_reply; }
    const std::string& OrderingKey() const noexcept    { return m_ordering_key; }
    std::uint64_t      Sequence() const noexcept       { return m_sequence; }
    const RequestContext& Context() const noexcept     { return m_context; }

    Request& SetService(std::string value)        { m_service = std::move(value); return *this; }
    Request& SetMethod(std::string value)         { m_method = std::move(value); return *this; }
    Request& SetRequestId(std::string value)      { m_request_id = std::move(value); return *this; }
    Request& SetMessageId(std::string value)      { m_message_id = std::move(value); return *this; }
    Request& SetRequestSchema(std::string value)  { m_request_schema = std::move(value); return *this; }
    Request& SetResponseSchema(std::string value) { m_response_schema = std::move(value); return *this; }
    Request& SetPayload(std::vector<std::uint8_t> value) { m_payload = std::move(value); return *this; }
    Request& SetMetadata(rpc::Metadata value)     { m_metadata = std::move(value); return *this; }
    Request& SetTraffic(TrafficClass value)       { m_traffic = value; return *this; }
    Request& SetReply(ReplyMode value)            { m_reply = value; return *this; }
    // ordering_key 是 opaque bytes；infra 只做长度、相等和同会话比较，不解析。
    Request& SetOrderingKey(std::string value)    { m_ordering_key = std::move(value); return *this; }
    Request& SetSequence(std::uint64_t value)     { m_sequence = value; return *this; }

private:
    // infra 填充入站 Context 的唯一接缝（session/adapter detail 使用）；
    // 公共面不提供 setter，业务不能伪造 peer_identity/peer_principal。
    friend struct detail::RequestContextAccess;
    void SetContext(RequestContext value) { m_context = std::move(value); }

    std::string              m_service;
    std::string              m_method;
    std::string              m_request_id;
    std::string              m_message_id;
    std::string              m_request_schema;
    std::string              m_response_schema;
    std::vector<std::uint8_t> m_payload;
    rpc::Metadata            m_metadata;
    TrafficClass             m_traffic = TrafficClass::Service;
    ReplyMode                m_reply   = ReplyMode::None;
    std::string              m_ordering_key;
    std::uint64_t            m_sequence = 0;
    RequestContext           m_context;
};

namespace detail {
// detail 接缝：infra 内部（Codec decode / session adapter）填充入站
// RequestContext；framework/业务无公共 setter 可用。命名 detail:: 表达
// 非业务入口——放公共头是因 Request 私有成员必须在此授权。
struct RequestContextAccess {
    static void Set(Request& request, RequestContext context) {
        request.SetContext(std::move(context));
    }
};
} // namespace detail

} // namespace bbt::infra::rpc

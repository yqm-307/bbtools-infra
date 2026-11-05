#pragma once

/**
 * @file Request.hpp
 * @brief RPC 值层 Request 与关联值类型。
 *
 * 契约来源：docs/decisions/0014-app-session.md §2.1 稳定签名。
 *
 * 本头只承载值类型：标准库 + bbt::infra::Result + bbt::coroutine::Deadline；
 * 不引用 framework、HTTP/net transport 或 protobuf 生成物。
 * wire 编解码见 bbt/infra/rpc/Codec.hpp（SessionFrame profile 2）。
 *
 * 语义要点（ADR 0014 §2.1）：
 *   - Request::Traffic() 是 traffic_class 的唯一真源；SendOptions 不再重复
 *     携带 traffic class。
 *   - Request::Reply() 是 wire reply_mode 的唯一真源：Call 要求 Required，
 *     Send 要求 None（远端确认走独立 SendAccepted 控制帧）。
 *   - System traffic 只能由 infra 内部生成；公共 setter 不阻止设置，由
 *     session/adapter 边界按契约拒绝业务伪造（值层不装守卫语义）。
 *   - Request::Context() 无公开 setter：入站期限/peer_identity/peer_principal
 *     由 infra 在交付 admission 前经 detail 接缝填充，业务不能伪造。
 *
 * 注意：访问器 Metadata() 与别名 rpc::Metadata 同名；类内所有别名使用均以
 * rpc:: 限定，避免成员函数声明隐藏类型名（GCC 「changes meaning」硬错误）。
 */

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra::rpc {

/**
 * @brief wire TrafficClass 对应枚举（ADR 0014 §5.1 enum TrafficClass）。
 *
 * 序号固定；System 只由 infra 内部生成。
 */
enum class TrafficClass : std::uint8_t {
    System = 0,
    Service = 1,
    Actor = 2,
};

/// wire ReplyMode 对应枚举（序号固定）。
enum class ReplyMode : std::uint8_t {
    None = 0,
    Required = 1,
};

/// envelope metadata：顺序保留；前缀白名单/条数/键值上限约束见 Codec.hpp。
using Metadata = std::vector<std::pair<std::string, std::string>>;

/**
 * @brief 入站请求上下文。
 *
 * handler 读取入站期限、远端 opaque identity 与已认证主体的唯一入口。
 * peer_identity 不是认证凭据（trusted-network 自报）；peer_principal 来自可信
 * transport/adapter，trusted-network 模式恒为空。无公开 setter——只能经
 * detail::RequestContextAccess 由 infra 填充。
 */
struct RequestContext {
    /// 入站时由 infra 按 min(wire remaining budget, 本地 handler 预算) 设置。
    bbt::coroutine::Deadline deadline{bbt::coroutine::Deadline::max()};
    std::string peer_identity;  ///< 远端自报身份（非认证凭据）
    std::string peer_principal; ///< 可信 transport/adapter 认证的主体
};

namespace detail {
struct RequestContextAccess;
}

/**
 * @brief RPC 请求值类型：服务/方法标识、payload、元数据与路由语义。
 *
 * 值层只存值不做校验；字段存在性与上限在 Codec/Session 边界校验。
 */
class Request {
public:
    Request() = default;
    /// 以服务名与方法名构造一个空请求。
    /// @param service 服务名
    /// @param method 方法名
    Request(std::string service, std::string method) : m_service(std::move(service)), m_method(std::move(method))
    {
    }

    /// 服务名。
    const std::string& Service() const noexcept
    {
        return m_service;
    }
    /// 方法名。
    const std::string& Method() const noexcept
    {
        return m_method;
    }
    /// 请求 id。
    const std::string& RequestId() const noexcept
    {
        return m_request_id;
    }
    /// 消息 id（Send 必填，用于 SendAccepted 关联）。
    const std::string& MessageId() const noexcept
    {
        return m_message_id;
    }
    /// 请求 schema 标识。
    const std::string& RequestSchema() const noexcept
    {
        return m_request_schema;
    }
    /// 响应 schema 标识。
    const std::string& ResponseSchema() const noexcept
    {
        return m_response_schema;
    }
    /// 位置参数 payload（原始字节）。
    const std::vector<std::uint8_t>& Payload() const noexcept
    {
        return m_payload;
    }
    /// envelope metadata（顺序保留）。
    const rpc::Metadata& Metadata() const noexcept
    {
        return m_metadata;
    }
    /// 流量类别；wire traffic_class 的唯一真源。
    TrafficClass Traffic() const noexcept
    {
        return m_traffic;
    }
    /// 回复模式；wire reply_mode 的唯一真源。
    ReplyMode Reply() const noexcept
    {
        return m_reply;
    }
    /// 顺序键（opaque bytes，infra 只比较不解析）。
    const std::string& OrderingKey() const noexcept
    {
        return m_ordering_key;
    }
    /// 会话内序号。
    std::uint64_t Sequence() const noexcept
    {
        return m_sequence;
    }
    /// 入站上下文（期限/对端身份/主体）。
    const RequestContext& Context() const noexcept
    {
        return m_context;
    }

    /// 设置服务名。
    Request& SetService(std::string value)
    {
        m_service = std::move(value);
        return *this;
    }
    /// 设置方法名。
    Request& SetMethod(std::string value)
    {
        m_method = std::move(value);
        return *this;
    }
    /// 设置请求 id。
    Request& SetRequestId(std::string value)
    {
        m_request_id = std::move(value);
        return *this;
    }
    /// 设置消息 id（Send 必填）。
    Request& SetMessageId(std::string value)
    {
        m_message_id = std::move(value);
        return *this;
    }
    /// 设置请求 schema 标识。
    Request& SetRequestSchema(std::string value)
    {
        m_request_schema = std::move(value);
        return *this;
    }
    /// 设置响应 schema 标识。
    Request& SetResponseSchema(std::string value)
    {
        m_response_schema = std::move(value);
        return *this;
    }
    /// 设置 payload 字节。
    Request& SetPayload(std::vector<std::uint8_t> value)
    {
        m_payload = std::move(value);
        return *this;
    }
    /// 设置 envelope metadata。
    Request& SetMetadata(rpc::Metadata value)
    {
        m_metadata = std::move(value);
        return *this;
    }
    /// 设置流量类别；System 由 session/adapter 边界按契约拒绝业务伪造。
    Request& SetTraffic(TrafficClass value)
    {
        m_traffic = value;
        return *this;
    }
    /// 设置回复模式（Call 要求 Required，Send 要求 None）。
    Request& SetReply(ReplyMode value)
    {
        m_reply = value;
        return *this;
    }
    // ordering_key 是 opaque bytes；infra 只做长度、相等和同会话比较，不解析。
    /// 设置顺序键。
    Request& SetOrderingKey(std::string value)
    {
        m_ordering_key = std::move(value);
        return *this;
    }
    /// 设置会话内序号。
    Request& SetSequence(std::uint64_t value)
    {
        m_sequence = value;
        return *this;
    }

private:
    // infra 填充入站 Context 的唯一接缝（session/adapter detail 使用）；
    // 公共面不提供 setter，业务不能伪造 peer_identity/peer_principal。
    friend struct detail::RequestContextAccess;
    void SetContext(RequestContext value)
    {
        m_context = std::move(value);
    }

    std::string m_service;
    std::string m_method;
    std::string m_request_id;
    std::string m_message_id;
    std::string m_request_schema;
    std::string m_response_schema;
    std::vector<std::uint8_t> m_payload;
    rpc::Metadata m_metadata;
    TrafficClass m_traffic = TrafficClass::Service;
    ReplyMode m_reply = ReplyMode::None;
    std::string m_ordering_key;
    std::uint64_t m_sequence = 0;
    RequestContext m_context;
};

namespace detail {
/**
 * @brief detail 接缝：infra 内部（Codec decode / session adapter）填充入站
 *        RequestContext；framework/业务无公共 setter 可用。
 *
 * 命名 detail:: 表达非业务入口——放公共头是因 Request 私有成员必须在此授权。
 */
struct RequestContextAccess {
    /// 以给定上下文覆写入站请求的 Context。
    /// @param request 目标请求
    /// @param context 入站期限与对端身份
    static void Set(Request& request, RequestContext context)
    {
        request.SetContext(std::move(context));
    }
};
} // namespace detail

} // namespace bbt::infra::rpc

#pragma once

/**
 * @file Response.hpp
 * @brief RPC 值层 Response。
 *
 * 契约来源：docs/decisions/0014-app-session.md §2.1/§2.4。
 *
 * 业务用 FromRequest 复制关联字段，再用 setter 填充回复；Response 不持有 socket/session/业务
 * 上下文。关联字段（request_id/service/method/request_schema/response_schema）
 * 必须与 Request 一致——由 Server 在 ReplyCompletion 边界校验，不一致返回
 * ProtocolError（值层不做跨对象相等校验，只承载字段）。
 *
 * 注意：访问器 Metadata() 与别名 rpc::Metadata 同名；类内别名一律 rpc:: 限定。
 */

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <bbt/infra/rpc/Request.hpp>

namespace bbt::infra::rpc {

/**
 * @brief RPC 响应值类型：payload 与回指请求的关联字段。
 *
 * 关联字段与 Request 相等性由 Server 在回复完成边界校验，值层只承载。
 */
class Response {
public:
    Response() = default;
    /// 以给定 payload 构造响应（关联字段留待 setter 填充）。
    /// @param payload 响应 payload 字节
    explicit Response(std::vector<std::uint8_t> payload) : m_payload(std::move(payload))
    {
    }

    /** @brief 复制请求的五个关联字段；回复 payload 与 metadata 初始为空，不保留请求引用。 */
    static Response FromRequest(const Request& request)
    {
        Response response;
        response.SetRequestId(request.RequestId())
            .SetService(request.Service())
            .SetMethod(request.Method())
            .SetRequestSchema(request.RequestSchema())
            .SetResponseSchema(request.ResponseSchema());
        return response;
    }

    /// 回指的请求 id。
    const std::string& RequestId() const noexcept
    {
        return m_request_id;
    }
    /// 服务名（须与请求一致）。
    const std::string& Service() const noexcept
    {
        return m_service;
    }
    /// 方法名（须与请求一致）。
    const std::string& Method() const noexcept
    {
        return m_method;
    }
    /// 请求 schema 标识（须与请求一致）。
    const std::string& RequestSchema() const noexcept
    {
        return m_request_schema;
    }
    /// 响应 schema 标识。
    const std::string& ResponseSchema() const noexcept
    {
        return m_response_schema;
    }
    /// 响应 payload（原始字节）。
    const std::vector<std::uint8_t>& Payload() const noexcept
    {
        return m_payload;
    }
    /// envelope metadata（顺序保留）。
    const rpc::Metadata& Metadata() const noexcept
    {
        return m_metadata;
    }

    /// 设置回指的请求 id。
    Response& SetRequestId(std::string value)
    {
        m_request_id = std::move(value);
        return *this;
    }
    /// 设置服务名（须与请求一致）。
    Response& SetService(std::string value)
    {
        m_service = std::move(value);
        return *this;
    }
    /// 设置方法名（须与请求一致）。
    Response& SetMethod(std::string value)
    {
        m_method = std::move(value);
        return *this;
    }
    /// 设置请求 schema 标识（须与请求一致）。
    Response& SetRequestSchema(std::string value)
    {
        m_request_schema = std::move(value);
        return *this;
    }
    /// 设置响应 schema 标识。
    Response& SetResponseSchema(std::string value)
    {
        m_response_schema = std::move(value);
        return *this;
    }
    /// 设置 payload 字节。
    Response& SetPayload(std::vector<std::uint8_t> value)
    {
        m_payload = std::move(value);
        return *this;
    }
    /// 设置 envelope metadata。
    Response& SetMetadata(rpc::Metadata value)
    {
        m_metadata = std::move(value);
        return *this;
    }

private:
    std::string m_request_id;
    std::string m_service;
    std::string m_method;
    std::string m_request_schema;
    std::string m_response_schema;
    std::vector<std::uint8_t> m_payload;
    rpc::Metadata m_metadata;
};

} // namespace bbt::infra::rpc

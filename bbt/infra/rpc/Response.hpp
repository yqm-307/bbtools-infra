#pragma once
// bbtools-infra v2：RPC 值层 Response。
// 契约来源：docs/decisions/0014-app-session-v2.md §2.1/§2.4。
//
// 业务用默认构造 + setter 生成回复；Response 构造不提供 socket/session/业务
// 上下文。关联字段（request_id/service/method/request_schema/response_schema）
// 必须与 Request 一致——由 Server 在 ReplyCompletion 边界校验，不一致返回
// ProtocolError（值层不做跨对象相等校验，只承载字段）。
//
// 注意：访问器 Metadata() 与别名 rpc::Metadata 同名；类内别名一律 rpc:: 限定。

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <bbt/infra/rpc/Request.hpp>

namespace bbt::infra::rpc {

class Response {
public:
    Response() = default;
    explicit Response(std::vector<std::uint8_t> payload)
        : m_payload(std::move(payload)) {}

    const std::string& RequestId() const noexcept      { return m_request_id; }
    const std::string& Service() const noexcept        { return m_service; }
    const std::string& Method() const noexcept         { return m_method; }
    const std::string& RequestSchema() const noexcept  { return m_request_schema; }
    const std::string& ResponseSchema() const noexcept { return m_response_schema; }
    const std::vector<std::uint8_t>& Payload() const noexcept { return m_payload; }
    const rpc::Metadata& Metadata() const noexcept     { return m_metadata; }

    Response& SetRequestId(std::string value)      { m_request_id = std::move(value); return *this; }
    Response& SetService(std::string value)        { m_service = std::move(value); return *this; }
    Response& SetMethod(std::string value)         { m_method = std::move(value); return *this; }
    Response& SetRequestSchema(std::string value)  { m_request_schema = std::move(value); return *this; }
    Response& SetResponseSchema(std::string value) { m_response_schema = std::move(value); return *this; }
    Response& SetPayload(std::vector<std::uint8_t> value) { m_payload = std::move(value); return *this; }
    Response& SetMetadata(rpc::Metadata value)     { m_metadata = std::move(value); return *this; }

private:
    std::string              m_request_id;
    std::string              m_service;
    std::string              m_method;
    std::string              m_request_schema;
    std::string              m_response_schema;
    std::vector<std::uint8_t> m_payload;
    rpc::Metadata            m_metadata;
};

} // namespace bbt::infra::rpc

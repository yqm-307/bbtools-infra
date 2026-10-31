// MCP 公共基础类型实现（McpTypes.hpp 的非内联部分）。
//
// 只有 RequestId 的两件非平凡行为需要编译单元：原值回包（ToJson）与稳定可比较
// 键（Key）。两者都不做输入回显、不依赖 Boost 类型。

#include <bbt/infra/mcp/McpTypes.hpp>

#include <cstddef>
#include <stdexcept>
#include <string>
#include <variant>

namespace bbt::infra::mcp {

namespace {

// 规范 base64（RFC 4648 §4 标准字母表）：长度 4 的倍数、仅尾部 '=' 填充（≤2 个）、
// 不接受 URL-safe 字母表、空白或其它字符。
bool IsCanonicalBase64(const std::string& text) {
    if (text.size() % 4 != 0) return false;
    std::size_t first_pad = std::string::npos;
    for (std::size_t i = 0; i < text.size(); ++i) {
        const unsigned char c = static_cast<unsigned char>(text[i]);
        if (c == '=') {
            if (first_pad == std::string::npos) first_pad = i;
            continue;
        }
        if (first_pad != std::string::npos) return false;  // 填充后不得再出现数据字符
        const bool alphabet = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                              (c >= '0' && c <= '9') || c == '+' || c == '/';
        if (!alphabet) return false;
    }
    if (first_pad == std::string::npos) return true;
    return text.size() - first_pad <= 2;
}

Error MakeTypeError(const char* domain_code, const char* message) {
    Error e;
    e.code = ErrorCode::InvalidArgument;
    e.domain_code = domain_code;
    e.message = message;
    return e;
}

} // namespace

JsonValue RequestId::ToJson() const {
    if (IsString()) return JsonValue(AsString());
    if (std::holds_alternative<std::int64_t>(m_value)) return JsonValue(AsInt64());
    return JsonValue(AsUInt64());
}

// 前缀区分类型：字符串 "1" 与整数 1 不得折叠成同一身份（在途去重/订阅 id 用）。
std::string RequestId::Key() const {
    if (IsString()) return "s:" + std::string(AsString());
    if (std::holds_alternative<std::int64_t>(m_value)) return "i:" + std::to_string(AsInt64());
    return "u:" + std::to_string(AsUInt64());
}

result<ResourceContents> ResourceContents::Text(std::string uri, std::string mime_type,
                                                std::string text) {
    if (uri.empty())
        return result<ResourceContents>::err(
            MakeTypeError("mcp.resource.uri", "resource uri is required"));
    if (!detail::IsValidUtf8(uri) || !detail::IsValidUtf8(mime_type))
        return result<ResourceContents>::err(
            MakeTypeError("mcp.resource.uri", "resource uri/mimeType must be valid UTF-8"));
    if (!detail::IsValidUtf8(text))
        return result<ResourceContents>::err(
            MakeTypeError("mcp.resource.text", "resource text must be valid UTF-8"));
    return result<ResourceContents>::ok(
        ResourceContents(std::move(uri), std::move(mime_type), std::move(text), true));
}

result<ResourceContents> ResourceContents::Blob(std::string uri, std::string mime_type,
                                                std::string base64) {
    if (uri.empty())
        return result<ResourceContents>::err(
            MakeTypeError("mcp.resource.uri", "resource uri is required"));
    if (!detail::IsValidUtf8(uri) || !detail::IsValidUtf8(mime_type))
        return result<ResourceContents>::err(
            MakeTypeError("mcp.resource.uri", "resource uri/mimeType must be valid UTF-8"));
    if (!IsCanonicalBase64(base64))
        return result<ResourceContents>::err(
            MakeTypeError("mcp.resource.blob", "resource blob must be canonical base64"));
    return result<ResourceContents>::ok(
        ResourceContents(std::move(uri), std::move(mime_type), std::move(base64), false));
}

Content Content::Text(std::string text) {
    if (!detail::IsValidUtf8(text))
        throw std::invalid_argument("content text must be valid UTF-8");
    Content content;
    content.m_kind = ContentKind::Text;
    content.m_text = std::move(text);
    return content;
}

} // namespace bbt::infra::mcp

namespace bbt::infra::mcp {

result<InputRequired> InputRequired::Create(
    std::map<std::string, ClientInputRequest> input_requests,
    std::optional<std::string> request_state) {
    if (input_requests.empty() && !request_state) {
        Error error;
        error.code = ErrorCode::InvalidArgument;
        error.domain_code = "mcp.mrtr.empty";
        error.message = "input_required requires inputRequests or requestState";
        return result<InputRequired>::err(std::move(error));
    }
    for (const auto& [key, request] : input_requests) {
        if (key.empty()) {
            Error error;
            error.code = ErrorCode::InvalidArgument;
            error.domain_code = "mcp.mrtr.key";
            error.message = "input request id must be non-empty";
            return result<InputRequired>::err(std::move(error));
        }
        if (request.method.empty()) {
            Error error;
            error.code = ErrorCode::InvalidArgument;
            error.domain_code = "mcp.mrtr.method";
            error.message = "input request method must be non-empty";
            return result<InputRequired>::err(std::move(error));
        }
    }
    InputRequired value;
    value.m_input_requests = std::move(input_requests);
    value.m_request_state = std::move(request_state);
    return result<InputRequired>::ok(std::move(value));
}

} // namespace bbt::infra::mcp

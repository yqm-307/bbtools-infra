#pragma once
// infra http 内部工具：URL 解析、method/header 注入校验、
// beast/asio 错误码到契约 Error 的分类、WaitStatus→CloseStatus 映射。
// 仅供 src/http/ 实现使用，不安装、不进公开面。
//
// 通用执行域/关闭态机/WaitStatus 映射在 src/detail/IoSupport.hpp
// （Issue #6 起与 Redis 模块共用一份实现）；下列 using 保持
// http_detail:: 旧名，http 实现无需改名。

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include <boost/system/error_code.hpp>

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/coroutine/sync/CompletionSignal.hpp>

#include "detail/IoSupport.hpp"

namespace bbt::infra::http_detail {

// 通用机制别名（实现在 bbt::infra::detail）。
using bbt::infra::detail::ManagedCloseState;
using bbt::infra::detail::WaitStatusToError;
using bbt::infra::detail::WaitStatusToCloseStatus;

// 首切片 origin-form URL 解析结果；port 缺省 80。
struct ParsedUrl {
    std::string   host;
    std::uint16_t port = 80;
    std::string   target; // path + ?query
};

// 仅接受 http://（大小写不敏感）；https:// 与其他 scheme 显式拒绝，
// 不降级明文。拒绝 userinfo、fragment、空白与控制字符。
result<ParsedUrl> ParseHttpUrl(std::string_view url);

// RFC 7230 tchar：method 与 header 名的合法字符集。
bool IsTokenChar(char c) noexcept;
bool IsValidToken(std::string_view s) noexcept;          // 非空且全 tchar
// header 值：允许 HTAB、0x20-0x7E、0x80-0xFF；拒绝 NUL/控制字符/CR/LF。
bool IsValidHeaderValue(std::string_view s) noexcept;
// target（origin-form）：可打印 ASCII，拒绝空白/控制/DEL。
bool IsValidTarget(std::string_view s) noexcept;

// adapter 自身控制 framing，下列头禁止由调用方提供（大小写不敏感）：
// host / content-length / transfer-encoding / connection / expect / upgrade
bool IsReservedRequestHeader(std::string_view name) noexcept;

// 发送前完整校验 HttpRequest（method、url、headers、长度与体积边界）。
// 违反返回 Error(InvalidArgument)，domain_code 精确标记违规点。
result<ParsedUrl> ValidateRequest(const HttpRequest& req,
                                  const NetworkLimits& limits);

// 服务端写出前校验 handler 产出：头名/值合法、体积受上限约束、
// framing 头由 adapter 接管。违规由边界转为 500（InternalError）。
result<void> ValidateResponse(const HttpResponse& res,
                              const NetworkLimits& limits);

// 该 error_code 是否属于 beast http 解析错误域（detail::http_error_category）。
bool IsHttpErrorCode(const boost::system::error_code& ec) noexcept;

// beast/asio 完成码 → 契约 ErrorCode：
//  - http::error::*（framing/header_limit/body_limit/partial 等）→ ProtocolError
//  - operation_aborted → Cancelled（后端被本地中止）
//  - 连接类（refused/reset/eof/timed_out/网络不可达）→ TransportError
//  - 解析/地址类（resolve 失败等）→ Unavailable
//  - 其余 → InternalError
Error ClassifyBackendError(const boost::system::error_code& ec,
                           std::string message);

// handler 返回 err 分支 → HTTP 状态码（服务端边界，err 不暴露内部细节）。
unsigned ErrorCodeToHttpStatus(ErrorCode code) noexcept;

// Runtime teardown 驱动点：实现方保证只在 io 域（engine strand，由共享
// executor 驱动线程执行）内执行一次性回收（acceptor/socket 关闭、会话
// 中止），完成后自行 MarkClosed。
class IIoTeardown {
public:
    virtual ~IIoTeardown() = default;
    virtual void TeardownOnIoDomain() noexcept = 0;
    // TryPost 失败：逻辑封口 + 子对象 off-domain 收口，不提前 MarkClosed。
    virtual void TeardownOffDomain() noexcept = 0;
};

} // namespace bbt::infra::http_detail

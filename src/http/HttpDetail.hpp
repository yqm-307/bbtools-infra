#pragma once
// infra http 内部工具：URL 解析、method/header 注入校验、
// beast/asio 错误码到契约 Error 的分类、WaitStatus→CloseStatus 映射。
// 仅供 src/http/ 实现使用，不安装、不进公开面。

#include <atomic>
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

namespace bbt::infra::http_detail {

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

// WaitStatus → 请求侧 Error 的固定映射（Completed 不出现）。
Error WaitStatusToError(bbt::coroutine::WaitStatus status);

// WaitStatus → CloseStatus 固定映射（契约 §关闭规则）。
CloseStatus WaitStatusToCloseStatus(bbt::coroutine::WaitStatus status) noexcept;

// handler 返回 err 分支 → HTTP 状态码（服务端边界，err 不暴露内部细节）。
unsigned ErrorCodeToHttpStatus(ErrorCode code) noexcept;

// 每个受管对象一份的关闭态机：Open→Closing→Closed，不重开。
// RequestClose 幂等可由任意线程发起；teardown 完成（io 域）后
// MarkClosed 使 WaitClosed 观察者返回 Closed。
class ManagedCloseState {
public:
    enum Phase : int { kOpen = 0, kClosing = 1, kClosed = 2 };

    explicit ManagedCloseState(
        std::shared_ptr<bbt::coroutine::CompletionSignal> sig)
        : m_sig(std::move(sig)) {}

    // Open→Closing；仅首次返回 true，调用方据此执行一次性 teardown。
    bool BeginClose() noexcept {
        int expected = kOpen;
        return m_phase.compare_exchange_strong(expected, kClosing);
    }
    // 物理清理落定：幂等（仅首次落实者发信号与回调）。
    // 「Closed = 后端不会再访问本组件拥有的操作资源」，而非仅收到关闭意图。
    void MarkClosed() noexcept {
        if (m_phase.exchange(kClosed) == kClosed)
            return;
        m_sig->Complete(); // one-shot：晚到/重复安全
        if (m_hook)
            m_hook();
    }
    // 发布前一次性注册：对象逃逸到其他线程之前由工厂设置。
    // 在 MarkClosed 落定后触发（可能在任意线程），供 runtime 聚合
    // 子对象的物理清理完成事件。
    void SetClosedHook(std::function<void()> hook) noexcept {
        m_hook = std::move(hook);
    }
    bool IsClosed() const noexcept { return m_phase.load() == kClosed; }
    bool IsOpen()    const noexcept { return m_phase.load() == kOpen; }

    // 契约顺序：先校验协程上下文与运行时代际，已关闭对象在合法上下文
    // 立即返回 Closed；否则经 CompletionSignal 挂起等待并按固定映射返回。
    CloseStatus WaitClosed(bbt::coroutine::Deadline          deadline,
                           bbt::coroutine::CancellationToken cancel,
                           bbt::coroutine::RuntimeGeneration generation);

private:
    std::shared_ptr<bbt::coroutine::CompletionSignal> m_sig;
    std::atomic<int> m_phase{kOpen};
    std::function<void()> m_hook;   // 仅发布前写一次，此后只读
};

// Runtime teardown 驱动点：实现方保证只在 io 域（engine strand，由共享
// executor 驱动线程执行）内执行一次性回收（acceptor/socket 关闭、会话
// 中止），完成后自行 MarkClosed。
class IIoTeardown {
public:
    virtual ~IIoTeardown() = default;
    virtual void TeardownOnIoDomain() noexcept = 0;
    // TryPost 失败且引擎未封：逻辑收口 + 计数门控，不触碰 asio 对象。
    virtual void TeardownOffDomain() noexcept = 0;
};

} // namespace bbt::infra::http_detail

#pragma once
/**
 * @brief stdio 内部确定性阶段屏障，不属于公共消费面；只暂停，不执行用户回调。
 */
#include <atomic>
#include <memory>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/infra/mcp/McpStdioTransport.hpp>

namespace bbt::infra::mcp {
struct StdioTestPause {
    std::atomic<bool> reached{false};
    bbt::coroutine::sync::CoWaiter::SPtr resume = bbt::coroutine::sync::CoWaiter::Create();
};

struct StdioTestAccess {
    enum class Stage { CloseRecheck, Release };
    static std::shared_ptr<StdioTestPause> Pause(McpStdioTransport& transport, Stage stage);
    static bool OnlyWritingFrame(const McpStdioTransport& transport);
};

// clang-format off
}
// clang-format on

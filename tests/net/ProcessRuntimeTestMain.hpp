// 测试私有进程退出边界（非产品头，不发布、不进入任何公共 target 的接口面）。
// 仅 tests/net 下已启动 coroutine runtime 的测试 main 入口经此收口。
//
// 已知限制（/exit-forensics-24 已复现，本轮未修复上游）：
// coroutine runtime 无 Stop/重启/优雅停机，调度线程随进程存活；main 正常
// return 进入静态析构后，runtime 线程仍可能 PollOnce 延迟销毁 Event →
// CancelListen → DelEventCallback → 对已析构 Event::m_callback_map 执行
// erase（是 erase 非 find）→ SIGSEGV（DSO 197a45）。因此启动 runtime 的
// 测试 main 不得直接 return/exit，统一走本边界：
// 1) run() 运行原测试主体并取得原退出码；失败保持非 0，未捕获异常/
// terminate/abort/信号/外层超时不会被转换成功；
// 2) run() 返回当刻，测试函数与内层作用域已全部离开，业务 RAII（socket/
// listener 物理 close、owner loop join、callback/ticket/snapshot 配对、
// FD/线程回 warm 基线）已真实完成，不把 OS 代清当通过证据；
// 3) 显式刷新 C++ 流与 C stdio；可检测的刷新失败不得映射为 0；
// 4) std::_Exit(code) 结束进程：跳过全部静态析构与 atexit，不展开当前
// 栈，因此本调用必须位于所有局部对象离开作用域之后（不在 fixture
// 局部仍存活的位置调用）。
// 该边界只规避测试进程退出竞态：不验证全局/静态 RAII，不是 runtime 资源
// 回收证据，不向生产库推荐；需要验证普通 return/静态析构/全局资源寿命的
// 用例须另设独立生命周期测试。跨进程资源（临时文件、外部服务、共享记录、
// 子进程）不能依赖 OS 终止自动恢复，必须在用例内清理或由外层监督回收。
// 本头不注册 atexit、不安装或吞掉任何信号（SIGABRT 等保持默认处置，崩溃/
// 超时仍非 0 并由 ctest/外层监督记录）、不改调度器、不 sleep 等待析构窗。
#pragma once

#include <cstdio>
#include <cstdlib>
#include <iostream>

namespace bbt_net_test {

// 刷新 C++ 与 C 标准流；返回 false 表示存在可检测到的刷新失败。
// 注意：默认 sync_with_stdio 下，stdio 写失败的错误标志不会同步进 C++ 流
// 状态，所以除 fflush 返回值外还要查 ferror(stdout/stderr)。
inline bool FlushTestOutput() {
    std::cout.flush();
    std::cerr.flush();
    std::clog.flush();
    if (std::fflush(nullptr) != 0) { return false; }
    if (std::ferror(stdout) != 0 || std::ferror(stderr) != 0) { return false; }
    return !std::cout.fail() && !std::cerr.fail() && !std::clog.fail();
}

// 测试进程唯一出口：run() 返回原退出码；此处所有测试局部对象已析构，
// 刷新输出后以 std::_Exit 结束，跳过静态析构/atexit 窗口。
template <typename Run>
int RunTestProcessExit(Run&& run) {
    int code = run();
    const bool output_flushed = FlushTestOutput();
    if (code == 0 && !output_flushed) { code = 1; }
    std::_Exit(code);
}

}  // namespace bbt_net_test

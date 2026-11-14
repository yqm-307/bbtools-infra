#pragma once

/**
 * ProcessRuntimeTestMain.hpp 的实现件：头内可见实现（inline + 模板），由
 * ProcessRuntimeTestMain.hpp 末尾 include。
 */
namespace bbt_net_test {

inline bool FlushTestOutput()
{
    std::cout.flush();
    std::cerr.flush();
    std::clog.flush();
    if (std::fflush(nullptr) != 0) {
        return false;
    }
    if (std::ferror(stdout) != 0 || std::ferror(stderr) != 0) {
        return false;
    }
    return !std::cout.fail() && !std::cerr.fail() && !std::clog.fail();
}

template <typename Run> int RunTestProcessExit(Run&& run)
{
    int code = run();
    const bool output_flushed = FlushTestOutput();
    if (code == 0 && !output_flushed) {
        code = 1;
    }
    std::_Exit(code);
}

// clang-format off
}
// clang-format on

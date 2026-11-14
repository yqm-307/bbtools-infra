#pragma once

/**
 * @file Annotations.hpp
 * @brief 静态分析语义标签。
 *
 * BBT_INFRA_NET_NONSUSPENDING 标注「该路径不挂起协程、不阻塞执行线程」，供 clang 静态分析识别。
 * 它不表示线程安全，也不等同于 noexcept，不影响运行时行为；非 clang 编译器上展开为空。
 */
#if defined(__clang__)
#define BBT_INFRA_NET_NONSUSPENDING [[clang::annotate("bbt.infra.net.nonsuspending")]]
#else
#define BBT_INFRA_NET_NONSUSPENDING
#endif

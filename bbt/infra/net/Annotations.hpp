#pragma once

// 语义标签：完整路径不挂起协程、不阻塞执行线程。不是线程安全/noexcept 标记。
#if defined(__clang__)
#define BBT_INFRA_NET_NONSUSPENDING [[clang::annotate("bbt.infra.net.nonsuspending")]]
#else
#define BBT_INFRA_NET_NONSUSPENDING
#endif

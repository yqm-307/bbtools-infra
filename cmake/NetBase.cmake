# 新 net 基础接入（幂等）：任一 *_NET 开关或 NETWORK_ONLY 都需要 bbt::infra_net。
# 公共依赖（coroutine/pin/common）已由 cmake/Common.cmake 保证；本文件只补
# 平台门禁、Boost 与 net 目录本体，普通模式与 NETWORK_ONLY 共用。
if(NOT TARGET bbt_infra_net)
    if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
        message(FATAL_ERROR "新 net socket 基础目前仅验证 Linux；不静默承诺其他平台")
    endif()
    find_package(Boost 1.90 REQUIRED)
    add_subdirectory("${PROJECT_SOURCE_DIR}/bbt/infra/net" bbt_infra_net)
endif()

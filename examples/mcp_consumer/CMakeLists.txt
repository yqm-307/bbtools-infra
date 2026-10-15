cmake_minimum_required(VERSION 3.16)
project(bbt_infra_mcp_consumer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 独立消费验证（waypoint mcp-server-foundation 4-2A，S24 consumer source-build 形态）：
# bbtools-infra 当前无 install/export，consumer 以源码方式接入整个 infra 树，但只链接
# mcp 协议核 target（bbt::infra_mcp_core，不链接 transport 面）。公共面只经
# bbt::infra_mcp_core 传播，不泄漏 framework/第三方类型。
#
# 本目录是**独立 CMake 工程**，不是本仓构建的一部分（不被任何 CMakeLists 引用），
# 用于从仓外视角复核模块 target 的可独立消费性。用法见同目录 README.md。
set(BBT_INFRA_SOURCE_DIR "" CACHE PATH "bbtools-infra 源码树（含顶层 CMakeLists.txt）")
if(NOT EXISTS "${BBT_INFRA_SOURCE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR "BBT_INFRA_SOURCE_DIR 无效：未找到顶层 CMakeLists.txt")
endif()
# bbtools-coroutine 源码树（infra 顶层 CMakeLists 自行校验其有效性）。
set(BBT_COROUTINE_SOURCE_DIR "" CACHE PATH "bbtools-coroutine 源码树（含顶层 CMakeLists.txt）")

# 不注册 infra 自身单测；只消费库 target。
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)

# 2-6 修复：tools×Schema 变体的开关与固定根必须在 add_subdirectory **之前**落定——
# Schema 依赖求值发生在 infra 树 configure 期间，事后设置不生效（前任候选的 RED）。
# 仅核心消费（默认）不强制任何 Schema/ICU/jsoncons 依赖；tools 变体显式给出固定根，
# 不回落系统安装（fail-closed）。
option(MCP_CONSUMER_TOOLS
    "构建 tools×Schema 消费者变体 mcp_consumer_tools（需要显式固定 jsoncons/ICU 根）" OFF)
if(MCP_CONSUMER_TOOLS)
    set(BBT_MCP_SCHEMA_ADAPTER ON CACHE BOOL "" FORCE)
    set(MCP_JSONCONS_ROOT "" CACHE PATH "固定 jsoncons 源树根（tools 消费者需要）")
    set(MCP_ICU_ROOT "" CACHE PATH "ICU 解压根（tools 消费者需要）")
    if(NOT MCP_JSONCONS_ROOT OR NOT MCP_ICU_ROOT)
        message(FATAL_ERROR
            "MCP_CONSUMER_TOOLS=ON 需要显式 MCP_JSONCONS_ROOT 与 MCP_ICU_ROOT（拒绝系统安装回落）")
    endif()
endif()

add_subdirectory("${BBT_INFRA_SOURCE_DIR}" _bbt_infra EXCLUDE_FROM_ALL)

add_executable(mcp_consumer main.cc)
target_link_libraries(mcp_consumer PRIVATE bbt::infra_mcp_core)
target_compile_options(mcp_consumer PRIVATE -Wall -Wextra)

if(MCP_CONSUMER_TOOLS)
    # configure 期硬断言：无真实 adapter 目标立即失败，绝不静默链接不存在的 target。
    if(NOT TARGET bbt_infra_mcp_schema)
        message(FATAL_ERROR
            "BBT_MCP_SCHEMA_ADAPTER=ON 已配置但 TARGET bbt_infra_mcp_schema 不存在（依赖求值失败）")
    endif()
    add_executable(mcp_consumer_tools tools_main.cc)
    target_link_libraries(mcp_consumer_tools PRIVATE
        bbt::infra_mcp_core bbt_infra_mcp_schema)
    target_compile_options(mcp_consumer_tools PRIVATE -Wall -Wextra)
endif()

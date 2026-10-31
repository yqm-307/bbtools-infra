# MCP 私有 JSON Schema adapter（BBT_INFRA_MCP_SCHEMA=ON 时接入；fail-closed）。
#
# 依赖只经显式前缀接入（cmake/McpSchema.cmake 已校验两个前缀是已存在目录，
# 且要求 BBT_INFRA_MCP=ON；本文件不再校验存在性，但要求配置级前缀值仍然在
# cache 中且非空）：
#   BBT_JSONCONS_PREFIX = 固定 jsoncons（v1.10.0 @9e57e421 + 已批准
#                         dynamic-scope overlay）源树根，需含
#                         <prefix>/include/jsoncons/json.hpp；
#   BBT_ICU_PREFIX      = ICU 78.2（发行包 78.2-2ubuntu1）deb 解压根，需含
#                         <prefix>/usr/include/unicode/utypes.h 与 usr/lib 下
#                         libicui18n/libicuuc/libicudata。
#
# bbt_infra_mcp_core 与公共消费者不链接本 target；jsoncons/ICU 头与句柄
# 不出 bbt/infra/mcp/schema/（公共头只引用自有 JsonValue）。
#
# proto-schema-provenance 结论（2026-10-31）：字节/来源/文件级 license 核验
# YES，但候选合入/发布审批 NO → validator 可编译但 tools capability 默认
# 禁用（core 未接通 EnableToolsSchemaValidation 时 AddTool fail-closed 拒绝；
# 本 target 只提供「可被显式接通」的编译产物，不自行宣告 tools 已启用）。
# 运行期 ICU 库不随本 target 传播 LD_LIBRARY_PATH：消费者按
# cmake/mcp-schema/README.md 的显式环境设定。

if(NOT BBT_JSONCONS_PREFIX OR NOT BBT_ICU_PREFIX)
    message(FATAL_ERROR
        "BBT_INFRA_MCP_SCHEMA=ON 需要显式 BBT_JSONCONS_PREFIX 与 BBT_ICU_PREFIX")
endif()

# 锁版核验（deps.py：archive/package SHA、preimage、patch apply、postimage、
# ICU control/资源/Unicode 头树；fail-closed，configure 期一次）。
include("${PROJECT_SOURCE_DIR}/cmake/mcp-schema/check.cmake")

# 逐路径复核（不轻信前缀「目录存在」：这里要的是特定子结构）。
if(NOT EXISTS "${BBT_JSONCONS_PREFIX}/include/jsoncons/json.hpp")
    message(FATAL_ERROR
        "BBT_JSONCONS_PREFIX 下未找到 include/jsoncons/json.hpp: ${BBT_JSONCONS_PREFIX}")
endif()
if(NOT EXISTS "${BBT_ICU_PREFIX}/usr/include/unicode/utypes.h")
    message(FATAL_ERROR
        "BBT_ICU_PREFIX 下未找到 usr/include/unicode/utypes.h: ${BBT_ICU_PREFIX}")
endif()

# ICU 库仅限前缀内查找；find_library 缓存解引用前先清空（换前缀不沿用旧解析）。
foreach(_icu_lib icui18n icuuc icudata)
    unset(BBT_MCP_ICU_${_icu_lib}_LIB CACHE)
    unset(BBT_MCP_ICU_${_icu_lib}_LIB)
    find_library(BBT_MCP_ICU_${_icu_lib}_LIB ${_icu_lib}
        PATHS "${BBT_ICU_PREFIX}/usr/lib/x86_64-linux-gnu" NO_DEFAULT_PATH)
    if(NOT BBT_MCP_ICU_${_icu_lib}_LIB)
        message(FATAL_ERROR
            "BBT_ICU_PREFIX 下未找到 lib${_icu_lib}（仅限 ${BBT_ICU_PREFIX}/usr/lib）")
    endif()
endforeach()

add_library(bbt_infra_mcp_schema STATIC
    SchemaValidator.cc)
add_library(bbt::infra_mcp_schema ALIAS bbt_infra_mcp_schema)
target_include_directories(bbt_infra_mcp_schema PRIVATE
    "${PROJECT_SOURCE_DIR}/bbt/infra"    # mcp/schema/SchemaValidator.hpp、mcp/detail/Server.hpp
    "${BBT_JSONCONS_PREFIX}/include"
    "${BBT_ICU_PREFIX}/usr/include")
target_link_libraries(bbt_infra_mcp_schema
    PUBLIC  bbt::infra_mcp_core
    PRIVATE Threads::Threads
            ${BBT_MCP_ICU_icui18n_LIB} ${BBT_MCP_ICU_icuuc_LIB} ${BBT_MCP_ICU_icudata_LIB})
target_compile_options(bbt_infra_mcp_schema PRIVATE -Wall -Wextra -Wpedantic)

# --- MCP 私有 JSON Schema adapter（2-3 切片，可选开发隔离目标）--------------
# 固定依赖只经显式根接入，禁止宿主默认路径误链：
#   MCP_JSONCONS_ROOT = 固定 jsoncons（v1.10.0 @9e57e421 + 已批准 dynamic-scope
#                       overlay）源树根，需含 <root>/include/jsoncons/json.hpp；
#   MCP_ICU_ROOT      = ICU 78.2（发行包 78.2-2ubuntu1）deb 解压根，需含
#                       <root>/usr/include/unicode/utypes.h 与 usr/lib 下
#                       libicui18n/libicuuc/libicudata。
# 缺失即 configure 失败（不导出能假成功的 target、不回退系统路径）。
# bbt_infra_mcp_core 与公共消费者不链接本 target；ICU/jsoncons 头与句柄不出
# src/adapters/mcp/（公共头只有 JsonValue 依赖）。
include("${CMAKE_CURRENT_LIST_DIR}/../../../cmake/mcp-schema/check.cmake")

if(NOT MCP_JSONCONS_ROOT OR NOT MCP_ICU_ROOT)
    message(FATAL_ERROR
        "BBT_MCP_SCHEMA_ADAPTER=ON 需要 -DMCP_JSONCONS_ROOT=<固定jsoncons源树根> "
        "与 -DMCP_ICU_ROOT=<ICU解压根>（任务隔离目录）；缺失时不构建本 adapter")
endif()
if(NOT EXISTS "${MCP_JSONCONS_ROOT}/include/jsoncons/json.hpp")
    message(FATAL_ERROR
        "MCP_JSONCONS_ROOT 下未找到 include/jsoncons/json.hpp: ${MCP_JSONCONS_ROOT}")
endif()
if(NOT EXISTS "${MCP_ICU_ROOT}/usr/include/unicode/utypes.h")
    message(FATAL_ERROR
        "MCP_ICU_ROOT 下未找到 usr/include/unicode/utypes.h: ${MCP_ICU_ROOT}")
endif()

foreach(_icu_lib icui18n icuuc icudata)
    find_library(BBT_MCP_ICU_${_icu_lib}_LIB ${_icu_lib}
        PATHS "${MCP_ICU_ROOT}/usr/lib/x86_64-linux-gnu" NO_DEFAULT_PATH)
    if(NOT BBT_MCP_ICU_${_icu_lib}_LIB)
        message(FATAL_ERROR
            "MCP_ICU_ROOT 下未找到 lib${_icu_lib}（仅限 ${MCP_ICU_ROOT}/usr/lib）")
    endif()
endforeach()

add_library(bbt_infra_mcp_schema STATIC
    SchemaValidator.cc)
add_library(bbt::infra_mcp_schema ALIAS bbt_infra_mcp_schema)

target_include_directories(bbt_infra_mcp_schema PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
    "${MCP_JSONCONS_ROOT}/include"
    "${MCP_ICU_ROOT}/usr/include")
target_link_libraries(bbt_infra_mcp_schema
    PUBLIC  bbt::infra_mcp_core
    PRIVATE Threads::Threads
            ${BBT_MCP_ICU_icui18n_LIB} ${BBT_MCP_ICU_icuuc_LIB} ${BBT_MCP_ICU_icudata_LIB})
target_compile_options(bbt_infra_mcp_schema PRIVATE -Wall -Wextra)

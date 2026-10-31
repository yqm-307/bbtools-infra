# MCP server 核心 + stdio transport（BBT_INFRA_MCP=ON 时经 cmake/McpCore.cmake 接入）。
#
# 布局对齐 bbt/infra/http、bbt/infra/rpc：公开头与实现同目录，include 路径是
# <bbt/infra/mcp/...>，以 PROJECT_SOURCE_DIR 为 include root；公开头仍引用
# include/bbt/infra/ 的 Result 值层（经 bbt::infra_common 的接口传播可见）。
# detail/ 与 schema/ 是内部部件，不导出给公共消费者（测试经模块根 include 路径
# 消费，见 tests/mcp/CMakeLists.txt）。
#
# target 划分（architecture §4）：
#   bbt_infra_mcp_core  —— JSON 值层 + 协议核（registry/dispatch/session），
#                          无 transport、无第三方前缀依赖；
#   bbt_infra_mcp_stdio —— stdio 行分帧 transport（协程 I/O、有界输出、
#                          close_drain 收口证据），依赖 mcp_core + coroutine；
#   bbt_infra_mcp_schema—— 可选 JSON Schema 校验私有 adapter（fail-closed），
#                          仅 BBT_INFRA_MCP_SCHEMA=ON 时构建（见 schema/CMakeLists）。
#
# Boost.JSON 以源码方式内联进 core（JsonValue.cc 是全仓唯一包含
# boost/json/src.hpp 的 TU），不链接 libboost_json；Boost::headers 仅承担
# 头文件可见性（boost::json 声明）。core 不依赖 HTTP/RPC/Redis/Mongo。
find_package(Boost 1.90 REQUIRED)

add_library(bbt_infra_mcp_core STATIC
    JsonValue.cc
    McpTypes.cc
    Protocol.cc
    Registry.cc)
add_library(bbt::infra_mcp_core ALIAS bbt_infra_mcp_core)
target_include_directories(bbt_infra_mcp_core
    PUBLIC  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>"
            "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>"
    # 内部实现面（detail/）：实现 TU 以 "mcp/detail/..." 形式包含，仅本 target 可见。
    PRIVATE "${PROJECT_SOURCE_DIR}/bbt/infra")
target_compile_features(bbt_infra_mcp_core PUBLIC cxx_std_17)
target_link_libraries(bbt_infra_mcp_core
    PUBLIC  bbt::infra_common
    PRIVATE Boost::headers Threads::Threads)
target_compile_options(bbt_infra_mcp_core PRIVATE -Wall -Wextra -Wpedantic)

add_library(bbt_infra_mcp_stdio STATIC
    StdioTransport.cc)
add_library(bbt::infra_mcp_stdio ALIAS bbt_infra_mcp_stdio)
target_include_directories(bbt_infra_mcp_stdio
    PUBLIC  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>"
            "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>"
    PRIVATE "${PROJECT_SOURCE_DIR}/bbt/infra")
target_compile_features(bbt_infra_mcp_stdio PUBLIC cxx_std_17)
target_link_libraries(bbt_infra_mcp_stdio
    PUBLIC  bbt::infra_mcp_core
    PRIVATE Boost::headers Threads::Threads)
target_compile_options(bbt_infra_mcp_stdio PRIVATE -Wall -Wextra -Wpedantic)

# bbt_infra_mcp_http —— Streamable HTTP transport（3-2）：依赖 mcp_core +
# infra_http_net（§8 显式依赖方向：HTTP transport 是 core 的消费者，不是其
# 依赖；core 闭包不含 HTTP）。仅 BBT_INFRA_HTTP_NET=ON 时构建（fail-closed，
# 双开缺一即 configure 失败，门禁在顶层 CMakeLists.txt 的 BBT_INFRA_MCP_HTTP
# 校验块）。
if(BBT_INFRA_MCP_HTTP)
    add_library(bbt_infra_mcp_http STATIC
        HttpTransport.cc)
    add_library(bbt::infra_mcp_http ALIAS bbt_infra_mcp_http)
    target_include_directories(bbt_infra_mcp_http
        PUBLIC  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>"
                "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>"
        PRIVATE "${PROJECT_SOURCE_DIR}/bbt/infra")
    target_compile_features(bbt_infra_mcp_http PUBLIC cxx_std_17)
    target_link_libraries(bbt_infra_mcp_http
        PUBLIC  bbt::infra_mcp_core bbt::infra_http_net
        PRIVATE Boost::headers Threads::Threads)
    target_compile_options(bbt_infra_mcp_http PRIVATE -Wall -Wextra -Wpedantic)
endif()

# 依赖方向门禁（architecture §4：core 不得传递依赖 HTTP/RPC/Redis/Mongo/旧栈；
# mcp_http 是唯一允许链接 infra_http_net 的 MCP target）。
foreach(_mcp_target IN ITEMS bbt_infra_mcp_core bbt_infra_mcp_stdio)
    get_target_property(_mcp_links ${_mcp_target} INTERFACE_LINK_LIBRARIES)
    if(_mcp_links MATCHES "infra_http|infra_rpc|infra_redis|infra_mongo|infra_transport|infra_tcp|infra_udp")
        message(FATAL_ERROR
            "MCP 依赖方向破坏：${_mcp_target} 的接口闭包含禁项（${_mcp_links}）")
    endif()
endforeach()
message(STATUS "MCP 闭包证据: core=[bbt_infra_common] stdio=[bbt_infra_mcp_core]")

# 可选 JSON Schema adapter（fail-closed）：依赖与校验逻辑在 schema/CMakeLists.txt。
if(BBT_INFRA_MCP_SCHEMA)
    add_subdirectory(schema)
endif()

# 公开头自给编译自检：公开头各自单独成 TU（detail/schema 内部头不列入）。
# McpHttpTransport.hpp 经 bbt_infra_mcp_http（HTTP_NET 路径）单独 selfcheck——
# 它引用 http::Server/net::Engine 类型，不能进 mcp_stdio 的闭包。
set(_mcp_headers JsonValue.hpp McpTypes.hpp McpServer.hpp McpStdioTransport.hpp)
set(_mcp_selfcheck_srcs "")
foreach(_hdr IN LISTS _mcp_headers)
    set(_tu "${CMAKE_CURRENT_BINARY_DIR}/header_selfcheck/${_hdr}.cc")
    file(WRITE "${_tu}" "#include <bbt/infra/mcp/${_hdr}>\n")
    list(APPEND _mcp_selfcheck_srcs "${_tu}")
endforeach()
add_library(bbt_infra_mcp_header_selfcheck STATIC ${_mcp_selfcheck_srcs})
target_link_libraries(bbt_infra_mcp_header_selfcheck PRIVATE bbt::infra_mcp_stdio)
target_compile_options(bbt_infra_mcp_header_selfcheck PRIVATE -Wall -Wextra -Wpedantic)
set_target_properties(bbt_infra_mcp_header_selfcheck PROPERTIES LINKER_LANGUAGE CXX)

if(TARGET bbt_infra_mcp_http)
    set(_mcp_http_tu "${CMAKE_CURRENT_BINARY_DIR}/header_selfcheck/McpHttpTransport.hpp.cc")
    file(WRITE "${_mcp_http_tu}" "#include <bbt/infra/mcp/McpHttpTransport.hpp>\n")
    add_library(bbt_infra_mcp_http_header_selfcheck STATIC "${_mcp_http_tu}")
    target_link_libraries(bbt_infra_mcp_http_header_selfcheck PRIVATE bbt::infra_mcp_http)
    target_compile_options(bbt_infra_mcp_http_header_selfcheck PRIVATE -Wall -Wextra -Wpedantic)
    set_target_properties(bbt_infra_mcp_http_header_selfcheck PROPERTIES LINKER_LANGUAGE CXX)
endif()

if(BUILD_TESTING)
    add_subdirectory("${PROJECT_SOURCE_DIR}/tests/mcp" mcp_tests)
endif()
add_subdirectory("${PROJECT_SOURCE_DIR}/examples/mcp_stdio" mcp_stdio_examples)
# MCP HTTP 示例仅 mcp_http target 存在时挂接（BBT_INFRA_MCP_HTTP=ON）。
if(TARGET bbt_infra_mcp_http)
    add_subdirectory("${PROJECT_SOURCE_DIR}/examples/mcp_http" mcp_http_examples)
endif()

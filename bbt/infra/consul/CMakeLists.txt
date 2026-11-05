# Consul agent HTTP adapter（BBT_INFRA_CONSUL=ON 时经 cmake/Consul.cmake 接入）。
# 布局对齐 bbt/infra/http、bbt/infra/redis：公开头与实现同目录，include 路径是
# <bbt/infra/consul/...>，以 PROJECT_SOURCE_DIR 为 include root；namespace
# bbt::infra::consul。detail/ 暂不需要——本模块只有一次 HTTP 交换与一段 JSON
# 解析，没有可独立测试的内部状态机。
#
# 依赖方向：只消费 bbt::infra_http_net（出网）与 bbt::infra_common（result/
# ErrorCode/ICoCloseable）。不依赖 RPC/Redis/Mongo/旧栈，也不被它们依赖。
# Boost.JSON 以源码方式内联进 Client.cc（不链接 libboost_json）；该 TU 是仓内
# 除 mcp/JsonValue.cc 之外唯一包含 boost/json/src.hpp 的位置，两者同开会重复
# 定义，故顶层 CMake 对 BBT_INFRA_CONSUL + BBT_INFRA_MCP 同做 fail-closed 门禁。
add_library(bbt_infra_consul STATIC Client.cc)
add_library(bbt::infra_consul ALIAS bbt_infra_consul)
target_include_directories(bbt_infra_consul
    PUBLIC "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>"
           "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>")
target_compile_features(bbt_infra_consul PUBLIC cxx_std_17)
target_link_libraries(bbt_infra_consul
    PUBLIC bbt::infra_http_net bbt::infra_common
    PRIVATE Boost::headers Threads::Threads)
target_compile_options(bbt_infra_consul PRIVATE -Wall -Wextra -Wpedantic)

# 公开头自给编译自检：公开头各自单独成 TU。
set(_consul_headers Client.hpp)
set(_consul_selfcheck_srcs "")
foreach(_hdr IN LISTS _consul_headers)
    set(_tu "${CMAKE_CURRENT_BINARY_DIR}/header_selfcheck/${_hdr}.cc")
    file(WRITE "${_tu}" "#include <bbt/infra/consul/${_hdr}>\n")
    list(APPEND _consul_selfcheck_srcs "${_tu}")
endforeach()
add_library(bbt_infra_consul_header_selfcheck STATIC ${_consul_selfcheck_srcs})
target_link_libraries(bbt_infra_consul_header_selfcheck PRIVATE bbt::infra_consul)
target_compile_options(bbt_infra_consul_header_selfcheck PRIVATE -Wall -Wextra -Wpedantic)
set_target_properties(bbt_infra_consul_header_selfcheck PROPERTIES LINKER_LANGUAGE CXX)

# 依赖方向门禁：本 target 的接口闭包不得引入旧栈或 RPC/Redis/Mongo。
get_target_property(_consul_links bbt_infra_consul INTERFACE_LINK_LIBRARIES)
if(_consul_links MATCHES "infra_rpc|infra_redis|infra_mongo|infra_transport|infra_tcp|infra_udp")
    message(FATAL_ERROR
        "Consul adapter 依赖方向破坏：接口闭包含禁项（${_consul_links}）")
endif()

if(BUILD_TESTING)
    add_subdirectory("${PROJECT_SOURCE_DIR}/tests/consul" consul_tests)
endif()

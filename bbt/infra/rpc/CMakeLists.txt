# RPC core（BBT_INFRA_RPC_NET=ON 时经 cmake/RpcNet.cmake 接入）：版本化
# Protobuf RpcEnvelope wire profile + 新 bbt::infra::http 栈上的 Client/Server。
# 公开头与实现同目录（对齐 bbt/infra/http、bbt/infra/net 布局）：include 路径是
# <bbt/infra/rpc/...>，以 PROJECT_SOURCE_DIR 为 include root；同时公开
# ${PROJECT_SOURCE_DIR}/include（NetworkTypes/RpcPayload/Result 等公共值层）。
# protobuf 生成物只进 bbt_infra_rpc_proto，不出现在本模块公开头。
add_library(bbt_infra_rpc_net STATIC
    RpcWire.cc
    Client.cc
    Server.cc)
add_library(bbt::infra_rpc_net ALIAS bbt_infra_rpc_net)
target_include_directories(bbt_infra_rpc_net
    PUBLIC "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>"
           "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>")
target_compile_features(bbt_infra_rpc_net PUBLIC cxx_std_17)
target_link_libraries(bbt_infra_rpc_net
    PUBLIC bbt::infra_http_net
    PRIVATE bbt_infra_rpc_proto)
target_compile_options(bbt_infra_rpc_net PRIVATE -Wall -Wextra -Wpedantic)

# 公开头自给编译自检：公开头各自单独成 TU（detail 内部头不列入）。
set(_rpc_net_headers RpcWire.hpp Client.hpp Server.hpp)
set(_rpc_net_selfcheck_srcs "")
foreach(_hdr IN LISTS _rpc_net_headers)
    set(_tu "${CMAKE_CURRENT_BINARY_DIR}/header_selfcheck/${_hdr}.cc")
    file(WRITE "${_tu}" "#include <bbt/infra/rpc/${_hdr}>\n")
    list(APPEND _rpc_net_selfcheck_srcs "${_tu}")
endforeach()
add_library(bbt_infra_rpc_net_header_selfcheck STATIC ${_rpc_net_selfcheck_srcs})
target_link_libraries(bbt_infra_rpc_net_header_selfcheck PRIVATE bbt::infra_rpc_net)
target_compile_options(bbt_infra_rpc_net_header_selfcheck PRIVATE -Wall -Wextra -Wpedantic)
set_target_properties(bbt_infra_rpc_net_header_selfcheck PROPERTIES LINKER_LANGUAGE CXX)

if(BUILD_TESTING)
    add_subdirectory("${PROJECT_SOURCE_DIR}/tests/rpc_net" rpc_net_tests)
    add_subdirectory("${PROJECT_SOURCE_DIR}/examples/rpc_net" rpc_net_examples)
endif()

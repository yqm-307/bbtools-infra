# RPC core + HTTP adapter（BBT_INFRA_RPC_NET=ON 时经 cmake/RpcNet.cmake 接入）：
# architecture §5 的 target 切分——
#   bbt_infra_rpc_core  版本化 Protobuf RpcEnvelope wire profile +
#                       IRpcTransport/IRpcInboundTransport seam +
#                       Client/Server façade（不依赖 HTTP/net 类型）；
#   bbt_infra_rpc_http  HttpRpcTransport/HttpRpcInboundTransport + HTTP body
#                       绑定助手 + 源兼容工厂（消费 bbt::infra_http_net）；
#   bbt_infra_rpc_net   兼容 aggregate（INTERFACE），链接 core+HTTP adapter，
#                       保持既有 include consumer 可链接。
# 公开头与实现同目录（对齐 bbt/infra/http、bbt/infra/net 布局）：include 路径是
# <bbt/infra/rpc/...>，以 PROJECT_SOURCE_DIR 为 include root；同时公开
# ${PROJECT_SOURCE_DIR}/include（NetworkTypes/RpcPayload/Result 等公共值层）。
# protobuf 生成物只进 bbt_infra_rpc_proto，不出现在本模块公开头。
add_library(bbt_infra_rpc_core STATIC
    RpcWire.cc
    Client.cc
    Server.cc)
add_library(bbt::infra_rpc_core ALIAS bbt_infra_rpc_core)
target_include_directories(bbt_infra_rpc_core
    PUBLIC "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>"
           "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>")
target_compile_features(bbt_infra_rpc_core PUBLIC cxx_std_17)
target_link_libraries(bbt_infra_rpc_core
    PUBLIC bbt::infra_common
    PRIVATE bbt_infra_rpc_proto)
target_compile_options(bbt_infra_rpc_core PRIVATE -Wall -Wextra -Wpedantic)

add_library(bbt_infra_rpc_http STATIC
    HttpTransport.cc)
add_library(bbt::infra_rpc_http ALIAS bbt_infra_rpc_http)
target_include_directories(bbt_infra_rpc_http
    PUBLIC "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>"
           "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>")
target_compile_features(bbt_infra_rpc_http PUBLIC cxx_std_17)
target_link_libraries(bbt_infra_rpc_http
    PUBLIC bbt::infra_rpc_core bbt::infra_http_net)
target_compile_options(bbt_infra_rpc_http PRIVATE -Wall -Wextra -Wpedantic)

# 兼容 aggregate：既有 consumer 继续链接 bbt::infra_rpc_net（target 与
# include 面不变），实际实现落在 core+http 两个编译单元上。
add_library(bbt_infra_rpc_net INTERFACE)
add_library(bbt::infra_rpc_net ALIAS bbt_infra_rpc_net)
target_link_libraries(bbt_infra_rpc_net
    INTERFACE bbt::infra_rpc_core bbt::infra_rpc_http)

# 公开头自给编译自检：公开头各自单独成 TU（detail 内部头不列入）。
# core 头不得拉入 HTTP/net 类型（IRpc* seam 是纯抽象），http adapter 头单独验。
set(_rpc_core_headers RpcWire.hpp Transport.hpp Client.hpp Server.hpp)
set(_rpc_net_selfcheck_srcs "")
foreach(_hdr IN LISTS _rpc_core_headers)
    set(_tu "${CMAKE_CURRENT_BINARY_DIR}/header_selfcheck/${_hdr}.cc")
    file(WRITE "${_tu}" "#include <bbt/infra/rpc/${_hdr}>\n")
    list(APPEND _rpc_net_selfcheck_srcs "${_tu}")
endforeach()
add_library(bbt_infra_rpc_net_header_selfcheck STATIC ${_rpc_net_selfcheck_srcs})
target_link_libraries(bbt_infra_rpc_net_header_selfcheck PRIVATE bbt::infra_rpc_core)
target_compile_options(bbt_infra_rpc_net_header_selfcheck PRIVATE -Wall -Wextra -Wpedantic)
set_target_properties(bbt_infra_rpc_net_header_selfcheck PROPERTIES LINKER_LANGUAGE CXX)

set(_tu "${CMAKE_CURRENT_BINARY_DIR}/header_selfcheck/HttpTransport.hpp.cc")
file(WRITE "${_tu}" "#include <bbt/infra/rpc/HttpTransport.hpp>\n")
add_library(bbt_infra_rpc_http_header_selfcheck STATIC "${_tu}")
target_link_libraries(bbt_infra_rpc_http_header_selfcheck PRIVATE bbt::infra_rpc_http)
target_compile_options(bbt_infra_rpc_http_header_selfcheck PRIVATE -Wall -Wextra -Wpedantic)
set_target_properties(bbt_infra_rpc_http_header_selfcheck PROPERTIES LINKER_LANGUAGE CXX)

# 第三方类型泄漏负向检查：core 公开头不得 include HTTP/net/protobuf 物理栈
# 头或暴露第三方实现类型（beast/asio/protobuf 生成物）。兼容工厂签名里的
# http::Client/net::Engine/http::ServerOptions 仅经前向声明引用（不产生头
# 依赖），属 architecture §10 的源兼容面，不算泄漏。
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/header_selfcheck/_third_party_guard.cc"
    "// generated: rpc core public headers must not leak physical-stack types\n")
set(_rpc_leak_check "${CMAKE_CURRENT_BINARY_DIR}/rpc_public_header_leak_check.cmake")
file(WRITE "${_rpc_leak_check}"
    "foreach(_hdr RpcWire.hpp Transport.hpp Client.hpp Server.hpp)\n"
    "    file(READ \"${CMAKE_CURRENT_SOURCE_DIR}/\${_hdr}\" _content)\n"
    "    if(_content MATCHES \"#include[^\n]*infra/http/|#include[^\n]*infra/net/|#include[^\n]*\\.pb\\.h|google::protobuf|boost::beast|boost::asio|beast::|asio::\")\n"
    "        message(FATAL_ERROR\n"
    "            \"rpc core public header \${_hdr} leaks HTTP/net/protobuf types\")\n"
    "    endif()\n"
    "endforeach()\n"
    "file(READ \"${CMAKE_CURRENT_SOURCE_DIR}/HttpTransport.hpp\" _http_hdr)\n"
    "if(_http_hdr MATCHES \"#include[^\n]*\\.pb\\.h|google::protobuf\")\n"
    "    message(FATAL_ERROR\n"
    "        \"rpc http adapter header leaks protobuf types\")\n"
    "endif()\n")
add_custom_target(bbt_infra_rpc_public_header_leak_check ALL
    COMMAND "${CMAKE_COMMAND}" -P "${_rpc_leak_check}"
    COMMENT "rpc public headers: no HTTP/net/protobuf type leakage")

if(BUILD_TESTING)
    add_subdirectory("${PROJECT_SOURCE_DIR}/tests/rpc_net" rpc_net_tests)
    add_subdirectory("${PROJECT_SOURCE_DIR}/examples/rpc_net" rpc_net_examples)
endif()

# 双向 RPC 会话唯一消费目标；protobuf 生成物不进入公开头。
add_library(bbt_infra_rpc_net STATIC
    Response.cc
    SessionCodec.cc
    Session.cc
    CoRpcClient.cc
    CoRpcServer.cc
    Hooks.cc
    Options.cc
    detail/RpcSession.cc
    detail/AdmissionImpl.cc
    detail/SessionHooks.cc
    detail/Tcp.cc)
add_library(bbt::infra_rpc_net ALIAS bbt_infra_rpc_net)
target_include_directories(bbt_infra_rpc_net
    PUBLIC "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>")
target_compile_features(bbt_infra_rpc_net PUBLIC cxx_std_17)
target_link_libraries(bbt_infra_rpc_net
    PUBLIC bbt::infra_common bbt::infra_net
    PRIVATE bbt_infra_rpc_proto)
target_compile_options(bbt_infra_rpc_net PRIVATE -Wall -Wextra -Wpedantic)

# 公开头各自单独成 TU；codec 目录头同列自检，detail 内部头不列入。
set(_rpc_headers CoRpcClient.hpp CoRpcServer.hpp Request.hpp Response.hpp Options.hpp
    Stats.hpp SessionCodec.hpp Admission.hpp Hooks.hpp codec/PositionalCodec.hpp)
set(_rpc_net_selfcheck_srcs "")
foreach(_hdr IN LISTS _rpc_headers)
    string(REPLACE "/" "_" _hdr_tu "${_hdr}")
    set(_tu "${CMAKE_CURRENT_BINARY_DIR}/header_selfcheck/${_hdr_tu}.cc")
    file(WRITE "${_tu}" "#include <bbt/infra/rpc/${_hdr}>\n")
    list(APPEND _rpc_net_selfcheck_srcs "${_tu}")
endforeach()
add_library(bbt_infra_rpc_net_header_selfcheck STATIC ${_rpc_net_selfcheck_srcs})
target_link_libraries(bbt_infra_rpc_net_header_selfcheck PRIVATE bbt::infra_rpc_net)
target_compile_options(bbt_infra_rpc_net_header_selfcheck PRIVATE -Wall -Wextra -Wpedantic)
set_target_properties(bbt_infra_rpc_net_header_selfcheck PROPERTIES LINKER_LANGUAGE CXX)

# net/Address.hpp 是公开监听地址的纯值类型；其它物理栈头不得泄漏。
set(_rpc_leak_check "${CMAKE_CURRENT_BINARY_DIR}/rpc_public_header_leak_check.cmake")
file(WRITE "${_rpc_leak_check}"
    "foreach(_hdr ${_rpc_headers})\n"
    "    file(READ \"${CMAKE_CURRENT_SOURCE_DIR}/\${_hdr}\" _content)\n"
    "    string(REGEX REPLACE \"#include[^\\n]*infra/net/Address\\.hpp\" \"\" _content \"\${_content}\")\n"
    "    if(_content MATCHES \"#include[^\\n]*infra/http/|#include[^\\n]*infra/net/|#include[^\\n]*\\.pb\\.h|google::protobuf|boost::beast|boost::asio|beast::|asio::\")\n"
    "        message(FATAL_ERROR \"rpc public header \${_hdr} leaks HTTP/net/protobuf types\")\n"
    "    endif()\n"
    "endforeach()\n")
add_custom_target(bbt_infra_rpc_public_header_leak_check ALL
    COMMAND "${CMAKE_COMMAND}" -P "${_rpc_leak_check}"
    COMMENT "rpc public headers: no HTTP/net/protobuf type leakage")

if(BUILD_TESTING)
    add_subdirectory("${PROJECT_SOURCE_DIR}/tests/rpc_net" rpc_net_tests)
    add_subdirectory("${PROJECT_SOURCE_DIR}/examples/rpc_net" rpc_net_examples)
endif()

# RPC 原生 TCP adapter（BBT_INFRA_RPC_TCP=ON 时经 cmake/RpcTcp.cmake 接入）：
# architecture §6 的 u32be 长度前缀帧实现，一连接一请求一回复，无
# multiplex/半关闭/连接池。
#
# 源文件物理位于 bbt/infra/rpc/（TcpTransport.{hpp,cc} + detail/Tcp.hpp，
# 与 HttpTransport 同布局：公开头以 <bbt/infra/rpc/...> 暴露，include root
# 是 PROJECT_SOURCE_DIR）；本目录只承担开关门控下的 target 注册，符合
# task-breakdown 对 impl-rpc-tcp「不改 bbt/infra/rpc/CMakeLists.txt」的分工。
#
# 依赖闭包（architecture §4）：只依赖 infra_rpc_core + infra_net；
# 不依赖 http_net / protobuf / Beast。
add_library(bbt_infra_rpc_tcp STATIC
    "${CMAKE_CURRENT_SOURCE_DIR}/../rpc/TcpTransport.cc")
add_library(bbt::infra_rpc_tcp ALIAS bbt_infra_rpc_tcp)
target_include_directories(bbt_infra_rpc_tcp
    PUBLIC "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>"
           "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>")
target_compile_features(bbt_infra_rpc_tcp PUBLIC cxx_std_17)
target_link_libraries(bbt_infra_rpc_tcp
    PUBLIC bbt::infra_rpc_core bbt::infra_net)
target_compile_options(bbt_infra_rpc_tcp PRIVATE -Wall -Wextra -Wpedantic)

# adapter 公开头单独自给编译自检（该头 include infra/net 头，不能进 core
# 集合；样式同 bbt_infra_rpc_http_header_selfcheck）。
set(_tu "${CMAKE_CURRENT_BINARY_DIR}/header_selfcheck/TcpTransport.hpp.cc")
file(WRITE "${_tu}" "#include <bbt/infra/rpc/TcpTransport.hpp>\n")
add_library(bbt_infra_rpc_tcp_header_selfcheck STATIC "${_tu}")
target_link_libraries(bbt_infra_rpc_tcp_header_selfcheck PRIVATE bbt::infra_rpc_tcp)
target_compile_options(bbt_infra_rpc_tcp_header_selfcheck PRIVATE -Wall -Wextra -Wpedantic)
set_target_properties(bbt_infra_rpc_tcp_header_selfcheck PROPERTIES LINKER_LANGUAGE CXX)

# TCP 专属契约测试与消费示例（rpc-net 范式：单可执行 + 用例名分发，
# 自有进程 + 动态端口真实 loopback）。测试注册挂在 BUILD_TESTING 下、
# 与 http 路径同一 tests/rpc_net 目录（该目录 CMakeLists 归 rpc-core
# owner；本目录自注册不触碰它）。
if(BUILD_TESTING)
    add_executable(Test_rpc_net_tcp
        "${PROJECT_SOURCE_DIR}/tests/rpc_net/Test_rpc_net_tcp.cc")
    target_link_libraries(Test_rpc_net_tcp
        PRIVATE bbt::infra_rpc_tcp bbt::infra_rpc_net Boost::headers)
    target_compile_options(Test_rpc_net_tcp PRIVATE -Wall -Wextra -Wpedantic)
    foreach(_case tcp_roundtrip tcp_fragmentation tcp_bad_prefix
            tcp_truncated tcp_drop_pre_commit tcp_drop_post_commit
            tcp_cancel tcp_close tcp_overloaded tcp_close_during_dial
            tcp_overloaded_no_dial tcp_late_trailing tcp_server_lifecycle)
        add_test(NAME rpc.net.${_case} COMMAND Test_rpc_net_tcp ${_case})
        set_tests_properties(rpc.net.${_case}
            PROPERTIES TIMEOUT 60 LABELS "rpc_net")
    endforeach()

    add_executable(rpc_tcp_example
        "${PROJECT_SOURCE_DIR}/examples/rpc_net/tcp_main.cc")
    target_link_libraries(rpc_tcp_example PRIVATE bbt::infra_rpc_tcp)
    target_compile_options(rpc_tcp_example PRIVATE -Wall -Wextra -Wpedantic)
    add_test(NAME rpc.net.tcp_consumer COMMAND rpc_tcp_example)
    set_tests_properties(rpc.net.tcp_consumer
        PROPERTIES TIMEOUT 30 LABELS "rpc_net")
endif()

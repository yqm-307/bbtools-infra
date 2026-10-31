# impl-rpc-tcp 交付片段（信息性）：说明 rpc-core owner 合入时使用的 target
# 定义。实际可构建的 target 已由 bbt/infra/rpc_tcp/CMakeLists.txt 在本节点
# 直接提供（cmake/RpcTcp.cmake 的 fail-closed 探测指向该目录，源文件按
# task-breakdown 物理位于 bbt/infra/rpc/）；本文件保留作 owner 合入参考，
# 不需要被 include。
#
# 依赖闭包（architecture §4）：bbt_infra_rpc_tcp 只依赖 infra_rpc_core +
# infra_net；不依赖 http_net / protobuf（wire 编解码在 core）。
#
# 若 rpc-core owner 最终决定把 target 收回 bbt/infra/rpc/CMakeLists.txt
# 同文件管理（与 http adapter 同文件风格），则用下方等价片段替换
# bbt/infra/rpc_tcp/CMakeLists.txt 方案，并让 impl-coord 同步调整
# cmake/RpcTcp.cmake 的探测目录。leak_check 追加规则：TcpTransport.hpp
# 禁 `.pb.h`/`google::protobuf`（不禁 infra/net——它是合法物理栈）。

if(BBT_INFRA_RPC_TCP)
    add_library(bbt_infra_rpc_tcp STATIC
        TcpTransport.cc)
    add_library(bbt::infra_rpc_tcp ALIAS bbt_infra_rpc_tcp)
    target_include_directories(bbt_infra_rpc_tcp
        PUBLIC "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>"
               "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>")
    target_compile_features(bbt_infra_rpc_tcp PUBLIC cxx_std_17)
    target_link_libraries(bbt_infra_rpc_tcp
        PUBLIC bbt::infra_rpc_core bbt::infra_net)
    target_compile_options(bbt_infra_rpc_tcp PRIVATE -Wall -Wextra -Wpedantic)

    # adapter 公开头单独自给编译自检（include infra/net 头，不能进 core 集合）。
    set(_tu "${CMAKE_CURRENT_BINARY_DIR}/header_selfcheck/TcpTransport.hpp.cc")
    file(WRITE "${_tu}" "#include <bbt/infra/rpc/TcpTransport.hpp>\n")
    add_library(bbt_infra_rpc_tcp_header_selfcheck STATIC "${_tu}")
    target_link_libraries(bbt_infra_rpc_tcp_header_selfcheck PRIVATE bbt::infra_rpc_tcp)
    target_compile_options(bbt_infra_rpc_tcp_header_selfcheck PRIVATE -Wall -Wextra -Wpedantic)
    set_target_properties(bbt_infra_rpc_tcp_header_selfcheck PROPERTIES LINKER_LANGUAGE CXX)
endif()

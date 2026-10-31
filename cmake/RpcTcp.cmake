# BBT_INFRA_RPC_TCP=ON 时的薄接线：长度前缀 TCP RPC adapter（bbt::infra_rpc_tcp）。
# 只依赖 net 基础（NetBase.cmake 已由根 CMakeLists.txt 接入），不需要第三方前缀，
# 不拉入 HTTP、Redis 或 Mongo。target/测试/示例由实现节点定义在
# bbt/infra/rpc_tcp/CMakeLists.txt。不复用 bbt/infra/rpc/：该目录由 RpcNet.cmake
# 以 HTTP 栈接入，同一 binary dir 不能二次 add_subdirectory。
# 目录缺失即 FATAL_ERROR，开关 ON 不静默通过。

if(NOT EXISTS "${PROJECT_SOURCE_DIR}/bbt/infra/rpc_tcp/CMakeLists.txt")
    message(FATAL_ERROR
        "BBT_INFRA_RPC_TCP=ON 需要 bbt/infra/rpc_tcp/CMakeLists.txt，但该实现节点尚未合入；"
        "开关 fail-closed，不静默跳过。")
endif()
add_subdirectory("${PROJECT_SOURCE_DIR}/bbt/infra/rpc_tcp" bbt_infra_rpc_tcp)

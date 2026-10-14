# examples/：Issue #27 最小真实消费示例。
# 目标形态：examples/ 目录 + add_executable 直接链接公开 target；
# 不为示例新增第三方依赖，Redis/Mongo 示例仅在对应模块 target 存在时注册。

find_package(Threads REQUIRED)
add_executable(Example_http_loopback http_loopback.cc)
target_link_libraries(Example_http_loopback PRIVATE
    bbt::infra_http Threads::Threads)
target_compile_options(Example_http_loopback PRIVATE -Wall -Wextra)

# Config 模块消费示例（Issue #35）：读取 → 上层校验 → 接受/拒绝；无新依赖。
add_executable(Example_config_consume config_consume.cc)
target_link_libraries(Example_config_consume PRIVATE
    bbt::infra_config Threads::Threads)
target_compile_options(Example_config_consume PRIVATE -Wall -Wextra)

# Redis/Mongo 调用形态示例：同时依赖两个模块 target；任一前缀未接入时
# 对应模块 target 不存在，本示例随之跳过（公共头仍可独立编译）。
# CoRedisCli::Create 与 CoMongoCli::Create 分属两模块实现，须同时链接。
if(TARGET bbt::infra_redis AND TARGET bbt::infra_mongo)
    add_executable(Example_redis_mongo_shape redis_mongo_shape.cc)
    target_link_libraries(Example_redis_mongo_shape PRIVATE
        bbt::infra_redis bbt::infra_mongo Threads::Threads)
    target_compile_options(Example_redis_mongo_shape PRIVATE -Wall -Wextra)
endif()

# MCP 可运行示例（waypoint mcp-server-foundation 4-2A）：--stdio / --http 双模式。
# 同时依赖 mcp_core（协议核）与 mcp_http（Streamable HTTP transport）；任一 target
# 缺失则整体跳过（与上方 redis/mongo 双前缀风格一致）。示例只是可执行目标，
# 不反向成为任何库 target 的依赖；只消费 include/ 公共头（含 bbtools-coroutine
# 的 Scheduler 装配头，经 bbt::infra_common 传递可见）。
if(TARGET bbt::infra_mcp_core AND TARGET bbt::infra_mcp_http)
    add_executable(Example_mcp_server mcp_server.cc)
    target_link_libraries(Example_mcp_server PRIVATE
        bbt::infra_mcp_core bbt::infra_mcp_http Threads::Threads)
    if(BBT_MCP_SCHEMA_ADAPTER)
        target_link_libraries(Example_mcp_server PRIVATE bbt::infra_mcp_schema)
        target_compile_definitions(Example_mcp_server PRIVATE BBT_MCP_SCHEMA_ADAPTER)
    endif()
    target_compile_options(Example_mcp_server PRIVATE -Wall -Wextra)
endif()

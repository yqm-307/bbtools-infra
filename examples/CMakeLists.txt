# examples/：每个示例一个目录，附 README（演示什么、怎么跑、期望输出）。
# 示例只演示用法；断言与验收放进 tests/，不在示例中用断言充当测试。
# CI 编译全部示例；依赖外部服务的示例只编译、不在普通 CI 中运行。
# 示例随模块 target 注册，目标缺失时跳过；不为示例新增第三方依赖。
find_package(Threads REQUIRED)

# HTTP loopback（旧栈）：无外部依赖，可独立编译运行。
if(BBT_INFRA_LEGACY_STACK)
    add_subdirectory(http_loopback)
endif()

# config 首切片：无新依赖。
if(BBT_INFRA_CONFIG)
    add_subdirectory(config_consume)
endif()

# Redis/Mongo 调用形态：同时依赖两个模块 target；任一缺失时跳过。
if(TARGET bbt::infra_redis AND TARGET bbt::infra_mongo)
    add_subdirectory(redis_mongo_shape)
endif()

# Mongo 公开 target 消费：只链接 bbt::infra_mongo，真实后端由 BBT_TEST_MONGO_URI 注入。
if(TARGET bbt::infra_mongo)
    add_subdirectory(mongo_client)
endif()

# ICoConnection 成员化收口 + co_net_service 协程网络：只链接 bbt::infra_net。
if(TARGET bbt::infra_net)
    add_subdirectory(co_net_service)
    add_subdirectory(ico_connection)
endif()

# 新协议示例（http_net/redis_net/rpc_net）的唯一注册方是各协议模块 CMake
# （bbt/infra/{http,redis,rpc}/CMakeLists.txt，经 cmake/*Net.cmake 接入）；
# 这里不再重复 add_subdirectory，否则普通模式会报重复 target/test 名。

# examples/：Issue #27 最小真实消费示例。
# 目标形态：examples/ 目录 + add_executable 直接链接公开 target；
# 不为示例新增第三方依赖，Redis/Mongo 示例仅在对应模块 target 存在时注册。

find_package(Threads REQUIRED)
add_executable(Example_http_loopback http_loopback.cc)
target_link_libraries(Example_http_loopback PRIVATE
    bbt::infra_http Threads::Threads)
target_compile_options(Example_http_loopback PRIVATE -Wall -Wextra)

# Redis/Mongo 调用形态示例：同时依赖两个模块 target；任一前缀未接入时
# 对应模块 target 不存在，本示例随之跳过（公共头仍可独立编译）。
# CoRedisCli::Create 与 CoMongoCli::Create 分属两模块实现，须同时链接。
if(TARGET bbt::infra_redis AND TARGET bbt::infra_mongo)
    add_executable(Example_redis_mongo_shape redis_mongo_shape.cc)
    target_link_libraries(Example_redis_mongo_shape PRIVATE
        bbt::infra_redis bbt::infra_mongo Threads::Threads)
    target_compile_options(Example_redis_mongo_shape PRIVATE -Wall -Wextra)
endif()

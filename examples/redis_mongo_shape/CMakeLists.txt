# Redis/Mongo 调用形态示例：静态形态演示，不连接真实服务。
add_executable(Example_redis_mongo_shape main.cc)
target_link_libraries(Example_redis_mongo_shape PRIVATE bbt::infra_redis bbt::infra_mongo Threads::Threads)
target_compile_options(Example_redis_mongo_shape PRIVATE -Wall -Wextra)

if(BUILD_TESTING)
    add_test(NAME example_redis_mongo_shape COMMAND Example_redis_mongo_shape)
    set_tests_properties(example_redis_mongo_shape PROPERTIES TIMEOUT 60 LABELS "integration")
endif()

# R2 推荐协程网络示例：只消费公开 target（bbt::infra_net + bbtools-coroutine）。
add_executable(Example_co_net_service main.cc)
target_link_libraries(Example_co_net_service PRIVATE bbt::infra_net Threads::Threads)
target_compile_options(Example_co_net_service PRIVATE -Wall -Wextra)

if(BUILD_TESTING)
    add_test(NAME example_co_net_service_roundtrip COMMAND Example_co_net_service)
    set_tests_properties(example_co_net_service_roundtrip PROPERTIES TIMEOUT 60)
endif()

# HTTP loopback 消费示例：只消费公开 target `bbt::infra_http` + bbtools-coroutine。
add_executable(Example_http_loopback main.cc)
target_link_libraries(Example_http_loopback PRIVATE bbt::infra_http Threads::Threads)
target_compile_options(Example_http_loopback PRIVATE -Wall -Wextra)

if(BUILD_TESTING)
    add_test(NAME example_http_loopback COMMAND Example_http_loopback)
    set_tests_properties(example_http_loopback PROPERTIES TIMEOUT 60 LABELS "integration")
endif()

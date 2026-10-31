# config 首切片消费示例：只消费公开 target `bbt::infra_config`。
add_executable(Example_config_consume main.cc)
target_link_libraries(Example_config_consume PRIVATE bbt::infra_config Threads::Threads)
target_compile_options(Example_config_consume PRIVATE -Wall -Wextra)

if(BUILD_TESTING)
    add_test(NAME example_config_consume COMMAND Example_config_consume)
    set_tests_properties(example_config_consume PROPERTIES TIMEOUT 30 LABELS "integration")
endif()

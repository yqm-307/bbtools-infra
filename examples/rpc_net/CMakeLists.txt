add_executable(rpc_net_example main.cc)
target_link_libraries(rpc_net_example PRIVATE bbt::infra_rpc_net)
target_compile_options(rpc_net_example PRIVATE -Wall -Wextra -Wpedantic)
if(BUILD_TESTING)
    add_test(NAME example_rpc_net_consumer COMMAND rpc_net_example)
    set_tests_properties(example_rpc_net_consumer PROPERTIES TIMEOUT 30 LABELS "integration")
endif()

# v2 AppSession 双进程 consumer：server 子进程绑定动态端口经 stdout 管道传
# PORT 行，client 子进程拨号 + Call/Send 验证一条双向 TCP link。
# session_consumer.py 是 CTest 前台受管父进程（对齐 tests/net +
# scripts/bench/driver.py 既有惯例）：管道交换端口、stdin EOF 触发 server
# 协作收口、检查两端退出码与关键断言；try/finally 兜底仅用于异常/超时路径。
add_executable(rpc_v2_session_example session_main.cc)
target_link_libraries(rpc_v2_session_example PRIVATE bbt::infra_rpc_net)
target_compile_options(rpc_v2_session_example PRIVATE -Wall -Wextra -Wpedantic)
if(BUILD_TESTING)
    find_package(Python3 COMPONENTS Interpreter REQUIRED)
    add_test(NAME rpc.net.v2_session_consumer
        COMMAND "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/session_consumer.py"
            "$<TARGET_FILE:rpc_v2_session_example>")
    set_tests_properties(rpc.net.v2_session_consumer
        PROPERTIES TIMEOUT 120 LABELS "rpc_net")
endif()

# P5 推荐协程网络示例：只消费公开 target（bbt::infra_net + bbtools-coroutine），
# 不引入新依赖、不触碰库目标与既有测试注册。
add_executable(Example_co_net_service main.cc)
target_link_libraries(Example_co_net_service PRIVATE bbt::infra_net Threads::Threads)
target_compile_options(Example_co_net_service PRIVATE -Wall -Wextra)

# CTest 注册（只在既有测试开关 BUILD_TESTING 打开时注册，不强启测试）：
# 四个模式各一个测试，每个都带 TIMEOUT。errors 模式按设计以退出码 3 结束，
# 用最小 cmake -P 脚本精确校验退出码，绝不用 WILL_FAIL —— WILL_FAIL 会把
# 任何非零退出（断言失败、崩溃、被信号杀死）与 CTest 超时统统反转成绿。
if(BUILD_TESTING)
    add_test(NAME example_co_net_service_lifecycle COMMAND Example_co_net_service lifecycle)
    add_test(NAME example_co_net_service_concurrent COMMAND Example_co_net_service concurrent)
    add_test(NAME example_co_net_service_limits COMMAND Example_co_net_service limits)
    add_test(NAME example_co_net_service_errors
        COMMAND ${CMAKE_COMMAND}
            -DEXAMPLE_EXE=$<TARGET_FILE:Example_co_net_service>
            -DEXAMPLE_MODE=errors
            -DEXPECT_RC=3
            -P ${CMAKE_CURRENT_SOURCE_DIR}/check_exit_code.cmake)
    set_tests_properties(
        example_co_net_service_lifecycle
        example_co_net_service_concurrent
        example_co_net_service_limits
        PROPERTIES TIMEOUT 60)
    # TIMEOUT 取固定有界值，不对具体秒数做推算：errors 模式**没有**统一的 30s 全局期限
    # （30s 是 limits 模式的协程预算，见 main.cc），errors 的耗时由各分步等待各自有界地累计，
    # 分步等待的上界与「整个进程的上界」不是同一件事，故不据此收窄或累加。
    # 另：check_exit_code.cmake 对被测进程还有脚本自身的一次执行超时 60s，独立于 CTest TIMEOUT。
    # 因此 errors 保持固定有界 TIMEOUT 90s 留足余量，其余三模式保持 60s。
    set_tests_properties(example_co_net_service_errors PROPERTIES TIMEOUT 90)
endif()

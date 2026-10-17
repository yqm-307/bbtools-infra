# 精确校验被测示例的退出码：rc 必须恰等于 EXPECT_RC，否则测试失败。
# 刻意不用 add_test(... WILL_FAIL)：WILL_FAIL 会把「任何非零退出、被信号杀死、
# 或 CTest 超时」统统反转成通过，等于拿错误/超时换绿灯。
if(NOT DEFINED EXAMPLE_EXE OR NOT DEFINED EXPECT_RC)
    message(FATAL_ERROR "需要 -DEXAMPLE_EXE=<exe> -DEXAMPLE_MODE=<mode> -DEXPECT_RC=<rc>")
endif()
execute_process(
    COMMAND "${EXAMPLE_EXE}" "${EXAMPLE_MODE}"
    RESULT_VARIABLE example_rc
    OUTPUT_VARIABLE example_out
    ERROR_VARIABLE example_err
    TIMEOUT 60)
if(NOT "${example_rc}" STREQUAL "${EXPECT_RC}")
    message(STATUS "--- ${EXAMPLE_MODE} stdout ---\n${example_out}\n--- stderr ---\n${example_err}")
    message(FATAL_ERROR "example 退出码 '${example_rc}' != 期望 '${EXPECT_RC}' (mode=${EXAMPLE_MODE})")
endif()
message(STATUS "example 退出码 ${example_rc} == 期望 ${EXPECT_RC} (mode=${EXAMPLE_MODE})")

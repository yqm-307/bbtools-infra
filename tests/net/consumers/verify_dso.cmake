# 核验 consumer 实际链接的 libbbt_coroutine.so 来自 fixture 内 add_subdirectory 的上游构建。
if(NOT BINARY OR NOT NEEDLE)
    message(FATAL_ERROR "verify_dso 需要 -DBINARY 与 -DNEEDLE")
endif()
execute_process(COMMAND ldd "${BINARY}"
    OUTPUT_VARIABLE _dso_out ERROR_VARIABLE _dso_err RESULT_VARIABLE _dso_res)
if(NOT _dso_res EQUAL 0)
    message(FATAL_ERROR "ldd 失败：${_dso_err}")
endif()
string(FIND "${_dso_out}" "${NEEDLE}" _dso_pos)
if(_dso_pos EQUAL -1)
    message(FATAL_ERROR "DSO 来源不符：期望 ldd 输出包含 '${NEEDLE}'\n${_dso_out}")
endif()
message(STATUS "consumer DSO 来自 fixture 上游构建（'${NEEDLE}' 命中）")

# 断言 mismatch fixture 的 configure 以指定标记失败；任意失败或意外成功都不算通过。
if(NOT BUILD_ROOT OR NOT GENERATOR OR NOT NET_CONSUMER_COROUTINE_DIR
    OR NOT NET_CONSUMER_INFRA_DIR OR NOT NET_CONSUMER_STUB_DIR)
    message(FATAL_ERROR "expect_configure_failure 缺少参数")
endif()
set(_build "${BUILD_ROOT}/consumer-mismatch")
file(REMOVE_RECURSE "${_build}")
set(_output "")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -S "${CMAKE_CURRENT_LIST_DIR}/source_dir_mismatch" -B "${_build}"
        -G "${GENERATOR}"
        "-DNET_CONSUMER_COROUTINE_DIR=${NET_CONSUMER_COROUTINE_DIR}"
        "-DNET_CONSUMER_INFRA_DIR=${NET_CONSUMER_INFRA_DIR}"
        "-DNET_CONSUMER_STUB_DIR=${NET_CONSUMER_STUB_DIR}"
        -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
    RESULT_VARIABLE _result OUTPUT_VARIABLE _output ERROR_VARIABLE _err)
string(APPEND _output "${_err}")
if(_result EQUAL 0)
    message(FATAL_ERROR "mismatch SOURCE_DIR 的 configure 意外成功：复用分支未拒绝错误来源\n${_output}")
endif()
string(FIND "${_output}" "[bbt-infra-net] 已有 bbt_coroutine" _pos)
if(_pos EQUAL -1)
    message(FATAL_ERROR "mismatch configure 失败原因不是预期标记：\n${_output}")
endif()
message(STATUS "mismatch configure 被明确拒绝（标记命中）")

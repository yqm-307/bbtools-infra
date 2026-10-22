# pin 复用分支 probe：configure tests/net/consumers/pin_reuse——
#   PIN_KIND=mismatch：已建 bbt_coroutine（真实 pin 树）+ 声明目录=占位 stub
#       → configure 必须非零且命中「来源…不一致」标记；
#   PIN_KIND=ok：同一目录两边 → configure 必须成功且命中 pin 命中标记。
# 需要：BUILD_ROOT/GENERATOR/PIN_MODULE/REAL_COROUTINE_DIR/PIN_KIND/STUB_DIR。
if(NOT BUILD_ROOT OR NOT GENERATOR OR NOT PIN_MODULE
    OR NOT REAL_COROUTINE_DIR OR NOT PIN_KIND OR NOT STUB_DIR)
    message(FATAL_ERROR "expect_pin_reuse 缺少参数")
endif()
if(PIN_KIND STREQUAL "mismatch")
    set(_declared "${STUB_DIR}")
elseif(PIN_KIND STREQUAL "ok")
    set(_declared "${REAL_COROUTINE_DIR}")
else()
    message(FATAL_ERROR "PIN_KIND 需要 mismatch|ok")
endif()
set(_build "${BUILD_ROOT}/pin-reuse-${PIN_KIND}")
file(REMOVE_RECURSE "${_build}")
execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -S "${CMAKE_CURRENT_LIST_DIR}/pin_reuse" -B "${_build}"
        -G "${GENERATOR}"
        "-DREAL_COROUTINE_DIR=${REAL_COROUTINE_DIR}"
        "-DSTUB_COROUTINE_DIR=${_declared}"
        "-DPIN_MODULE=${PIN_MODULE}"
        "-DPROBE_KIND=${PIN_KIND}"
        -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _err)
string(APPEND _out "${_err}")
if(PIN_KIND STREQUAL "mismatch")
    if(_rc EQUAL 0)
        message(FATAL_ERROR
            "pin_reuse mismatch configure 意外成功：复用分支未拒绝不同来源\n${_out}")
    endif()
    string(FIND "${_out}" "不一致" _pos)
    if(_pos EQUAL -1)
        message(FATAL_ERROR
            "pin_reuse mismatch 拒绝原因不是预期标记：\n${_out}")
    endif()
    message(STATUS "pin_reuse mismatch 被明确拒绝（标记命中）")
else()
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "pin_reuse ok configure 意外失败：\n${_out}")
    endif()
    string(FIND "${_out}" "coroutine pin 命中" _pos)
    if(_pos EQUAL -1)
        message(FATAL_ERROR "pin_reuse ok 通过但缺 pin 命中标记：\n${_out}")
    endif()
    message(STATUS "pin_reuse ok：同源复用放行且 pin 命中")
endif()

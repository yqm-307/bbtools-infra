# pin 负向/正向 probe 驱动（cmake -P）：不建 helper framework，单个脚本按
# PIN_CASE 分派——每 case 在临时目录构造最小 coroutine 源码树形态，调
# bbt_coroutine_pin_verify() 断言其被「指定标记」拒绝或命中。
#
# 需要的参数：
#   -DPIN_CASE=<case>      positive|wrong_sha|dirty|no_git|untracked_src|
#                          manifest_rejected
#   -DPIN_MODULE=<path>    CoroutinePin.cmake 绝对路径
#   -DWORK_DIR=<path>      本 case 的临时根（脚本内 file(REMOVE_RECURSE)）
#   -DREAL_COROUTINE_DIR=<path>  固定 U 的真实干净检出
#
# 任一 case 非零即 message(FATAL_ERROR)：期望拒绝的意外通过或期望通过的意外
# 失败都算失败；拒绝消息必须命中预期标记（不拿「碰巧报错」当拒绝证据）。
# stub 树构造只发生在 WORK_DIR 临时副本上，不触碰 REAL_COROUTINE_DIR。

if(NOT PIN_CASE OR NOT PIN_MODULE OR NOT WORK_DIR OR NOT REAL_COROUTINE_DIR)
    message(FATAL_ERROR "pin_expect 缺少 PIN_CASE/PIN_MODULE/WORK_DIR/REAL_COROUTINE_DIR")
endif()
file(REMOVE_RECURSE "${WORK_DIR}")
file(MAKE_DIRECTORY "${WORK_DIR}")

include("${PIN_MODULE}")
set(_pin "${BBT_COROUTINE_PINNED_SHA}")

function(_run_verify dir out_rc out_text)
    set(_probe "${WORK_DIR}/_probe_${PIN_CASE}.cmake")
    file(WRITE "${_probe}"
        "include(\"${PIN_MODULE}\")\nbbt_coroutine_pin_verify(\"${dir}\")\n")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -P "${_probe}"
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _err)
    set(${out_rc} "${_rc}" PARENT_SCOPE)
    set(${out_text} "${_out}${_err}" PARENT_SCOPE)
endfunction()

function(_expect_reject dir marker)
    _run_verify("${dir}" _rc _out)
    if(_rc EQUAL 0)
        message(FATAL_ERROR
            "pin_expect(${PIN_CASE})：bbt_coroutine_pin_verify(${dir}) 意外通过，"
            "期望拒绝标记 '${marker}'\n${_out}")
    endif()
    string(FIND "${_out}" "${marker}" _pos)
    if(_pos EQUAL -1)
        message(FATAL_ERROR
            "pin_expect(${PIN_CASE})：拒绝消息未命中标记 '${marker}'：\n${_out}")
    endif()
    message(STATUS "pin_expect(${PIN_CASE})：拒绝命中 '${marker}'")
endfunction()

function(_expect_accept dir)
    _run_verify("${dir}" _rc _out)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR
            "pin_expect(${PIN_CASE})：bbt_coroutine_pin_verify(${dir}) 意外拒绝：\n${_out}")
    endif()
    string(FIND "${_out}" "coroutine pin 命中" _pos)
    if(_pos EQUAL -1)
        message(FATAL_ERROR
            "pin_expect(${PIN_CASE})：通过但缺命中标记：\n${_out}")
    endif()
    message(STATUS "pin_expect(${PIN_CASE})：命中 ${_pin}（${dir}）")
endfunction()

function(_copy_checkout src dst)
    execute_process(
        COMMAND git clone --quiet --no-hardlinks --no-checkout "${src}" "${dst}"
        RESULT_VARIABLE _rc)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "isolated clone ${src} → ${dst} 失败")
    endif()
    execute_process(COMMAND git -C "${dst}" checkout --quiet --detach "${_pin}"
        RESULT_VARIABLE _rc)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "isolated checkout ${_pin} 失败")
    endif()
endfunction()

function(_export_tree dst tarname)
    file(MAKE_DIRECTORY "${dst}")
    execute_process(
        COMMAND git -C "${REAL_COROUTINE_DIR}" archive HEAD -o "${WORK_DIR}/${tarname}"
        RESULT_VARIABLE _arc OUTPUT_VARIABLE _ao ERROR_VARIABLE _ae)
    if(NOT _arc EQUAL 0)
        message(FATAL_ERROR "git archive 失败：${_ae}")
    endif()
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E tar xzf "${WORK_DIR}/${tarname}"
        WORKING_DIRECTORY "${dst}"
        RESULT_VARIABLE _xrc OUTPUT_VARIABLE _xo ERROR_VARIABLE _xe)
    if(NOT _xrc EQUAL 0)
        message(FATAL_ERROR "解包 ${tarname} 失败：${_xe}")
    endif()
endfunction()

if(PIN_CASE STREQUAL "positive")
    _expect_accept("${REAL_COROUTINE_DIR}")

elseif(PIN_CASE STREQUAL "wrong_sha")
    # 复制真实检出到 stub，造一个空提交使 HEAD != pin（不动真实树）。
    set(_stub "${WORK_DIR}/stub_wrong_sha")
    _copy_checkout("${REAL_COROUTINE_DIR}" "${_stub}")
    execute_process(
        COMMAND git -C "${_stub}" -c user.email=pin-probe@local
                -c user.name=pin-probe commit --allow-empty -m "pin-probe"
        RESULT_VARIABLE _crc OUTPUT_VARIABLE _co ERROR_VARIABLE _ce)
    if(NOT _crc EQUAL 0)
        message(FATAL_ERROR "构造 wrong-SHA stub 失败：${_ce}")
    endif()
    _expect_reject("${_stub}" "SHA 漂移拒绝")

elseif(PIN_CASE STREQUAL "dirty")
    # 复制检出后在消费面改一个 tracked 文件（不 commit）→ diff --quiet 非零。
    set(_stub "${WORK_DIR}/stub_dirty")
    _copy_checkout("${REAL_COROUTINE_DIR}" "${_stub}")
    file(APPEND "${_stub}/CMakeLists.txt" "# pin-probe dirty\n")
    # 标记须单行内连续：CMake message 会在宽行插入换行+缩进。
    _expect_reject("${_stub}" "内容不干净")

elseif(PIN_CASE STREQUAL "untracked_src")
    # 干净检出 + 消费面内 untracked 新源码输入 → 拒绝（untracked 面核验）。
    set(_stub "${WORK_DIR}/stub_untracked")
    _copy_checkout("${REAL_COROUTINE_DIR}" "${_stub}")
    file(WRITE "${_stub}/bbt/pin_probe_injected.hpp" "// injected\n")
    _expect_reject("${_stub}" "未追踪或未提交条目")

elseif(PIN_CASE STREQUAL "no_git")
    # 只有源码形态、无 .git：必须拒绝（git-only，无 manifest/归档放行通道）。
    set(_stub "${WORK_DIR}/stub_no_git")
    _export_tree("${_stub}" "tree.tar")
    _expect_reject("${_stub}" "不是可核验的 git 检出")

elseif(PIN_CASE STREQUAL "manifest_rejected")
    # 非 git 树里伪造 .bbt-coroutine-pin（即使写对 SHA）也不得放行——
    # manifest 分支已删除，任何「自我声明」都不是固定来源证据。
    set(_stub "${WORK_DIR}/stub_manifest")
    _export_tree("${_stub}" "tree_m.tar")
    file(WRITE "${_stub}/.bbt-coroutine-pin" "${_pin} -\n")
    _expect_reject("${_stub}" "不是可核验的 git 检出")

else()
    message(FATAL_ERROR "未知 PIN_CASE='${PIN_CASE}'")
endif()

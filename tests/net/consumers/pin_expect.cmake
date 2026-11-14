# pin 负向/正向 probe 驱动（cmake -P）：不建 helper framework，单个脚本按
# PIN_CASE 分派——每 case 在临时目录构造最小 coroutine 源码树形态，调
# bbt_coroutine_pin_verify() 断言其被「指定标记」拒绝或命中。
#
# 需要的参数：
#   -DPIN_CASE=<case>      positive|wrong_sha|dirty|no_git|untracked_src|
#                          manifest_rejected|latest_clean|latest_dirty|invalid_sha|
#                          ci_clean|ci_dirty|ci_empty|ci_wrong_sha
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

# 固定版本用例显式选择 CI 参考 pin，不依赖产品默认锁版。
get_filename_component(_module_dir "${PIN_MODULE}" DIRECTORY)
file(READ "${_module_dir}/coroutine.sha" _pin_raw)
string(REGEX REPLACE "\n$" "" _pin "${_pin_raw}")
set(_requested "${_pin}")

function(_run_verify dir out_rc out_text)
    set(_probe "${WORK_DIR}/_probe_${PIN_CASE}.cmake")
    set(_selection "")
    if(NOT PIN_CASE MATCHES "^(latest_|ci_)")
        set(_selection "set(BBT_COROUTINE_PINNED_SHA \"${_requested}\")\n")
    endif()
    set(_ci false)
    if(PIN_CASE MATCHES "^ci_")
        set(_ci true)
        if(PIN_CASE STREQUAL "ci_empty")
            set(_selection "set(BBT_COROUTINE_PINNED_SHA \"\" CACHE STRING \"\")\n")
        elseif(PIN_CASE STREQUAL "ci_wrong_sha")
            set(_selection "set(BBT_COROUTINE_PINNED_SHA \"0000000000000000000000000000000000000000\")\n")
        endif()
    endif()
    file(WRITE "${_probe}"
        "${_selection}include(\"${PIN_MODULE}\")\nbbt_coroutine_pin_verify(\"${dir}\")\n")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E env "GITHUB_ACTIONS=${_ci}"
                "${CMAKE_COMMAND}" -P "${_probe}"
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
    if(PIN_CASE MATCHES "^latest_")
        set(_marker "coroutine 使用本地最新源码")
    else()
        set(_marker "coroutine pin 命中")
    endif()
    string(FIND "${_out}" "${_marker}" _pos)
    if(_pos EQUAL -1)
        message(FATAL_ERROR
            "pin_expect(${PIN_CASE})：通过但缺命中标记：\n${_out}")
    endif()
    message(STATUS "pin_expect(${PIN_CASE})：${_marker}（${dir}）")
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
    # 真实目录可能正处于开发中；固定版本正例用隔离的干净检出。
    set(_stub "${WORK_DIR}/stub_positive")
    _copy_checkout("${REAL_COROUTINE_DIR}" "${_stub}")
    _expect_accept("${_stub}")

elseif(PIN_CASE STREQUAL "wrong_sha")
    set(_requested "0000000000000000000000000000000000000000")
    _expect_reject("${REAL_COROUTINE_DIR}" "SHA 漂移拒绝")

elseif(PIN_CASE STREQUAL "invalid_sha")
    # CMake 的 false-like 值也必须当作非法指定，不能误走最新模式。
    set(_requested "0")
    _expect_reject("${REAL_COROUTINE_DIR}" "必须是完整")

elseif(PIN_CASE STREQUAL "latest_clean" OR PIN_CASE STREQUAL "latest_dirty")
    set(_stub "${WORK_DIR}/stub_latest")
    _copy_checkout("${REAL_COROUTINE_DIR}" "${_stub}")
    if(PIN_CASE STREQUAL "latest_dirty")
        file(APPEND "${_stub}/CMakeLists.txt" "# local development\n")
        file(WRITE "${_stub}/bbt/pin_probe_injected.hpp" "// local source\n")
    endif()
    _expect_accept("${_stub}")

elseif(PIN_CASE MATCHES "^ci_")
    set(_stub "${WORK_DIR}/stub_ci")
    _copy_checkout("${REAL_COROUTINE_DIR}" "${_stub}")
    if(PIN_CASE STREQUAL "ci_dirty" OR PIN_CASE STREQUAL "ci_empty")
        file(APPEND "${_stub}/CMakeLists.txt" "# CI must reject modified source\n")
        _expect_reject("${_stub}" "内容不干净")
    elseif(PIN_CASE STREQUAL "ci_wrong_sha")
        _expect_reject("${_stub}" "CI 指定版本与 coroutine.sha 不一致")
    elseif(PIN_CASE STREQUAL "ci_clean")
        _expect_accept("${_stub}")
    else()
        message(FATAL_ERROR "未知 PIN_CASE='${PIN_CASE}'")
    endif()

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

# coroutine 源码消费：默认使用所选本地检出的当前内容；显式 SHA 才锁版。
# 不 fetch/checkout，不自动把 CI 的 cmake/coroutine.sha 当作本地开发要求。
# 来源目录一致性在两种模式下都检查，最新模式不冒充固定版本证据。
if(NOT DEFINED BBT_COROUTINE_PINNED_SHA)
    set(BBT_COROUTINE_PINNED_SHA "" CACHE STRING
        "可选的 coroutine 完整 commit SHA；留空使用所选本地检出的当前源码（允许未提交改动）")
endif()
if("$ENV{GITHUB_ACTIONS}" STREQUAL "true")
    # 保留现有 GitHub CI 的固定版本门禁，不把本地默认值传播成 CI 放宽。
    # 使用普通变量，不将 CI 选择写回本地 cache。
    file(READ "${CMAKE_CURRENT_LIST_DIR}/coroutine.sha" _bbt_ci_pin_raw)
    string(REGEX REPLACE "\n$" "" _bbt_ci_pin "${_bbt_ci_pin_raw}")
    if(NOT "${BBT_COROUTINE_PINNED_SHA}" STREQUAL ""
       AND NOT "${BBT_COROUTINE_PINNED_SHA}" STREQUAL "${_bbt_ci_pin}")
        message(FATAL_ERROR "[bbt-infra-pin] CI 指定版本与 coroutine.sha 不一致")
    endif()
    set(BBT_COROUTINE_PINNED_SHA "${_bbt_ci_pin}")
    if("${BBT_COROUTINE_PINNED_SHA}" STREQUAL "")
        message(FATAL_ERROR "[bbt-infra-pin] CI coroutine.sha 不得为空")
    endif()
endif()
if(NOT "${BBT_COROUTINE_PINNED_SHA}" STREQUAL "")
    string(LENGTH "${BBT_COROUTINE_PINNED_SHA}" _bbt_pin_length)
    if(NOT _bbt_pin_length EQUAL 40 OR NOT BBT_COROUTINE_PINNED_SHA MATCHES "^[0-9a-f]+$")
        message(FATAL_ERROR
            "[bbt-infra-pin] BBT_COROUTINE_PINNED_SHA 必须是完整 40 位小写 hex SHA，或留空使用本地最新源码")
    endif()
endif()

# bbt_coroutine_verify_existing_target(<expected_source_dir_realpath>)
#   普通/NetworkOnly 两条消费路径共用：已有 bbt_coroutine target 时核其真实
#   SOURCE_DIR（解 alias、realpath）与声明目录一致；不一致/无 SOURCE_DIR
#   （imported/同名 target）即 FATAL_ERROR——声明目录核验过了不代表实际链接
#   的是同一棵树。应在 bbt_coroutine_pin_verify() 之前调用；通过后对声明目录
#   核验等价于对实际来源核验。
function(bbt_coroutine_verify_existing_target _expected)
    if(NOT TARGET bbt_coroutine)
        return()
    endif()
    set(_real bbt_coroutine)
    get_target_property(_alias bbt_coroutine ALIASED_TARGET)
    if(_alias)
        set(_real "${_alias}")
    endif()
    get_target_property(_source "${_real}" SOURCE_DIR)
    if(NOT _source OR _source MATCHES "NOTFOUND$")
        message(FATAL_ERROR
            "[bbt-infra-pin] 已有 bbt_coroutine 没有真实 SOURCE_DIR"
            "（可能是 imported/同名 target）；拒绝不明来源")
    endif()
    get_filename_component(_actual "${_source}" REALPATH)
    if(NOT _actual STREQUAL _expected)
        message(FATAL_ERROR
            "[bbt-infra-pin] 已有 bbt_coroutine 来源(${_actual})"
            "与声明 BBT_COROUTINE_SOURCE_DIR(${_expected})不一致")
    endif()
endfunction()

# bbt_coroutine_pin_verify(<resolved_source_dir>)
#   resolved_source_dir: 调用方已 REALPATH 解析的 coroutine 源码树根。
function(bbt_coroutine_pin_verify _dir)
    set(_expected "${BBT_COROUTINE_PINNED_SHA}")

    # 两种模式都要求真实 Git 来源；最新模式只放宽版本/洁净要求。
    find_package(Git QUIET)
    if(NOT GIT_FOUND)
        message(FATAL_ERROR
            "[bbt-infra-pin] 无法核验 coroutine pin：git 不可用。"
            "请提供 git，并以 -DBBT_COROUTINE_SOURCE_DIR=<git 检出树> 指向 bbtools-coroutine。")
    endif()

    # stub 树（git archive 输出、无 .git）必须直接拒绝——rev-parse 会向上
    # 发现父级 .git 误判为 git 检出。先核 .git 存在再跑 rev-parse。
    if(NOT EXISTS "${_dir}/.git")
        message(FATAL_ERROR
            "[bbt-infra-pin] ${_dir} 不是可核验的 git 检出：无 .git。"
            "请指向 bbtools-coroutine 的 git 检出。")
    endif()

    execute_process(
        COMMAND "${GIT_EXECUTABLE}" -C "${_dir}" rev-parse HEAD
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _head ERROR_VARIABLE _err
        OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR
            "[bbt-infra-pin] ${_dir} 不是可核验的 git 检出：${_err}。"
            "请指向 bbtools-coroutine 的 git 检出。")
    endif()
    if("${_expected}" STREQUAL "")
        message(STATUS
            "[bbt-infra-pin] coroutine 使用本地最新源码，允许未提交改动；HEAD=${_head} 仅为基线，不代表完整源码快照（${_dir}）")
        return()
    endif()
    if(NOT _head STREQUAL _expected)
        message(FATAL_ERROR
            "[bbt-infra-pin] coroutine SHA 漂移拒绝："
            "实际 HEAD=${_head}，要求固定 ${_expected}（${_dir}）。"
            "消费标识是不可变 commit，不用分支/标签/新 main 代替。")
    endif()

    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E env "GIT_CEILING_DIRECTORIES=${_dir}"
                "${GIT_EXECUTABLE}" -C "${_dir}" diff --quiet HEAD --
        RESULT_VARIABLE _dirty OUTPUT_VARIABLE _out ERROR_VARIABLE _derr)
    if(NOT _dirty EQUAL 0)
        message(FATAL_ERROR
            "[bbt-infra-pin] coroutine 检出 tracked 内容不干净（${_dir}）："
            "git diff --quiet HEAD 非零（${_derr}），拒绝把已修改源码当固定版本消费。")
    endif()

    # tracked 干净之外，已暂存（cached）差异也属漂移。
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E env "GIT_CEILING_DIRECTORIES=${_dir}"
                "${GIT_EXECUTABLE}" -C "${_dir}" diff --cached --quiet
        RESULT_VARIABLE _staged OUTPUT_VARIABLE _sout ERROR_VARIABLE _serr)
    if(NOT _staged EQUAL 0)
        message(FATAL_ERROR
            "[bbt-infra-pin] coroutine 检出存在已暂存差异（${_dir}），拒绝继续。")
    endif()

    # untracked 全量不阻塞（构建残留允许存在），但落在消费面内的 untracked
    # 源码输入——顶层 CMakeLists.txt、cmake/、bbt/ 下的新增条目——会改变实际
    # 消费内容，同样拒绝（干净检出之外混入的源码不是固定版本的一部分）。
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E env "GIT_CEILING_DIRECTORIES=${_dir}"
                "${GIT_EXECUTABLE}" -C "${_dir}" status --porcelain --
                CMakeLists.txt cmake bbt
        RESULT_VARIABLE _porc OUTPUT_VARIABLE _porcelain ERROR_VARIABLE _perr)
    if(NOT _porc EQUAL 0)
        message(FATAL_ERROR
            "[bbt-infra-pin] 无法核验 ${_dir} 的 untracked 源码面：${_perr}")
    endif()
    if(NOT _porcelain STREQUAL "")
        message(FATAL_ERROR
            "[bbt-infra-pin] coroutine 检出在消费面（CMakeLists.txt/cmake//bbt/）"
            "存在未追踪或未提交条目（${_dir}）：\n${_porcelain}"
            "拒绝把混入额外源码输入的树当固定版本消费。")
    endif()

    message(STATUS
        "[bbt-infra-pin] coroutine pin 命中 ${_expected}，检出干净（${_dir}）")
endfunction()

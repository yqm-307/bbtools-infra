# bbtools-coroutine 固定 commit 消费门禁（infra-perf loop T1）。
#
# 单一锁版真源：cmake/coroutine.sha（本模块同目录、40 hex 单行）。
#   - CMake 在加载本模块时读取并校验格式；CI 的 load-pin 步骤读同一文件产出
#     COROUTINE_PIN_SHA（.github/workflows/ci.yml），checkout ref/prepare-deps/
#     network job 复核都用它——一处真源，不维护多份字面量；
#   - 普通消费与 BBT_INFRA_NETWORK_ONLY 都在解析完上游真实路径后调用
#     bbt_coroutine_pin_verify()，在 include() 作用域内 fail-closed；
#   - 出处/语义见 docs/dependencies/bbtools-coroutine-pin.provenance.md。
#
# 核验口径（诚实边界，git-only）：
#   - 必须是 git 仓库检出：HEAD == pin 且 tracked 内容干净（git diff --quiet
#     HEAD 覆盖 tracked modify/delete；git diff --cached --quiet 覆盖暂存；
#     另查 untracked 中落在 bbt/、cmake/、CMakeLists.txt 上的新增源码输入——
#     消费面内混入未追踪源码同样不是固定版本）。
#   - 无 .git / git 不可用 / 非 git 归档树：一律拒绝。来历不明的源码不能证明
#     自己是固定版本；没有「非 git 放行通道」（原 .bbt-coroutine-pin manifest
#     模式已删除：其 SHA 段正则恒不匹配属死代码，且只自我声明、不锚定内容，
#     不证明来源）。已安装 find_package 包同样无法证明 pin（CMakeLists.txt
#     回退分支发 WARNING，不冒充锁版证据）。
#   - coroutine.sha 格式：恰好 40 个 [0-9a-f] 字符，不做规范化/截断/大小写
#     折叠——格式不符直接 FATAL_ERROR（pin 文件本身是门禁的一部分）。
#
# 本模块只核验，不取替 add_subdirectory / target 复用检查（NetworkOnly.cmake
# 与普通 CMakeLists.txt 的 SOURCE_DIR 一致性检查仍然独立存在）。

set(_bbt_pin_file "${CMAKE_CURRENT_LIST_DIR}/coroutine.sha")
if(NOT EXISTS "${_bbt_pin_file}")
    message(FATAL_ERROR
        "[bbt-infra-pin] coroutine pin 真源缺失：${_bbt_pin_file}；"
        "锁版实现要求 cmake/coroutine.sha 单行 40hex commit。")
endif()
file(READ "${_bbt_pin_file}" _bbt_pin_raw)
string(REGEX REPLACE "\n$" "" BBT_COROUTINE_PINNED_SHA "${_bbt_pin_raw}")
string(LENGTH "${BBT_COROUTINE_PINNED_SHA}" _bbt_pin_length)
if(NOT _bbt_pin_length EQUAL 40)
    message(FATAL_ERROR "[bbt-infra-pin] coroutine.sha 必须只有一个完整 40hex SHA，可带一个末尾换行")
endif()
if(NOT BBT_COROUTINE_PINNED_SHA MATCHES "^[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]$")
    message(FATAL_ERROR
        "[bbt-infra-pin] coroutine.sha 格式非法：'${BBT_COROUTINE_PINNED_SHA}'；"
        "要求恰好 40 个小写 hex 字符，不做规范化/截断。")
endif()
unset(_bbt_pin_file)

# bbt_coroutine_verify_existing_target(<expected_source_dir_realpath>)
#   普通/NetworkOnly 两条消费路径共用：已有 bbt_coroutine target 时核其真实
#   SOURCE_DIR（解 alias、realpath）与声明目录一致；不一致/无 SOURCE_DIR
#   （imported/同名 target）即 FATAL_ERROR——声明目录核验过了不代表实际链接
#   的是同一棵树。应在 bbt_coroutine_pin_verify() 之前调用；通过后对声明目录
#   pin 等价于对实际来源 pin。
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

    # git-only：没有 git 可执行文件就没有可证明的固定来源。
    find_package(Git QUIET)
    if(NOT GIT_FOUND)
        message(FATAL_ERROR
            "[bbt-infra-pin] 无法核验 coroutine pin：git 不可用。"
            "请提供 git，并以 -DBBT_COROUTINE_SOURCE_DIR=<git 检出树> 指向 "
            "bbtools-coroutine 在 ${_expected} 的干净检出。")
    endif()

    execute_process(
        COMMAND "${GIT_EXECUTABLE}" -C "${_dir}" rev-parse HEAD
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _head ERROR_VARIABLE _err
        OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR
            "[bbt-infra-pin] ${_dir} 不是可核验的 git 检出：${_err}。"
            "请指向 bbtools-coroutine 在 ${_expected} 的 git 检出"
            "（git clone + git checkout <sha>）。")
    endif()
    if(NOT _head STREQUAL _expected)
        message(FATAL_ERROR
            "[bbt-infra-pin] coroutine SHA 漂移拒绝："
            "实际 HEAD=${_head}，要求固定 ${_expected}（${_dir}）。"
            "消费标识是不可变 commit，不用分支/标签/新 main 代替。")
    endif()

    execute_process(
        COMMAND "${GIT_EXECUTABLE}" -C "${_dir}" diff --quiet HEAD --
        RESULT_VARIABLE _dirty OUTPUT_VARIABLE _out ERROR_VARIABLE _derr)
    if(NOT _dirty EQUAL 0)
        message(FATAL_ERROR
            "[bbt-infra-pin] coroutine 检出 tracked 内容不干净（${_dir}）："
            "git diff --quiet HEAD 非零（${_derr}），拒绝把已修改源码当固定版本消费。")
    endif()

    # tracked 干净之外，已暂存（cached）差异也属漂移。
    execute_process(
        COMMAND "${GIT_EXECUTABLE}" -C "${_dir}" diff --cached --quiet
        RESULT_VARIABLE _staged OUTPUT_VARIABLE _sout ERROR_VARIABLE _serr)
    if(NOT _staged EQUAL 0)
        message(FATAL_ERROR
            "[bbt-infra-pin] coroutine 检出存在已暂存差异（${_dir}），拒绝继续。")
    endif()

    # untracked 全量不阻塞（构建残留允许存在），但落在消费面内的 untracked
    # 源码输入——顶层 CMakeLists.txt、cmake/、bbt/ 下的新增条目——会改变实际
    # 消费内容，同样拒绝（干净检出之外混入的源码不是固定版本的一部分）。
    execute_process(
        COMMAND "${GIT_EXECUTABLE}" -C "${_dir}" status --porcelain --
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

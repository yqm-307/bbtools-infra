# 公共依赖段：普通模式与 NETWORK_ONLY 共用，一次定义，禁止两条路径各自接线。
#
# 统一提供：bbt_infra_common（旧公开头树 include/ 的 header-only 契约层，
# 公共头引用 bbt/coroutine 故必须随 coroutine 接口传播）、
# bbt_infra_internal_debug（STRINGENT_DEBUG 的目标级传播面）、
# pin 核验过的 bbt_coroutine 源码接入、AsyncMisuseErrors、Threads。
#
# 本文件被顶层 CMakeLists.txt include 一次；不自行 add_subdirectory 任何模块。

if(TARGET bbt_infra_common)
    message(FATAL_ERROR
        "cmake/Common.cmake 被重复 include：公共依赖只允许接入一次")
endif()

# --- 内部 debug 观测层开关（PR #70 review 整改）--------------------------------
# 与 bbtools-coroutine 的 STRINGENT_DEBUG（其 add_definitions）对齐，但修正两处
# review 指出的边界：
#   1) 配置边界：宏只在 **Debug 配置** 生效。用生成器表达式 $<CONFIG:Debug> 固化，
#      单/多配置生成器一致；Release（及任何非 Debug 配置）结构上不可能带上该宏
#      ⇒ 结构上零开销，不依赖使用者记得关 option，也不会「只开 option 就让 Release
#      带宏还宣称零开销」。option 只作为 Debug 下的显式开关。
#   2) 传播机制：用目标级 INTERFACE compile definition（经 bbt::infra_internal_debug
#      这一 INTERFACE 库）随链接接口传播，而不是目录级 add_compile_definitions
#      （后者是目录属性，不随 target/链接接口传播，无法保证外部 add_subdirectory
#      父级或链接消费者与库 TU 的宏取值一致 ⇒ 潜在 ODR/ABI 混配）。只有真正链接
#      该 INTERFACE 的模块库及其内部测试消费者拿到宏，不污染无关 target。
option(BBT_INFRA_STRINGENT_DEBUG
    "启用 bbtools-infra 内部 debug 观测层（仅 Debug 配置定义宏）" OFF)
add_library(bbt_infra_internal_debug INTERFACE)
add_library(bbt::infra_internal_debug ALIAS bbt_infra_internal_debug)
if(BBT_INFRA_STRINGENT_DEBUG)
    target_compile_definitions(bbt_infra_internal_debug INTERFACE
        $<$<CONFIG:Debug>:BBT_INFRA_STRINGENT_DEBUG>)
    message(STATUS
        "bbtools-infra: 内部 debug 观测层已启用（仅 Debug 配置定义 "
        "BBT_INFRA_STRINGENT_DEBUG）")
else()
    message(STATUS "bbtools-infra: 内部 debug 观测层已关闭（BBT_INFRA_STRINGENT_DEBUG=OFF）")
endif()

# 公共契约头库：header-only，向外暴露 include/（旧公开头树）。新协议模块
# （bbt/infra/{redis,http,rpc}）的公开头仍引用该树的 NetworkTypes/Result 等
# 值层类型，因此 common 不属于「旧栈」，普通模式与 NETWORK_ONLY 共用。
add_library(bbt_infra_common INTERFACE)
add_library(bbt::infra_common ALIAS bbt_infra_common)
target_include_directories(bbt_infra_common INTERFACE
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)
target_compile_features(bbt_infra_common INTERFACE cxx_std_17)

# --- bbtools-coroutine 源码接入 + pin（两条消费路径共用）-------------------------
# 上游不导出可消费的 CMake 包，源码消费只能经显式路径 add_subdirectory 接入。
# 禁止把本机私有绝对路径写进默认值或提交内容。
set(BBT_COROUTINE_SOURCE_DIR "" CACHE PATH
    "bbtools-coroutine 源码树路径（含顶层 CMakeLists.txt 与 bbt/；固定 commit 见 cmake/coroutine.sha）")
if(NOT BBT_COROUTINE_SOURCE_DIR)
    message(FATAL_ERROR
        "需要显式 -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树路径>；"
        "公共头与全部模块都消费 coroutine，缺失时明确拒绝而非假成功。")
endif()

# 以源码方式接入一个未导出包的上游。上游 cmake_minimum_required(≥3.16 现版本，
# 仍保留兼容旧版的 CMakeLists 历史形态) 默认会把自己的单测/示例注册进来；
# 这里统一收敛：临时遮蔽上游开关、临时给出 CMAKE_POLICY_VERSION_MINIMUM，并把
# 上游源码根补成目标级接口，使消费经正式 target 传播而不是全局 -I。
function(bbt_add_source_dependency _target _src_dir _bin_subdir)
    if(NOT EXISTS "${_src_dir}/CMakeLists.txt")
        message(FATAL_ERROR
            "源码依赖 '${_src_dir}' 无效：未找到顶层 CMakeLists.txt")
    endif()

    # ARGN 是上游默认 ON 的开关（NEED_TEST/NEED_EXAMPLE 等）：不遮蔽就会把上游
    # 单测/示例注册进本仓 ctest。还原必须基于 CACHE 原值——调用方可能用
    # -DNEED_TEST=ON 传入 CACHE，若读作用域内变量，会把调用方的开关静默改成 OFF。
    set(_shadowed "")
    set(_saved_names "")
    set(_saved_values "")
    foreach(_opt IN LISTS ARGN)
        if(DEFINED CACHE{${_opt}})
            list(APPEND _saved_names "${_opt}")
            list(APPEND _saved_values "$CACHE{${_opt}}")
        endif()
        list(APPEND _shadowed "${_opt}")
        set(${_opt} OFF CACHE BOOL "temporarily shadowed for source dependency" FORCE)
    endforeach()

    # 上游 cmake_minimum_required 可能 <3.5：CMake ≥4 已移除兼容，configure 直接
    # 报错。按官方提示只在依赖作用域内临时给出 CMAKE_POLICY_VERSION_MINIMUM
    # （函数作用域，返回即自动还原）。
    set(CMAKE_POLICY_VERSION_MINIMUM 3.5)
    add_subdirectory("${_src_dir}"
                     "${CMAKE_BINARY_DIR}/_deps/${_bin_subdir}"
                     EXCLUDE_FROM_ALL)

    # 上游目录级 include_directories 不导出头路径：把上游源码根补成目标级接口，
    # 链接该 target 的消费者经 use requirements 拿到头，而不是靠全局 -I。
    # SYSTEM 使上游头告警不计入本仓告警面；BUILD_INTERFACE 使其不进安装接口。
    target_include_directories(${_target} SYSTEM INTERFACE
        "$<BUILD_INTERFACE:${_src_dir}>")

    foreach(_opt IN LISTS _shadowed)
        list(FIND _saved_names "${_opt}" _saved_index)
        if(_saved_index GREATER -1)
            list(GET _saved_values ${_saved_index} _saved_value)
            set(${_opt} "${_saved_value}" CACHE BOOL
                "上游 option 语义；对本仓无作用" FORCE)
        else()
            unset(${_opt} CACHE)
        endif()
    endforeach()
endfunction()

# T1 pin：固定 commit 由 cmake/coroutine.sha 唯一提供（include 时读取并校验
# 40hex 格式）；git-only 核验对本模块所有调用方可见。
include("${CMAKE_CURRENT_LIST_DIR}/CoroutinePin.cmake")
get_filename_component(_bbt_co_expected "${BBT_COROUTINE_SOURCE_DIR}" REALPATH)

# 复用分支：pin 声明目录之前先核已建 target 的真实 SOURCE_DIR——声明目录
# 核验过了不代表实际链接的是同一棵树（coroutine-first 父级已接入情形）。
bbt_coroutine_verify_existing_target("${_bbt_co_expected}")

# EXISTS 校验移到 verify_existing_target 之后——复用分支的负向 marker
# （声明目录与真实 SOURCE_DIR 不一致）必须先触发 verify_existing_target 的
# FATAL_ERROR，而不是被 EXISTS 抢先拒绝。
if(NOT EXISTS "${BBT_COROUTINE_SOURCE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR
        "BBT_COROUTINE_SOURCE_DIR='${BBT_COROUTINE_SOURCE_DIR}' 无效："
        "未找到顶层 CMakeLists.txt")
endif()

# 源码消费的声明目录必须先命中固定 commit
# （git HEAD+tracked/staged 干净+消费面无 untracked 源码），再做接入。
bbt_coroutine_pin_verify("${_bbt_co_expected}")
unset(_bbt_co_expected)

if(NOT TARGET bbt_coroutine)
    bbt_add_source_dependency(bbt_coroutine "${BBT_COROUTINE_SOURCE_DIR}"
        bbtools-coroutine NEED_TEST NEED_EXAMPLE NEED_BENCHMARK NEED_DEBUG)
else()
    # 复用分支：coroutine 已由父级接入，但 include interface 不一定带上源码根。
    # 与新建分支统一补齐，保证 consumer 经 use requirements 拿到头。
    target_include_directories(bbt_coroutine SYSTEM INTERFACE
        "$<BUILD_INTERFACE:${BBT_COROUTINE_SOURCE_DIR}>")
endif()

# 公共头引用 bbt/coroutine：coroutine 经 common 的链接接口传播给所有模块消费者。
target_link_libraries(bbt_infra_common INTERFACE bbt_coroutine)

# bbt_co_async/bbt_co_await 误用兜底升级为错误；必须在 coroutine 接入之后
# include（directory 级 COMPILE_OPTIONS 只作用于之后创建的目标）。
include("${CMAKE_CURRENT_LIST_DIR}/AsyncMisuseErrors.cmake")

find_package(Threads REQUIRED)

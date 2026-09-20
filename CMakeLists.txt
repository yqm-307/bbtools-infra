cmake_minimum_required(VERSION 3.16)
project(bbtools-infra LANGUAGES CXX)

# Boost>=1.70 自带 BoostConfig.cmake；明确跳过已废弃的 FindBoost 模块，
# 消除 CMP0167 dev 警告（新版 CMake 上 find_package(Boost) 否则告警）。
if(POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 公共契约头库：header-only，向外暴露 include/
add_library(bbt_infra_common INTERFACE)
add_library(bbt::infra_common ALIAS bbt_infra_common)
target_include_directories(bbt_infra_common INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)
target_compile_features(bbt_infra_common INTERFACE cxx_std_17)

# --- 上游源码依赖接入：bbtools-coroutine / bbtools-core -----------------------
# 两个上游仓当前都不导出可消费的 CMake 包（core 没有 install/export 规则），源码
# 消费只能经显式路径 add_subdirectory 接入，不能依赖机器上的隐式安装产物。正式接入
# 待上游提供固定 SHA 的消费方式后收敛。禁止把本机私有绝对路径写进默认值或提交内容。
set(BBT_COROUTINE_SOURCE_DIR "" CACHE PATH
    "本机/候选分支开发用：bbtools-coroutine 源码树路径（含顶层 CMakeLists.txt 与 bbt/）")
set(BBT_CORE_SOURCE_DIR "" CACHE PATH
    "源码消费用：bbtools-core 源码树路径（含顶层 CMakeLists.txt 与 bbt/core/）")

# 以源码方式接入一个未导出包的上游。上游用目录级 include_directories（不导出 use
# requirements）、cmake_minimum_required(<3.5)、且默认会把自己的单测/示例注册进来；
# 这里统一收敛：临时遮蔽上游开关、临时给出 CMAKE_POLICY_VERSION_MINIMUM，并把上游
# 源码根补成目标级接口，使消费经正式 target 传播而不是全局编译/链接 flags。
function(bbt_add_source_dependency _target _src_dir _bin_subdir)
    if(NOT EXISTS "${_src_dir}/CMakeLists.txt")
        message(FATAL_ERROR
            "源码依赖 '${_src_dir}' 无效：未找到顶层 CMakeLists.txt")
    endif()

    # ARGN 是上游默认 ON 的开关（NEED_TEST/ENABLE_EXAMPLE 等）：不遮蔽就会把上游
    # 单测/示例注册进本仓 ctest（其可执行目标并不在本构建内编译）。上游
    # cmake_minimum_required(3.1/3.2) 使其作用域内 CMP0077=OLD：option() 不看普通
    # 变量，必须临时改 CACHE 再还原；还原必须基于 CACHE 原值——调用方（例如
    # bbt-framework）可能用 -DNEED_TEST=ON 传入 CACHE，若读作用域内变量，会把调用
    # 方的开关静默改成 OFF。
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

    # 上游 cmake_minimum_required(3.1/3.2)：CMake ≥4 已移除对 <3.5 的兼容，configure
    # 直接报错。按 CMake 官方提示只在依赖作用域内临时给出 CMAKE_POLICY_VERSION_MINIMUM
    # （函数作用域，返回即自动还原；3.5 远小于 CMP0077 的 3.13，上游 option() 的 OLD
    # 语义不变，上面的遮蔽仍然必要）。
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

if(BBT_COROUTINE_SOURCE_DIR)
    if(NOT EXISTS "${BBT_COROUTINE_SOURCE_DIR}/CMakeLists.txt")
        message(FATAL_ERROR
            "BBT_COROUTINE_SOURCE_DIR='${BBT_COROUTINE_SOURCE_DIR}' 无效："
            "未找到顶层 CMakeLists.txt")
    endif()

    # core 必须在 coroutine 之前接入：coroutine 顶层直接
    # target_link_libraries(bbt_coroutine ... bbt_core)，此刻 bbt_core 若还不是
    # target，CMake 只会记下裸库名 -lbbt_core（隐式依赖机器上的安装产物），之后再
    # 定义同名 target 也不会把它升级成正式依赖。
    if(NOT BBT_CORE_SOURCE_DIR)
        # 未显式给出时按「多仓并排检出」布局在上游/本仓同级目录探测，且必须同时
        # 具备顶层 CMakeLists.txt 与 bbt/core/，避免绑定到同名空目录或错误源码树。
        foreach(_anchor "${BBT_COROUTINE_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}")
            get_filename_component(_sibling "${_anchor}/../bbtools-core" ABSOLUTE)
            if(EXISTS "${_sibling}/CMakeLists.txt" AND IS_DIRECTORY "${_sibling}/bbt/core")
                set(BBT_CORE_SOURCE_DIR "${_sibling}")
                break()
            endif()
        endforeach()
    endif()
    if(BBT_CORE_SOURCE_DIR AND NOT TARGET bbt_core)
        bbt_add_source_dependency(bbt_core "${BBT_CORE_SOURCE_DIR}" bbtools-core
            NEED_TEST ENABLE_EXAMPLE)
        message(STATUS
            "bbtools-infra: bbtools-core 源码接入 '${BBT_CORE_SOURCE_DIR}'")
    endif()

    bbt_add_source_dependency(bbt_coroutine "${BBT_COROUTINE_SOURCE_DIR}"
        bbtools-coroutine NEED_TEST)
    target_link_libraries(bbt_infra_common INTERFACE bbt_coroutine)

    # 契约校验：coroutine 仍按核心库名消费 core（拆分前形态）时，本构建必须已有
    # 真实 bbt_core target，否则链接期会退化成机器上的隐式 -lbbt_core 与隐式
    # /usr/local 头文件——本仓不做静默兜底。coroutine 拆分自闭环后不再引用
    # bbt_core，此处自动放行。
    if(NOT TARGET bbt_core)
        file(READ "${BBT_COROUTINE_SOURCE_DIR}/CMakeLists.txt" _bbt_co_cmake)
        if(_bbt_co_cmake MATCHES "(^|[^_A-Za-z0-9])bbt_core([^_A-Za-z0-9]|$)")
            message(FATAL_ERROR
                "bbtools-coroutine('${BBT_COROUTINE_SOURCE_DIR}') 依赖 bbtools-core，"
                "但本构建未提供 bbt_core target。bbtools-core 当前没有 install/export "
                "规则，不能经 find_package 获得；请显式传 "
                "-DBBT_CORE_SOURCE_DIR=<bbtools-core 源码树路径>。")
        endif()
    endif()
else()
    # 回退：查找已安装的 bbt_coroutine 包（导出 bbt_coroutine 目标）。
    find_package(bbt_coroutine CONFIG QUIET)
    if(NOT TARGET bbt_coroutine)
        message(FATAL_ERROR
            "未找到 bbt_coroutine。本机/候选分支开发请显式传 "
            "-DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树路径>；"
            "已安装包需提供 bbt_coroutineConfig.cmake 并导出 bbt_coroutine 目标。")
    endif()
    target_link_libraries(bbt_infra_common INTERFACE bbt_coroutine)
endif()

add_subdirectory(src)

# 每个公共头单独编译 TU，验证头文件自给
set(BBT_INFRA_PUBLIC_HEADERS
    Result.hpp
    ICoObject.hpp
    ICoNetwork.hpp
    ICoCloseable.hpp
    NetworkTypes.hpp
    Codec.hpp
    HttpClient.hpp
    HttpServer.hpp
    NetworkRuntime.hpp)
set(_selfcheck_dir "${CMAKE_CURRENT_BINARY_DIR}/header_selfcheck")
file(MAKE_DIRECTORY "${_selfcheck_dir}")
set(_selfcheck_srcs "")
foreach(_hdr IN LISTS BBT_INFRA_PUBLIC_HEADERS)
    get_filename_component(_name "${_hdr}" NAME_WE)
    set(_tu "${_selfcheck_dir}/${_name}.cc")
    file(WRITE "${_tu}" "#include <bbt/infra/${_hdr}>\n")
    list(APPEND _selfcheck_srcs "${_tu}")
endforeach()
add_library(bbt_infra_header_selfcheck STATIC ${_selfcheck_srcs})
target_link_libraries(bbt_infra_header_selfcheck PRIVATE bbt::infra_common)
target_compile_options(bbt_infra_header_selfcheck PRIVATE -Wall -Wextra)
set_target_properties(bbt_infra_header_selfcheck PROPERTIES LINKER_LANGUAGE CXX)

include(CTest)
if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

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

# --- bbt_coroutine 依赖接入 --------------------------------------------------
# 正式接入待上游可读后收敛为固定 SHA 的消费方式；当前仅允许本机/候选分支
# 经显式路径覆盖接入。禁止把本机私有绝对路径写进默认值或任何提交内容。
set(BBT_COROUTINE_SOURCE_DIR "" CACHE PATH
    "本机/候选分支开发用：bbtools-coroutine 源码树路径（含顶层 CMakeLists.txt 与 bbt/）")
if(BBT_COROUTINE_SOURCE_DIR)
    if(NOT EXISTS "${BBT_COROUTINE_SOURCE_DIR}/CMakeLists.txt")
        message(FATAL_ERROR
            "BBT_COROUTINE_SOURCE_DIR='${BBT_COROUTINE_SOURCE_DIR}' 无效："
            "未找到顶层 CMakeLists.txt")
    endif()
    # NEED_TEST 只语义于本仓。上游 cmake_minimum_required(3.1) 使其作用域
    # 内 CMP0077=OLD：option() 不看普通变量，必须临时改 CACHE 再还原，
    # 避免上游把自身单测注册进本仓 ctest（其可执行目标不在本构建内编译）。
    # 还原必须基于 CACHE 原值：调用方（例如 bbt-framework）可能在自身作用域
    # 用普通变量遮蔽 NEED_TEST，若读作用域内变量，会把调用方的 -DNEED_TEST=ON
    # 还原成被遮蔽的 OFF，静默改掉别人的开关。
    if(DEFINED CACHE{NEED_TEST})
        set(_bbt_had_need_test_cache TRUE)
        set(_bbt_saved_need_test "$CACHE{NEED_TEST}")
    else()
        set(_bbt_had_need_test_cache FALSE)
    endif()
    set(NEED_TEST OFF CACHE BOOL "temporarily shadowed for dependency" FORCE)
    add_subdirectory("${BBT_COROUTINE_SOURCE_DIR}"
                     "${CMAKE_BINARY_DIR}/_deps/bbtools-coroutine"
                     EXCLUDE_FROM_ALL)
    if(_bbt_had_need_test_cache)
        set(NEED_TEST "${_bbt_saved_need_test}" CACHE BOOL
            "编译unit test文件（上游 option 语义；对本仓无作用）" FORCE)
        unset(_bbt_saved_need_test)
    else()
        unset(NEED_TEST CACHE)
    endif()
    unset(_bbt_had_need_test_cache)
    # 上游用目录级 include_directories 不导出 include 路径，这里显式补；
    # SYSTEM 使上游头告警不计入本仓告警面。仅构建期有效，不进安装接口。
    target_include_directories(bbt_infra_common SYSTEM INTERFACE
        "$<BUILD_INTERFACE:${BBT_COROUTINE_SOURCE_DIR}>")
    target_link_libraries(bbt_infra_common INTERFACE bbt_coroutine)
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

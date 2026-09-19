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
# --- bbt_core 依赖接入：coroutine 目标需链接 bbt_core 并包含其头 -------------
# 与 coroutine 同规约：固定源码树经显式路径接入；父工程已提供 bbt_core
# target 时复用，不重复添加。
set(BBT_CORE_SOURCE_DIR "" CACHE PATH
    "本机/候选分支开发用：bbtools-core 源码树路径（含顶层 CMakeLists.txt 与 bbt/）")
if(BBT_COROUTINE_SOURCE_DIR)
    if(NOT EXISTS "${BBT_COROUTINE_SOURCE_DIR}/CMakeLists.txt")
        message(FATAL_ERROR
            "BBT_COROUTINE_SOURCE_DIR='${BBT_COROUTINE_SOURCE_DIR}' 无效："
            "未找到顶层 CMakeLists.txt")
    endif()
    if(NOT TARGET bbt_core)
        if(BBT_CORE_SOURCE_DIR STREQUAL "")
            message(FATAL_ERROR
                "以源码接入 coroutine 时需要 bbt_core target：请传 "
                "-DBBT_CORE_SOURCE_DIR=<bbtools-core 源码树路径>，"
                "或由父工程预先提供 bbt_core target。")
        endif()
        if(NOT EXISTS "${BBT_CORE_SOURCE_DIR}/CMakeLists.txt")
            message(FATAL_ERROR
                "BBT_CORE_SOURCE_DIR='${BBT_CORE_SOURCE_DIR}' 无效："
                "未找到顶层 CMakeLists.txt")
        endif()
        # 与 coroutine 同款遮蔽：上游 option() 在 CMP0077=OLD 下不看普通
        # 变量，必须临时改 CACHE 再还原，防止上游单测/例程混入本仓构建。
        foreach(_opt NEED_TEST ENABLE_EXAMPLE)
            if(DEFINED CACHE{${_opt}})
                set(_bbt_had_${_opt} TRUE)
                set(_bbt_saved_${_opt} "$CACHE{${_opt}}")
            else()
                set(_bbt_had_${_opt} FALSE)
            endif()
            set(${_opt} OFF CACHE BOOL "temporarily shadowed for dependency" FORCE)
        endforeach()
        add_subdirectory("${BBT_CORE_SOURCE_DIR}"
                         "${CMAKE_BINARY_DIR}/_deps/bbtools-core"
                         EXCLUDE_FROM_ALL)
        foreach(_opt NEED_TEST ENABLE_EXAMPLE)
            if(_bbt_had_${_opt})
                set(${_opt} "${_bbt_saved_${_opt}}" CACHE BOOL
                    "restored after dependency add_subdirectory" FORCE)
            else()
                unset(${_opt} CACHE)
            endif()
        endforeach()
        # core 用目录级 include_directories 不导出 include 路径；
        # SYSTEM 使上游头告警不计入本仓告警面。
        target_include_directories(bbt_infra_common SYSTEM INTERFACE
            "$<BUILD_INTERFACE:${BBT_CORE_SOURCE_DIR}>")
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
    # coroutine 编译自身源码需要 core 头（其 CMake 只做自身目录级
    # include_directories，不导出 core 路径）；SYSTEM 避免上游头告警
    # 计入本仓告警面。
    if(TARGET bbt_core AND BBT_CORE_SOURCE_DIR)
        target_include_directories(bbt_coroutine SYSTEM PUBLIC
            "$<BUILD_INTERFACE:${BBT_CORE_SOURCE_DIR}>")
    endif()
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

# --- mongo-cxx-driver 依赖接入（Issue #7，Mongo 模块）-----------------------
# 与 coroutine 同规约：仅允许经显式前缀接入，禁止把本机私有绝对路径
# 写进默认值或任何提交内容。src/CMakeLists.txt 内 find_package 限定
# NO_DEFAULT_PATH，杜绝误链系统位置 mongocxx/libmongoc；mongocxx 的
# package config 依赖 mongoc/bsoncxx 包，C driver 前缀经独立变量限定。
# 未设置时跳过 Mongo 模块。
set(BBT_MONGOCXX_PREFIX "" CACHE PATH
    "mongo-cxx-driver 安装前缀（含 include/mongocxx 与 lib/cmake/mongocxx）")
set(BBT_MONGOC_PREFIX "" CACHE PATH
    "mongo-c-driver 安装前缀（含 include/mongoc 与 lib/cmake/mongoc）；mongocxxConfig.cmake 内 find_dependency(mongoc) 需要")

# find_package 放在顶层作用域：包导出的 mongo::mongocxx_shared 等
# IMPORTED target 只有目录级可见性，src/ 与 tests/ 都需要引用。
# mongocxxConfig.cmake 内 find_dependency(mongoc/bsoncxx) 的嵌套
# find_package 经 <pkg>_DIR 钉到私有前缀唯一版本目录（NO_DEFAULT_PATH
# 会传播给嵌套调用使 CMAKE_PREFIX_PATH 失效，故不用 CMAKE_PREFIX_PATH）。
# 成对门禁：两个前缀必须同时给或同时不给——只给 mongocxx 前缀会让
# find_dependency(mongoc) 回落默认搜索路径，可能误链系统 libmongoc。
if(BBT_MONGOCXX_PREFIX OR BBT_MONGOC_PREFIX)
    if(NOT BBT_MONGOCXX_PREFIX OR NOT BBT_MONGOC_PREFIX)
        message(FATAL_ERROR
            "BBT_MONGOCXX_PREFIX 与 BBT_MONGOC_PREFIX 必须成对设置："
            "mongo-cxx-driver 的 package config 依赖 mongo-c-driver 前缀；"
            "只给一个会导致 find_dependency 回落系统路径。")
    endif()
endif()
if(BBT_MONGOCXX_PREFIX)
    if(BBT_MONGOC_PREFIX)
        file(GLOB mongoc_DIR "${BBT_MONGOC_PREFIX}/lib/cmake/mongoc-*")
        file(GLOB bson_DIR   "${BBT_MONGOC_PREFIX}/lib/cmake/bson-*")
        if(NOT IS_DIRECTORY "${mongoc_DIR}" OR
           NOT IS_DIRECTORY "${bson_DIR}")
            message(FATAL_ERROR
                "BBT_MONGOC_PREFIX='${BBT_MONGOC_PREFIX}' 无效：未找到 "
                "lib/cmake/mongoc-* 或 lib/cmake/bson-*")
        endif()
    endif()
    file(GLOB bsoncxx_DIR "${BBT_MONGOCXX_PREFIX}/lib/cmake/bsoncxx-*")
    if(NOT IS_DIRECTORY "${bsoncxx_DIR}")
        message(FATAL_ERROR
            "BBT_MONGOCXX_PREFIX='${BBT_MONGOCXX_PREFIX}' 无效：未找到 "
            "lib/cmake/bsoncxx-*")
    endif()
    find_package(mongocxx CONFIG REQUIRED
        PATHS "${BBT_MONGOCXX_PREFIX}" NO_DEFAULT_PATH)
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
    NetworkRuntime.hpp
    CoMongoCli.hpp)
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

cmake_minimum_required(VERSION 3.16)
project(infra_perf_baseline_consumer LANGUAGES CXX)

# infra-perf 闭环：baseline（B0=c35a2a25…）专用外部 consumer。
#
# 与 candidate tests/net/perf 用**同一份 harness 源**编译基准目标，但消费的是
# B0 已构建产物（bbt_infra_net 静态库 + 公共头），经显式变量定位——baseline
# 产品树只参与构建，不修改、不安装到系统路径；coroutine 只经
# BBT_COROUTINE_SOURCE_DIR（固定 U，与 candidate 同一份 pin）接入。
#
# 用途：给 verifier 提供「同一 harness 测 B0 与 candidate」的配对入口；本目录
# 不进任何父构建，独立 cmake -S/-B 使用。
#
# 必要输入（全部显式，无默认探测）：
#   BBT_BASELINE_BUILD_DIR   B0 已构建 build 目录（含 libbbt_infra_net.a）
#   BBT_BASELINE_SOURCE_DIR  B0 源码树（公共头 bbt/infra/net/*.hpp 在其内）
#   BBT_COROUTINE_SOURCE_DIR 固定 coroutine 源码树
#   BBT_PERF_HARNESS_SRC     candidate tests/net/perf 源目录
# 可选：BBT_PERF_SMOKE=ON 注册冒烟 ctest。

if(POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif()
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(BBT_BASELINE_BUILD_DIR "" CACHE PATH "B0 已构建 build 目录")
set(BBT_BASELINE_SOURCE_DIR "" CACHE PATH "B0 源码树（公共头所在仓根）")
set(BBT_COROUTINE_SOURCE_DIR "" CACHE PATH "固定 coroutine 源码树")
set(BBT_PERF_HARNESS_SRC "" CACHE PATH "candidate tests/net/perf 源目录")
option(BBT_PERF_SMOKE "注册冒烟 ctest" OFF)

foreach(_var BBT_BASELINE_BUILD_DIR BBT_BASELINE_SOURCE_DIR
            BBT_COROUTINE_SOURCE_DIR BBT_PERF_HARNESS_SRC)
    if(NOT DEFINED ${_var} OR "${${_var}}" STREQUAL "" OR NOT EXISTS "${${_var}}")
        message(FATAL_ERROR "${_var} 必须指向存在的目录（当前：'${${_var}}'）")
    endif()
endforeach()
if(NOT EXISTS "${BBT_COROUTINE_SOURCE_DIR}/bbt/coroutine/coroutine.hpp")
    message(FATAL_ERROR "BBT_COROUTINE_SOURCE_DIR 缺少 coroutine.hpp")
endif()
if(NOT EXISTS "${BBT_PERF_HARNESS_SRC}/tcp_echo_server.cc")
    message(FATAL_ERROR "BBT_PERF_HARNESS_SRC 缺少 tcp_echo_server.cc")
endif()
if(NOT EXISTS "${BBT_BASELINE_SOURCE_DIR}/bbt/infra/net/Engine.hpp")
    message(FATAL_ERROR "BBT_BASELINE_SOURCE_DIR 缺少公共头 bbt/infra/net/Engine.hpp")
endif()
set(_b0_lib "${BBT_BASELINE_BUILD_DIR}/bbt/infra/net/libbbt_infra_net.a")
if(NOT EXISTS "${_b0_lib}")
    message(FATAL_ERROR "B0 已构建产物缺失：${_b0_lib}（先在 B0 build 里编 bbt_infra_net）")
endif()

# 固定消费必须在本次 configure 重新核验，不能继承别的构建的旧声明。
include("${BBT_PERF_HARNESS_SRC}/../../../cmake/CoroutinePin.cmake")
bbt_coroutine_verify_existing_target("${BBT_COROUTINE_SOURCE_DIR}")
bbt_coroutine_pin_verify("${BBT_COROUTINE_SOURCE_DIR}")

# ---- 固定 coroutine（与 NetworkOnly 同一遮蔽策略） ----
set(NEED_TEST OFF CACHE BOOL "" FORCE)
set(NEED_EXAMPLE OFF CACHE BOOL "" FORCE)
set(NEED_BENCHMARK OFF CACHE BOOL "" FORCE)
set(NEED_DEBUG OFF CACHE BOOL "" FORCE)
if(NOT TARGET bbt_coroutine)
    add_subdirectory("${BBT_COROUTINE_SOURCE_DIR}"
        "${CMAKE_CURRENT_BINARY_DIR}/_deps/bbtools-coroutine"
        EXCLUDE_FROM_ALL)
endif()
target_include_directories(bbt_coroutine SYSTEM INTERFACE
    "$<BUILD_INTERFACE:${BBT_COROUTINE_SOURCE_DIR}>")

# ---- B0 产物 imported target：静态库 + 公共头路径；链接闭包随 use-req 传递 --
add_library(bbt_infra_net_baseline STATIC IMPORTED)
set_target_properties(bbt_infra_net_baseline PROPERTIES
    IMPORTED_LOCATION "${_b0_lib}"
    INTERFACE_INCLUDE_DIRECTORIES "${BBT_BASELINE_SOURCE_DIR}"
    INTERFACE_LINK_LIBRARIES "bbt_coroutine;Boost::headers")

find_package(Boost 1.90 REQUIRED)
find_package(Threads REQUIRED)

# ---- 同一 harness 源，对 B0 构建（源内引用 <bbt/infra/net/...> 公共头） ----
add_executable(bbt_net_tcp_echo_server_b0 "${BBT_PERF_HARNESS_SRC}/tcp_echo_server.cc")
target_include_directories(bbt_net_tcp_echo_server_b0 PRIVATE
    "${BBT_PERF_HARNESS_SRC}")
target_link_libraries(bbt_net_tcp_echo_server_b0 PRIVATE
    bbt_infra_net_baseline Boost::headers)
target_compile_options(bbt_net_tcp_echo_server_b0 PRIVATE
    -Wall -Wextra -Wpedantic)

add_executable(bbt_net_tcp_loadgen_b0 "${BBT_PERF_HARNESS_SRC}/tcp_loadgen.cc")
target_include_directories(bbt_net_tcp_loadgen_b0 PRIVATE
    "${BBT_PERF_HARNESS_SRC}")
target_compile_options(bbt_net_tcp_loadgen_b0 PRIVATE
    -Wall -Wextra -Wpedantic)
target_link_libraries(bbt_net_tcp_loadgen_b0 PRIVATE Threads::Threads)

if(BBT_PERF_SMOKE)
    find_package(Python3 COMPONENTS Interpreter REQUIRED)
    enable_testing()
    foreach(_mode raw co)
        add_test(NAME b0.perf_smoke_${_mode}
            COMMAND ${Python3_EXECUTABLE}
                "${BBT_PERF_HARNESS_SRC}/../../../scripts/bench/driver.py"
                --mode ${_mode} --workload pingpong
                --server-bin "$<TARGET_FILE:bbt_net_tcp_echo_server_b0>"
                --loadgen-bin "$<TARGET_FILE:bbt_net_tcp_loadgen_b0>"
                --clients 2 --payload-bytes 64
                --duration-ms 800 --warmup-ms 100 --timeout-ms 15000)
        set_tests_properties(b0.perf_smoke_${_mode} PROPERTIES TIMEOUT 60)
    endforeach()
endif()

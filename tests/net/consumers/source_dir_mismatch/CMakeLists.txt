cmake_minimum_required(VERSION 3.16)
project(net_consumer_source_dir_mismatch LANGUAGES CXX)
# 负向 fixture：bbt_coroutine target 已由真实上游创建，但声明的
# BBT_COROUTINE_SOURCE_DIR 指向只有占位头的另一棵树；NetworkOnly 必须核真实
# SOURCE_DIR 并明确拒绝（见 expect_configure_failure.cmake 的标记断言）。
set(NET_CONSUMER_COROUTINE_DIR "" CACHE PATH "真实 coroutine 源码树")
set(NET_CONSUMER_INFRA_DIR "" CACHE PATH "bbtools-infra 候选源码树")
set(NET_CONSUMER_STUB_DIR "" CACHE PATH "故意错误的声明来源")
if(NOT EXISTS "${NET_CONSUMER_COROUTINE_DIR}/bbt/coroutine/coroutine.hpp"
    OR NOT EXISTS "${NET_CONSUMER_INFRA_DIR}/bbt/infra/net/NativeSocket.hpp"
    OR NOT EXISTS "${NET_CONSUMER_STUB_DIR}/bbt/coroutine/coroutine.hpp")
    message(FATAL_ERROR "mismatch fixture 需要显式三个目录")
endif()
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(NEED_TEST OFF CACHE BOOL "" FORCE)
set(NEED_EXAMPLE OFF CACHE BOOL "" FORCE)
set(NEED_BENCHMARK OFF CACHE BOOL "" FORCE)
set(NEED_DEBUG OFF CACHE BOOL "" FORCE)
add_subdirectory("${NET_CONSUMER_COROUTINE_DIR}" "${CMAKE_BINARY_DIR}/_co" EXCLUDE_FROM_ALL)
set(BBT_COROUTINE_SOURCE_DIR "${NET_CONSUMER_STUB_DIR}" CACHE PATH "故意错误的声明" FORCE)
set(BBT_INFRA_NETWORK_ONLY ON CACHE BOOL "new net only" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "consumer only" FORCE)
add_subdirectory("${NET_CONSUMER_INFRA_DIR}" "${CMAKE_BINARY_DIR}/_infra")
message(FATAL_ERROR "mismatch fixture 不应走到这里：NetworkOnly 未拒绝不一致来源")

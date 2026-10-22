cmake_minimum_required(VERSION 3.16)
project(pin_reuse_probe LANGUAGES CXX)
# 复用分支 probe（普通消费路径的同款检查）：先用 add_subdirectory 把
# bbt_coroutine 建出来（ACTUAL_COROUTINE_DIR 指向一棵真实 coroutine 树或
# 只有占位头的 stub 源树——stub 由 pin_reuse_stub/ 提供，顶层 CMakeLists
# 只 add_library(bbt_coroutine)，不编译真实上游），随后按 CMakeLists.txt
# 普通路径顺序：bbt_coroutine_verify_existing_target（声明目录复核）→
# bbt_coroutine_pin_verify（声明目录 pin）。
#   mismatch：ACTUAL=REAL pin 树，DECLARED=stub → verify_existing_target 必须
#             以「来源…不一致」拒绝；
#   ok      ：ACTUAL=DECLARED=REAL pin 树 → 复核放行且 pin 命中（输出含标记）。
set(REAL_COROUTINE_DIR "" CACHE PATH "已建 target 的真实来源（add_subdirectory）")
set(STUB_COROUTINE_DIR "" CACHE PATH "BBT_COROUTINE_SOURCE_DIR 声明来源")
set(PIN_MODULE "" CACHE PATH "CoroutinePin.cmake 路径")
set(PROBE_KIND "" CACHE STRING "mismatch|ok")
if(NOT IS_DIRECTORY "${REAL_COROUTINE_DIR}" OR NOT EXISTS "${PIN_MODULE}")
    message(FATAL_ERROR "pin_reuse probe 需要 REAL_COROUTINE_DIR + PIN_MODULE")
endif()
set(CMAKE_POLICY_VERSION_MINIMUM 3.5)
set(NEED_TEST OFF CACHE BOOL "" FORCE)
set(NEED_EXAMPLE OFF CACHE BOOL "" FORCE)
set(NEED_BENCHMARK OFF CACHE BOOL "" FORCE)
set(NEED_DEBUG OFF CACHE BOOL "" FORCE)
add_subdirectory("${REAL_COROUTINE_DIR}" "${CMAKE_BINARY_DIR}/_co" EXCLUDE_FROM_ALL)
if(NOT TARGET bbt_coroutine)
    message(FATAL_ERROR "fixture 失效：add_subdirectory 未产生 bbt_coroutine")
endif()
include("${PIN_MODULE}")
get_filename_component(_expected "${STUB_COROUTINE_DIR}" REALPATH)
bbt_coroutine_verify_existing_target("${_expected}")
bbt_coroutine_pin_verify("${_expected}")
message(STATUS "pin_reuse probe 完成（${PROBE_KIND}）")

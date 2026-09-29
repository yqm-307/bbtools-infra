cmake_minimum_required(VERSION 3.16)
project(bbt_infra_transport_consumer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 独立消费验证（Issue #31）：bbtools-infra 当前无 install/export，consumer 以源码
# 方式接入整个 infra 树，但只链接 transport 模块 target。证明「受管 TCP/UDP 消费者
# 无需 HTTP」在仓外（独立 CMake 工程、独立构建目录）同样成立，且公共头不泄漏
# src 内部装配面。
#
# 本目录是**独立 CMake 工程**，不是本仓构建的一部分（不被任何 CMakeLists 引用）。
# 用法见同目录 README.md。
set(BBT_INFRA_SOURCE_DIR "" CACHE PATH "bbtools-infra 源码树（含顶层 CMakeLists.txt）")
if(NOT EXISTS "${BBT_INFRA_SOURCE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR "BBT_INFRA_SOURCE_DIR 无效：未找到顶层 CMakeLists.txt")
endif()

# 不注册 infra 自身单测；只消费库 target。
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
add_subdirectory("${BBT_INFRA_SOURCE_DIR}" _bbt_infra EXCLUDE_FROM_ALL)

add_executable(transport_consumer main.cc)
target_link_libraries(transport_consumer PRIVATE bbt::infra_transport)
target_compile_options(transport_consumer PRIVATE -Wall -Wextra)

# 消费面闭包门禁：只链接 transport 的消费者不得被传递 HTTP target。
get_target_property(_consumer_links transport_consumer LINK_LIBRARIES)
if(_consumer_links MATCHES "infra_http")
    message(FATAL_ERROR
        "依赖方向破坏：transport-only consumer 不得链接 HTTP（实际: ${_consumer_links}）")
endif()

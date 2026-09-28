cmake_minimum_required(VERSION 3.16)
project(bbt_infra_config_consumer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 独立消费验证（Issue #35）：bbtools-infra 当前无 install/export，consumer 以源码
# 方式接入整个 infra 树，但只链接 config 模块 target。公共面只经 bbt::infra_config
# 传播，不泄漏 framework/第三方类型。
#
# 本目录是**独立 CMake 工程**，不是本仓构建的一部分（不被任何 CMakeLists 引用），
# 用于从仓外视角复核模块 target 的可独立消费性。用法见同目录 README.md。
set(BBT_INFRA_SOURCE_DIR "" CACHE PATH "bbtools-infra 源码树（含顶层 CMakeLists.txt）")
if(NOT EXISTS "${BBT_INFRA_SOURCE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR "BBT_INFRA_SOURCE_DIR 无效：未找到顶层 CMakeLists.txt")
endif()

# 不注册 infra 自身单测；只消费库 target。
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
add_subdirectory("${BBT_INFRA_SOURCE_DIR}" _bbt_infra EXCLUDE_FROM_ALL)

add_executable(config_consumer main.cc)
target_link_libraries(config_consumer PRIVATE bbt::infra_config)
target_compile_options(config_consumer PRIVATE -Wall -Wextra)

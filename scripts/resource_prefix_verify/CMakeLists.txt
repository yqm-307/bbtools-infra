cmake_minimum_required(VERSION 3.16)
project(bbt_resource_prefix_verify LANGUAGES CXX)

# 资源前缀消费者验收：证明 prepare_resource_deps.sh 产出的私有前缀确实按
# infra 根 CMakeLists.txt 的消费契约导出 imported target，且链接/运行版本等于锁定值。
# 这只是消费侧自检，不引入依赖编排平台；框架资源 CI 可在缓存命中后直接复用，
# 避免「缓存里躺着同名异版库却冒充锁定版本」。
#
# 三条断言（任一不成立即构建失败）：
#   1. imported target 存在，且按契约 find_package 拿到（NO_DEFAULT_PATH）；
#   2. 运行期版本（hiredis 头宏 / mongoc_get_version() / MONGOCXX_VERSION_STRING）等于锁定值；
#   3. 真实调用 hiredis 导出符号，并用 ldd 断言解析到的 hiredis/mongoc/mongocxx 全在本次前缀内。
#
# 用法：
#   cmake -S scripts/resource_prefix_verify -B <build> -G Ninja \
#     -DRESOURCE_PREFIX_ROOT=<prepare_resource_deps.sh 前缀根目录>
#   cmake --build <build>
# 构建期即会运行验证据，版本/链接不符时非零退出。
#
# 锁定值来源：infra docs/dependencies/resource-prefixes.provenance.md
#   （hiredis v1.4.1 -> 库内 1.4.0 / mongoc 2.5.4 / mongocxx r4.6.0）。

set(RESOURCE_PREFIX_ROOT "" CACHE PATH "prepare_resource_deps.sh 产出的前缀根目录")
if(NOT RESOURCE_PREFIX_ROOT)
    message(FATAL_ERROR "请传 -DRESOURCE_PREFIX_ROOT=<前缀根目录>")
endif()
get_filename_component(_root "${RESOURCE_PREFIX_ROOT}" ABSOLUTE)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

function(_bbt_require_dir _label _path)
    if(NOT IS_DIRECTORY "${_path}")
        message(FATAL_ERROR "资源前缀缺失 ${_label}: ${_path}")
    endif()
endfunction()

_bbt_require_dir("hiredis"  "${_root}/hiredis")
_bbt_require_dir("mongoc"   "${_root}/mongoc")
_bbt_require_dir("mongocxx" "${_root}/mongocxx")

# hiredis：只允许经私有前缀接入（NO_DEFAULT_PATH，杜绝误链系统同名库）。
find_package(hiredis CONFIG REQUIRED PATHS "${_root}/hiredis" NO_DEFAULT_PATH)

# mongocxx：mongocxxConfig.cmake 内 find_dependency(mongoc/bsoncxx) 的嵌套
# find_package 不吃 CMAKE_PREFIX_PATH（外层 NO_DEFAULT_PATH 会传播），按 infra 根
# CMakeLists.txt 的做法把 <pkg>_DIR 钉到私有前缀下 glob 出的版本目录。
file(GLOB mongoc_DIR   "${_root}/mongoc/lib/cmake/mongoc-*")
file(GLOB bson_DIR     "${_root}/mongoc/lib/cmake/bson-*")
file(GLOB bsoncxx_DIR  "${_root}/mongocxx/lib/cmake/bsoncxx-*")
find_package(mongocxx CONFIG REQUIRED PATHS "${_root}/mongocxx" NO_DEFAULT_PATH)

foreach(_t hiredis::hiredis mongo::mongocxx_shared mongo::bsoncxx_shared mongoc::shared)
    if(NOT TARGET ${_t})
        message(FATAL_ERROR "消费契约缺失 imported target: ${_t}")
    endif()
    get_target_property(_loc ${_t} IMPORTED_LOCATION)
    if(_loc)
        message(STATUS "bbt::resource-prefix target ${_t} -> ${_loc}")
    endif()
endforeach()

set(_gen "${CMAKE_CURRENT_BINARY_DIR}/verify_main.cc")
file(WRITE "${_gen}" [=[
// 由 resource_prefix_verify/CMakeLists.txt 生成；校验实际链接的库版本。
#include <cstdio>
#include <cstring>

#include <hiredis/hiredis.h>
#include <mongoc/mongoc.h>
#include <mongocxx/v1/config/version.hpp>
#include <mongocxx/v1/instance.hpp>

namespace {
int check(const char* what, const char* got, const char* want) {
    const bool ok = std::strcmp(got, want) == 0;
    std::printf("%-9s %s (expect %s)%s\n", what, got, want, ok ? "" : "  <-- MISMATCH");
    return ok ? 0 : 1;
}
}  // namespace

#define BBT_STRINGIFY_IMPL(value) #value
#define BBT_STRINGIFY(value) BBT_STRINGIFY_IMPL(value)

int main() {
    // hiredis 没有运行时版本函数，必须从当前消费到的头文件宏生成版本。
    const char* hiredis_version = BBT_STRINGIFY(HIREDIS_MAJOR) "."
        BBT_STRINGIFY(HIREDIS_MINOR) "." BBT_STRINGIFY(HIREDIS_PATCH);

    // 真实调用 hiredis 导出符号，证明 libhiredis 被实际链接：只取函数地址的写法会被
    // 优化消掉（不构成链接证据）。redisReader* 是纯内存路径，不发起任何网络连接。
    int hiredis_linked = 1;
    if (redisReader* reader = redisReaderCreate()) {
        redisReaderFree(reader);
    } else {
        hiredis_linked = 0;
    }

    // mongoc 运行时版本函数。
    const char* mongoc_version = mongoc_get_version();

    int rc = 0;
    if (!hiredis_linked) {
        std::printf("hiredis   redisReaderCreate() 返回 NULL  <-- MISMATCH\n");
        rc = 1;
    }
    rc |= check("hiredis", hiredis_version, "1.4.0");
    rc |= check("mongoc", mongoc_version, "2.5.4");
    rc |= check("mongocxx", MONGOCXX_VERSION_STRING, "4.6.0");

    // 构造 v1::instance 强制链接 libmongocxx（真实导出符号，不发起网络）。
    mongocxx::v1::instance inst{};
    (void)inst;
    return rc;
}
]=])

add_executable(verify_resource_prefix "${_gen}")
target_link_libraries(verify_resource_prefix PRIVATE
    hiredis::hiredis mongo::mongocxx_shared mongo::bsoncxx_shared mongoc::shared)

# prepare_resource_deps.sh 已把 RUNPATH 指向最终前缀，正常情况可直接运行；但链接器
# 默认写 RUNPATH（非传递），若前缀被移动则自带的绝对 RUNPATH 失效。这里显式给出
# 三份前缀的 lib 目录，使自检对「前缀被恢复/移动到别处」也稳健（CI 缓存场景）。
set(_lib_path "${_root}/hiredis/lib:${_root}/mongoc/lib:${_root}/mongocxx/lib")

# 构建期即运行：版本或链接不符 -> 非零退出 -> 构建失败。
# 第二条断言把链接来源钉死在本前缀：ldd 里 hiredis/mongoc/mongocxx 的任何一条解析结果
# 不落在 ${_root}/ 下（含 /usr/local 同名库、'not found'）都会让本次构建失败。
add_custom_command(TARGET verify_resource_prefix POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E env "LD_LIBRARY_PATH=${_lib_path}"
            $<TARGET_FILE:verify_resource_prefix>
    COMMAND sh -c "! ldd '$<TARGET_FILE:verify_resource_prefix>' | grep -E 'lib(hiredis|bsoncxx|mongocxx|mongoc|bson)[0-9]*\\.so' | grep -v -F '${_root}/'"
    COMMENT "运行资源前缀验证据（版本锁定 + 链接来源校验）"
    VERBATIM)

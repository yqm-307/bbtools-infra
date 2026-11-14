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

# --- 公共依赖段（一次定义）----------------------------------------------------
# 新模块开关不再是互斥分流：公共依赖（bbt_infra_common / bbt_infra_internal_debug /
# coroutine 源码接入 + pin / AsyncMisuse / Threads）在 cmake/Common.cmake 统一定义，
# 普通模式与 NETWORK_ONLY 共用同一套 target 与同一棵 pin 过的 coroutine 树。
# 新协议模块共用 NetworkTypes/Result 等公共值类型，common
# 属于公共面而非旧栈——因此「新 HTTP + common + config」可在同一
# add_subdirectory 消费里链接，不再被开关互斥断开。
option(BBT_INFRA_NETWORK_ONLY "只构建新 gnet 网络基础及其实际依赖闭包" OFF)
option(BBT_INFRA_REDIS_NET "构建基于新 net 的 Redis 窄适配及其依赖闭包" OFF)
if(BBT_INFRA_NETWORK_ONLY)
    set(_bbt_http_default OFF)
else()
    set(_bbt_http_default ON)
endif()
option(BBT_INFRA_HTTP_NET "构建基于 net 的 HTTP 客户端与服务端" ${_bbt_http_default})
option(BBT_INFRA_RPC_NET "构建双向 TCP RPC 会话及其依赖闭包" OFF)
option(BBT_INFRA_MCP "构建 MCP server 核心与 stdio（bbt::infra_mcp_core / bbt::infra_mcp_stdio）；核心不需要第三方前缀" OFF)
option(BBT_INFRA_CONSUL "构建 Consul agent HTTP adapter（bbt::infra_consul）；需 BBT_INFRA_HTTP_NET 同开" OFF)
option(BBT_INFRA_MCP_HTTP "构建 MCP Streamable HTTP transport（bbt::infra_mcp_http）；需 BBT_INFRA_MCP 与 BBT_INFRA_HTTP_NET 同开" OFF)
option(BBT_INFRA_MCP_SCHEMA "构建 MCP JSON Schema 校验器私有 adapter（需 BBT_JSONCONS_PREFIX 与 BBT_ICU_PREFIX）" OFF)

# Consul adapter 开关门禁（fail-closed）：它消费新 HTTP Client，缺 HTTP 闭包时
# 不能静默跳过；同时它的 Client.cc 以 boost/json/src.hpp 源码内联 Boost.JSON，
# 与 MCP 的 JsonValue.cc 同开会重复定义，故两者同开直接拒绝，由消费者择一。
if(BBT_INFRA_CONSUL AND NOT BBT_INFRA_HTTP_NET)
    message(FATAL_ERROR
        "BBT_INFRA_CONSUL=ON 需要 BBT_INFRA_HTTP_NET=ON；开关 fail-closed，不静默跳过。")
endif()
if(BBT_INFRA_CONSUL AND BBT_INFRA_MCP)
    message(FATAL_ERROR
        "BBT_INFRA_CONSUL 与 BBT_INFRA_MCP 同开会重复内联 Boost.JSON"
        "（两个 TU 都包含 boost/json/src.hpp）；同一构建内请只开其一。")
endif()

option(BBT_INFRA_NET "构建 net 网络基础" ON)
if(BBT_INFRA_NETWORK_ONLY)
    set(_bbt_module_default OFF)
else()
    set(_bbt_module_default ON)
endif()
option(BBT_INFRA_CONFIG "构建 config 动态配置模块" ${_bbt_module_default})
option(BBT_INFRA_MONGO "构建 Mongo 模块（需显式 driver 前缀）" ${_bbt_module_default})
if(BBT_INFRA_NETWORK_ONLY AND (BBT_INFRA_CONFIG OR BBT_INFRA_MONGO))
    message(FATAL_ERROR "NETWORK_ONLY 与 CONFIG/MONGO=ON 冲突；请关闭非网络模块")
endif()
if(BBT_INFRA_NETWORK_ONLY AND (BBT_INFRA_MCP OR BBT_INFRA_MCP_HTTP OR BBT_INFRA_MCP_SCHEMA))
    message(FATAL_ERROR "NETWORK_ONLY 不接入 MCP，请关闭这些开关")
endif()
include(cmake/Common.cmake)

# 严格闭包仅接入 net 与显式选择的协议，不探测 config/Mongo。
if(BBT_INFRA_NETWORK_ONLY)
    include(cmake/NetworkOnly.cmake)
    return()
endif()

# --- mongo-cxx-driver 依赖接入（Issue #7，Mongo 模块）-----------------------
# 与 coroutine 同规约：仅允许经显式前缀接入，禁止把本机私有绝对路径
# 写进默认值或任何提交内容。bbt/infra/CMakeLists.txt 内 find_package 限定
# NO_DEFAULT_PATH，杜绝误链系统位置 mongocxx/libmongoc；mongocxx 的
# package config 依赖 mongoc/bsoncxx 包，C driver 前缀经独立变量限定。
# 未设置时跳过 Mongo 模块。
set(BBT_MONGOCXX_PREFIX "" CACHE PATH
    "mongo-cxx-driver 安装前缀（含 include/mongocxx 与 lib/cmake/mongocxx）")
set(BBT_MONGOC_PREFIX "" CACHE PATH
    "mongo-c-driver 安装前缀（含 include/mongoc 与 lib/cmake/mongoc）；mongocxxConfig.cmake 内 find_dependency(mongoc) 需要")

# find_package 放在顶层作用域：包导出的 mongo::mongocxx_shared 等
# IMPORTED target 只有目录级可见性，bbt/infra/ 与 tests/ 都需要引用。
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
if(BBT_INFRA_MONGO AND BBT_MONGOCXX_PREFIX)
    if(BBT_MONGOC_PREFIX)
        # file(GLOB) 返回列表：多命中时 IS_DIRECTORY 对整条列表为假，旧写法
        # 会把「命中多个版本目录」误报成「未找到」。这里按命中数区分：0 命中
        # = 前缀无效，多命中 = 版本不确定；均 fail-closed，诊断必须可区分
        # （Issue #12 M3）。单命中仍须通过原有目录检查，禁止包搜索静默回退。
        file(GLOB mongoc_DIR "${BBT_MONGOC_PREFIX}/lib/cmake/mongoc-*")
        file(GLOB bson_DIR   "${BBT_MONGOC_PREFIX}/lib/cmake/bson-*")
        list(LENGTH mongoc_DIR _bbt_mongoc_n)
        list(LENGTH bson_DIR   _bbt_bson_n)
        if(_bbt_mongoc_n EQUAL 0)
            message(FATAL_ERROR
                "BBT_MONGOC_PREFIX='${BBT_MONGOC_PREFIX}' 无效：未找到 "
                "lib/cmake/mongoc-*")
        elseif(NOT _bbt_mongoc_n EQUAL 1)
            message(FATAL_ERROR
                "BBT_MONGOC_PREFIX='${BBT_MONGOC_PREFIX}' 无效：命中多个版本目录。\n"
                "lib/cmake/mongoc-* 命中 ${_bbt_mongoc_n} 个目录\n"
                "命中目录：${mongoc_DIR}")
        elseif(NOT IS_DIRECTORY "${mongoc_DIR}")
            message(FATAL_ERROR
                "BBT_MONGOC_PREFIX='${BBT_MONGOC_PREFIX}' 无效："
                "lib/cmake/mongoc-* 命中路径不是目录")
        endif()
        if(_bbt_bson_n EQUAL 0)
            message(FATAL_ERROR
                "BBT_MONGOC_PREFIX='${BBT_MONGOC_PREFIX}' 无效：未找到 "
                "lib/cmake/bson-*")
        elseif(NOT _bbt_bson_n EQUAL 1)
            message(FATAL_ERROR
                "BBT_MONGOC_PREFIX='${BBT_MONGOC_PREFIX}' 无效：命中多个版本目录。\n"
                "lib/cmake/bson-* 命中 ${_bbt_bson_n} 个目录\n"
                "命中目录：${bson_DIR}")
        elseif(NOT IS_DIRECTORY "${bson_DIR}")
            message(FATAL_ERROR
                "BBT_MONGOC_PREFIX='${BBT_MONGOC_PREFIX}' 无效："
                "lib/cmake/bson-* 命中路径不是目录")
        endif()
    endif()
    file(GLOB bsoncxx_DIR "${BBT_MONGOCXX_PREFIX}/lib/cmake/bsoncxx-*")
    list(LENGTH bsoncxx_DIR _bbt_bsoncxx_n)
    if(_bbt_bsoncxx_n EQUAL 0)
        message(FATAL_ERROR
            "BBT_MONGOCXX_PREFIX='${BBT_MONGOCXX_PREFIX}' 无效：未找到 "
            "lib/cmake/bsoncxx-*")
    elseif(NOT _bbt_bsoncxx_n EQUAL 1)
        message(FATAL_ERROR
            "BBT_MONGOCXX_PREFIX='${BBT_MONGOCXX_PREFIX}' 无效：命中多个版本目录。\n"
            "lib/cmake/bsoncxx-* 命中 ${_bbt_bsoncxx_n} 个目录\n"
            "命中目录：${bsoncxx_DIR}")
    elseif(NOT IS_DIRECTORY "${bsoncxx_DIR}")
        message(FATAL_ERROR
            "BBT_MONGOCXX_PREFIX='${BBT_MONGOCXX_PREFIX}' 无效："
            "lib/cmake/bsoncxx-* 命中路径不是目录")
    endif()
    find_package(mongocxx CONFIG REQUIRED
        PATHS "${BBT_MONGOCXX_PREFIX}" NO_DEFAULT_PATH)
endif()

add_subdirectory(bbt/infra)

# CTest 必须先于模块，保证模块测试实际注册。
include(CTest)
if(BBT_INFRA_NET OR BBT_INFRA_REDIS_NET OR BBT_INFRA_HTTP_NET OR BBT_INFRA_RPC_NET)
    include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/NetBase.cmake")
    if(BUILD_TESTING)
        add_subdirectory(tests/net)
    endif()
endif()
if(BBT_INFRA_REDIS_NET)
    include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/RedisNet.cmake")
endif()
if(BBT_INFRA_HTTP_NET)
    include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/HttpNet.cmake")
endif()
if(BBT_INFRA_RPC_NET)
    include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/RpcNet.cmake")
endif()
if(BBT_INFRA_CONSUL)
    include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/Consul.cmake")
endif()
# MCP schema 前缀门禁先于 MCP 核心接入（核心 add_subdirectory 时需已知 schema 开关）。
if(BBT_INFRA_MCP_SCHEMA)
    include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/McpSchema.cmake")
endif()
# MCP HTTP transport 开关校验：必须依赖 MCP + HTTP_NET 双开（fail-closed，
# 双开缺一即 configure 失败，不静默跳过）。
if(BBT_INFRA_MCP_HTTP AND NOT (BBT_INFRA_MCP AND BBT_INFRA_HTTP_NET))
    message(FATAL_ERROR
        "BBT_INFRA_MCP_HTTP=ON 需要同时开启 BBT_INFRA_MCP 与 BBT_INFRA_HTTP_NET；"
        "开关 fail-closed，不静默跳过。")
endif()
if(BBT_INFRA_MCP)
    include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/McpCore.cmake")
endif()

# Issue #27：最小真实消费示例（examples/）；不影响库目标与测试注册。
add_subdirectory(examples)

# 每个公共头单独编译 TU，验证头文件自给
set(BBT_INFRA_PUBLIC_HEADERS
    Result.hpp
    NetError.hpp
    ICoObject.hpp
    ICoNetwork.hpp
    ICoCloseable.hpp
    NetworkTypes.hpp
    Codec.hpp
    MongoClientConfig.hpp
    CoMongoCli.hpp
    mongo/Bson.hpp
    mongo/CoMongoDb.hpp
    mongo/CoMongoColl.hpp
    mongo/MongoTypes.hpp
    config/Value.hpp
    config/Snapshot.hpp
    config/Source.hpp
    config/MemorySource.hpp
    config/FileSource.hpp
    config/Watch.hpp
    rpc/codec/PositionalCodec.hpp)

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
# A5 接缝头 NetError.hpp 跨公共值层与 bbt/infra/net/ 原始错误码：
# 自检只补编译期可见性（项目根 include 路径），不引入链接依赖。
target_include_directories(bbt_infra_header_selfcheck PRIVATE
    "${PROJECT_SOURCE_DIR}")
target_compile_options(bbt_infra_header_selfcheck PRIVATE -Wall -Wextra)
set_target_properties(bbt_infra_header_selfcheck PROPERTIES LINKER_LANGUAGE CXX)

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

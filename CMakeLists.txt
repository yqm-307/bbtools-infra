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
# 新协议模块公开头仍引用旧公开头树的 NetworkTypes/Result 等值层类型，common
# 属于公共面而非旧栈——因此「新 HTTP + common + config」可在同一
# add_subdirectory 消费里链接，不再被开关互斥断开。
option(BBT_INFRA_NETWORK_ONLY "只构建新 gnet 网络基础及其实际依赖闭包" OFF)
option(BBT_INFRA_REDIS_NET "构建基于新 net 的 Redis 窄适配及其依赖闭包" OFF)
option(BBT_INFRA_HTTP_NET "构建基于新 net 的 HTTP 客户端窄适配及其依赖闭包" OFF)
option(BBT_INFRA_RPC_NET "构建消费新 HTTP 的 RPC core（wire + Client/Server）及其依赖闭包" OFF)
option(BBT_INFRA_RPC_TCP "构建长度前缀 TCP 的 RPC adapter（bbt::infra_rpc_tcp）；依赖 net 基础，不需要第三方前缀" OFF)
option(BBT_INFRA_MCP "构建 MCP server 核心与 stdio（bbt::infra_mcp_core / bbt::infra_mcp_stdio）；核心不需要第三方前缀" OFF)
option(BBT_INFRA_MCP_HTTP "构建 MCP Streamable HTTP transport（bbt::infra_mcp_http）；需 BBT_INFRA_MCP 与 BBT_INFRA_HTTP_NET 同开" OFF)
option(BBT_INFRA_MCP_SCHEMA "构建 MCP JSON Schema 校验器私有 adapter（需 BBT_JSONCONS_PREFIX 与 BBT_ICU_PREFIX）" OFF)

# 旧栈/普通模式选择开关（P1-6）：声明并真实消费。OFF 的模块不接入 src/ 对应
# 切片；默认 ON 保持既有「普通模式全量」语义。
#
# NETWORK_ONLY 冲突判定（P1-6）：先判「显式 -D 传入」再声明 option——
# option() 之后开关一定已在 cache，「用户 -D=ON」与「option 默认 ON」在
# 取值上不可区分。判据：cache 条目的 HELPSTRING 不等于本 option 的文档串
# 即为外部显式设置（-D 不给条目写文档串；option() 会写入下方一致的串）。
set(_bbt_doc_net          "构建新 gnet 网络基础（普通模式与 NETWORK_ONLY 共用真源）")
set(_bbt_doc_config       "构建 config 动态配置模块（src/config）")
set(_bbt_doc_mongo        "构建 Mongo 模块（需 BBT_MONGOCXX_PREFIX/BBT_MONGOC_PREFIX）")
set(_bbt_doc_legacy       "构建旧 HTTP/TCP/UDP/transport 栈（src/{http,tcp,udp,transport}）")
if(BBT_INFRA_NETWORK_ONLY)
    # BBT_INFRA_NET 不是冲突项：network-only 闭包本身就构建 net——
    # 它属于「开启的正是本闭包内容」而非旧栈选择。
    foreach(_pair IN ITEMS "BBT_INFRA_CONFIG|${_bbt_doc_config}"
                           "BBT_INFRA_MONGO|${_bbt_doc_mongo}"
                           "BBT_INFRA_LEGACY_STACK|${_bbt_doc_legacy}")
        string(REGEX MATCH "^[^|]+" _conflicting "${_pair}")
        string(REGEX MATCH "[^|]+$" _doc "${_pair}")
        if(DEFINED CACHE{${_conflicting}} AND ${_conflicting})
            get_property(_help CACHE ${_conflicting} PROPERTY HELPSTRING)
            if(NOT _help STREQUAL _doc)
                message(FATAL_ERROR
                    "BBT_INFRA_NETWORK_ONLY=ON 与 ${_conflicting}=ON 冲突："
                    "network-only 闭包不接入旧栈/旧模块。请显式 "
                    "-D${_conflicting}=OFF 或移除该开关。")
            endif()
        endif()
    endforeach()
endif()

# 新协议开关（RPC_TCP / MCP*）尚未接入 NetworkOnly 闭包：显式拒绝，不静默忽略。
if(BBT_INFRA_NETWORK_ONLY AND (BBT_INFRA_RPC_TCP OR BBT_INFRA_MCP OR BBT_INFRA_MCP_HTTP OR BBT_INFRA_MCP_SCHEMA))
    message(FATAL_ERROR
        "BBT_INFRA_NETWORK_ONLY=ON 不接入 RPC TCP / MCP 开关（network-only 闭包只含已声明的 net 协议）；请关闭这些开关。")
endif()

option(BBT_INFRA_NET "${_bbt_doc_net}" ON)
option(BBT_INFRA_CONFIG "${_bbt_doc_config}" ON)
option(BBT_INFRA_MONGO "${_bbt_doc_mongo}" ON)
option(BBT_INFRA_LEGACY_STACK "${_bbt_doc_legacy}" ON)

include(cmake/Common.cmake)

# NETWORK_ONLY=ON 走严格闭包：net + 显式打开的新协议模块，不接入旧 src/ 栈、
# 不探测旧模块前缀（BBT_INFRA_REDIS_NET 等是「额外接入新协议」而非旧 Redis）。OFF
# 时继续普通模式：旧栈全量 + 新协议开关按需叠加（各 *_NET 开关语义不变）。
if(BBT_INFRA_NETWORK_ONLY)
    include(cmake/NetworkOnly.cmake)
    return()
endif()

# --- hiredis 依赖接入（Issue #6，Redis 模块）---------------------------------
# 与 coroutine 同规约：仅允许经显式前缀接入，禁止把本机私有绝对路径
# 写进默认值或任何提交内容。src/CMakeLists.txt 内 find_package 限定
# NO_DEFAULT_PATH，杜绝误链系统位置 hiredis；未设置时跳过 Redis 模块。
set(BBT_HIREDIS_PREFIX "" CACHE PATH
    "hiredis 安装前缀（含 include/hiredis 与 lib/cmake/hiredis）")


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

add_subdirectory(src)

# 新协议模块按开关叠加接入：BBT_INFRA_*_NET 不再触发互斥分流，普通模式下与
# 旧栈/common/config 同图共存（各模块 cmake/*.cmake 只做模块接入与其私有
# 前缀门禁，不重复公共依赖）。模块目录经 PROJECT_SOURCE_DIR 绝对路径接入。
# BBT_INFRA_NET=ON 也要求 net 基础 target（P1-6：开关须真实消费）。
# 先确定测试开关：模块内 tests/examples 子目录按 BUILD_TESTING 注册，
# include(CTest) 必须先于模块接入（否则 BUILD_TESTING 未定义、测试被静默跳过）。
include(CTest)
if(BBT_INFRA_NET OR BBT_INFRA_REDIS_NET OR BBT_INFRA_HTTP_NET OR BBT_INFRA_RPC_NET OR BBT_INFRA_RPC_TCP)
    include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/NetBase.cmake")
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
if(BBT_INFRA_RPC_TCP)
    include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/RpcTcp.cmake")
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
    HttpClient.hpp
    HttpServer.hpp
    NetworkRuntime.hpp
    TransportRuntime.hpp
    CoRedisCli.hpp
    CoMongoCli.hpp
    mongo/Bson.hpp
    mongo/Client.hpp
    CoTCP.hpp
    CoUDP.hpp
    config/Value.hpp
    config/Snapshot.hpp
    config/Source.hpp
    config/MemorySource.hpp
    config/FileSource.hpp
    config/Watch.hpp
    rpc/RpcPayload.hpp)

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
# A5 接缝头 NetError.hpp 跨 include/ 值层与 bbt/infra/net/ 原始错误码：
# 自检只补编译期可见性（项目根 include 路径），不引入链接依赖。
target_include_directories(bbt_infra_header_selfcheck PRIVATE
    "${PROJECT_SOURCE_DIR}")
target_compile_options(bbt_infra_header_selfcheck PRIVATE -Wall -Wextra)
set_target_properties(bbt_infra_header_selfcheck PROPERTIES LINKER_LANGUAGE CXX)

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

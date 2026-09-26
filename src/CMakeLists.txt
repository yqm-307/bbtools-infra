# src/：HTTP 模块实现（Boost.Beast + Asio）与 Redis 模块实现（hiredis
# async + 自定义 event adapter）；公开面仍在 include/bbt/infra/。

find_package(Boost REQUIRED)
find_package(Threads REQUIRED)

add_library(bbt_infra_http STATIC
    http/HttpDetail.cc
    http/HttpClientImpl.cc
    http/HttpServerImpl.cc
    http/NetworkRuntimeImpl.cc)
add_library(bbt::infra_http ALIAS bbt_infra_http)

target_include_directories(bbt_infra_http PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(bbt_infra_http
    PUBLIC  bbt::infra_common
    PRIVATE Boost::boost Threads::Threads)
target_compile_options(bbt_infra_http PRIVATE -Wall -Wextra)

# --- CoTCP 基础传输（Issue #31）----------------------------------------------
add_library(bbt_infra_tcp STATIC
    tcp/CoTCP.cc)
add_library(bbt::infra_tcp ALIAS bbt_infra_tcp)

target_include_directories(bbt_infra_tcp PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(bbt_infra_tcp
    PUBLIC bbt::infra_common)
target_compile_options(bbt_infra_tcp PRIVATE -Wall -Wextra)

# CoUDP：POSIX 非阻塞 datagram + coroutine 组合等待。
add_library(bbt_infra_udp STATIC udp/CoUDP.cc)
add_library(bbt::infra_udp ALIAS bbt_infra_udp)
target_include_directories(bbt_infra_udp PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(bbt_infra_udp PUBLIC bbt::infra_common)
target_compile_options(bbt_infra_udp PRIVATE -Wall -Wextra)

# NetworkRuntimeImpl 的受管 TCP/UDP 工厂由 HTTP runtime 对外装配；静态库
# 必须显式传播 CoTCP/CoUDP，避免下游 example/test 链接时出现未定义符号
# （TCP 侧 DialTCP/ListenTCP、UDP 侧 BindUDP/RequestClose）。
target_link_libraries(bbt_infra_http PUBLIC bbt::infra_tcp bbt::infra_udp)

# --- Redis 模块（Issue #6）---------------------------------------------------
# hiredis 只允许经显式前缀接入：find_package 限定 NO_DEFAULT_PATH，
# 不可能误链 /usr/local 等系统位置的同名库。未给前缀时跳过本模块
# （不导出能假成功的 target），公共头仍可独立编译。
if(BBT_HIREDIS_PREFIX)
    find_package(hiredis CONFIG REQUIRED
        PATHS "${BBT_HIREDIS_PREFIX}" NO_DEFAULT_PATH)

    add_library(bbt_infra_redis STATIC
        redis/RedisDetail.cc
        redis/RedisConnection.cc
        redis/CoRedisCliImpl.cc)
    add_library(bbt::infra_redis ALIAS bbt_infra_redis)

    target_include_directories(bbt_infra_redis PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(bbt_infra_redis
        PUBLIC  bbt::infra_common
        PRIVATE Boost::boost Threads::Threads hiredis::hiredis)
    target_compile_options(bbt_infra_redis PRIVATE -Wall -Wextra)
else()
    message(STATUS
        "BBT_HIREDIS_PREFIX 未设置：跳过 bbt_infra_redis 模块。"
        "接入 Redis 客户端请传 -DBBT_HIREDIS_PREFIX=<hiredis 安装前缀>")
endif()

# --- Mongo 模块（Issue #7）--------------------------------------------------
# mongocxx 只允许经显式前缀接入：find_package 在根 CMakeLists.txt 顶层
# 作用域限定 NO_DEFAULT_PATH 执行（mongo::mongocxx_shared 等 IMPORTED
# target 只有目录级可见性，顶层解析后 src/ 与 tests/ 均可见），嵌套
# find_dependency(mongoc/bsoncxx) 经 <pkg>_DIR 钉到私有前缀。
# 未给前缀时跳过本模块（不导出能假成功的 target），公共头仍可独立编译。
if(BBT_MONGOCXX_PREFIX)
    # mongo::mongocxx_shared 等 IMPORTED target 由顶层 find_package 提供
    # （目录级可见性，见根 CMakeLists.txt 的接入注释）。
    add_library(bbt_infra_mongo STATIC
        mongo/MongoDetail.cc
        mongo/MongoProcess.cc
        mongo/MongoRuntime.cc)
    add_library(bbt::infra_mongo ALIAS bbt_infra_mongo)

    target_include_directories(bbt_infra_mongo PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(bbt_infra_mongo
        PUBLIC  bbt::infra_common
        PRIVATE Boost::boost Threads::Threads mongo::mongocxx_shared)
    target_compile_options(bbt_infra_mongo PRIVATE -Wall -Wextra)
else()
    message(STATUS
        "BBT_MONGOCXX_PREFIX 未设置：跳过 bbt_infra_mongo 模块。"
        "接入 Mongo 客户端请传 -DBBT_MONGOCXX_PREFIX=<mongo-cxx-driver 安装前缀>"
        " 与 -DBBT_MONGOC_PREFIX=<mongo-c-driver 安装前缀>")
endif()

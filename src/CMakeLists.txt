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

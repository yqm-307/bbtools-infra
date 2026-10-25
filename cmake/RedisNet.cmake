# 仅在明确请求新 Redis target 时发现后端；私有前缀没有默认值。
set(BBT_HIREDIS_PREFIX "" CACHE PATH "hiredis 固定私有前缀（0003）")
if(NOT BBT_HIREDIS_PREFIX)
    message(FATAL_ERROR "BBT_INFRA_REDIS_NET 需要显式 BBT_HIREDIS_PREFIX")
endif()
find_package(hiredis 1.4.0 EXACT CONFIG REQUIRED PATHS "${BBT_HIREDIS_PREFIX}" NO_DEFAULT_PATH)
add_library(bbt_infra_redis_net STATIC
    "${PROJECT_SOURCE_DIR}/src/redis/CoRedisCliNet.cc"
    "${PROJECT_SOURCE_DIR}/src/redis/RedisDetail.cc")
add_library(bbt::infra_redis_net ALIAS bbt_infra_redis_net)
target_include_directories(bbt_infra_redis_net
    PUBLIC "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>"
    PRIVATE "${PROJECT_SOURCE_DIR}/src" "${PROJECT_SOURCE_DIR}/src/redis")
target_compile_features(bbt_infra_redis_net PUBLIC cxx_std_17)
target_link_libraries(bbt_infra_redis_net PUBLIC bbt::infra_net PRIVATE hiredis::hiredis)
target_compile_options(bbt_infra_redis_net PRIVATE -Wall -Wextra -Wpedantic)
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/redis_net_header.cc" "#include <bbt/infra/redis/CoRedisCli.hpp>\n")
add_library(bbt_infra_redis_net_header_selfcheck STATIC "${CMAKE_CURRENT_BINARY_DIR}/redis_net_header.cc")
target_link_libraries(bbt_infra_redis_net_header_selfcheck PRIVATE bbt::infra_redis_net)
if(BUILD_TESTING)
    add_subdirectory(tests/redis_net)
endif()
add_subdirectory(examples/redis_net)

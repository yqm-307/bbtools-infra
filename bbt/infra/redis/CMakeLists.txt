# 新 net Redis 窄适配（BBT_INFRA_REDIS_NET=ON 时经 cmake/RedisNet.cmake 接入）。
# 公开头与实现同目录（对齐 bbt/infra/net 布局）：include 路径仍是
# <bbt/infra/redis/...>，以 PROJECT_SOURCE_DIR 为 include root；namespace
# bbt::infra::redis 不变。detail/ 是内部部件（hiredis 编解码共享层），不公开。
find_package(hiredis 1.4.0 EXACT CONFIG REQUIRED
    PATHS "${BBT_HIREDIS_PREFIX}" NO_DEFAULT_PATH)

add_library(bbt_infra_redis_net STATIC
    CoRedisCli.cc
    CoRedisPool.cc
    detail/RedisDetail.cc)
add_library(bbt::infra_redis_net ALIAS bbt_infra_redis_net)
target_include_directories(bbt_infra_redis_net
    PUBLIC "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>"
           # 公开头仍引用旧公开头树的 NetworkTypes/Result（include/bbt/infra/）。
           "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
target_compile_features(bbt_infra_redis_net PUBLIC cxx_std_17)
# hiredis 是实现细节，不经公开头/安装接口传播。
target_link_libraries(bbt_infra_redis_net PUBLIC bbt::infra_net PRIVATE hiredis::hiredis)
target_compile_options(bbt_infra_redis_net PRIVATE -Wall -Wextra -Wpedantic)

# 公开头自给编译自检：两个公开头各自单独成 TU。
set(_redis_net_headers CoRedisCli.hpp CoRedisPool.hpp)
set(_redis_net_selfcheck_srcs "")
foreach(_hdr IN LISTS _redis_net_headers)
    set(_tu "${CMAKE_CURRENT_BINARY_DIR}/header_selfcheck/${_hdr}.cc")
    file(WRITE "${_tu}" "#include <bbt/infra/redis/${_hdr}>\n")
    list(APPEND _redis_net_selfcheck_srcs "${_tu}")
endforeach()
add_library(bbt_infra_redis_net_header_selfcheck STATIC ${_redis_net_selfcheck_srcs})
target_link_libraries(bbt_infra_redis_net_header_selfcheck PRIVATE bbt::infra_redis_net)
target_compile_options(bbt_infra_redis_net_header_selfcheck PRIVATE -Wall -Wextra)
set_target_properties(bbt_infra_redis_net_header_selfcheck PROPERTIES LINKER_LANGUAGE CXX)

if(BUILD_TESTING)
    add_subdirectory("${PROJECT_SOURCE_DIR}/tests/redis_net" redis_net_tests)
endif()
add_subdirectory("${PROJECT_SOURCE_DIR}/examples/redis_net" redis_net_examples)

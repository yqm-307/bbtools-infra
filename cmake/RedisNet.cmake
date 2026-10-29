# BBT_INFRA_REDIS_NET=ON 时的薄接线：hiredis 前缀门禁 + 模块目录接入。
# target/测试/示例的完整定义在 bbt/infra/redis/CMakeLists.txt；普通模式与
# NETWORK_ONLY 共用本入口（公共依赖已在 cmake/Common.cmake 一次性接入）。
set(BBT_HIREDIS_PREFIX "" CACHE PATH "hiredis 固定私有前缀（0003）")
if(NOT BBT_HIREDIS_PREFIX)
    message(FATAL_ERROR
        "BBT_INFRA_REDIS_NET=ON 需要显式 BBT_HIREDIS_PREFIX；缺必需前缀明确拒绝。")
endif()
add_subdirectory("${PROJECT_SOURCE_DIR}/bbt/infra/redis" bbt_infra_redis_net)

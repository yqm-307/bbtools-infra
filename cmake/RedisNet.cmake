# BBT_INFRA_REDIS_NET=ON 时的薄接线： hiredis 前缀检查 + 模块目录接入。
# target/测试/示例的完整定义在 bbt/infra/redis/CMakeLists.txt。
set(BBT_HIREDIS_PREFIX "" CACHE PATH "hiredis 固定私有前缀（0003）")
if(NOT BBT_HIREDIS_PREFIX)
    message(FATAL_ERROR "BBT_INFRA_REDIS_NET 需要显式 BBT_HIREDIS_PREFIX")
endif()
add_subdirectory("${PROJECT_SOURCE_DIR}/bbt/infra/redis" redis_net)

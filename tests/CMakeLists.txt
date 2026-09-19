add_executable(Test_contract_basics Test_contract_basics.cc)
target_link_libraries(Test_contract_basics PRIVATE bbt::infra_common)
target_compile_options(Test_contract_basics PRIVATE -Wall -Wextra)
add_test(NAME contract.basics COMMAND Test_contract_basics)

# N1b-1：真实 HTTP/1.1 loopback 闭环（Boost.Beast + Asio）。
# 与上游单测一致：Boost.Test 经 included/unit_test.hpp 静态内嵌。
find_package(Boost REQUIRED)
find_package(Threads REQUIRED)
add_executable(Test_http_loopback Test_http_loopback.cc)
target_link_libraries(Test_http_loopback
    PRIVATE bbt::infra_http Boost::boost Threads::Threads)
target_compile_options(Test_http_loopback PRIVATE -Wall -Wextra)
add_test(NAME http.loopback COMMAND Test_http_loopback)
set_tests_properties(http.loopback PROPERTIES TIMEOUT 60)

# N1b-2：取消与关闭完整语义（在途 token 寿命、取消源、逻辑/物理
# 清理分离、诚实 WaitClosed、drain 顺序）。
add_executable(Test_http_lifecycle Test_http_lifecycle.cc)
target_link_libraries(Test_http_lifecycle
    PRIVATE bbt::infra_http Boost::boost Threads::Threads)
target_compile_options(Test_http_lifecycle PRIVATE -Wall -Wextra)
add_test(NAME http.lifecycle COMMAND Test_http_lifecycle)
set_tests_properties(http.lifecycle PROPERTIES TIMEOUT 120)

# --- Redis 模块测试（Issue #6）：仅在构建 bbt_infra_redis 时注册。
# 单元套件不依赖外部 Redis（内置 FakeRedis/静默对端）；live 套件按
# BBT_TEST_REDIS_ADDR 环境变量驱动真实容器验收，未提供时整件跳过。
if(TARGET bbt_infra_redis)
    add_executable(Test_redis_unit Test_redis_unit.cc)
    target_link_libraries(Test_redis_unit PRIVATE
        bbt_infra_redis bbt::infra_common Boost::boost Threads::Threads)
    target_compile_options(Test_redis_unit PRIVATE -Wall -Wextra)
    add_test(NAME redis.unit COMMAND Test_redis_unit)
    set_tests_properties(redis.unit PROPERTIES TIMEOUT 120)

    add_executable(Test_redis_live Test_redis_live.cc)
    target_link_libraries(Test_redis_live PRIVATE
        bbt_infra_redis bbt::infra_common Boost::boost Threads::Threads)
    target_compile_options(Test_redis_live PRIVATE -Wall -Wextra)
    add_test(NAME redis.live COMMAND Test_redis_live)
    set_tests_properties(redis.live PROPERTIES TIMEOUT 120)
endif()

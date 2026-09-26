add_executable(Test_contract_basics Test_contract_basics.cc)
target_link_libraries(Test_contract_basics PRIVATE bbt::infra_common)
target_compile_options(Test_contract_basics PRIVATE -Wall -Wextra)
add_test(NAME contract.basics COMMAND Test_contract_basics)

# --- CoTCP loopback（Issue #31）----------------------------------------------
add_executable(Test_tcp_loopback Test_tcp_loopback.cc)
target_link_libraries(Test_tcp_loopback
    PRIVATE bbt::infra_tcp)
target_compile_options(Test_tcp_loopback PRIVATE -Wall -Wextra)
add_test(NAME tcp.loopback COMMAND Test_tcp_loopback)
set_tests_properties(tcp.loopback PROPERTIES TIMEOUT 30)

# CoTCP Runtime 托管工厂（co-io-adapter/v1 §4.0.1）：需要 bbt_infra_http
# （NetworkRuntime 实现在 HTTP 切片内）。覆盖未 Start 拒绝、容量门禁
# Overloaded、数值直连闭环与关闭后容量释放。
add_executable(Test_tcp_runtime_factory Test_tcp_runtime_factory.cc)
target_link_libraries(Test_tcp_runtime_factory
    PRIVATE bbt::infra_http bbt::infra_tcp Boost::boost Threads::Threads)
target_compile_options(Test_tcp_runtime_factory PRIVATE -Wall -Wextra)
add_test(NAME tcp.runtime_factory COMMAND Test_tcp_runtime_factory)
set_tests_properties(tcp.runtime_factory PROPERTIES TIMEOUT 60)

# CoUDP：loopback、截断、取消与关闭生命周期。
add_executable(Test_udp_loopback Test_udp_loopback.cc)
target_link_libraries(Test_udp_loopback PRIVATE bbt::infra_udp)
target_compile_options(Test_udp_loopback PRIVATE -Wall -Wextra)
add_test(NAME udp.loopback COMMAND Test_udp_loopback)
set_tests_properties(udp.loopback PROPERTIES TIMEOUT 30)

# 实测 detail::CoPollEvent 单事件上 FD readable + custom wakeup + timeout
# 组合的确定性四场景。使用 detail 非稳定头与非公开组合路径，仅用于能力裁决，
# 不作为生产 IoWait 实现依据；不涉及 Stop 安全断言。
add_executable(Test_iowait_probe Test_iowait_probe.cc)
target_link_libraries(Test_iowait_probe PRIVATE bbt::infra_common)
target_compile_options(Test_iowait_probe PRIVATE -Wall -Wextra)
add_test(NAME iowait.probe COMMAND Test_iowait_probe)
set_tests_properties(iowait.probe PROPERTIES TIMEOUT 60)

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

# Issue #38：子对象物理关闭后从 Runtime 托管集合反登记——弱引用失效、
# 调用者提前释放仍托管、并发/重复 close 与工厂竞争、handler 捕获资源
# 析构计数。
add_executable(Test_http_child_unregister Test_http_child_unregister.cc)
target_include_directories(Test_http_child_unregister PRIVATE
    ${CMAKE_SOURCE_DIR}/src)  # impl 测试接缝头（http/NetworkRuntimeImpl.hpp）
target_link_libraries(Test_http_child_unregister
    PRIVATE bbt::infra_http Boost::boost Threads::Threads)
target_compile_options(Test_http_child_unregister PRIVATE -Wall -Wextra)
add_test(NAME http.child_unregister COMMAND Test_http_child_unregister)
set_tests_properties(http.child_unregister PROPERTIES TIMEOUT 120)

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

# --- Mongo 模块测试（Issue #7）：仅在构建 bbt_infra_mongo 时注册。
# 单元套件不依赖外部 MongoDB（不可达地址 + 驱动超时上界驱动确定性
# 失败路径；worker 并发/容量经 impl 观测钩子核验）；live 套件按
# BBT_TEST_MONGO_URI 环境变量驱动真实容器验收，未提供时整件跳过。
if(TARGET bbt_infra_mongo)
    add_executable(Test_mongo_unit Test_mongo_unit.cc)
    target_include_directories(Test_mongo_unit PRIVATE
        ${CMAKE_SOURCE_DIR}/src)
    # mongo::mongocxx_shared 只为 impl 内部头（src/mongo/）的 include
    # 路径与符号；测试不直接调 driver API。
    target_link_libraries(Test_mongo_unit PRIVATE
        bbt_infra_mongo bbt::infra_common Boost::boost Threads::Threads
        mongo::mongocxx_shared)
    target_compile_options(Test_mongo_unit PRIVATE -Wall -Wextra)
    add_test(NAME mongo.unit COMMAND Test_mongo_unit)
    set_tests_properties(mongo.unit PROPERTIES TIMEOUT 300)

    add_executable(Test_mongo_live Test_mongo_live.cc)
    target_link_libraries(Test_mongo_live PRIVATE
        bbt_infra_mongo bbt::infra_common Boost::boost Threads::Threads
        mongo::bsoncxx_shared)
    target_compile_options(Test_mongo_live PRIVATE -Wall -Wextra)
    add_test(NAME mongo.live COMMAND Test_mongo_live)
    set_tests_properties(mongo.live PROPERTIES TIMEOUT 120)
endif()

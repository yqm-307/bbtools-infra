# tests/ 平铺层注册：Mongo/config/contract 与
# 通用位置参数 codec 测试。命名与 label 约定见 docs/conventions.md §6：
#   文件   tests/Test_<行为>.cc
#   ctest  <module>.<行为>
#   label  smoke（快速冒烟）/ unit（模块内行为与边界）/
#          integration（真实组件协作，无外部服务）/ live（外部服务）/ perf
#
# smoke 分配口径：每模块/每覆盖轴至少 1 项秒级-亚秒级代表用例（实测见
# build-baseline CTestCostData；mongo.unit 129s、config.watch 等重项刻意
# 不打 smoke，归 full/extended 路径）。PR CI 只跑 -L smoke 选集，缺 smoke
# 的模块在 PR 层无保护，新增测试须同步决定 label。
#
# 非 Boost.Test 的 case-dispatch 形态（自建 main + RunTestProcessExit）与
# tests/net 的 RunCases 约定一致，见 tests/README.md。

add_executable(Test_contract_basics Test_contract_basics.cc)
target_link_libraries(Test_contract_basics PRIVATE bbt::infra_common)
target_compile_options(Test_contract_basics PRIVATE -Wall -Wextra)
add_test(NAME contract.basics COMMAND Test_contract_basics)
set_tests_properties(contract.basics PROPERTIES TIMEOUT 60 LABELS "smoke;unit")

# --- waypoint 3-3 前置：infra 通用位置参数 payload codec。----------------------
# 只链接 bbt::infra_common（模板实现随头提供，非模板实现由库目标提供）。
# HTTP-only 闭包不注册 RPC 测试。
if(NOT BBT_INFRA_HTTP_NET OR BBT_INFRA_RPC_NET)
    add_executable(Test_rpc_payload Test_rpc_payload.cc)
    target_link_libraries(Test_rpc_payload PRIVATE bbt::infra_common)
    target_compile_options(Test_rpc_payload PRIVATE -Wall -Wextra)
    add_test(NAME rpc.payload COMMAND Test_rpc_payload)
    set_tests_properties(rpc.payload PROPERTIES TIMEOUT 60 LABELS "smoke;unit")
endif()

# 实测 detail::CoPollEvent 单事件上 FD readable + custom wakeup + timeout
# 组合的确定性四场景。使用 detail 非稳定头与非公开组合路径，仅用于能力裁决，
# 不作为生产 IoWait 实现依据；不涉及关闭/Stop 安全断言（新契约已无 Stop）。
add_executable(Test_iowait_combined_wait Test_iowait_combined_wait.cc)
target_link_libraries(Test_iowait_combined_wait PRIVATE bbt::infra_common)
target_compile_options(Test_iowait_combined_wait PRIVATE -Wall -Wextra)
add_test(NAME iowait.combined_wait COMMAND Test_iowait_combined_wait)
set_tests_properties(iowait.combined_wait PROPERTIES TIMEOUT 60 LABELS "smoke;unit")

# --- Config 基础配置模块测试（Issue #35）--------------------------------------
# unit：Value 类型化读取/指纹、内存源版本、文件源读取/更新/格式错误（无需调度器）。
# P1-6：随 BBT_INFRA_CONFIG 开关注册。
if(BBT_INFRA_CONFIG)
add_executable(Test_config_unit Test_config_unit.cc)
target_link_libraries(Test_config_unit PRIVATE
    bbt::infra_config Boost::boost Threads::Threads)
target_compile_options(Test_config_unit PRIVATE -Wall -Wextra)
add_test(NAME config.unit COMMAND Test_config_unit)
set_tests_properties(config.unit PROPERTIES TIMEOUT 60 LABELS "smoke;unit")

# watch：初始/去重/更新、关闭后不再回调、失败/恢复、Close 停止轮询并释放源。
add_executable(Test_config_watch Test_config_watch.cc)
target_link_libraries(Test_config_watch PRIVATE
    bbt::infra_config Boost::boost Threads::Threads)
target_compile_options(Test_config_watch PRIVATE -Wall -Wextra)
add_test(NAME config.watch COMMAND Test_config_watch)
set_tests_properties(config.watch PROPERTIES TIMEOUT 120 LABELS "unit")
endif()

# --- Mongo 模块测试（Issue #7 + #40 owner/句柄归属）：仅在构建
# bbt_infra_mongo 时注册。单元套件不依赖外部 MongoDB（不可达地址 +
# 驱动超时上界驱动确定性失败路径；worker 并发/容量/多句柄共享经 impl
# 观测钩子核验）；live 套件按 BBT_TEST_MONGO_URI 环境变量驱动真实
# 容器验收，未提供时整件跳过。
if(TARGET bbt_infra_mongo)
    add_executable(Test_mongo_unit Test_mongo_unit.cc)
    target_include_directories(Test_mongo_unit PRIVATE
        ${PROJECT_SOURCE_DIR})
    # mongo::mongocxx_shared 只为 impl 内部头（bbt/infra/mongo/detail/）的 include
    # 路径与符号；测试不直接调 driver API。
    target_link_libraries(Test_mongo_unit PRIVATE
        bbt_infra_mongo bbt::infra_common Boost::boost Threads::Threads
        mongo::mongocxx_shared)
    target_compile_options(Test_mongo_unit PRIVATE -Wall -Wextra)
    add_test(NAME mongo.unit COMMAND Test_mongo_unit)
    set_tests_properties(mongo.unit PROPERTIES TIMEOUT 300 LABELS "unit")

    add_executable(Test_mongo_live Test_mongo_live.cc)
    # Issue #34：live 用例需要真实生产状态观测（runtime 的 worker/执行槽计数、
    # MongoCollImpl::Runtime）——读取 bbt/infra/mongo/detail/ 内部头；mongo::mongocxx_shared
    # 只为该内部头的 include 路径与符号，测试不直接调 driver API。仅本目标可见。
    target_include_directories(Test_mongo_live PRIVATE
        ${PROJECT_SOURCE_DIR})
    target_link_libraries(Test_mongo_live PRIVATE
        bbt_infra_mongo bbt::infra_common Boost::boost Threads::Threads
        mongo::bsoncxx_shared mongo::mongocxx_shared)
    target_compile_options(Test_mongo_live PRIVATE -Wall -Wextra)
    add_test(NAME mongo.live COMMAND Test_mongo_live)
    # 与 redis.live 相同：无 BBT_TEST_MONGO_URI 时测试体打印 "skip:" 后
    # return 0，须经 SKIP_REGULAR_EXPRESSION/SKIP_RETURN_CODE 标为真正
    # 的 Skipped，避免无容器环境下冒充「live 已通过」。
    set_tests_properties(mongo.live PROPERTIES
        TIMEOUT 120 LABELS "live"
        SKIP_REGULAR_EXPRESSION "skip:"
        SKIP_RETURN_CODE 77)

    # 公开 BSON/async 入口的消费示例（examples/mongo_client）：只链接
    # bbt::infra_mongo、只含公开头。真实 CRUD 由 BBT_TEST_MONGO_URI 注入；
    # 未注入时示例打印 "skip:" 并返回 77，与 mongo.live 同规约。注册点必须在
    # include(CTest) 之后（tests/ 由根 CMakeLists 的 BUILD_TESTING 块加入），
    # examples/ 自身处理时 add_test 尚未 enable_testing。
    if(TARGET mongo_client_example)
        add_test(NAME mongo.client.consumer COMMAND mongo_client_example)
        set_tests_properties(mongo.client.consumer PROPERTIES
            TIMEOUT 120 LABELS "live"
            SKIP_REGULAR_EXPRESSION "skip:"
            SKIP_RETURN_CODE 77)
    endif()

    # Mongo 新入口 async/await 正负编译门禁（与 tests/redis_net 同规约）：
    # 正例（4 个 CRUD 经 bbt_co_await 消费）必须可编译；负例（漏写 await、
    # 直接按 result 访问）必须被拒绝。只做 -fsyntax-only，不需要后端/链接。
    # 包含面用显式列表：公开头 + pin 的 coroutine 源码根（源码消费是唯一能通过
    # pin 核验的路径，故该变量必非空）。
    file(GENERATE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/mongo-async-includes.txt"
        CONTENT "${PROJECT_SOURCE_DIR}\n${BBT_COROUTINE_SOURCE_DIR}\n")
    add_test(NAME mongo.async_compile COMMAND ${CMAKE_COMMAND}
        "-DCXX=${CMAKE_CXX_COMPILER}"
        "-DINCLUDES=${CMAKE_CURRENT_BINARY_DIR}/mongo-async-includes.txt"
        "-DSOURCE=${CMAKE_CURRENT_SOURCE_DIR}/mongo_async/AsyncCompile.cc"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/mongo_async/AsyncCompile.cmake")
    set_tests_properties(mongo.async_compile PROPERTIES TIMEOUT 60 LABELS "integration")
endif()

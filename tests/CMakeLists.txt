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
# Issue #32：另加 max_inflight 验收矩阵——共享账本跨 listener/UDP、
# 受管 DialTCP 等待段在途/终态归还、未托管入口豁免；直接驱动 impl 层
# 读取账本探针，需要 src/ 内部头 include 路径——仅本目标可见。
add_executable(Test_tcp_runtime_factory Test_tcp_runtime_factory.cc)
target_include_directories(Test_tcp_runtime_factory PRIVATE
    ${CMAKE_SOURCE_DIR}/src)
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

# --- P2 transport owner：transport-only 消费者（无 HTTP 依赖）----------------
# 本目标只链接 bbt::infra_transport，源文件不包含任何 HTTP 头：它是「受管
# TCP/UDP 消费者无需 HTTP」的编译/链接证据。覆盖 owner 门禁与参数校验、
# 容量名额与物理关闭后才归还、真实 loopback 收发 + 关闭生命周期、多 owner
# 配额隔离、关闭期已接纳挂起 op 的唤醒与归还。
# Issue #32/内部装配面：本目标直接驱动 transport owner 的测试探针与
# 等待入口 gate（detail::TransportWiring，src 内部头）——仅本目标可见。
add_executable(Test_transport_runtime Test_transport_runtime.cc)
target_include_directories(Test_transport_runtime PRIVATE
    ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(Test_transport_runtime
    PRIVATE bbt::infra_transport Boost::boost Threads::Threads)
target_compile_options(Test_transport_runtime PRIVATE -Wall -Wextra)
add_test(NAME transport.runtime COMMAND Test_transport_runtime)
set_tests_properties(transport.runtime PROPERTIES TIMEOUT 60)

# P2 依赖门禁：transport-only 消费者的接口闭包不得含 HTTP target。
get_target_property(_bbt_tr_links Test_transport_runtime LINK_LIBRARIES)
if(_bbt_tr_links MATCHES "infra_http")
    message(FATAL_ERROR
        "P2 依赖方向破坏：transport-only 消费者不得链接 HTTP"
        "（实际: ${_bbt_tr_links}）")
endif()

# 实测 detail::CoPollEvent 单事件上 FD readable + custom wakeup + timeout
# 组合的确定性四场景。使用 detail 非稳定头与非公开组合路径，仅用于能力裁决，
# 不作为生产 IoWait 实现依据；不涉及关闭/Stop 安全断言（新契约已无 Stop）。
add_executable(Test_iowait_probe Test_iowait_probe.cc)
target_link_libraries(Test_iowait_probe PRIVATE bbt::infra_common)
target_compile_options(Test_iowait_probe PRIVATE -Wall -Wextra)
add_test(NAME iowait.probe COMMAND Test_iowait_probe)
set_tests_properties(iowait.probe PROPERTIES TIMEOUT 60)

# --- Config 基础配置模块测试（Issue #35）--------------------------------------
# unit：Value 类型化读取/指纹、内存源版本、文件源读取/更新/格式错误（无需调度器）。
add_executable(Test_config_unit Test_config_unit.cc)
target_link_libraries(Test_config_unit PRIVATE
    bbt::infra_config Boost::boost Threads::Threads)
target_compile_options(Test_config_unit PRIVATE -Wall -Wextra)
add_test(NAME config.unit COMMAND Test_config_unit)
set_tests_properties(config.unit PROPERTIES TIMEOUT 60)

# watch：初始/去重/更新、关闭后不再回调、失败/恢复、Close 停止轮询并释放源。
add_executable(Test_config_watch Test_config_watch.cc)
target_link_libraries(Test_config_watch PRIVATE
    bbt::infra_config Boost::boost Threads::Threads)
target_compile_options(Test_config_watch PRIVATE -Wall -Wextra)
add_test(NAME config.watch COMMAND Test_config_watch)
set_tests_properties(config.watch PROPERTIES TIMEOUT 120)

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

# N1b-2：关闭与终态交付语义（owner 主动同步 Close()；未发送请求丢弃；
# 已发送请求在 Close 后立即终态；迟到响应只消费不再交付；每请求只交付一次
# 终态；已接纳 handler 不被抢占；不用 Scheduler::Stop 做隔离或验收）。
add_executable(Test_http_lifecycle Test_http_lifecycle.cc)
target_link_libraries(Test_http_lifecycle
    PRIVATE bbt::infra_http Boost::boost Threads::Threads)
target_compile_options(Test_http_lifecycle PRIVATE -Wall -Wextra)
add_test(NAME http.lifecycle COMMAND Test_http_lifecycle)
set_tests_properties(http.lifecycle PROPERTIES TIMEOUT 120)

# Issue #62 回归探针：keep-alive deadline timer cancel/re-arm 记账 + 并发
# Close/peer-FIN。直接读 impl 层只读探针 DebugSessionCount()，需要 src/ 内部
# 头 include 路径——仅本目标可见。ASan/UBSan 构建下运行并扫描日志。
add_executable(Test_http_timer_rearm_probe Test_http_timer_rearm_probe.cc)
target_include_directories(Test_http_timer_rearm_probe PRIVATE
    ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(Test_http_timer_rearm_probe
    PRIVATE bbt::infra_http Boost::boost Threads::Threads)
target_compile_options(Test_http_timer_rearm_probe PRIVATE -Wall -Wextra)
add_test(NAME http.timer_rearm COMMAND Test_http_timer_rearm_probe)
set_tests_properties(http.timer_rearm PROPERTIES TIMEOUT 120)

# Issue #62 补充探针：在途 handler 与 peer-FIN / owner Close/Teardown 并发。
# Case A（peer-FIN in-flight）断言 OnPeerWatch→Close 完成、会话 drain、晚到回投
# 不触碰已关资源；Case B（Close vs in-flight）断言同步 Close 契约与 drain 归零返回；
# Case B2 直接并发 Teardown()。直接读 impl 层只读探针 DebugSessionCount()/OpenForIo()，
# 需要 src/ 内部头 include 路径——仅本目标可见。ASan/UBSan 构建下运行并扫描日志。
add_executable(Test_http_inflight_close_probe Test_http_inflight_close_probe.cc)
target_include_directories(Test_http_inflight_close_probe PRIVATE
    ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(Test_http_inflight_close_probe
    PRIVATE bbt::infra_http Boost::boost Threads::Threads)
target_compile_options(Test_http_inflight_close_probe PRIVATE -Wall -Wextra)
add_test(NAME http.inflight_close COMMAND Test_http_inflight_close_probe)
set_tests_properties(http.inflight_close PROPERTIES TIMEOUT 120)

# Issue #37：HTTP 出站连接/在途配额的原子接纳与物理归还。
# 与 transport #32 不同入口：覆盖 HttpClientImpl::Request 的 owner
# 预算门禁（max_connections/max_inflight）、Overloaded 确定性、
# 各结束路径名额归还与多 client 共享作用域。
add_executable(Test_http_outbound_quota Test_http_outbound_quota.cc)
# 复审回归用例直接驱动 impl 层（注入配额钩子、io 域顺序探针、并发收口），
# 需要 src/ 内部头 include 路径——仅本目标可见，不扩散到其他测试。
target_include_directories(Test_http_outbound_quota PRIVATE
    ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(Test_http_outbound_quota
    PRIVATE bbt::infra_http Boost::boost Threads::Threads)
target_compile_options(Test_http_outbound_quota PRIVATE -Wall -Wextra)
add_test(NAME http.outbound_quota COMMAND Test_http_outbound_quota)
set_tests_properties(http.outbound_quota PROPERTIES TIMEOUT 120)

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

# Issue #55/#56/#57：失败路径物理收口、resolver 寿命边界、composed connect
# 串行化。直接驱动 impl 层 ClientOp（注入配额钩子、io_fault/on_resolve/
# on_connect_attempt seam、endpoint 注入与物理状态探针），需要 src/ 内部头。
add_executable(Test_http_failure_close Test_http_failure_close.cc)
target_include_directories(Test_http_failure_close PRIVATE
    ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(Test_http_failure_close
    PRIVATE bbt::infra_http Boost::boost Threads::Threads)
target_compile_options(Test_http_failure_close PRIVATE -Wall -Wextra)
add_test(NAME http.failure_close COMMAND Test_http_failure_close)
set_tests_properties(http.failure_close PROPERTIES TIMEOUT 120)

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

    # B 级 owner binding 候选隔离验收：直接驱动 src/redis 候选实现与
    # detail::TransportWiring 装配面探针（fd 读/原生 fd），故需要 src 内部头。
    # 真实 Redis loopback 由 BBT_TEST_REDIS_ADDR 驱动；原始对端故障用例始终执行。
    add_executable(Test_redis_cotcp_binding Test_redis_cotcp_binding.cc)
    target_include_directories(Test_redis_cotcp_binding PRIVATE
        ${CMAKE_SOURCE_DIR}/src)
    # 候选内部头引用 hiredis 类型（RawReply 解码路径）：src 作用域解析出的
    # IMPORTED target 不跨目录，故此处按同一显式前缀复现接入（仅测试目标可见）。
    find_package(hiredis CONFIG REQUIRED PATHS "${BBT_HIREDIS_PREFIX}" NO_DEFAULT_PATH)
    target_link_libraries(Test_redis_cotcp_binding PRIVATE
        bbt_infra_redis bbt::infra_tcp bbt::infra_common hiredis::hiredis
        Boost::boost Threads::Threads)
    target_compile_options(Test_redis_cotcp_binding PRIVATE -Wall -Wextra)
    add_test(NAME redis.cotcp_binding
             COMMAND Test_redis_cotcp_binding
                     --run_test=!redis_cotcp_binding/t_resp2_roundtrip_live)
    set_tests_properties(redis.cotcp_binding PROPERTIES TIMEOUT 240)

    # 真实 Redis round-trip：只以专用退出码 77 表示「环境不可用」。不使用
    # SKIP_REGULAR_EXPRESSION —— 那会把整轮输出里（含原始对端用例）的真实断言失败
    # 一并降级为 skipped；断言失败/崩溃的退出码不是 77，一律 Failed。
    add_test(NAME redis.cotcp_binding.live
             COMMAND Test_redis_cotcp_binding
                     --run_test=redis_cotcp_binding/t_setup_scheduler:redis_cotcp_binding/t_resp2_roundtrip_live)
    set_tests_properties(redis.cotcp_binding.live PROPERTIES
        TIMEOUT 240
        SKIP_RETURN_CODE 77)

    add_executable(Test_redis_live Test_redis_live.cc)
    target_link_libraries(Test_redis_live PRIVATE
        bbt_infra_redis bbt::infra_common Boost::boost Threads::Threads)
    target_compile_options(Test_redis_live PRIVATE -Wall -Wextra)
    add_test(NAME redis.live COMMAND Test_redis_live)
    # 无 BBT_TEST_REDIS_ADDR/PHASE 时测试体以 "skip: ..." 打印后 return，
    # 进程退出码仍是 0。没有 SKIP_REGULAR_EXPRESSION 时 CTest 会把它记为
    # Passed——正是 Issue #34 要堵的「skip 冒充绿」。钉上 SKIP_* 让无环境
    # 的 live 显式标为 Skipped，并给 distinct return code 供断言。
    set_tests_properties(redis.live PROPERTIES
        TIMEOUT 120
        SKIP_REGULAR_EXPRESSION "skip:"
        SKIP_RETURN_CODE 77)
endif()

# --- Mongo 模块测试（Issue #7 + #40 owner/句柄归属）：仅在构建
# bbt_infra_mongo 时注册。单元套件不依赖外部 MongoDB（不可达地址 +
# 驱动超时上界驱动确定性失败路径；worker 并发/容量/多句柄共享经 impl
# 观测钩子核验）；live 套件按 BBT_TEST_MONGO_URI 环境变量驱动真实
# 容器验收，未提供时整件跳过。
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
    # 与 redis.live 相同：无 BBT_TEST_MONGO_URI 时测试体打印 "skip:" 后
    # return 0，须经 SKIP_REGULAR_EXPRESSION/SKIP_RETURN_CODE 标为真正
    # 的 Skipped，避免无容器环境下冒充「live 已通过」。
    set_tests_properties(mongo.live PROPERTIES
        TIMEOUT 120
        SKIP_REGULAR_EXPRESSION "skip:"
        SKIP_RETURN_CODE 77)
endif()

# --- RPC wire 模块测试（Issue #8）：仅在构建 bbt_infra_rpc 时注册。----------
if(TARGET bbt_infra_rpc)
    add_executable(Test_rpc_wire_contract Test_rpc_wire_contract.cc)
    target_link_libraries(Test_rpc_wire_contract
        PRIVATE bbt::infra_rpc bbt::infra_common Boost::boost Threads::Threads)
    target_compile_options(Test_rpc_wire_contract PRIVATE -Wall -Wextra)
    add_test(NAME rpc.wire_contract COMMAND Test_rpc_wire_contract)
    set_tests_properties(rpc.wire_contract PROPERTIES TIMEOUT 60)

    add_executable(Test_rpc_wire_roundtrip
        Test_rpc_wire_roundtrip.cc)
    target_link_libraries(Test_rpc_wire_roundtrip PRIVATE
        bbt::infra_rpc bbt::infra_http Boost::boost Threads::Threads)
    target_compile_options(Test_rpc_wire_roundtrip PRIVATE -Wall -Wextra)
    add_test(NAME rpc.wire_roundtrip COMMAND Test_rpc_wire_roundtrip)
    set_tests_properties(rpc.wire_roundtrip PROPERTIES TIMEOUT 120)

    # 双进程 round-trip 的 server/client 对端（不经 Boost.Test）。
    add_executable(bbt_rpc_wire_peer rpc_wire_peer.cc)
    target_link_libraries(bbt_rpc_wire_peer PRIVATE
        bbt::infra_rpc bbt::infra_http Boost::boost Threads::Threads)
    target_compile_options(bbt_rpc_wire_peer PRIVATE -Wall -Wextra)
endif()

# --- 硬停 quiescence / owner 收口两项旧语义用例（Scheduler::Stop 之后经
# ForceCloseAfterQuiescence 强制收口）随 Stop 语义一起删除：新契约下 runtime
# 进程寿命常驻、Close 同步收口，已无「硬停后强制关闭」入口，不再保留目标。

# --- Issue #31 收尾验收缺口剩余项：DNS 在途到期（非调用前已过期）。
# 晚到结果只能间接验证：名额不重复归还且 DNS 通道仍可用；本用例未直接观察
# worker 侧「丢弃晚到结果」的分支。需要 src 内部装配面（DNS 扣持接缝）与协议
# owner 的 dial 等待入口，故链接 infra_http + tcp + udp。
add_executable(Test_transport_acceptance Test_transport_acceptance.cc)
target_include_directories(Test_transport_acceptance PRIVATE
    ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(Test_transport_acceptance
    PRIVATE bbt::infra_http bbt::infra_tcp bbt::infra_udp
            Boost::boost Threads::Threads)
target_compile_options(Test_transport_acceptance PRIVATE -Wall -Wextra)
add_test(NAME transport.acceptance COMMAND Test_transport_acceptance)
set_tests_properties(transport.acceptance PROPERTIES TIMEOUT 120)

# --- R1：NO_LOOP 自驱动边界 —— Close 唤醒在途受管 DialTCP 后，工厂必须在
# 不依赖 Close 调用方继续驱动 poller 的情况下自行走到 EndFactory。本目标刻意
# 以 SCHE_START_OPT_SCHE_NO_LOOP 启动运行时且全程不调用 LoopOnce。需要 src/
# 内部装配面（detail::TransportWiring 的 R1 探针与 dial 等待入口 gate），
# 仅本目标可见；只链接 transport（无 HTTP）。
add_executable(Test_transport_runtime_no_loop Test_transport_runtime_no_loop.cc)
target_include_directories(Test_transport_runtime_no_loop PRIVATE
    ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(Test_transport_runtime_no_loop
    PRIVATE bbt::infra_transport Boost::boost Threads::Threads)
target_compile_options(Test_transport_runtime_no_loop PRIVATE -Wall -Wextra)
add_test(NAME transport.runtime_no_loop COMMAND Test_transport_runtime_no_loop)
set_tests_properties(transport.runtime_no_loop PROPERTIES TIMEOUT 60)

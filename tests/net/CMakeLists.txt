# R2 网络测试面：保留不受影响的原语/值层测试；旧自建线程内核的测试随接口
# 收敛移除，改由 R2 driver / raw / Co 测试覆盖同一职责。

# 纯逻辑/值层：net.buffer / net.options / net.load_balancing。
add_executable(Test_net_primitives net_primitives.cc)
target_link_libraries(Test_net_primitives PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_primitives PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case buffer options load_balancing)
    add_test(NAME net.${_case} COMMAND Test_net_primitives ${_case})
    set_tests_properties(net.${_case} PROPERTIES TIMEOUT 60
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

# 独立失败注入可执行文件：纯头部件，不链接 coroutine/runtime。
add_executable(Test_net_allocation net_allocation.cc)
target_include_directories(Test_net_allocation PRIVATE ${PROJECT_SOURCE_DIR})
target_link_libraries(Test_net_allocation PRIVATE Boost::headers)
target_compile_features(Test_net_allocation PRIVATE cxx_std_17)
target_compile_options(Test_net_allocation PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME net.buffer_allocation COMMAND Test_net_allocation)
set_tests_properties(net.buffer_allocation PROPERTIES TIMEOUT 30)

# 原语/身份/Address/NativeSocket（不依赖旧网络内核）。
add_executable(Test_net_foundation net_foundation.cc)
target_link_libraries(Test_net_foundation PRIVATE bbt::infra_net)
target_compile_options(Test_net_foundation PRIVATE -Wall -Wextra -Wpedantic)
target_compile_definitions(Test_net_foundation PRIVATE
    BBT_NET_TEST_DIR="${CMAKE_CURRENT_BINARY_DIR}/runtime")
target_link_options(Test_net_foundation PRIVATE -Wl,--wrap=send)
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
foreach(_case address_ownership socket context native_accept_connect flags_contract context_no_create)
    add_test(NAME net.${_case} COMMAND Test_net_foundation ${_case})
    set_tests_properties(net.${_case} PROPERTIES TIMEOUT 60
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

# R2 Processer driver 私有适配：真实绑定/早到 Wake/预算/槽回滚/同步解绑/Failed。
add_executable(Test_net_reactor_driver reactor_driver.cc)
target_link_libraries(Test_net_reactor_driver PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_reactor_driver PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case bind wake budget rollback unbind failed
        bind_preserve unbind_refusal exception stop_restart)
    add_test(NAME net.reactor_driver_${_case} COMMAND Test_net_reactor_driver ${_case})
    set_tests_properties(net.reactor_driver_${_case} PROPERTIES TIMEOUT 45)
endforeach()

# R2 真实 TCP raw/Co：port0 回环、Accept/Read/Write/Dial、关闭后残留 Conn、
# 有界发送背压与容量等待超时（未提交）。
add_executable(Test_net_r2 r2_tcp.cc)
target_link_libraries(Test_net_r2 PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_r2 PRIVATE -Wall -Wextra -Wpedantic)
target_link_options(Test_net_r2 PRIVATE -rdynamic)
foreach(_case raw_roundtrip co_accept_read_write co_dial close_retained
        write_backpressure write_timeout multi_listener close_waiters_settle
        ring_combined write_fifo_prefix co_read_timeout perf_baseline)
    add_test(NAME net.r2_${_case} COMMAND Test_net_r2 ${_case})
    set_tests_properties(net.r2_${_case} PROPERTIES TIMEOUT 60
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

# API 冻结契约（计划 §7 D1–D6）：公共面守卫寿命、执行域 gate、Options 契约、
# 输出归属、建连单一交付与未交付回收、accept 配置与共存。
# 编译期冻结（公共头不含 State/内部构造/测试屏障/旧等待 API）由
# net.freeze_public_surface 证明；这里只放需要真实资源与时序的用例。
add_executable(Test_net_freeze freeze_contract.cc)
target_link_libraries(Test_net_freeze PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_freeze PRIVATE -Wall -Wextra -Wpedantic)
target_link_options(Test_net_freeze PRIVATE -rdynamic)
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
foreach(_case guard_lifetime co_context_gate options_contract write_output_reuse
        output_preservation connect_delivery connect_failure_cleanup accept_configuration)
    add_test(NAME net.freeze_${_case} COMMAND Test_net_freeze ${_case})
    set_tests_properties(net.freeze_${_case} PROPERTIES TIMEOUT 60
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

# D1 公共面正/负编译门禁：正向 TU 只用公共头；负向 TU 逐项证明被删除/收回的
# 符号真实编译失败。不用 WILL_FAIL（会把任意失败翻成通过）。
add_test(NAME net.freeze_public_surface COMMAND ${CMAKE_COMMAND}
    "-DCXX=${CMAKE_CXX_COMPILER}"
    "-DINFRA_DIR=${PROJECT_SOURCE_DIR}"
    "-DCOROUTINE_DIR=${BBT_COROUTINE_SOURCE_DIR}"
    "-DBOOST_INCLUDE_DIRS=${Boost_INCLUDE_DIRS}"
    "-DWORK_DIR=${CMAKE_CURRENT_BINARY_DIR}/public_surface_probe"
    -P "${CMAKE_CURRENT_SOURCE_DIR}/consumers/expect_public_surface.cmake")
set_tests_properties(net.freeze_public_surface PROPERTIES TIMEOUT 300)

# B4 契约聚焦回归：针对父级处置的真实根因（ring 异常安全/释放通知/write 事务/
# connect 仲裁/Engine Failed/接线/公平）。
add_executable(Test_net_b4_contract b4_contract.cc)
target_link_libraries(Test_net_b4_contract PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_b4_contract PRIVATE -Wall -Wextra -Wpedantic)
target_link_options(Test_net_b4_contract PRIVATE -rdynamic -Wl,--wrap=sendmsg)
foreach(_case ring_alloc_no_consume ring_release_notify hub_early_notify_retained
        write_total_ops_bound write_query_monotonic onwrite_throw_continues
        wait_accepted_existing_out run_preconditions engine_driver_failed
        invalid_options_rejected handoff_full_resume lb_subs fairness
        ring_append_fault_shared_out co_read_append_fault_nonempty_out co_read_idle_no_spin
        staged_close_tail connect_complete_before_wait connect_timeout_recycles_adopt
        connect_hard_cap connect_raw_completion_cleanup lb_least_connections
        socket_options_getsockopt install_failure_fd_baseline run_managed_domain_reject co_read_rapid write_terminal_no_alloc
        write_capacity_after_drain write_capacity_after_partial accept_after_close
        connect_cancel_late_callback fairness_one_sub staged_recv_busy_close
        run_teardown_no_residue co_busy_release_phases co_wouldblock_closed_race
        co_pingpong_strict ring_deferred_backing_reuse ring_deferred_nonblocking_try async_connect_public_fast_complete async_connect_raw_not_waitable waitconnect_add_failure_recycles accept_auth_barriers connect_auth_barriers seal_throw_error_exit install_delay_no_deadline dirty_remove_erase_joint raw_pending_wait_rejected codial_raw_rejected
        waitconnect_single_receiver staged_close_before_release install_wait_driver_failed_exit seal_wait_driver_failed_exit request_close_read_terminal_order
        stage_detached_notify_fault_no_duplicate seal_notify_fault_callbacks
        waitconnect_claim_create_failure wakehub_capacity_bounded
        co_read_hub_full_bounded dirty_lifetime_across_removeconn submit_gate_paused_before_post codial_fast_complete codial_timeout_recycles
        codial_cancel_post_failure write_terminal_interleaved_no_alloc
        raw_busy_deferred_onread seal_post_failure_retries seal_delay_beyond_deadline callback_auth_barriers
        request_close_write_error_intent request_close_write_readiness sealed_write_error_read_publication)
    add_test(NAME net.b4_${_case} COMMAND Test_net_b4_contract ${_case})
    set_tests_properties(net.b4_${_case} PROPERTIES TIMEOUT 45)
endforeach()

# 仓外式最小消费：只依赖新 target，不包含旧 infra 头。
add_executable(net_foundation_consumer
    ${PROJECT_SOURCE_DIR}/examples/net_foundation_consumer/main.cc)
target_link_libraries(net_foundation_consumer PRIVATE bbt::infra_net)
target_compile_options(net_foundation_consumer PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME net.consumer COMMAND net_foundation_consumer)
set_tests_properties(net.consumer PROPERTIES TIMEOUT 30)

# network-only 的 CTest 不能退化成零测试；目标名也不应带入旧模块。
if(NOT TARGET Test_net_foundation OR NOT TARGET net_foundation_consumer OR NOT TARGET Test_net_r2)
    message(FATAL_ERROR "network-only 缺少 R2 测试/消费目标")
endif()

# L2 回归：真实 coroutine-first parent consumer——先 add_subdirectory 固定上游再接
# infra（命中 NetworkOnly 复用分支），只 link bbt::infra_net 消费 CoObject/NativeSocket。
set(_net_consumers "${CMAKE_CURRENT_SOURCE_DIR}/consumers")
set(_net_co_first_build "${CMAKE_CURRENT_BINARY_DIR}/consumer-coroutine-first")
set(_net_co_first_configure_args
    -S "${_net_consumers}/coroutine_first"
    -B "${_net_co_first_build}"
    "-DNET_CONSUMER_COROUTINE_DIR=${BBT_COROUTINE_SOURCE_DIR}"
    "-DNET_CONSUMER_INFRA_DIR=${PROJECT_SOURCE_DIR}"
    "-DCMAKE_CXX_COMPILER_LAUNCHER=${CMAKE_CXX_COMPILER_LAUNCHER}"
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON)
if(CMAKE_BUILD_TYPE)
    list(APPEND _net_co_first_configure_args "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}")
endif()
add_test(NAME net.consumer_coroutine_first_configure
    COMMAND ${CMAKE_COMMAND} ${_net_co_first_configure_args})
add_test(NAME net.consumer_coroutine_first_build
    COMMAND ${CMAKE_COMMAND} --build "${_net_co_first_build}"
        --target consume_net_coroutine_first --parallel 2)
add_test(NAME net.consumer_coroutine_first_run
    COMMAND "${_net_co_first_build}/consume_net_coroutine_first")
set_tests_properties(net.consumer_coroutine_first_configure PROPERTIES TIMEOUT 300)
set_tests_properties(net.consumer_coroutine_first_build PROPERTIES
    DEPENDS net.consumer_coroutine_first_configure TIMEOUT 300)
set_tests_properties(net.consumer_coroutine_first_run PROPERTIES
    DEPENDS net.consumer_coroutine_first_build TIMEOUT 60)

# 负向：复用 target 的声明来源与真实 SOURCE_DIR 不一致时，configure 必须明确失败。
add_test(NAME net.consumer_source_dir_mismatch COMMAND ${CMAKE_COMMAND}
    "-DBUILD_ROOT=${CMAKE_CURRENT_BINARY_DIR}"
    "-DGENERATOR=${CMAKE_GENERATOR}"
    "-DNET_CONSUMER_COROUTINE_DIR=${BBT_COROUTINE_SOURCE_DIR}"
    "-DNET_CONSUMER_INFRA_DIR=${PROJECT_SOURCE_DIR}"
    "-DNET_CONSUMER_STUB_DIR=${_net_consumers}/source_dir_mismatch/stub"
    -P "${_net_consumers}/expect_configure_failure.cmake")
set_tests_properties(net.consumer_source_dir_mismatch PROPERTIES TIMEOUT 300)

# 高层 GNet UDP 本轮暂缓（用户 2026-10-17 裁定）：Udp.hpp 是显式使用时编译拒绝。
add_test(NAME net.udp_high_level_compile_reject COMMAND ${CMAKE_COMMAND}
    "-DCXX=${CMAKE_CXX_COMPILER}"
    "-DINFRA_DIR=${PROJECT_SOURCE_DIR}"
    "-DCOROUTINE_DIR=${BBT_COROUTINE_SOURCE_DIR}"
    "-DBOOST_INCLUDE_DIRS=${Boost_INCLUDE_DIRS}"
    "-DWORK_DIR=${CMAKE_CURRENT_BINARY_DIR}/udp_reject_probe"
    -P "${_net_consumers}/expect_udp_compile.cmake")
set_tests_properties(net.udp_high_level_compile_reject PROPERTIES TIMEOUT 120)

# --- T1 coroutine pin 负向/正向 ctest（cmake -P 级，轻量不编译）--------------
# pin 唯一真源 cmake/coroutine.sha → CoroutinePin.cmake 读取；本组覆盖：
#   命中（positive）、错误 SHA（wrong_sha）、tracked 脏（dirty）、消费面内
#   untracked 源码输入（untracked_src）、非 git 树（no_git）、伪造 manifest
#   （manifest_rejected——自我声明不是来源证据）、普通路径复用 target 来源
#   不一致（reuse_mismatch）与同源放行（reuse_ok）。
# 所有 stub 树在 build 临时目录构造，不触碰真实 coroutine 检出。
set(_pin_module "${PROJECT_SOURCE_DIR}/cmake/CoroutinePin.cmake")
set(_pin_probe_root "${CMAKE_CURRENT_BINARY_DIR}/pin-probes")
foreach(_case positive wrong_sha dirty no_git untracked_src manifest_rejected)
    add_test(NAME net.pin_${_case} COMMAND ${CMAKE_COMMAND}
        "-DPIN_CASE=${_case}"
        "-DPIN_MODULE=${_pin_module}"
        "-DWORK_DIR=${_pin_probe_root}/${_case}"
        "-DREAL_COROUTINE_DIR=${BBT_COROUTINE_SOURCE_DIR}"
        -P "${_net_consumers}/pin_expect.cmake")
    set_tests_properties(net.pin_${_case} PROPERTIES TIMEOUT 120)
endforeach()
# 普通路径「已有 bbt_coroutine target 来源复核」的正负两侧（configure 级）。
foreach(_kind mismatch ok)
    add_test(NAME net.pin_reuse_${_kind} COMMAND ${CMAKE_COMMAND}
        "-DBUILD_ROOT=${CMAKE_CURRENT_BINARY_DIR}"
        "-DGENERATOR=${CMAKE_GENERATOR}"
        "-DPIN_MODULE=${_pin_module}"
        "-DREAL_COROUTINE_DIR=${BBT_COROUTINE_SOURCE_DIR}"
        "-DSTUB_DIR=${_net_consumers}/source_dir_mismatch/stub"
        "-DPIN_KIND=${_kind}"
        -P "${_net_consumers}/expect_pin_reuse.cmake")
    set_tests_properties(net.pin_reuse_${_kind} PROPERTIES TIMEOUT 300)
endforeach()

# --- infra-perf 闭环（可选，默认 OFF）：纯 TCP echo 基准 harness。---------------
# server(raw/co 双模式) 与 loadgen(独立进程参照实现，不链接被测库)；由
# scripts/bench/driver.py 跨进程编排。不进 ctest 默认负载、不进全量构建目标；
# 仅 -DBBT_INFRA_PERF_BENCH=ON 时注册目标与 net.perf_* 冒烟。正式计时由
# 验证者经 driver.py 驱动；这里只保证目标可编译+短冒烟成立。
option(BBT_INFRA_PERF_BENCH "注册 infra-perf 纯 TCP 基准 harness（默认 OFF）" OFF)
if(BBT_INFRA_PERF_BENCH)
    add_executable(bbt_net_tcp_echo_server perf/tcp_echo_server.cc)
    target_include_directories(bbt_net_tcp_echo_server PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/perf)
    target_link_libraries(bbt_net_tcp_echo_server PRIVATE
        bbt::infra_net Boost::headers)
    target_compile_features(bbt_net_tcp_echo_server PRIVATE cxx_std_17)
    target_compile_options(bbt_net_tcp_echo_server PRIVATE -Wall -Wextra -Wpedantic)

    add_executable(bbt_net_tcp_loadgen perf/tcp_loadgen.cc)
    target_include_directories(bbt_net_tcp_loadgen PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/perf)
    target_compile_features(bbt_net_tcp_loadgen PRIVATE cxx_std_17)
    target_compile_options(bbt_net_tcp_loadgen PRIVATE -Wall -Wextra -Wpedantic)
    find_package(Threads REQUIRED)
    target_link_libraries(bbt_net_tcp_loadgen PRIVATE Threads::Threads)

    find_package(Python3 COMPONENTS Interpreter REQUIRED)
    # 冒烟只证明进程能起/收/出 JSON：driver.py 编排 + 每进程硬超时 + 协议校验，
    # 不做正式计时结论。raw 与 co 各一条；driver.py 内部另有 watchdog。
    foreach(_mode raw co)
        add_test(NAME net.perf_smoke_${_mode}
            COMMAND ${Python3_EXECUTABLE}
                "${CMAKE_CURRENT_SOURCE_DIR}/../../scripts/bench/driver.py"
                --mode ${_mode} --workload pingpong
                --server-bin "$<TARGET_FILE:bbt_net_tcp_echo_server>"
                --loadgen-bin "$<TARGET_FILE:bbt_net_tcp_loadgen>"
                --clients 2 --payload-bytes 64
                --duration-ms 800 --warmup-ms 100 --timeout-ms 15000)
        set_tests_properties(net.perf_smoke_${_mode} PROPERTIES TIMEOUT 60)
    endforeach()
endif()

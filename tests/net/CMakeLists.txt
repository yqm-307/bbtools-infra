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
# 早到交付、等待返回非 Completed 的三个退出路径、raw 投递失败/Idle/真实建连失败
# 与 CoAccept 阶段矩阵都经 detail/EngineTestAccess 的一次性注入确定性制造，
# 不靠串行重跑碰运气；每条负例都带正对照。
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

# A3/A4 成员化 ICoConnection 写/关闭观察：AsyncWrite 拒绝票、成员 Write
# （等容量→接纳→等本地完成）、WriteOperation::Wait、WaitClosed close hub、
# Status 投影、RequestClose 默认 reason 下未发完写非成功、facade 释放后 op
# 可查、driver Failed 唤醒。所有等待挂起由 ConnectionState 的
# Write/CloseAddHookForTest 屏障证明后才触发终态，不靠 sleep。
add_executable(Test_net_ico_write_close ico_write_close.cc)
target_link_libraries(Test_net_ico_write_close PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_ico_write_close PRIVATE -Wall -Wextra -Wpedantic)
target_link_options(Test_net_ico_write_close PRIVATE -rdynamic)
foreach(_case member_write_done async_write_reject_ticket
        write_unsubmitted_timeout write_accepted_timeout write_accepted_cancel
        op_wait_partial_error write_zero_len close_default_reason
        facade_released_op_queryable waitclosed_driver_failed
        waitclosed_late_no_miss status_projection)
    add_test(NAME net.ico_${_case} COMMAND Test_net_ico_write_close ${_case})
    set_tests_properties(net.ico_${_case} PROPERTIES TIMEOUT 60
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

# 物理回收观察句柄契约：ReclaimReceipt / Connection::Released / 带句柄 CoDial。
# 未交付窗口由 delivery-delay seam 与服务端真实 accept 证明，不靠固定 sleep。
add_executable(Test_net_reclaim_receipt reclaim_receipt.cc)
target_link_libraries(Test_net_reclaim_receipt PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_reclaim_receipt PRIVATE -Wall -Wextra -Wpedantic)
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
foreach(_case receipt_invalid_default codial_success_close_released
        codial_timeout_undelivered dial_refused_released dial_pre_submit_rejections
        released_not_before_physical_close engine_stop_releases driver_failed_releases driver_failed_live_fd)
    add_test(NAME net.reclaim_${_case} COMMAND Test_net_reclaim_receipt ${_case})
    set_tests_properties(net.reclaim_${_case} PROPERTIES TIMEOUT 60
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

# 接纳独立封口（net.stop_*）：真实 listener 物理关闭后才 stopped、已交付连接
# 双向读写继续、CoAccept 被唤醒并取消、Run 前/Starting 封口不重开、重复并发、
# client-only 拨号继续、Failed 与最终 RequestClose 收口、未交付 handoff 回收。
add_executable(Test_net_accept_stop net_accept_stop.cc)
target_link_libraries(Test_net_accept_stop PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_accept_stop PRIVATE -Wall -Wextra -Wpedantic)
target_link_options(Test_net_accept_stop PRIVATE -rdynamic)
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
foreach(_case stop_running_delivered_conn_rw stop_wakes_coaccept_waiters
        stop_late_coaccept_rejected stop_before_run_no_listener
        stop_client_only_dial_continues stop_client_only_before_run stop_post_failure_retry stop_repeat_and_concurrent
        stop_during_starting_race stop_with_driver_failed
        stop_undelivered_handoff_reclaimed)
    add_test(NAME net.stop_${_case} COMMAND Test_net_accept_stop ${_case})
    set_tests_properties(net.stop_${_case} PROPERTIES TIMEOUT 120
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

# F0 回归：已 accept 但未交付的 fd 在停止竞态下必须被物理关闭（--wrap=syscall）。
# 独立 target，避免将 wrapped syscall 带入其它 net 测试进程。
add_executable(net_accept_stop_fd net_accept_stop_fd.cc)
target_link_libraries(net_accept_stop_fd PRIVATE bbt::infra_net Boost::headers)
target_compile_options(net_accept_stop_fd PRIVATE -Wall -Wextra -Wpedantic)
target_link_options(net_accept_stop_fd PRIVATE -rdynamic -Wl,--wrap=syscall)
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
add_test(NAME net.stop_fd_regression COMMAND net_accept_stop_fd)
set_tests_properties(net.stop_fd_regression PROPERTIES TIMEOUT 120
    WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")

# 接收路径确定性交错（Boost.Test + 产品内置接缝）：
# 验证 pending 未合入前的 recv 不得越过它。用 EngineTestAccess::SetAppendBusyHook
# 在 TryAppend Busy 与 AppendDeferred 之间取得控制权，取代此前仓外复制 Engine.cc
# 的单对象替换手法。接缝为空 function 时产品行为零差异（既有 103 项回归仍全过）。
# Boost.Test 用 included（header-only），不引入 libboost_unit_test_framework。
add_executable(Test_net_append_busy_hook detail/append_busy_hook.cc)
target_link_libraries(Test_net_append_busy_hook PRIVATE bbt::infra_net Boost::headers)
target_include_directories(Test_net_append_busy_hook PRIVATE ${PROJECT_SOURCE_DIR})
target_compile_features(Test_net_append_busy_hook PRIVATE cxx_std_17)
target_compile_options(Test_net_append_busy_hook PRIVATE -Wall -Wextra -Wpedantic)
target_link_options(Test_net_append_busy_hook PRIVATE -rdynamic)
foreach(_case pending_not_flushed_before_next_recv pending_without_next_recv_is_ordered
              pending_pause_recovers_at_low_water)
    add_test(NAME net.${_case} COMMAND Test_net_append_busy_hook --run_test=net_append_busy_hook/${_case})
    set_tests_properties(net.${_case} PROPERTIES TIMEOUT 90)
endforeach()

# A2 pop-all 移交契约（Boost.Test + detail 接缝）：
# TryPopAll 的绕环/pending/detached 分段移交、Busy 零消费、额度即时释放、
# 分配失败不变、尾先交尽再 Eof，以及 CoTcpConnection::Read 的 gate/挂起/
# 超时/EOF 路径。自定义 operator new 仅本目标 thread_local 注入分配失败。
add_executable(Test_net_popall detail/pop_all.cc)
target_link_libraries(Test_net_popall PRIVATE bbt::infra_net Boost::headers)
target_include_directories(Test_net_popall PRIVATE ${PROJECT_SOURCE_DIR})
target_compile_features(Test_net_popall PRIVATE cxx_std_17)
target_compile_options(Test_net_popall PRIVATE -Wall -Wextra -Wpedantic)
target_link_options(Test_net_popall PRIVATE -rdynamic)
foreach(_case wrap_two_segments_one_backing pending_moves_as_own_segment
              detached_tail_after_pending busy_contention_zero_consume
              quota_releases_immediately alloc_failure_keeps_ring_and_quota
              tail_delivered_before_eof no_pollute_after_pop
              read_gate_rejects_non_coroutine read_waits_and_delivers_across_wakeup
              read_timeout_does_not_consume read_buffer_identity_and_eof_drain
              multi_reader_linearizes zero_payload_copy_probe)
    add_test(NAME net.popall_${_case}
        COMMAND Test_net_popall --run_test=net_popall/${_case})
    set_tests_properties(net.popall_${_case} PROPERTIES TIMEOUT 45)
endforeach()

# infra-ico-conn 独立审查 6 个 confirmed defect 的修复后契约断言（原
# 「缺陷存在」断言已翻转）：post-pop OOM 不丢字节、append OOM 不抛出、
# SSO 移交不复制、拷贝删除、公共构造收回、CoAccept reclaim 同事实源。
add_executable(Test_net_repro_buffer_oom detail/repro_buffer_oom.cc)
target_link_libraries(Test_net_repro_buffer_oom PRIVATE bbt::infra_net Boost::headers)
target_include_directories(Test_net_repro_buffer_oom PRIVATE ${PROJECT_SOURCE_DIR})
target_compile_features(Test_net_repro_buffer_oom PRIVATE cxx_std_17)
target_compile_options(Test_net_repro_buffer_oom PRIVATE -Wall -Wextra -Wpedantic)
target_link_options(Test_net_repro_buffer_oom PRIVATE -rdynamic)
foreach(_case d1_read_post_pop_oom_no_loss d2_grant_pop_append_oom_no_throw
              d3_sso_payload_not_copied d4_copy_assign_deleted
              d5_public_ctor_forge_denied d6_coaccept_reclaim_valid_ticket)
    add_test(NAME net.repro_oom_${_case}
        COMMAND Test_net_repro_buffer_oom --run_test=net_repro_buffer_oom/${_case})
    set_tests_properties(net.repro_oom_${_case} PROPERTIES TIMEOUT 60)
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

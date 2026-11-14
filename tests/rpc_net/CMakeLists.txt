# rpc_net 契约测试：单可执行 + 用例名分发（case-dispatch 约定见
# tests/README.md）。只保留 V2 单一会话面（SessionFrame profile 2）。

# v2 SessionFrame profile 2 值层/wire/分帧与无状态帧协议接缝（纯字节层，
# 不连网络）。
add_executable(Test_rpc_v2_wire Test_rpc_v2_wire.cc)
target_link_libraries(Test_rpc_v2_wire PRIVATE bbt::infra_rpc_net Boost::headers)
target_compile_options(Test_rpc_v2_wire PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case v2_roundtrip v2_reject v2_malformed v2_framing v2_values v2_protocol_seam
        response_from_request)
    add_test(NAME rpc.net.${_case} COMMAND Test_rpc_v2_wire ${_case})
    set_tests_properties(rpc.net.${_case} PROPERTIES TIMEOUT 30 LABELS "unit")
endforeach()

# v2 AppSession：真实 loopback TCP 双端（每进程 Engine+Client owner+Server
# 同 owner），握手/epoch、Call/Send、单 FIFO admission、lane 隔离、关闭、
# 客户端输入边界/取消/畸形回复。
add_executable(Test_rpc_v2_session Test_rpc_v2_session.cc)
target_link_libraries(Test_rpc_v2_session PRIVATE bbt::infra_rpc_net Boost::headers)
target_compile_options(Test_rpc_v2_session PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case v2s_roundtrip v2s_registered_route v2s_bidirectional v2s_send_acceptance
        v2s_ordering v2s_ordering_ledger v2s_close_negative_timeout v2s_server_cross_engine
        v2s_overload v2s_lane_fairness v2s_disconnect v2s_deadline
        v2s_handshake_raw v2s_handshake_coalesced
        v2s_handshake_coalesced_big
        v2s_handshake_coalesced_big_pinned
        v2s_handshake_stash_tail
        v2s_handshake_prefix_reject v2s_close_drain v2s_close_reason
        v2s_close_reason_pending v2s_close_graceful_timeout v2s_close_self_wait
        v2s_co_start_roundtrip v2s_co_start_close_race v2s_co_start_registration_rollback
        v2s_admission_inflight v2s_admission_reject
        v2s_admission_bytes v2s_admission_sealed
        v2s_reply_write_deferred v2s_reply_write_fail_close
        v2s_reply_write_budget v2s_reply_corr_mismatch
        v2s_close_released v2s_close_handshake v2s_close_no_publish
        v2s_quota_peers v2s_simultaneous_dial v2s_epoch_reconnect
        v2s_quota_establish_dial v2s_quota_release_retry
        v2s_never_started_drain_close v2s_queue_full_drain_stats
        v2s_hook_state v2s_hook_pause_deadline
        v2s_hook_pause_cancel_close
        v2s_hook_outbound_throw v2s_hook_write_throw
        v2s_shutdown_timeout_draining
        v2s_shutdown_interrupted_by_close
        v2s_close_inflight_reason
        v2s_client_input v2s_cancel_post_commit v2s_malformed_reply
        v2s_send_no_acceptance_drain write_terminal_once)
    add_test(NAME rpc.net.${_case} COMMAND Test_rpc_v2_session ${_case})
    set_tests_properties(rpc.net.${_case} PROPERTIES TIMEOUT 60 LABELS "rpc_net")
endforeach()
set_tests_properties(rpc.net.write_terminal_once PROPERTIES LABELS "integration")

# Co 门禁、关闭回调、非拥有观测与 wire 终态（Boost.Test）。
add_executable(Test_rpc_co_lifecycle Test_rpc_co_lifecycle.cc)
target_link_libraries(Test_rpc_co_lifecycle PRIVATE bbt::infra_rpc_net Boost::headers)
target_compile_options(Test_rpc_co_lifecycle PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME rpc.net.co_lifecycle COMMAND Test_rpc_co_lifecycle)
set_tests_properties(rpc.net.co_lifecycle PROPERTIES TIMEOUT 30 LABELS "unit")

# V1 退役负向门禁：正/负 TU 对照 + 源码树/构建图事实（旧头、旧限定名、
# 旧 proto、旧 target/开关均不可消费）。与 tests/net 的公共面门禁同一范式。
add_test(NAME rpc.net.v1_absent COMMAND ${CMAKE_COMMAND}
    "-DCXX=${CMAKE_CXX_COMPILER}"
    "-DINFRA_DIR=${PROJECT_SOURCE_DIR}"
    "-DCOROUTINE_DIR=${BBT_COROUTINE_SOURCE_DIR}"
    "-DBOOST_INCLUDE_DIRS=${Boost_INCLUDE_DIRS}"
    "-DWORK_DIR=${CMAKE_CURRENT_BINARY_DIR}/v1_absent_probe"
    "-DBUILD_DIR=${CMAKE_BINARY_DIR}"
    -P "${CMAKE_CURRENT_SOURCE_DIR}/consumers/expect_rpc_v2_surface.cmake")
set_tests_properties(rpc.net.v1_absent PROPERTIES TIMEOUT 300 LABELS "integration")

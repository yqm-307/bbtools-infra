# rpc_net 契约测试：单可执行 + 用例名分发（case-dispatch 约定见
# tests/README.md）。
add_executable(Test_rpc_net_wire Test_rpc_net_wire.cc)
target_link_libraries(Test_rpc_net_wire PRIVATE bbt::infra_rpc_net Boost::headers)
target_compile_options(Test_rpc_net_wire PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case wire_roundtrip wire_reject wire_malformed wire_request_phase)
    add_test(NAME rpc.net.${_case} COMMAND Test_rpc_net_wire ${_case})
    set_tests_properties(rpc.net.${_case} PROPERTIES TIMEOUT 30 LABELS "unit")
endforeach()

# v2 SessionFrame profile 2 值层/wire/分帧契约（纯字节层，不连网络）。
add_executable(Test_rpc_v2_wire Test_rpc_v2_wire.cc)
target_link_libraries(Test_rpc_v2_wire PRIVATE bbt::infra_rpc_net Boost::headers)
target_compile_options(Test_rpc_v2_wire PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case v2_roundtrip v2_reject v2_malformed v2_framing v2_values)
    add_test(NAME rpc.net.${_case} COMMAND Test_rpc_v2_wire ${_case})
    set_tests_properties(rpc.net.${_case} PROPERTIES TIMEOUT 30 LABELS "unit")
endforeach()

# v2 AppSession：真实 loopback TCP 双端（每进程 Engine+Client owner+Server
# 同 owner），握手/epoch、Call/Send、单 FIFO admission、lane 隔离、关闭。
add_executable(Test_rpc_v2_session Test_rpc_v2_session.cc)
target_link_libraries(Test_rpc_v2_session PRIVATE bbt::infra_rpc_net Boost::headers)
target_compile_options(Test_rpc_v2_session PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case v2s_roundtrip v2s_bidirectional v2s_send_acceptance
        v2s_ordering v2s_overload v2s_lane_fairness v2s_disconnect v2s_deadline
        v2s_handshake_raw v2s_handshake_coalesced
        v2s_handshake_coalesced_big
        v2s_handshake_coalesced_big_pinned
        v2s_handshake_stash_tail
        v2s_handshake_prefix_reject v2s_close_drain
        v2s_admission_inflight v2s_admission_reject
        v2s_admission_bytes v2s_admission_sealed
        v2s_reply_write_deferred v2s_reply_write_fail_close
        v2s_reply_write_budget v2s_reply_corr_mismatch
        v2s_close_released v2s_close_handshake v2s_close_no_publish
        v2s_quota_peers v2s_simultaneous_dial v2s_epoch_reconnect
        v2s_quota_establish_dial v2s_quota_release_retry
        v2s_never_started_drain_close v2s_queue_full_drain_stats)
    add_test(NAME rpc.net.${_case} COMMAND Test_rpc_v2_session ${_case})
    set_tests_properties(rpc.net.${_case} PROPERTIES TIMEOUT 60 LABELS "rpc_net")
endforeach()

add_executable(Test_rpc_net_roundtrip Test_rpc_net_roundtrip.cc)
target_link_libraries(Test_rpc_net_roundtrip PRIVATE bbt::infra_rpc_net Boost::headers)
target_compile_options(Test_rpc_net_roundtrip PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case roundtrip association budget_local_min reuse transport_defaults)
    add_test(NAME rpc.net.${_case} COMMAND Test_rpc_net_roundtrip ${_case})
    set_tests_properties(rpc.net.${_case} PROPERTIES TIMEOUT 60 LABELS "unit")
endforeach()

add_executable(Test_rpc_net_faults Test_rpc_net_faults.cc)
target_link_libraries(Test_rpc_net_faults PRIVATE bbt::infra_rpc_net Boost::headers)
target_compile_options(Test_rpc_net_faults PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case client_input transport_errors unknown_outcome cancelled
        cancel_post_commit malformed_reply server_malformed)
    add_test(NAME rpc.net.${_case} COMMAND Test_rpc_net_faults ${_case})
    set_tests_properties(rpc.net.${_case} PROPERTIES TIMEOUT 60 LABELS "unit")
endforeach()

add_executable(Test_rpc_net_lifetime Test_rpc_net_lifetime.cc)
target_link_libraries(Test_rpc_net_lifetime PRIVATE bbt::infra_rpc_net Boost::headers)
target_compile_options(Test_rpc_net_lifetime PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case client_close server_lifecycle inflight_authorization
        close_drops_inflight released_client_handle compat_factory
        inbound_cycle_release destroy_during_inflight)
    add_test(NAME rpc.net.${_case} COMMAND Test_rpc_net_lifetime ${_case})
    set_tests_properties(rpc.net.${_case} PROPERTIES TIMEOUT 60 LABELS "unit")
endforeach()

# Co 门禁、关闭回调、非拥有观测与 wire 终态（Boost.Test）。
add_executable(Test_rpc_co_lifecycle Test_rpc_co_lifecycle.cc)
target_link_libraries(Test_rpc_co_lifecycle PRIVATE bbt::infra_rpc_net Boost::headers)
target_compile_options(Test_rpc_co_lifecycle PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME rpc.net.co_lifecycle COMMAND Test_rpc_co_lifecycle)
set_tests_properties(rpc.net.co_lifecycle PROPERTIES TIMEOUT 30 LABELS "unit")

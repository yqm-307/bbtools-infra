# rpc_net 契约测试：单可执行 + 用例名分发（case-dispatch 约定见
# tests/README.md）。
add_executable(Test_rpc_net_wire Test_rpc_net_wire.cc)
target_link_libraries(Test_rpc_net_wire PRIVATE bbt::infra_rpc_net Boost::headers)
target_compile_options(Test_rpc_net_wire PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case wire_roundtrip wire_reject wire_malformed wire_request_phase)
    add_test(NAME rpc.net.${_case} COMMAND Test_rpc_net_wire ${_case})
    set_tests_properties(rpc.net.${_case} PROPERTIES TIMEOUT 30 LABELS "unit")
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
        close_drops_inflight released_client_handle)
    add_test(NAME rpc.net.${_case} COMMAND Test_rpc_net_lifetime ${_case})
    set_tests_properties(rpc.net.${_case} PROPERTIES TIMEOUT 60 LABELS "unit")
endforeach()

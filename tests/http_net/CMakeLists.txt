# http_net 契约测试：单可执行 + 用例名分发（case-dispatch 约定见
# tests/README.md；命名与 label 约定见 docs/conventions.md §6）。
add_executable(Test_http_net_contract Test_http_net_contract.cc)
target_link_libraries(Test_http_net_contract PRIVATE bbt::infra_http_net)
target_compile_options(Test_http_net_contract PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case get_post_reuse pool_queue_timeout close_cancel_race bad_stream_retire
        keepalive_expiry omitted_call_options client_close_reason client_shutdown_drain
        client_shutdown_interrupted_by_close close_options_co lease_ownership)
    add_test(NAME http.net.${_case} COMMAND Test_http_net_contract ${_case})
    set_tests_properties(http.net.${_case} PROPERTIES TIMEOUT 30 LABELS "unit")
endforeach()

add_executable(Test_http_net_regressions Test_http_net_regressions.cc)
target_link_libraries(Test_http_net_regressions PRIVATE bbt::infra_http_net)
target_compile_options(Test_http_net_regressions PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case fragment_header fragment_body chunked head_interim eof_truncated body_limit quotas
        input_boundary preexpired_cancelled phase_timeouts per_read_total cross_origin
        handoff_concurrency physical_budget physical_budget_two physical_cancel abandon_holds_inflight)
    add_test(NAME http.net.${_case} COMMAND Test_http_net_regressions ${_case})
    set_tests_properties(http.net.${_case} PROPERTIES TIMEOUT 30 LABELS "unit")
endforeach()

add_executable(Test_http_net_server Test_http_net_server.cc)
target_link_libraries(Test_http_net_server PRIVATE bbt::infra_http_net)
target_compile_options(Test_http_net_server PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case server_wire server_stop_authorization server_reply_drain server_close_timeout
        server_shutdown_drain server_shutdown_timeout server_shutdown_interrupted_by_close
        server_security server_incoming_deadline server_start_capacity server_physical_capacity server_inflight_limit
        server_pipeline_order server_pipeline_close_immediate server_pipeline_parse_failure server_pipeline_disconnect)
    add_test(NAME http.net.${_case} COMMAND Test_http_net_server ${_case})
    set_tests_properties(http.net.${_case} PROPERTIES TIMEOUT 30 LABELS "unit")
endforeach()

# 路由装配契约（Boost.Test + 自包 main 进程退出边界；Boost.Test 用 included，不引入
# libboost_unit_test_framework）。round_trip 走真实 loopback Server，其余直接驱动
# Build 产出的 ServerHandler。
add_executable(Test_http_router Test_http_router.cc)
target_link_libraries(Test_http_router PRIVATE bbt::infra_http_net Boost::headers)
target_compile_options(Test_http_router PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case matching method_fallback params_per_method target_rules middleware
        build_snapshot config_errors next_twice route_decorator_and_dependencies unified_error_middleware
        framework_wiring group_param round_trip)
    add_test(NAME http.router.${_case} COMMAND Test_http_router --run_test=http_router/${_case})
    set_tests_properties(http.router.${_case} PROPERTIES TIMEOUT 60 LABELS "unit")
endforeach()
set_tests_properties(http.router.matching http.router.framework_wiring http.router.round_trip
    http.router.route_decorator_and_dependencies PROPERTIES LABELS "smoke;unit")

add_executable(Test_http_net_streaming Test_http_net_streaming.cc)
target_link_libraries(Test_http_net_streaming PRIVATE bbt::infra_http_net)
target_compile_options(Test_http_net_streaming PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case stream_wire stream_variant_buffered stream_disallowed stream_commit_boundary
        stream_peer_abort stream_slow_reader stream_lifecycle stream_lifetime stream_start_deadline
        stream_close_in_handler response_header_parity stream_pipeline_order)
    add_test(NAME http.net.${_case} COMMAND Test_http_net_streaming ${_case})
    set_tests_properties(http.net.${_case} PROPERTIES TIMEOUT 30 LABELS "unit")
endforeach()

# 停止封口回归：任务登记窗口与 Stop/WaitStopped 竞争（detail 私有注入
# SetRegistrationBarrierOnce），迟到 Start 不得预约任务或复活。
add_executable(Test_http_net_stop_regressions Test_http_net_stop_regressions.cc)
target_link_libraries(Test_http_net_stop_regressions PRIVATE bbt::infra_http_net Boost::headers)
target_compile_options(Test_http_net_stop_regressions PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME http.net.stop_regressions COMMAND Test_http_net_stop_regressions)
set_tests_properties(http.net.stop_regressions PROPERTIES TIMEOUT 60 LABELS "unit")

set_tests_properties(http.net.get_post_reuse http.net.close_cancel_race http.net.client_shutdown_drain http.net.server_wire http.net.server_reply_drain http.net.server_shutdown_drain http.net.server_inflight_limit PROPERTIES LABELS "smoke;unit")

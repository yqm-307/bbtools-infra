# http_net 契约测试：单可执行 + 用例名分发（case-dispatch 约定见
# tests/README.md；命名与 label 约定见 docs/conventions.md §6）。
add_executable(Test_http_net_contract Test_http_net_contract.cc)
target_link_libraries(Test_http_net_contract PRIVATE bbt::infra_http_net)
target_compile_options(Test_http_net_contract PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case get_post_reuse pool_queue_timeout close_cancel_race bad_stream_retire
        keepalive_expiry omitted_call_options)
    add_test(NAME http.net.${_case} COMMAND Test_http_net_contract ${_case})
    set_tests_properties(http.net.${_case} PROPERTIES TIMEOUT 30 LABELS "unit")
endforeach()

add_executable(Test_http_net_regressions Test_http_net_regressions.cc)
target_link_libraries(Test_http_net_regressions PRIVATE bbt::infra_http_net)
target_compile_options(Test_http_net_regressions PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case fragment_header fragment_body chunked head_interim eof_truncated body_limit
        input_boundary preexpired_cancelled phase_timeouts per_read_total cross_origin
        handoff_concurrency physical_budget physical_budget_two physical_cancel)
    add_test(NAME http.net.${_case} COMMAND Test_http_net_regressions ${_case})
    set_tests_properties(http.net.${_case} PROPERTIES TIMEOUT 30 LABELS "unit")
endforeach()

add_executable(Test_http_net_server Test_http_net_server.cc)
target_link_libraries(Test_http_net_server PRIVATE bbt::infra_http_net)
target_compile_options(Test_http_net_server PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case server_wire server_stop_authorization server_reply_drain server_close_timeout
        server_security server_incoming_deadline server_start_capacity server_physical_capacity server_inflight_limit)
    add_test(NAME http.net.${_case} COMMAND Test_http_net_server ${_case})
    set_tests_properties(http.net.${_case} PROPERTIES TIMEOUT 30 LABELS "unit")
endforeach()

# 停止封口回归：Server::Submit 停止封口后不再预约新任务，TaskLease 未计数
# 回退不下溢（--wrap=_Znwm）。
# 独立 target，避免将 wrapped operator new 带入其它 http 测试进程。
add_executable(Test_http_net_stop_regressions Test_http_net_stop_regressions.cc)
target_link_libraries(Test_http_net_stop_regressions PRIVATE bbt::infra_http_net Boost::headers)
target_compile_options(Test_http_net_stop_regressions PRIVATE -Wall -Wextra -Wpedantic)
target_link_options(Test_http_net_stop_regressions PRIVATE -rdynamic -Wl,--wrap=_Znwm)
add_test(NAME http.net.stop_regressions COMMAND Test_http_net_stop_regressions)
set_tests_properties(http.net.stop_regressions PROPERTIES TIMEOUT 60 LABELS "unit")

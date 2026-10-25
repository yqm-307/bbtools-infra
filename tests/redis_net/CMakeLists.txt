add_executable(Test_redis_net_contract Test_redis_net_contract.cc)
target_link_libraries(Test_redis_net_contract PRIVATE bbt::infra_redis_net hiredis::hiredis)
target_compile_options(Test_redis_net_contract PRIVATE -Wall -Wextra -Wpedantic)
# 仅测试可执行文件观察 hiredis reader 寿命/分帧和注入 Feed 失败；无公共测试 API。
target_link_options(Test_redis_net_contract PRIVATE
    -Wl,--wrap=redisReaderCreate -Wl,--wrap=redisReaderFree
    -Wl,--wrap=redisReaderGetReply -Wl,--wrap=redisReaderFeed)
foreach(_case connect_failure_owned close_invalidates_immediately close_idempotent
        close_during_connect close_pending_command_once handle_destroy_late_callback
        shared_engine_isolation host_drain_no_residue set_get_remote_error
        fragmented_reply unknown_disconnect framing_and_faults)
    add_test(NAME redis.net.${_case} COMMAND Test_redis_net_contract ${_case})
    set_tests_properties(redis.net.${_case} PROPERTIES TIMEOUT 30 LABELS "redis_net")
endforeach()

add_executable(Test_redis_net_contract Test_redis_net_contract.cc)
target_link_libraries(Test_redis_net_contract PRIVATE bbt::infra_redis_net hiredis::hiredis)
target_compile_options(Test_redis_net_contract PRIVATE -Wall -Wextra -Wpedantic)
# 仅测试可执行文件观察 hiredis reader 寿命/分帧和注入 Feed 失败；无公共测试 API。
target_link_options(Test_redis_net_contract PRIVATE
    -Wl,--wrap=redisReaderCreate -Wl,--wrap=redisReaderFree
    -Wl,--wrap=redisReaderGetReply -Wl,--wrap=redisReaderFeed)
foreach(_case connect_failure_owned seal_during_connect_start close_invalidates_immediately close_idempotent
        close_during_connect close_pending_command_once handle_destroy_late_callback
        shared_engine_isolation host_drain_no_residue set_get_remote_error
        fragmented_reply unknown_disconnect framing_and_faults
        pool_connect pool_basic_commands pool_concurrent pool_overloaded pool_close
        pool_stages pool_wait_exits pool_wait_windows pool_repair_waiter pool_connect_rollback
        set_options_contract expire_ttl_contract incr_family_contract hash_contract
        handshake_auth_select pool_new_commands pool_handshake_auth hgetall_protocol_faults
        close_during_handshake)
    add_test(NAME redis.net.${_case} COMMAND Test_redis_net_contract ${_case})
    set_tests_properties(redis.net.${_case} PROPERTIES TIMEOUT 30 LABELS "unit")
endforeach()

file(GENERATE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/async-includes.txt"
    CONTENT "$<JOIN:$<TARGET_PROPERTY:Test_redis_net_contract,INCLUDE_DIRECTORIES>,\n>\n")
add_test(NAME redis.net.async_compile COMMAND ${CMAKE_COMMAND}
    "-DCXX=${CMAKE_CXX_COMPILER}"
    "-DINCLUDES=${CMAKE_CURRENT_BINARY_DIR}/async-includes.txt"
    "-DSOURCE=${CMAKE_CURRENT_SOURCE_DIR}/AsyncCompile.cc"
    -P "${CMAKE_CURRENT_SOURCE_DIR}/AsyncCompile.cmake")
set_tests_properties(redis.net.async_compile PROPERTIES TIMEOUT 60 LABELS "integration")

# ICoConnection 成员化收口消费示例：只链接公开 target（bbt::infra_net
# + bbtools-coroutine），不触碰 net detail。真实 TCP 回环覆盖
# Engine::CoAccept/CoDial → conn.Read→Buffer / conn.Write→WriteReport /
# conn.WaitClosed / conn.Status 全部成员形态。
add_executable(Example_ico_connection main.cc)
target_link_libraries(Example_ico_connection PRIVATE bbt::infra_net Threads::Threads)
target_compile_options(Example_ico_connection PRIVATE -Wall -Wextra)

if(BUILD_TESTING)
    add_test(NAME example_ico_connection_roundtrip COMMAND Example_ico_connection)
    set_tests_properties(example_ico_connection_roundtrip PROPERTIES TIMEOUT 60
        LABELS "integration")
endif()

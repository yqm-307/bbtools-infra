add_executable(Test_net_reactor net_reactor.cc)
target_link_libraries(Test_net_reactor PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_reactor PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case owner bridge lifecycle)
    add_test(NAME net.reactor_${_case} COMMAND Test_net_reactor ${_case})
    set_tests_properties(net.reactor_${_case} PROPERTIES TIMEOUT 45)
endforeach()

# P3-b 私有 stream 连接 backend：真实 TCP 动态端口/对端；七场景各 45s。
add_executable(Test_net_stream net_stream.cc)
target_link_libraries(Test_net_stream PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_stream PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case stream_traffic stream_write stream_close stream_peer_rst stream_adoption
        stream_owner_rejection stream_fd_reuse)
    add_test(NAME net.${_case} COMMAND Test_net_stream ${_case})
    set_tests_properties(net.${_case} PROPERTIES TIMEOUT 45
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

# 分配失败注入 stream 连接回归：真实 TCP 动态端口；仅本可执行文件做线程
# 局部一次分配注入与 link-time recv 记录（--wrap=recv，同 foundation 的
# --wrap=send 范式，不用 LD_PRELOAD，不波及其他测试与产品）。
# --wrap=aligned_alloc 同样仅本可执行文件：Boost 1.90 Asio 的 async_wait
# 等待登记分配实际走 std::aligned_alloc，用于等待登记失败出口回归。
add_executable(Test_net_stream_allocation net_stream_allocation.cc)
target_link_libraries(Test_net_stream_allocation PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_stream_allocation PRIVATE -Wall -Wextra -Wpedantic)
target_link_options(Test_net_stream_allocation PRIVATE -Wl,--wrap=recv -Wl,--wrap=close
    -Wl,--wrap=aligned_alloc)
foreach(_case stream_close_allocation stream_read_allocation stream_wait_registration)
    add_test(NAME net.${_case} COMMAND Test_net_stream_allocation ${_case})
    set_tests_properties(net.${_case} PROPERTIES TIMEOUT 45
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

# 有限 deadline 零/负剩余时间窗口回归：仅该可执行文件在自身内 interpose
# clock_gettime（编译期符号，不用 LD_PRELOAD，不波及其他测试与产品），
# 覆盖私有 CoIoOperation 与上游等待入口两次采样间的接缝。
add_executable(Test_net_deadline net_deadline.cc)
target_link_libraries(Test_net_deadline PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_deadline PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case zero_window negative_window)
    add_test(NAME net.reactor_deadline_${_case} COMMAND Test_net_deadline ${_case})
    set_tests_properties(net.reactor_deadline_${_case} PROPERTIES TIMEOUT 45)
endforeach()

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

# 纯逻辑包定向测试：net.buffer / net.options / net.load_balancing，
# 与 net_foundation.cc 分文件；detail 内部头直接同仓引用（非公开 API），
# CRC32 用 Boost 头，故显式接 Boost::headers。
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

# 仓外式最小消费：只依赖新 target，不包含旧 infra 头。
add_executable(net_foundation_consumer
    ${PROJECT_SOURCE_DIR}/examples/net_foundation_consumer/main.cc)
target_link_libraries(net_foundation_consumer PRIVATE bbt::infra_net)
target_compile_options(net_foundation_consumer PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME net.consumer COMMAND net_foundation_consumer)
set_tests_properties(net.consumer PROPERTIES TIMEOUT 30)

# network-only 的 CTest 不能退化成零测试；目标名也不应带入旧模块。
get_property(_net_tests DIRECTORY PROPERTY BUILDSYSTEM_TARGETS)
if(NOT TARGET Test_net_foundation OR NOT TARGET net_foundation_consumer)
    message(FATAL_ERROR "network-only 缺少首包测试/消费目标")
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
    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
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
add_test(NAME net.consumer_coroutine_first_dso
    COMMAND ${CMAKE_COMMAND}
        "-DBINARY=${_net_co_first_build}/consume_net_coroutine_first"
        "-DNEEDLE=_co/lib"
        -P "${_net_consumers}/verify_dso.cmake")
set_tests_properties(net.consumer_coroutine_first_configure PROPERTIES TIMEOUT 300)
set_tests_properties(net.consumer_coroutine_first_build PROPERTIES
    DEPENDS net.consumer_coroutine_first_configure TIMEOUT 300)
set_tests_properties(net.consumer_coroutine_first_run PROPERTIES
    DEPENDS net.consumer_coroutine_first_build TIMEOUT 60)
set_tests_properties(net.consumer_coroutine_first_dso PROPERTIES
    DEPENDS net.consumer_coroutine_first_build TIMEOUT 60)

# 负向：复用 target 的声明来源与真实 SOURCE_DIR 不一致时，configure 必须以明确
# 标记失败；任意失败或意外成功都不算通过。
add_test(NAME net.consumer_source_dir_mismatch COMMAND ${CMAKE_COMMAND}
    "-DBUILD_ROOT=${CMAKE_CURRENT_BINARY_DIR}"
    "-DGENERATOR=${CMAKE_GENERATOR}"
    "-DNET_CONSUMER_COROUTINE_DIR=${BBT_COROUTINE_SOURCE_DIR}"
    "-DNET_CONSUMER_INFRA_DIR=${PROJECT_SOURCE_DIR}"
    "-DNET_CONSUMER_STUB_DIR=${_net_consumers}/source_dir_mismatch/stub"
    -P "${_net_consumers}/expect_configure_failure.cmake")
set_tests_properties(net.consumer_source_dir_mismatch PROPERTIES TIMEOUT 300)

# P3-c1 公共连接操作与私有创建/业务 drain 接缝：真实 TCP 动态端口/对端；
# 七场景各 45s（G1 drain / G4 有界队列 / G6 迟到资源独立命名）。
# F2：仅拦截本测试中 owner 域的 dispatcher 登记，fixture 任务不受影响。
add_executable(Test_net_co_bridge_dispatch_failure co_bridge_dispatch_failure.cc)
target_link_libraries(Test_net_co_bridge_dispatch_failure PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_co_bridge_dispatch_failure PRIVATE -Wall -Wextra -Wpedantic)
target_link_options(Test_net_co_bridge_dispatch_failure PRIVATE
    -Wl,--wrap=_ZN3bbt9coroutine6detail9Scheduler19RegistCoroutineTaskEmRKSt8functionIFvvEEPKc)
foreach(_stage initial traffic wake close)
    foreach(_mode once persistent)
        add_test(NAME net.co_bridge_dispatch_${_stage}_${_mode}
            COMMAND Test_net_co_bridge_dispatch_failure ${_stage} ${_mode})
        set_tests_properties(net.co_bridge_dispatch_${_stage}_${_mode} PROPERTIES TIMEOUT 25
            WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
    endforeach()
endforeach()

add_executable(Test_net_co_bridge_exception_safety co_bridge_exception_safety.cc)
target_link_libraries(Test_net_co_bridge_exception_safety PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_co_bridge_exception_safety PRIVATE -Wall -Wextra -Wpedantic)
target_link_options(Test_net_co_bridge_exception_safety PRIVATE
    -Wl,--wrap=close
    -Wl,--wrap=_ZN3bbt5infra3net16StreamConnection6CreateERNS1_9EventLoopEONS1_12NativeSocketERKNS1_17NormalizedOptionsENS2_9CallbacksERSt10error_code
    -Wl,--wrap=_ZN3bbt5infra3net16StreamConnection5CloseESt10error_code
    -Wl,--wrap=_ZN3bbt9coroutine6detail9Scheduler19RegistCoroutineTaskEmRKSt8functionIFvvEEPKc)
foreach(_case adopt_construct_oom_rollback adopt_member_oom_rollback adopt_bound_exception
        stop_no_snapshot_allocation stop_bad_alloc_classification stop_other_exception_classification)
    add_test(NAME net.co_bridge_${_case} COMMAND Test_net_co_bridge_exception_safety ${_case})
    set_tests_properties(net.co_bridge_${_case} PROPERTIES TIMEOUT 25
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()


# F8 停止开始授权；测试独立 linkwrap，不增加生产注入接口。
add_executable(Test_net_co_bridge_service_gate co_bridge_service_gate.cc)
target_link_libraries(Test_net_co_bridge_service_gate PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_co_bridge_service_gate PRIVATE -Wall -Wextra -Wpedantic)
target_link_options(Test_net_co_bridge_service_gate PRIVATE
    -Wl,--wrap=_ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEE
    -Wl,--wrap=_ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEERSt10error_code
    -Wl,--wrap=_ZN3bbt5infra3net16StreamConnection6CreateERNS1_9EventLoopEONS1_12NativeSocketERKNS1_17NormalizedOptionsENS2_9CallbacksERSt10error_code
    -Wl,--wrap=_ZN3bbt5infra3net16StreamConnection12TakeSnapshotERSt10error_code)
foreach(_case queued_write pending_open pending_traffic engine_handoff
        traffic_take_then_seal engine_multisub_seal_interleave)
    add_test(NAME net.co_bridge_service_gate_${_case} COMMAND Test_net_co_bridge_service_gate ${_case})
    set_tests_properties(net.co_bridge_service_gate_${_case} PROPERTIES TIMEOUT 25
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

add_executable(Test_net_co_bridge_force_service co_bridge_force_service.cc)
target_link_libraries(Test_net_co_bridge_force_service PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_co_bridge_force_service PRIVATE -Wall -Wextra -Wpedantic)
target_link_options(Test_net_co_bridge_force_service PRIVATE
    -Wl,--wrap=_ZN3bbt5infra3net16StreamConnection5CloseESt10error_code)
foreach(_case traffic pending failure_before failure_after)
    add_test(NAME net.co_bridge_force_service_${_case}
        COMMAND Test_net_co_bridge_force_service ${_case})
    set_tests_properties(net.co_bridge_force_service_${_case} PROPERTIES TIMEOUT 25
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

add_executable(Test_net_co_bridge co_bridge_contract.cc)
target_link_libraries(Test_net_co_bridge PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_co_bridge PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case co_bridge_lifecycle co_bridge_close co_bridge_handler_exception
        co_bridge_context_reject co_bridge_queue_full co_bridge_g1_drain
        co_bridge_g6_late_resource)
    add_test(NAME net.${_case} COMMAND Test_net_co_bridge ${_case})
    set_tests_properties(net.${_case} PROPERTIES TIMEOUT 45
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

# P3-c1 owner 安全脱离切片：真实 TCP 动态端口/自有线程；每 case 45s。
# context 先毁/归还落账/析构重入均单独命名，不弱化既有断言与超时。
add_executable(Test_net_co_bridge_lifetime co_bridge_lifetime.cc)
target_link_libraries(Test_net_co_bridge_lifetime PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_co_bridge_lifetime PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case detach_lifetime snapshot_return_on_stop detach_handler_destroy
        snapshot_return_on_join detach_no_allocation)
    add_test(NAME net.co_bridge_${_case} COMMAND Test_net_co_bridge_lifetime ${_case})
    set_tests_properties(net.co_bridge_${_case} PROPERTIES TIMEOUT 45
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

# 首包公共 Engine（真实 TCP 单 main + N sub 闭环）：动态端口/自有线程；
# 十场景各 60s（roundtrip 双 sub 身份/回显、挂起并发、强停+残留 shell、
# 生命周期负例、受管域拒绝、OnClose 抛出计数回落 F2、OnClose 挂起跨销毁 F1、
# null 拥有关系拒绝、拥有型 handler 挂起跨销毁寿命、Boot 挂起 10s 超时回落 F9）。
# 见 OUT/engine-implementation-41/plan.md 与 OUT/engine-fix-43/plan.md。
add_executable(Test_net_engine_server engine_server_accept.cc)
target_link_libraries(Test_net_engine_server PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_engine_server PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case roundtrip concurrent_hold stop_with_suspended_handler negative_lifecycle
        managed_reject onclose_throw_count onclose_suspended_stop_destroy
        null_handler handler_owned_across_destroy boot_suspended_timeout_destroy
        boot_observation stop_during_boot stop_before_run_and_contention
        boot_pending_abandon boot_crossgen_reentry_busy least_connections_closed_registry
        multiloop_owner_identity lb_source_addr_hash
        accept_handoff_rejected_bounded bounded_write_reject
        unsupported_family_reject unsupported_family_error_precedence)
    add_test(NAME net.engine_${_case} COMMAND Test_net_engine_server ${_case})
    set_tests_properties(net.engine_${_case} PROPERTIES TIMEOUT 60
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

# 首包 ticker/OnTick 最小接线：真实 TCP 动态端口；七场景各 60s（周期投递与间隔下界、
# 挂起不重入且其它连接进展、Stop 门与在途 tick 跨销毁寿命/释放、返回 0/负值停止投递、
# 抛出与极大返回值饱和/可取消 + 其它 unsupported 仍拒绝、Stop 长锁窗口内重挂不阻塞唯一
# Processer、owner 队列 Full 拒绝 ⇒ ticker 明确停止且服务保留）。见 enginetick-impl-132
# 与 enginetick-fix-134。
# 测试私有 linkwrap（不新增生产注入接口，同 co_bridge_service_gate/engine_stop_failure 范式）：
#   - EventLoop::Submit(Command)：非 owner 线程注入一次「队列满」拒绝；
#   - EventLoop::Submit(Command, error_code&)：制造 Stop 持 control_mutex_ 的真实长窗口。
# （PostArmTick/TickTask 同 TU，--wrap 只改名未定义引用 ⇒ 用白盒直调同一生产函数覆盖。）
add_executable(Test_net_engine_tick engine_tick.cc)
target_link_libraries(Test_net_engine_tick PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_engine_tick PRIVATE -Wall -Wextra -Wpedantic)
target_link_options(Test_net_engine_tick PRIVATE
    -Wl,--wrap=_ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEE
    -Wl,--wrap=_ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEERSt10error_code)
foreach(_case periodic no_reentry_and_others_progress stop_gate_and_release
        stop_on_zero_or_negative throw_and_extreme_delay stop_lock_interleave
        submit_reject_stops_ticker rearm_window_blocks_stop)
    add_test(NAME net.engine_tick_${_case} COMMAND Test_net_engine_tick ${_case})
    set_tests_properties(net.engine_tick_${_case} PROPERTIES TIMEOUT 60
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

# F5 启动 accept 装配失败回归：独立 target，仅本可执行文件 --wrap=fcntl，
# 精确注入 listener dup（F_DUPFD_CLOEXEC）一次 EMFILE；其他命令按真实第三参数
# 类型保真转发（不改动既有 52 case 的行为）。见 OUT/engine-fix-45/plan.md。
add_executable(Test_net_engine_start_failure engine_start_failure.cc)
target_link_libraries(Test_net_engine_start_failure PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_engine_start_failure PRIVATE -Wall -Wextra -Wpedantic)
target_link_options(Test_net_engine_start_failure PRIVATE -Wl,--wrap=fcntl)
foreach(_case start_dup_failure start_retry_roundtrip)
    add_test(NAME net.engine_${_case} COMMAND Test_net_engine_start_failure ${_case})
    set_tests_properties(net.engine_${_case} PROPERTIES TIMEOUT 60
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

# F4 P3 切片：Engine 一次性控制/关闭边界故障的安全收尾回归。独立 target，测试私有
# link-wrap（--wrap=close/fcntl 与 StreamConnection::Close 的 C++ mangled wrap，
# 同 engine_start_failure / co_bridge_exception_safety 范式）+ 本可执行文件内
# operator new 覆盖，不新增生产注入接口。见 engine-stop-fix-68/plan.md。
add_executable(Test_net_engine_stop_failure engine_stop_failure.cc)
target_link_libraries(Test_net_engine_stop_failure PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_engine_stop_failure PRIVATE -Wall -Wextra -Wpedantic)
target_link_options(Test_net_engine_stop_failure PRIVATE
    -Wl,--wrap=aligned_alloc
    -Wl,--wrap=_ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEERSt10error_code
    -Wl,--wrap=_ZN3bbt5infra3net9EventLoop4JoinEv
    -rdynamic
    -Wl,--wrap=close
    -Wl,--wrap=fcntl
    -Wl,--wrap=_ZN3bbt5infra3net12NativeSocket5CloseEv
    -Wl,--wrap=_ZN3bbt5infra3net16StreamConnection5CloseESt10error_code)
foreach(_case stop_close_before_fault stop_close_after_fault stop_submit_alloc_fault
        rollback_close_faults_not_idle stop_post_alloc_fault
        stop_accepted_late destructor_close_before_fault)
    add_test(NAME net.engine_${_case} COMMAND Test_net_engine_stop_failure ${_case})
    set_tests_properties(net.engine_${_case} PROPERTIES TIMEOUT 60
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

# P3-c2 薄 Client 最小 Dial：一个 EventLoop + 一个 ConnectionBridge，无 listener/
# accept。真实回环成功/寻址、关闭端口拒绝(ECONNREFUSED)、真实未决窗口 deadline
# 晚到清理(fd 回落)、Stop 与未决 Dial 竞争(有界返回+拒绝后续 Dial)、域门禁拒绝。
# 真实 TCP 动态端口；每 case 60s。
add_executable(Test_net_client_connect client_connect.cc)
target_link_libraries(Test_net_client_connect PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_client_connect PRIVATE -Wall -Wextra -Wpedantic)
# 取消/关闭收口与启动交错的定向回归：仅本可执行文件 linkwrap 真实提交来源
#   - Submit(std::function<void()>)：Dial 侧投递/取消（按「队列满」拒绝一次）；
#   - Submit(std::function<void()>, std::error_code&)：Stop 的 RunOwner 控制步（按
#     「分配失败」拒绝一次，验证一次有界补偿）；
#   - EventLoop::Start：Start 的「owner 已起、未发布」窗口同步闸（确定性交错）。
# 同 co_bridge_service_gate / engine_stop_failure 的测试私有 wrap 范式；不新增生产注入接口。
target_link_options(Test_net_client_connect PRIVATE
    -Wl,--wrap=_ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEE
    -Wl,--wrap=_ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEERSt10error_code
    -Wl,--wrap=_ZN3bbt5infra3net9EventLoop5StartEv)
foreach(_case roundtrip refuse deadline_late_cleanup stop_pending_dial domain_reject
        start_race_single_owner destroy_with_live_shell cancel_handoff_retained
        stop_cancel_submit_fault start_stop_prepublish)
    add_test(NAME net.client_${_case} COMMAND Test_net_client_connect ${_case})
    set_tests_properties(net.client_${_case} PROPERTIES TIMEOUT 60
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

# peer FIN/RST 自动收口（真实 TCP 回环）：无显式 Close、无 Engine::Stop 时
# OnClose exactly-once、活跃连接计数归零、尾帧不丢、FIN vs RST reason 可区分。
# 复用 co_bridge_contract.cc 既有 fixture（同 co_bridge_lifetime 的 include 范式）。
add_executable(Test_net_co_bridge_peer_eof_full co_bridge_peer_eof_full.cc)
target_link_libraries(Test_net_co_bridge_peer_eof_full PRIVATE bbt::infra_net Boost::headers ${CMAKE_DL_LIBS})
target_compile_options(Test_net_co_bridge_peer_eof_full PRIVATE -Wall -Wextra -Wpedantic)
target_link_options(Test_net_co_bridge_peer_eof_full PRIVATE -rdynamic
    -Wl,--wrap=_ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEE
    -Wl,--wrap=_ZN3bbt5infra3net16StreamConnection5CloseESt10error_code)
foreach(_case once recover)
    add_test(NAME net.peer_eof_full_${_case} COMMAND Test_net_co_bridge_peer_eof_full ${_case})
    set_tests_properties(net.peer_eof_full_${_case} PROPERTIES TIMEOUT 30
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

add_executable(Test_net_co_bridge_peer_eof co_bridge_peer_eof.cc)
target_link_libraries(Test_net_co_bridge_peer_eof PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_co_bridge_peer_eof PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case peer_eof_fin_auto_close peer_eof_tail_frame_then_fin
        peer_eof_rst_reason peer_eof_fin_during_suspended_handler
        peer_eof_idempotent_after_auto_close)
    add_test(NAME net.${_case} COMMAND Test_net_co_bridge_peer_eof ${_case})
    set_tests_properties(net.${_case} PROPERTIES TIMEOUT 45
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

# EOF 自动收口 与 Engine::Stop 竞争的确定性窗口（窗口 A：EOF 收口 Submit 未接纳 →
# Stop 直接收口；窗口 B：EOF 收口 Submit 已接纳、物理收口在途 → Stop 并发，且等 Stop 侧
# 真实封门收据 ConnectionBridge::SealService 返回后才放行 owner）。只按调用栈注入真实 EOF
# 收口 Submit，并在唯一 fd 关闭边界 StreamConnection::Close 计数/开闸；不新增生产 hook，
# 不用 sleep 猜阶段。每 case 30s。
add_executable(Test_net_co_bridge_close_race co_bridge_close_race.cc)
target_link_libraries(Test_net_co_bridge_close_race PRIVATE bbt::infra_net Boost::headers ${CMAKE_DL_LIBS})
target_compile_options(Test_net_co_bridge_close_race PRIVATE -Wall -Wextra -Wpedantic)
target_link_options(Test_net_co_bridge_close_race PRIVATE -rdynamic
    -Wl,--wrap=_ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEE
    -Wl,--wrap=_ZN3bbt5infra3net16StreamConnection5CloseESt10error_code
    -Wl,--wrap=_ZN3bbt5infra3net16ConnectionBridge11SealServiceEv)
foreach(_case eof_rejected_then_stop eof_accepted_then_stop)
    add_test(NAME net.co_bridge_close_race_${_case} COMMAND Test_net_co_bridge_close_race ${_case})
    set_tests_properties(net.co_bridge_close_race_${_case} PROPERTIES TIMEOUT 30
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

# Action::Shutdown 向上请求上报（用户裁定：向上层传递、自由度给使用者；取代旧的
# 「Shutdown 自动收口」表述）。真实 TCP 回环/动态端口/自有进程；七场景各 60s：
# 请求收到且连接仍收发/引擎仍接纳；ticker 不停；逐次上报无首胜；挂起只让出唯一 Processer
# （同连接不重入、其它连接进展、不同连接请求可真并发）；回调内 Stop 域拒绝无副作用 +
# 控制线程信号后显式 Stop 真实关闭；请求挂起跨 Engine 销毁寿命安全 + 请求抛出按既有
# handler 异常策略可观察；OnOpen 返回 Action::Shutdown 的上报路径（契约面另一格）。
# 只用公共 Engine/NativeSocket，不直接调用 detail 适配器。
add_executable(Test_net_engine_shutdown_request engine_shutdown_request.cc)
target_link_libraries(Test_net_engine_shutdown_request PRIVATE bbt::infra_net Boost::headers)
target_compile_options(Test_net_engine_shutdown_request PRIVATE -Wall -Wextra -Wpedantic)
foreach(_case reports_and_keeps_serving keeps_ticker_and_accept repeatable_no_first_win
        suspended_yields_and_progress stop_domain_and_upper_signal suspended_across_destroy
        open_request_keeps_serving)
    add_test(NAME net.engine_shutdown_request_${_case}
        COMMAND Test_net_engine_shutdown_request ${_case})
    set_tests_properties(net.engine_shutdown_request_${_case} PROPERTIES TIMEOUT 60
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/runtime")
endforeach()

# 高层 GNet UDP 本轮暂缓（用户 2026-10-17 裁定：不做高层 UDP，只留使用时编译报错）。
# <bbt/infra/net/Udp.hpp> 是唯一显式使用点的编译拒绝入口；本用例真实调用编译器做
# 正/负编译对照，只有「datagram 原语与普通 Engine 头仍可编译」+「Udp.hpp 以自身
# 拒绝标记真实失败」同时成立才算通过，不用 WILL_FAIL（它会把任意非零翻成通过）。
# 见 udp-unsupported-147/plan.md 与 docs/decisions/0010-gnet-network-foundation.md。
add_test(NAME net.udp_high_level_compile_reject COMMAND ${CMAKE_COMMAND}
    "-DCXX=${CMAKE_CXX_COMPILER}"
    "-DINFRA_DIR=${PROJECT_SOURCE_DIR}"
    "-DCOROUTINE_DIR=${BBT_COROUTINE_SOURCE_DIR}"
    "-DWORK_DIR=${CMAKE_CURRENT_BINARY_DIR}/udp_reject_probe"
    -P "${_net_consumers}/expect_udp_compile.cmake")
set_tests_properties(net.udp_high_level_compile_reject PROPERTIES TIMEOUT 120)

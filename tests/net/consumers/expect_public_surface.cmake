# 公共面编译门禁（D1）：正/负 TU 对照，证明"冻结后的公共头不含内部状态、
# 可写测试接缝与旧等待 API"，同时保证编译环境本身有效。
#
# 与 expect_udp_compile.cmake 同一范式：只认"预期原因"的失败，不用 WILL_FAIL，
# 不产目标文件（-fsyntax-only），不进构建图。
#
#   1) 正对照：只 include 公共头的 TU 必须编译通过（真实使用全部保留符号）。
#   2) 逐项负对照：每个被删除/收回的符号必须真实编译失败，且
#      (a) 失败输出含该探针首行声明的期望原因片段，
#      (b) 失败落在该探针 TU 上（不是"头不存在"或"缺依赖"）。
#
# 负对照探针是 tests/net/consumers/public_surface_negative/neg_*.cc，首行
# `// expect-error: <片段>` 声明期望原因；新增删除项只需加一个探针文件。
#
# 入参（由 tests/net/CMakeLists.txt 传入）：CXX、INFRA_DIR、COROUTINE_DIR、
# BOOST_INCLUDE_DIRS、WORK_DIR。

if(NOT CXX OR NOT INFRA_DIR OR NOT COROUTINE_DIR OR NOT BOOST_INCLUDE_DIRS OR NOT WORK_DIR)
    message(FATAL_ERROR "expect_public_surface 缺少参数（CXX/INFRA_DIR/COROUTINE_DIR/BOOST_INCLUDE_DIRS/WORK_DIR）")
endif()
file(MAKE_DIRECTORY "${WORK_DIR}")

# 与 bbt_infra_net_header_selfcheck 相同的编译口径。
set(_flags -DBOOST_CONTEXT_DYN_LINK -DBOOST_CONTEXT_NO_LIB
    "-I${INFRA_DIR}" -isystem "${COROUTINE_DIR}" -std=c++17 -Wall -Wextra -Wpedantic)
foreach(_boost_include IN LISTS BOOST_INCLUDE_DIRS)
    list(APPEND _flags "-I${_boost_include}")
endforeach()

set(_case_dir "${CMAKE_CURRENT_LIST_DIR}/public_surface_negative")

set(_positive_tu "${WORK_DIR}/public_surface_positive.cc")
file(WRITE "${_positive_tu}"
    "// 只经公共面使用保留符号：值类型、守卫、句柄、Engine、Co 四入口。\n"
    "#include <bbt/infra/net/Address.hpp>\n"
    "#include <bbt/infra/net/Annotations.hpp>\n"
    "#include <bbt/infra/net/CoConnection.hpp>\n"
    "#include <bbt/infra/net/CoObject.hpp>\n"
    "#include <bbt/infra/net/Connection.hpp>\n"
    "#include <bbt/infra/net/Engine.hpp>\n"
    "#include <bbt/infra/net/Error.hpp>\n"
    "#include <bbt/infra/net/ICoObject.hpp>\n"
    "#include <bbt/infra/net/LoadBalancing.hpp>\n"
    "#include <bbt/infra/net/NativeSocket.hpp>\n"
    "#include <bbt/infra/net/Options.hpp>\n"
    "#include <bbt/infra/net/RawHandler.hpp>\n"
    "#include <bbt/infra/net/ReadAccess.hpp>\n"
    "#include <bbt/infra/net/Types.hpp>\n"
    "\n"
    "#include <chrono>\n"
    "#include <cstdint>\n"
    "#include <memory>\n"
    "#include <string>\n"
    "#include <system_error>\n"
    "#include <vector>\n"
    "\n"
    "#include <bbt/coroutine/coroutine.hpp>\n"
    "\n"
    "namespace {\n"
    "std::error_code UseAll(bbt::infra::net::Connection& connection,\n"
    "                       bbt::infra::net::Engine& engine,\n"
    "                       const bbt::infra::net::Address& address) {\n"
    "    using namespace bbt::infra::net;\n"
    "    Options options;\n"
    "    options.lb = LoadBalancing::RoundRobin;\n"
    "    options.reuse_addr = true;\n"
    "    options.reuse_port = false;\n"
    "    options.num_event_loop = 1;\n"
    "    options.tcp_keep_alive = std::chrono::seconds{1};\n"
    "    options.tcp_keep_interval = std::chrono::seconds{1};\n"
    "    options.tcp_keep_count = 3;\n"
    "    options.tcp_no_delay = true;\n"
    "    options.socket_recv_buffer = 0;\n"
    "    options.socket_send_buffer = 0;\n"
    "    options.max_inbound_bytes = 1024;\n"
    "    options.max_outbound_bytes = 1024;\n"
    "    options.max_pending_events = 4;\n"
    "    EngineConfig config;\n"
    "    config.options = options;\n"
    "    config.runtime_progress = RuntimeProgress::Independent;\n"
    "    (void)config;\n"
    "    EnginePhase phase = engine.Phase();\n"
    "    if (phase == EnginePhase::Idle || phase == EnginePhase::Starting\n"
    "        || phase == EnginePhase::Running || phase == EnginePhase::Closing\n"
    "        || phase == EnginePhase::Stopped) { (void)0; }\n"
    "    (void)connection.Valid();\n"
    "    (void)connection.Id();\n"
    "    (void)connection.ReadableBytes();\n"
    "    (void)connection.ReadError();\n"
    "    (void)connection.ReadEof();\n"
    "    (void)connection.LastWriteError();\n"
    "    (void)connection.Closed();\n"
    "    (void)connection.CloseReason();\n"
    "    connection.RequestClose();\n"
    "    std::string bytes;\n"
    "    ReadResult read = connection.TryRead(bytes);\n"
    "    if (read.status == ReadStatus::Ok || read.status == ReadStatus::Busy\n"
    "        || read.status == ReadStatus::WouldBlock || read.status == ReadStatus::Eof\n"
    "        || read.status == ReadStatus::Error) { (void)read.bytes; }\n"
    "    WriteOperation operation;\n"
    "    std::error_code write_error = connection.AsyncWrite(std::string(\"x\"), operation);\n"
    "    (void)operation.Valid();\n"
    "    (void)operation.Id();\n"
    "    WriteOutcome outcome = operation.Query();\n"
    "    if (outcome.phase == WritePhase::Done || outcome.phase == WritePhase::Rejected\n"
    "        || outcome.phase == WritePhase::InFlight || outcome.phase == WritePhase::Unknown) {\n"
    "        (void)outcome.sent;\n"
    "        (void)outcome.error;\n"
    "    }\n"
    "    AcceptanceState acceptance = connection.WriteAcceptance();\n"
    "    if (acceptance == AcceptanceState::Open || acceptance == AcceptanceState::Settling\n"
    "        || acceptance == AcceptanceState::Closed) { (void)0; }\n"
    "    ReadAccess guard = connection.TryAccess();\n"
    "    if (guard) {\n"
    "        (void)guard.Size();\n"
    "        (void)guard.Peek();\n"
    "        std::string copied;\n"
    "        (void)guard.CopyAll(copied);\n"
    "        (void)guard.ConsumeInto(copied);\n"
    "        (void)guard.Consume(0);\n"
    "    }\n"
    "    if (guard.Busy() || guard.WouldBlock() || guard.Eof() || guard.Error()) {\n"
    "        (void)guard.ErrorCode();\n"
    "    }\n"
    "    ReadAccess other(std::move(guard));\n"
    "    (void)other;\n"
    "    std::uint64_t connect_id = 0;\n"
    "    (void)engine.AsyncConnect(address, connect_id);\n"
    "    (void)engine.EnableAcceptHandoff(1);\n"
    "    (void)engine.LastError();\n"
    "    (void)engine.LastFailure();\n"
    "    (void)engine.CountConnections();\n"
    "    (void)engine.HandlerErrorCount();\n"
    "    std::vector<Address> addresses;\n"
    "    (void)engine.LocalAddresses(addresses);\n"
    "    std::vector<std::uint64_t> main_owners;\n"
    "    std::vector<std::uint64_t> sub_owners;\n"
    "    (void)engine.ReactorOwnerIds(main_owners, sub_owners);\n"
    "    bbt::coroutine::WaitOptions wait_options{};\n"
    "    (void)CoRead(connection, bytes);   // 缺省 WaitOptions 形式\n"
    "    (void)CoRead(connection, bytes, wait_options);\n"
    "    WriteOutcome co_outcome;\n"
    "    WriteOperation co_operation;\n"
    "    (void)CoWrite(connection, \"y\", wait_options, co_outcome, co_operation);\n"
    "    Connection accepted;\n"
    "    (void)CoAccept(engine, accepted);   // 缺省 WaitOptions 形式\n"
    "    (void)CoAccept(engine, accepted, wait_options);\n"
    "    Connection dialed;\n"
    "    (void)CoDial(engine, address, dialed);   // 缺省 WaitOptions 形式\n"
    "    (void)CoDial(engine, address, dialed, wait_options);\n"
    "    return write_error;\n"
    "}\n"
    "}  // namespace\n")

execute_process(COMMAND "${CXX}" ${_flags} -fsyntax-only "${_positive_tu}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _err)
string(APPEND _out "${_err}")
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "正对照编译失败（rc=${_rc}）：公共面本身不可用，负对照无意义\n${_out}")
endif()
message(STATUS "正对照通过：仅公共头 TU（不使用任何 detail/ 头）编译成功")

file(GLOB _negative_tus "${_case_dir}/neg_*.cc")
list(LENGTH _negative_tus _negative_count)
if(_negative_count LESS 1)
    message(FATAL_ERROR "负对照探针集合为空（${_case_dir}/neg_*.cc），不接受零例通过")
endif()

foreach(_tu IN LISTS _negative_tus)
    file(STRINGS "${_tu}" _first_line LIMIT_COUNT 1)
    set(_mode "expect-error")
    string(FIND "${_first_line}" "// expect-error: " _marker_pos)
    if(NOT _marker_pos EQUAL 0)
        set(_mode "expect-missing-header")
        string(FIND "${_first_line}" "// expect-missing-header: " _marker_pos)
    endif()
    if(NOT _marker_pos EQUAL 0)
        message(FATAL_ERROR "负对照探针缺首行『// expect-error: …』或『// expect-missing-header: …』：${_tu}")
    endif()
    if(_mode STREQUAL "expect-error")
        string(SUBSTRING "${_first_line}" 17 -1 _marker)
    else()
        string(SUBSTRING "${_first_line}" 26 -1 _marker)
    endif()
    string(STRIP "${_marker}" _marker)
    if(_marker STREQUAL "")
        message(FATAL_ERROR "负对照探针的期望原因片段为空：${_tu}")
    endif()

    execute_process(COMMAND "${CXX}" ${_flags} -fsyntax-only "${_tu}"
        RESULT_VARIABLE _nrc OUTPUT_VARIABLE _nout ERROR_VARIABLE _nerr)
    string(APPEND _nout "${_nerr}")
    get_filename_component(_name "${_tu}" NAME)
    if(_nrc EQUAL 0)
        message(FATAL_ERROR "负对照 ${_name}：编译器接受了应被删除/收在内部的符号\n${_nout}")
    endif()
    string(FIND "${_nout}" "${_marker}" _found)
    if(_found EQUAL -1)
        message(FATAL_ERROR "负对照 ${_name} 的失败原因不含预期片段『${_marker}』（rc=${_nrc}），不接受任意失败\n${_nout}")
    endif()
    # 片段必须出现在**首条 error 附近**（GCC 会折行长标识符，故取 240 字符窗口并
    # 折叠换行）：避免"别处也提到了这个词"蒙过门禁。
    string(FIND "${_nout}" "error:" _err_pos)
    if(_err_pos EQUAL -1)
        message(FATAL_ERROR "负对照 ${_name} 的编译失败没有可解析的 error 行（rc=${_nrc}）\n${_nout}")
    endif()
    string(SUBSTRING "${_nout}" ${_err_pos} 240 _err_window)
    string(REPLACE "\n" " " _err_window "${_err_window}")
    string(FIND "${_err_window}" "${_marker}" _in_error)
    if(_in_error EQUAL -1)
        message(FATAL_ERROR "负对照 ${_name}：预期片段『${_marker}』不在首条 error 附近\n  首条 error 窗口: ${_err_window}\n${_nout}")
    endif()
    if(_mode STREQUAL "expect-error")
        # 普通负例：失败必须来自探针 TU 自身的符号使用，不能来自环境缺文件。
        string(FIND "${_nout}" "No such file or directory" _missing)
        if(NOT _missing EQUAL -1)
            message(FATAL_ERROR "负对照 ${_name} 混有『文件缺失』失败，不能算对已有头的编译拒绝\n${_nout}")
        endif()
        string(FIND "${_nout}" "${_tu}" _source)
        if(_source EQUAL -1)
            message(FATAL_ERROR "负对照 ${_name} 的失败没有落在该探针 TU 上（rc=${_nrc}）：拒绝来源不明\n${_nout}")
        endif()
    else()
        # 缺头负例：必须是该公共路径头确实不存在（而不是依赖或 include 目录问题）。
        string(FIND "${_nout}" "No such file or directory" _missing)
        if(_missing EQUAL -1)
            message(FATAL_ERROR "负对照 ${_name} 未以『头不存在』失败（rc=${_nrc}）：公共路径上的测试接缝头仍有可编译入口\n${_nout}")
        endif()
    endif()
    message(STATUS "负对照成立：${_name} 以预期原因真实编译失败")
endforeach()

message(STATUS "公共面正/负编译门禁全部成立（负例 ${_negative_count} 项）")

# 高层 GNet UDP 拒绝头（<bbt/infra/net/Udp.hpp>）的正/负编译门禁。
#
# 与 expect_configure_failure.cmake 同一范式：只认「预期原因」的失败，且
# **不用 WILL_FAIL**——WILL_FAIL 会把断言失败、崩溃、超时等任意非零结果翻成通过。
#
# 三个最小 TU 都真编（-fsyntax-only，不产目标文件、不进构建图、不引入新 target）：
#   1) 正对照·原语：<bbt/infra/net/NativeSocket.hpp> 的 SocketType::Datagram /
#      SendTo / ReceiveFrom 仍可编译（原语未被本包破坏、include 路径可用）；
#   2) 正对照·高层头：普通 <bbt/infra/net/Engine.hpp> 仍可编译（已交付的 IPv4 TCP
#      高层路径未被连带拒绝破坏）；
#   3) 负对照：显式 #include <bbt/infra/net/Udp.hpp> 必须真实编译失败，且失败原因
#      必须是该头自身的拒绝标记，不能是「头不存在/缺依赖」。
# 1) 或 2) 任一失败 ⇒ 环境或回归坏，本 case 直接不通过；不允许用 3) 的失败掩盖它。
#
# 入参：CXX、INFRA_DIR、COROUTINE_DIR、WORK_DIR（由 tests/net/CMakeLists.txt 传入）。
if(NOT CXX OR NOT INFRA_DIR OR NOT COROUTINE_DIR OR NOT WORK_DIR)
    message(FATAL_ERROR "expect_udp_compile 缺少参数（CXX/INFRA_DIR/COROUTINE_DIR/WORK_DIR）")
endif()
file(MAKE_DIRECTORY "${WORK_DIR}")

# 与 bbt_infra_net_header_selfcheck 相同的编译口径（见 B/compile_commands.json）。
set(_flags -DBOOST_CONTEXT_DYN_LINK -DBOOST_CONTEXT_NO_LIB
    "-I${INFRA_DIR}" -isystem "${COROUTINE_DIR}" -std=c++17 -Wall -Wextra -Wpedantic)

set(_primitive_tu "${WORK_DIR}/udp_primitive_positive.cc")
file(WRITE "${_primitive_tu}"
    "#include <bbt/infra/net/NativeSocket.hpp>\n"
    "using namespace bbt::infra::net;\n"
    "void ProbePrimitive(std::error_code& ec, Address& peer) {\n"
    "    NativeSocket socket = NativeSocket::Open(AddressFamily::IPv4, SocketType::Datagram, ec);\n"
    "    char buffer[8] = {0};\n"
    "    (void)socket.SendTo(buffer, sizeof(buffer), peer);\n"
    "    (void)socket.ReceiveFrom(buffer, sizeof(buffer), peer);\n"
    "    (void)socket.Send(buffer, sizeof(buffer));\n"
    "    (void)socket.Receive(buffer, sizeof(buffer));\n"
    "}\n")

set(_engine_tu "${WORK_DIR}/engine_positive.cc")
file(WRITE "${_engine_tu}" "#include <bbt/infra/net/Engine.hpp>\n")

set(_reject_tu "${WORK_DIR}/udp_reject_negative.cc")
file(WRITE "${_reject_tu}" "#include <bbt/infra/net/Udp.hpp>\n")

execute_process(COMMAND "${CXX}" ${_flags} -fsyntax-only "${_primitive_tu}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _err)
string(APPEND _out "${_err}")
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "正对照(primitive) 编译失败（rc=${_rc}）：高层拒绝没理由破坏 datagram 原语\n${_out}")
endif()
message(STATUS "正对照(primitive) 编译通过：${_primitive_tu}")

execute_process(COMMAND "${CXX}" ${_flags} -fsyntax-only "${_engine_tu}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _err)
string(APPEND _out "${_err}")
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "正对照(engine) 编译失败（rc=${_rc}）：高层拒绝没理由破坏普通 Engine 头\n${_out}")
endif()
message(STATUS "正对照(engine) 编译通过：${_engine_tu}")

execute_process(COMMAND "${CXX}" ${_flags} -fsyntax-only "${_reject_tu}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _err)
string(APPEND _out "${_err}")
if(_rc EQUAL 0)
    message(FATAL_ERROR "负对照：<bbt/infra/net/Udp.hpp> 被编译器接受，未发生编译拒绝\n${_out}")
endif()
string(FIND "${_out}" "UDP is not supported yet" _marker)
if(_marker EQUAL -1)
    message(FATAL_ERROR "负对照失败原因不是预期拒绝标记（rc=${_rc}）：不接受「任意失败」\n${_out}")
endif()
string(FIND "${_out}" "bbt/infra/net/Udp.hpp" _source)
if(_source EQUAL -1)
    message(FATAL_ERROR "负对照的失败没有落在 <bbt/infra/net/Udp.hpp> 上（rc=${_rc}）：拒绝来源不明\n${_out}")
endif()
string(FIND "${_out}" "No such file or directory" _missing)
if(NOT _missing EQUAL -1)
    message(FATAL_ERROR "负对照混有『文件缺失』失败，不能算作对已有头的编译拒绝\n${_out}")
endif()
message(STATUS "负对照成立：<bbt/infra/net/Udp.hpp> 以自身拒绝标记真实编译失败（rc=${_rc}）")

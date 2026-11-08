# RPC V2 唯一消费面门禁（rpc.net.v1_absent）：正/负 TU 对照 + 源码树/构建图
# 事实检查，证明「V1 wire/adapter/proto/target 已退役」且「V2 公共面可用」。
#
# 与 tests/net/consumers/expect_public_surface.cmake 同一范式：只认预期原因的
# 失败，不用 WILL_FAIL，不产目标文件（-fsyntax-only），不进构建图。
#
#   1) 正对照：只 include V2 公共头并使用保留符号的 TU 必须编译通过。
#   2) 逐项负对照：旧限定名/旧头必须真实编译失败，且
#      (a) 失败输出含该探针首行声明的期望原因片段，
#      (b) 失败落在该探针 TU 上（不是别处的连带错误）。
#   3) 源码树事实：旧实现文件、旧目录、旧 proto、旧 CMake 模块确实不存在。
#   4) 构建图事实：configure 后的 target 集合不含旧 RPC target/开关，且确实
#      含唯一 V2 target（避免「图里什么都没有」的空通过）。
#
# 负对照探针是 consumers/negative/neg_*.cc，首行 `// expect-error: <片段>` 或
# `// expect-missing-header: <路径>` 声明期望原因。
#
# 入参（由 tests/rpc_net/CMakeLists.txt 传入）：CXX、INFRA_DIR、COROUTINE_DIR、
# BOOST_INCLUDE_DIRS、WORK_DIR、BUILD_DIR。

if(NOT CXX OR NOT INFRA_DIR OR NOT COROUTINE_DIR OR NOT BOOST_INCLUDE_DIRS OR NOT WORK_DIR OR NOT BUILD_DIR)
    message(FATAL_ERROR "expect_rpc_v2_surface 缺少参数（CXX/INFRA_DIR/COROUTINE_DIR/BOOST_INCLUDE_DIRS/WORK_DIR/BUILD_DIR）")
endif()
file(MAKE_DIRECTORY "${WORK_DIR}")

# 与 bbt_infra_rpc_net_header_selfcheck 相同的编译口径：公共头不泄漏
# HTTP/net/protobuf 类型，故只需仓根 + coroutine + Boost 头。
set(_flags -DBOOST_CONTEXT_DYN_LINK -DBOOST_CONTEXT_NO_LIB
    "-I${INFRA_DIR}" -isystem "${COROUTINE_DIR}" -std=c++17 -Wall -Wextra -Wpedantic)
foreach(_boost_include IN LISTS BOOST_INCLUDE_DIRS)
    list(APPEND _flags "-I${_boost_include}")
endforeach()

# --- 1) 正对照 ---------------------------------------------------------------
set(_positive_tu "${CMAKE_CURRENT_LIST_DIR}/positive_rpc_v2_surface.cc")
if(NOT EXISTS "${_positive_tu}")
    message(FATAL_ERROR "正对照 TU 缺失：${_positive_tu}")
endif()
execute_process(COMMAND "${CXX}" ${_flags} -fsyntax-only "${_positive_tu}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _err)
string(APPEND _out "${_err}")
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "正对照编译失败（rc=${_rc}）：V2 公共面本身不可用，负对照无意义\n${_out}")
endif()
message(STATUS "正对照通过：仅 V2 公共头 TU 编译成功（含 CoRpcServer::RpcHandler / AdmissionHandler）")

# --- 2) 负对照 ---------------------------------------------------------------
set(_case_dir "${CMAKE_CURRENT_LIST_DIR}/negative")
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
        message(FATAL_ERROR "负对照 ${_name}：编译器接受了应被删除的旧面\n${_nout}")
    endif()
    string(FIND "${_nout}" "${_marker}" _found)
    if(_found EQUAL -1)
        message(FATAL_ERROR "负对照 ${_name} 的失败原因不含预期片段『${_marker}』（rc=${_nrc}），不接受任意失败\n${_nout}")
    endif()
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
        string(FIND "${_nout}" "No such file or directory" _missing)
        if(NOT _missing EQUAL -1)
            message(FATAL_ERROR "负对照 ${_name} 混有『文件缺失』失败，不能算对已有头的编译拒绝\n${_nout}")
        endif()
        string(FIND "${_nout}" "${_tu}" _source)
        if(_source EQUAL -1)
            message(FATAL_ERROR "负对照 ${_name} 的失败没有落在该探针 TU 上（rc=${_nrc}）：拒绝来源不明\n${_nout}")
        endif()
    else()
        string(FIND "${_nout}" "No such file or directory" _missing)
        if(_missing EQUAL -1)
            message(FATAL_ERROR "负对照 ${_name} 未以『头不存在』失败（rc=${_nrc}）：旧头仍有可编译入口\n${_nout}")
        endif()
    endif()
    message(STATUS "负对照成立：${_name} 以预期原因真实编译失败")
endforeach()

# --- 3) 源码树事实 -----------------------------------------------------------
# 旧实现整文件/目录、旧 proto、旧 CMake 模块、旧专属测试与示例。
set(_absent_paths
    bbt/infra/rpc/RpcWire.hpp
    bbt/infra/rpc/RpcWire.cc
    bbt/infra/rpc/Transport.hpp
    bbt/infra/rpc/Transport.cc
    bbt/infra/rpc/HttpTransport.hpp
    bbt/infra/rpc/HttpTransport.cc
    bbt/infra/rpc/TcpTransport.hpp
    bbt/infra/rpc/TcpTransport.cc
    bbt/infra/rpc/detail/Core.hpp
    bbt/infra/rpc/detail/Core.cc
    bbt/infra/rpc_tcp
    proto/bbt/infra/rpc/v1
    cmake/RpcTcp.cmake
    cmake/RpcTcpTarget.cmake
    tests/rpc_net/Test_rpc_net_wire.cc
    tests/rpc_net/Test_rpc_net_roundtrip.cc
    tests/rpc_net/Test_rpc_net_faults.cc
    tests/rpc_net/Test_rpc_net_lifetime.cc
    tests/rpc_net/Test_rpc_net_tcp.cc
    examples/rpc_net/main.cc
    examples/rpc_net/tcp_main.cc)
foreach(_path IN LISTS _absent_paths)
    if(EXISTS "${INFRA_DIR}/${_path}")
        message(FATAL_ERROR "旧面仍存在：${_path}")
    endif()
endforeach()
# 正向对照：V2 唯一消费面与它的测试/示例必须真实存在，避免路径检查空转。
foreach(_present_path
        bbt/infra/rpc/SessionCodec.hpp
        proto/bbt/infra/rpc/v2/session_frame.proto
        tests/rpc_net/Test_rpc_v2_session.cc
        examples/rpc_net/session_main.cc)
    if(NOT EXISTS "${INFRA_DIR}/${_present_path}")
        message(FATAL_ERROR "V2 面缺失：${_present_path}")
    endif()
endforeach()

# --- 4) 构建图事实 -----------------------------------------------------------
if(NOT EXISTS "${BUILD_DIR}/CMakeCache.txt")
    message(FATAL_ERROR "缺少 ${BUILD_DIR}/CMakeCache.txt：需先 configure 才能核验 target/开关")
endif()
file(STRINGS "${BUILD_DIR}/CMakeCache.txt" _tcp_flag REGEX "BBT_INFRA_RPC_TCP")
if(_tcp_flag)
    message(FATAL_ERROR "构建缓存仍暴露旧开关 BBT_INFRA_RPC_TCP：${_tcp_flag}")
endif()
if(NOT EXISTS "${BUILD_DIR}/build.ninja")
    message(FATAL_ERROR "缺少 ${BUILD_DIR}/build.ninja：本门禁按 Ninja 构建图核验 target 集合")
endif()
file(READ "${BUILD_DIR}/build.ninja" _graph)
foreach(_old_target bbt_infra_rpc_core bbt_infra_rpc_http bbt_infra_rpc_tcp)
    string(FIND "${_graph}" "build ${_old_target}" _hit)
    if(NOT _hit EQUAL -1)
        message(FATAL_ERROR "构建图仍含旧 target：${_old_target}")
    endif()
endforeach()
string(FIND "${_graph}" "build bbt_infra_rpc_net" _v2_hit)
if(_v2_hit EQUAL -1)
    message(FATAL_ERROR "构建图不含唯一 V2 target bbt_infra_rpc_net：target 核验不成立")
endif()

message(STATUS "RPC V2 唯一消费面门禁全部成立（负例 ${_negative_count} 项 + 源码树/构建图）")

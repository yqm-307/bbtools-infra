# BBT_INFRA_RPC_NET=ON 时的接线：RPC core（wire + Client/Server）消费新
# bbt::infra_http_net 与锁定 protobuf。本文件在 network-only 闭包内是 protobuf
# 锁定与 wire target 的唯一真源：protoc 与 C++ runtime 必须同源精确版本；只允许
# 经显式前缀接入，禁止把本机私有绝对路径写进默认值或提交内容。
#
# 期望前缀布局（Ubuntu .deb 解包或自建安装均可满足）：
#   <pfx>/usr/bin/protoc
#   <pfx>/usr/include/google/protobuf/...
#   <pfx>/usr/lib/<triplet>/libprotobuf.so / .a
#   <pfx>/usr/lib/<triplet>/pkgconfig/protobuf.pc
#
# 锁定版本与来源见 docs/dependencies/protobuf-3.21.12.provenance.md：
# 上游 google/protobuf v3.21.12 源码 SHA 与 Ubuntu 二进制包元数据均在文件固定；
# 本文件只负责校验前缀内 protoc/runtime 与锁定值一致。

if(NOT BBT_INFRA_HTTP_NET)
    message(FATAL_ERROR
        "BBT_INFRA_RPC_NET=ON 需要 BBT_INFRA_HTTP_NET=ON："
        "RPC core 消费 bbt::infra_http_net，不单独构建。")
endif()

set(BBT_PROTOBUF_PREFIX "" CACHE PATH
    "protobuf 安装前缀（含 bin/protoc、include/google/protobuf 与 libprotobuf）")

if(NOT BBT_PROTOBUF_PREFIX)
    message(FATAL_ERROR
        "BBT_INFRA_RPC_NET=ON 需要显式 BBT_PROTOBUF_PREFIX（锁定 protobuf "
        "3.21.12 私有前缀）；不需要 RPC 时请关闭 BBT_INFRA_RPC_NET，"
        "不静默跳过。")
endif()

# Issue #8 锁定的精确版本（docs/dependencies/protobuf-3.21.12.provenance.md）。
# 上游 protobuf versioning：x.y.z → x*1000000 + y*1000 + z（3.21.12→3021012）。
set(BBT_PROTOBUF_PINNED_VERSION      "3.21.12")
set(BBT_PROTOBUF_PINNED_VERSION_NUM  3021012)

file(GLOB _bbt_protoc "${BBT_PROTOBUF_PREFIX}/usr/bin/protoc"
                      "${BBT_PROTOBUF_PREFIX}/bin/protoc")
if(NOT _bbt_protoc)
    message(FATAL_ERROR
        "BBT_PROTOBUF_PREFIX='${BBT_PROTOBUF_PREFIX}' 无效："
        "未找到 protoc 可执行文件（usr/bin/protoc 或 bin/protoc）")
endif()
# 优先静态库（.a）：消除运行时 LD_LIBRARY_PATH/rpath 依赖，保证
# bbt_infra_rpc_net、测试与下游 consumer 拿到自包含产物。so 仅作兜底。
# 禁止无序 GLOB 取第一个命中：候选路径显式列出并按顺序探测，命中即锁定。
set(_bbt_protobuf_lib_candidates
    "${BBT_PROTOBUF_PREFIX}/usr/lib/x86_64-linux-gnu/libprotobuf.a"
    "${BBT_PROTOBUF_PREFIX}/usr/lib/aarch64-linux-gnu/libprotobuf.a"
    "${BBT_PROTOBUF_PREFIX}/lib/libprotobuf.a"
    "${BBT_PROTOBUF_PREFIX}/usr/lib/x86_64-linux-gnu/libprotobuf.so"
    "${BBT_PROTOBUF_PREFIX}/usr/lib/aarch64-linux-gnu/libprotobuf.so"
    "${BBT_PROTOBUF_PREFIX}/lib/libprotobuf.so")
set(BBT_PROTOBUF_LIBRARY "")
foreach(_cand IN LISTS _bbt_protobuf_lib_candidates)
    if(EXISTS "${_cand}")
        set(BBT_PROTOBUF_LIBRARY "${_cand}")
        break()
    endif()
endforeach()
if(NOT BBT_PROTOBUF_LIBRARY)
    message(FATAL_ERROR
        "BBT_PROTOBUF_PREFIX='${BBT_PROTOBUF_PREFIX}' 无效："
        "未找到 libprotobuf 库文件（探测路径：${_bbt_protobuf_lib_candidates}）")
endif()
list(GET _bbt_protoc 0 BBT_PROTOC_EXECUTABLE)
# protoc 的 shared-lib 依赖必须在同一前缀内；Ubuntu .deb 解包后 RUNPATH
# 只保留原始打包路径，本机无法可靠执行。统一用 LD_LIBRARY_PATH 限定
# 到前缀自身，防止误拉系统 libprotobuf/libprotoc。
get_filename_component(_bbt_protobuf_libdir "${BBT_PROTOBUF_LIBRARY}" DIRECTORY)
set(BBT_PROTOBUF_LIB_DIR "${_bbt_protobuf_libdir}")

set(BBT_PROTOBUF_INCLUDE_DIR "${BBT_PROTOBUF_PREFIX}/usr/include")
if(NOT IS_DIRECTORY "${BBT_PROTOBUF_INCLUDE_DIR}/google/protobuf")
    set(BBT_PROTOBUF_INCLUDE_DIR "${BBT_PROTOBUF_PREFIX}/include")
endif()
if(NOT IS_DIRECTORY "${BBT_PROTOBUF_INCLUDE_DIR}/google/protobuf")
    message(FATAL_ERROR
        "BBT_PROTOBUF_PREFIX='${BBT_PROTOBUF_PREFIX}' 无效："
        "未找到 include/google/protobuf 头目录")
endif()

# 同源版本钉死：protoc --version 必须与 runtime 头文件中的
# GOOGLE_PROTOBUF_VERSION 一致，且二者都必须等于锁定版本（3.21.12 → 3021012）。
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
            "LD_LIBRARY_PATH=${BBT_PROTOBUF_LIB_DIR}"
            "${BBT_PROTOC_EXECUTABLE}" --version
    OUTPUT_VARIABLE _bbt_protoc_ver_raw
    ERROR_VARIABLE  _bbt_protoc_ver_err
    RESULT_VARIABLE _bbt_protoc_ver_rc)
if(NOT _bbt_protoc_ver_rc EQUAL 0)
    message(FATAL_ERROR "protoc 无法执行：${_bbt_protoc_ver_err}")
endif()
string(REGEX MATCH "[0-9]+\\.[0-9]+\\.[0-9]+"
    BBT_PROTOC_VERSION "${_bbt_protoc_ver_raw}")
if(NOT BBT_PROTOC_VERSION)
    message(FATAL_ERROR "无法从 protoc --version 解析版本号：${_bbt_protoc_ver_raw}")
endif()
string(REGEX MATCH "^([0-9]+)\\.([0-9]+)\\.([0-9]+)$" _m "${BBT_PROTOC_VERSION}")
math(EXPR BBT_PROTOC_VERSION_NUM
    "${CMAKE_MATCH_1} * 1000000 + ${CMAKE_MATCH_2} * 1000 + ${CMAKE_MATCH_3}")

file(STRINGS "${BBT_PROTOBUF_INCLUDE_DIR}/google/protobuf/stubs/common.h"
    _bbt_runtime_ver_line
    REGEX "^#define GOOGLE_PROTOBUF_VERSION")
string(REGEX MATCH "[0-9]+" BBT_PROTOBUF_RUNTIME_VERSION_NUM
    "${_bbt_runtime_ver_line}")
if(NOT BBT_PROTOBUF_RUNTIME_VERSION_NUM)
    message(FATAL_ERROR "无法从 stubs/common.h 解析 GOOGLE_PROTOBUF_VERSION")
endif()
if(NOT BBT_PROTOC_VERSION_NUM EQUAL BBT_PROTOBUF_RUNTIME_VERSION_NUM)
    message(FATAL_ERROR
        "protoc 与 runtime 版本不同源："
        "protoc=${BBT_PROTOC_VERSION}(${BBT_PROTOC_VERSION_NUM}) "
        "runtime=${BBT_PROTOBUF_RUNTIME_VERSION_NUM}。"
        "Issue #8 要求二者精确版本一致")
endif()
if(NOT BBT_PROTOC_VERSION_NUM EQUAL BBT_PROTOBUF_PINNED_VERSION_NUM)
    message(FATAL_ERROR
        "protobuf 版本不符合锁定："
        "实际=${BBT_PROTOC_VERSION}(${BBT_PROTOC_VERSION_NUM}) "
        "要求=${BBT_PROTOBUF_PINNED_VERSION}(${BBT_PROTOBUF_PINNED_VERSION_NUM})。"
        "锁定来源见 docs/dependencies/protobuf-3.21.12.provenance.md")
endif()

# --- runtime 库精确来源核验 ----------------------------------------------------
# 前缀可能混有同名异版 libprotobuf；实际选中的库文件钉到显式 SHA256 清单，
# 命中失败即 FATAL_ERROR。清单来源：docs/dependencies/protobuf-3.21.12.provenance.md。
set(BBT_PROTOBUF_KNOWN_LIB_SHA256
    # Ubuntu 3.21.12-15ubuntu1 libprotobuf-dev / libprotobuf32t64（amd64）
    "3e3e4ae1f24b2dd52e8104e7af229918a2d504274a08f7d660aaff59bae7a2de"  # libprotobuf.a
    "09a7b17e8f42aac853a4833b37191439442db92fce9da28f690bfa7a34c7ef9f"  # libprotobuf.so(.32.0.12)
)
file(SHA256 "${BBT_PROTOBUF_LIBRARY}" _bbt_lib_sha256)
list(FIND BBT_PROTOBUF_KNOWN_LIB_SHA256 "${_bbt_lib_sha256}" _sha_idx)
if(_sha_idx LESS 0)
    message(FATAL_ERROR
        "libprotobuf 文件 SHA256 不在锁定清单：\n"
        "  实际文件: ${BBT_PROTOBUF_LIBRARY}\n"
        "  SHA256  : ${_bbt_lib_sha256}\n"
        "  允许值  : ${BBT_PROTOBUF_KNOWN_LIB_SHA256}\n"
        "该库与 docs/dependencies/protobuf-3.21.12.provenance.md 锁定的"
        " Ubuntu 3.21.12-15ubuntu1 二进制不一致，拒绝继续。")
endif()
set(BBT_PROTOBUF_LIBRARY_SHA256 "${_bbt_lib_sha256}")
message(STATUS
    "bbtools-infra: libprotobuf 锁定命中 "
    "file='${BBT_PROTOBUF_LIBRARY}' sha256=${_bbt_lib_sha256}")

# pkg-config 交叉核验：同前缀 protobuf.pc 的 Version 须等于 pin 版本。
set(_bbt_pc "${BBT_PROTOBUF_LIB_DIR}/pkgconfig/protobuf.pc")
if(EXISTS "${_bbt_pc}")
    file(STRINGS "${_bbt_pc}" _bbt_pc_version_line REGEX "^Version:")
    string(REGEX MATCH "[0-9]+\\.[0-9]+\\.[0-9]+" BBT_PROTOBUF_PC_VERSION
        "${_bbt_pc_version_line}")
    if(NOT BBT_PROTOBUF_PC_VERSION STREQUAL BBT_PROTOBUF_PINNED_VERSION)
        message(FATAL_ERROR
            "protobuf.pc Version=${BBT_PROTOBUF_PC_VERSION} 与锁定 "
            "${BBT_PROTOBUF_PINNED_VERSION} 不一致（${_bbt_pc}）")
    endif()
else()
    message(FATAL_ERROR
        "缺少 ${_bbt_pc}：无法交叉核验前缀内 protobuf 包元数据来源")
endif()
message(STATUS
    "bbtools-infra: protobuf 同源锁定 "
    "protoc=${BBT_PROTOC_VERSION} runtime=${BBT_PROTOBUF_RUNTIME_VERSION_NUM} "
    "(pinned=${BBT_PROTOBUF_PINNED_VERSION}) prefix='${BBT_PROTOBUF_PREFIX}'")

# 从 proto 真源生成 C++；生成物落在 build 目录，不进仓库、不污染源码树。
set(BBT_RPC_PROTO "${PROJECT_SOURCE_DIR}/proto/bbt/infra/rpc/v1/rpc_envelope.proto")
set(BBT_RPC_GEN_DIR "${CMAKE_BINARY_DIR}/generated/rpc_net")
file(MAKE_DIRECTORY "${BBT_RPC_GEN_DIR}")
add_custom_command(
    OUTPUT "${BBT_RPC_GEN_DIR}/bbt/infra/rpc/v1/rpc_envelope.pb.cc"
           "${BBT_RPC_GEN_DIR}/bbt/infra/rpc/v1/rpc_envelope.pb.h"
    COMMAND "${CMAKE_COMMAND}" -E env
            "LD_LIBRARY_PATH=${BBT_PROTOBUF_LIB_DIR}"
            "${BBT_PROTOC_EXECUTABLE}"
            --proto_path "${PROJECT_SOURCE_DIR}/proto"
            --cpp_out "${BBT_RPC_GEN_DIR}"
            "${BBT_RPC_PROTO}"
    DEPENDS "${BBT_RPC_PROTO}" "${BBT_PROTOC_EXECUTABLE}"
    COMMENT "protoc generate rpc_envelope.pb.{h,cc} (v${BBT_PROTOC_VERSION})"
    VERBATIM)
add_library(bbt_infra_rpc_proto STATIC
    "${BBT_RPC_GEN_DIR}/bbt/infra/rpc/v1/rpc_envelope.pb.cc")
target_include_directories(bbt_infra_rpc_proto PUBLIC
    "${BBT_RPC_GEN_DIR}" "${BBT_PROTOBUF_INCLUDE_DIR}")
target_link_libraries(bbt_infra_rpc_proto PUBLIC "${BBT_PROTOBUF_LIBRARY}")
# protobuf 3.21 生成代码仍兼容 C++17；不提升本仓标准。
target_compile_features(bbt_infra_rpc_proto PUBLIC cxx_std_17)
set_target_properties(bbt_infra_rpc_proto PROPERTIES
    POSITION_INDEPENDENT_CODE ON)

# RPC core 模块本体 + 测试/示例注册。
add_subdirectory("${PROJECT_SOURCE_DIR}/bbt/infra/rpc" rpc_net)

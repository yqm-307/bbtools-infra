cmake_minimum_required(VERSION 3.16)
project(bbtools-infra LANGUAGES CXX)

# Boost>=1.70 自带 BoostConfig.cmake；明确跳过已废弃的 FindBoost 模块，
# 消除 CMP0167 dev 警告（新版 CMake 上 find_package(Boost) 否则告警）。
if(POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# --- 内部 debug 观测层开关（PR #70 review 整改）--------------------------------
# 与 bbtools-coroutine 的 STRINGENT_DEBUG（其 add_definitions）对齐，但修正两处
# review 指出的边界：
#   1) 配置边界：宏只在 **Debug 配置** 生效。用生成器表达式 $<CONFIG:Debug> 固化，
#      单/多配置生成器一致；Release（及任何非 Debug 配置）结构上不可能带上该宏
#      ⇒ 结构上零开销，不依赖使用者记得关 option，也不会「只开 option 就让 Release
#      带宏还宣称零开销」。option 只作为 Debug 下的显式开关。
#   2) 传播机制：用目标级 INTERFACE compile definition（经 bbt::infra_internal_debug
#      这一 INTERFACE 库）随链接接口传播，而不是目录级 add_compile_definitions
#      （后者是目录属性，不随 target/链接接口传播，无法保证外部 add_subdirectory
#      父级或链接消费者与库 TU 的宏取值一致 ⇒ 潜在 ODR/ABI 混配）。只有真正链接
#      该 INTERFACE 的三个模块库及其内部测试消费者拿到宏，不污染无关 target。
option(BBT_INFRA_STRINGENT_DEBUG
    "启用 bbtools-infra 内部 debug 观测层（仅 Debug 配置定义宏）" OFF)
add_library(bbt_infra_internal_debug INTERFACE)
add_library(bbt::infra_internal_debug ALIAS bbt_infra_internal_debug)
if(BBT_INFRA_STRINGENT_DEBUG)
    target_compile_definitions(bbt_infra_internal_debug INTERFACE
        $<$<CONFIG:Debug>:BBT_INFRA_STRINGENT_DEBUG>)
    message(STATUS
        "bbtools-infra: 内部 debug 观测层已启用（仅 Debug 配置定义 "
        "BBT_INFRA_STRINGENT_DEBUG）")
else()
    message(STATUS "bbtools-infra: 内部 debug 观测层已关闭（BBT_INFRA_STRINGENT_DEBUG=OFF）")
endif()

# 公共契约头库：header-only，向外暴露 include/
add_library(bbt_infra_common INTERFACE)
add_library(bbt::infra_common ALIAS bbt_infra_common)
target_include_directories(bbt_infra_common INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)
target_compile_features(bbt_infra_common INTERFACE cxx_std_17)

# --- 上游源码依赖接入：bbtools-coroutine / bbtools-core -----------------------
# 两个上游仓当前都不导出可消费的 CMake 包（core 没有 install/export 规则），源码
# 消费只能经显式路径 add_subdirectory 接入，不能依赖机器上的隐式安装产物。正式接入
# 待上游提供固定 SHA 的消费方式后收敛。禁止把本机私有绝对路径写进默认值或提交内容。
set(BBT_COROUTINE_SOURCE_DIR "" CACHE PATH
    "本机/候选分支开发用：bbtools-coroutine 源码树路径（含顶层 CMakeLists.txt 与 bbt/）")
set(BBT_CORE_SOURCE_DIR "" CACHE PATH
    "源码消费用：bbtools-core 源码树路径（含顶层 CMakeLists.txt 与 bbt/core/）")

# 以源码方式接入一个未导出包的上游。上游用目录级 include_directories（不导出 use
# requirements）、cmake_minimum_required(<3.5)、且默认会把自己的单测/示例注册进来；
# 这里统一收敛：临时遮蔽上游开关、临时给出 CMAKE_POLICY_VERSION_MINIMUM，并把上游
# 源码根补成目标级接口，使消费经正式 target 传播而不是全局编译/链接 flags。
function(bbt_add_source_dependency _target _src_dir _bin_subdir)
    if(NOT EXISTS "${_src_dir}/CMakeLists.txt")
        message(FATAL_ERROR
            "源码依赖 '${_src_dir}' 无效：未找到顶层 CMakeLists.txt")
    endif()

    # ARGN 是上游默认 ON 的开关（NEED_TEST/ENABLE_EXAMPLE 等）：不遮蔽就会把上游
    # 单测/示例注册进本仓 ctest（其可执行目标并不在本构建内编译）。上游
    # cmake_minimum_required(3.1/3.2) 使其作用域内 CMP0077=OLD：option() 不看普通
    # 变量，必须临时改 CACHE 再还原；还原必须基于 CACHE 原值——调用方（例如
    # bbt-framework）可能用 -DNEED_TEST=ON 传入 CACHE，若读作用域内变量，会把调用
    # 方的开关静默改成 OFF。
    set(_shadowed "")
    set(_saved_names "")
    set(_saved_values "")
    foreach(_opt IN LISTS ARGN)
        if(DEFINED CACHE{${_opt}})
            list(APPEND _saved_names "${_opt}")
            list(APPEND _saved_values "$CACHE{${_opt}}")
        endif()
        list(APPEND _shadowed "${_opt}")
        set(${_opt} OFF CACHE BOOL "temporarily shadowed for source dependency" FORCE)
    endforeach()

    # 上游 cmake_minimum_required(3.1/3.2)：CMake ≥4 已移除对 <3.5 的兼容，configure
    # 直接报错。按 CMake 官方提示只在依赖作用域内临时给出 CMAKE_POLICY_VERSION_MINIMUM
    # （函数作用域，返回即自动还原；3.5 远小于 CMP0077 的 3.13，上游 option() 的 OLD
    # 语义不变，上面的遮蔽仍然必要）。
    set(CMAKE_POLICY_VERSION_MINIMUM 3.5)
    add_subdirectory("${_src_dir}"
                     "${CMAKE_BINARY_DIR}/_deps/${_bin_subdir}"
                     EXCLUDE_FROM_ALL)

    # 上游目录级 include_directories 不导出头路径：把上游源码根补成目标级接口，
    # 链接该 target 的消费者经 use requirements 拿到头，而不是靠全局 -I。
    # SYSTEM 使上游头告警不计入本仓告警面；BUILD_INTERFACE 使其不进安装接口。
    target_include_directories(${_target} SYSTEM INTERFACE
        "$<BUILD_INTERFACE:${_src_dir}>")

    foreach(_opt IN LISTS _shadowed)
        list(FIND _saved_names "${_opt}" _saved_index)
        if(_saved_index GREATER -1)
            list(GET _saved_values ${_saved_index} _saved_value)
            set(${_opt} "${_saved_value}" CACHE BOOL
                "上游 option 语义；对本仓无作用" FORCE)
        else()
            unset(${_opt} CACHE)
        endif()
    endforeach()
endfunction()

if(BBT_COROUTINE_SOURCE_DIR)
    if(NOT EXISTS "${BBT_COROUTINE_SOURCE_DIR}/CMakeLists.txt")
        message(FATAL_ERROR
            "BBT_COROUTINE_SOURCE_DIR='${BBT_COROUTINE_SOURCE_DIR}' 无效："
            "未找到顶层 CMakeLists.txt")
    endif()

    # core 必须在 coroutine 之前接入：coroutine 顶层直接
    # target_link_libraries(bbt_coroutine ... bbt_core)，此刻 bbt_core 若还不是
    # target，CMake 只会记下裸库名 -lbbt_core（隐式依赖机器上的安装产物），之后再
    # 定义同名 target 也不会把它升级成正式依赖。
    if(NOT BBT_CORE_SOURCE_DIR)
        # 未显式给出时按「多仓并排检出」布局在上游/本仓同级目录探测，且必须同时
        # 具备顶层 CMakeLists.txt 与 bbt/core/，避免绑定到同名空目录或错误源码树。
        foreach(_anchor "${BBT_COROUTINE_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}")
            get_filename_component(_sibling "${_anchor}/../bbtools-core" ABSOLUTE)
            if(EXISTS "${_sibling}/CMakeLists.txt" AND IS_DIRECTORY "${_sibling}/bbt/core")
                set(BBT_CORE_SOURCE_DIR "${_sibling}")
                break()
            endif()
        endforeach()
    endif()
    if(BBT_CORE_SOURCE_DIR AND NOT TARGET bbt_core)
        bbt_add_source_dependency(bbt_core "${BBT_CORE_SOURCE_DIR}" bbtools-core
            NEED_TEST ENABLE_EXAMPLE)
        message(STATUS
            "bbtools-infra: bbtools-core 源码接入 '${BBT_CORE_SOURCE_DIR}'")
    endif()

    bbt_add_source_dependency(bbt_coroutine "${BBT_COROUTINE_SOURCE_DIR}"
        bbtools-coroutine NEED_TEST)
    target_link_libraries(bbt_infra_common INTERFACE bbt_coroutine)

    # 契约校验：coroutine 仍按核心库名消费 core（拆分前形态）时，本构建必须已有
    # 真实 bbt_core target，否则链接期会退化成机器上的隐式 -lbbt_core 与隐式
    # /usr/local 头文件——本仓不做静默兜底。coroutine 拆分自闭环后不再引用
    # bbt_core，此处自动放行。
    if(NOT TARGET bbt_core)
        file(READ "${BBT_COROUTINE_SOURCE_DIR}/CMakeLists.txt" _bbt_co_cmake)
        if(_bbt_co_cmake MATCHES "(^|[^_A-Za-z0-9])bbt_core([^_A-Za-z0-9]|$)")
            message(FATAL_ERROR
                "bbtools-coroutine('${BBT_COROUTINE_SOURCE_DIR}') 依赖 bbtools-core，"
                "但本构建未提供 bbt_core target。bbtools-core 当前没有 install/export "
                "规则，不能经 find_package 获得；请显式传 "
                "-DBBT_CORE_SOURCE_DIR=<bbtools-core 源码树路径>。")
        endif()
    endif()
else()
    # 回退：查找已安装的 bbt_coroutine 包（导出 bbt_coroutine 目标）。
    find_package(bbt_coroutine CONFIG QUIET)
    if(NOT TARGET bbt_coroutine)
        message(FATAL_ERROR
            "未找到 bbt_coroutine。本机/候选分支开发请显式传 "
            "-DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树路径>；"
            "已安装包需提供 bbt_coroutineConfig.cmake 并导出 bbt_coroutine 目标。")
    endif()
    target_link_libraries(bbt_infra_common INTERFACE bbt_coroutine)
endif()

# --- hiredis 依赖接入（Issue #6，Redis 模块）---------------------------------
# 与 coroutine 同规约：仅允许经显式前缀接入，禁止把本机私有绝对路径
# 写进默认值或任何提交内容。src/CMakeLists.txt 内 find_package 限定
# NO_DEFAULT_PATH，杜绝误链系统位置 hiredis；未设置时跳过 Redis 模块。
set(BBT_HIREDIS_PREFIX "" CACHE PATH
    "hiredis 安装前缀（含 include/hiredis 与 lib/cmake/hiredis）")


# --- mongo-cxx-driver 依赖接入（Issue #7，Mongo 模块）-----------------------
# 与 coroutine 同规约：仅允许经显式前缀接入，禁止把本机私有绝对路径
# 写进默认值或任何提交内容。src/CMakeLists.txt 内 find_package 限定
# NO_DEFAULT_PATH，杜绝误链系统位置 mongocxx/libmongoc；mongocxx 的
# package config 依赖 mongoc/bsoncxx 包，C driver 前缀经独立变量限定。
# 未设置时跳过 Mongo 模块。
set(BBT_MONGOCXX_PREFIX "" CACHE PATH
    "mongo-cxx-driver 安装前缀（含 include/mongocxx 与 lib/cmake/mongocxx）")
set(BBT_MONGOC_PREFIX "" CACHE PATH
    "mongo-c-driver 安装前缀（含 include/mongoc 与 lib/cmake/mongoc）；mongocxxConfig.cmake 内 find_dependency(mongoc) 需要")

# find_package 放在顶层作用域：包导出的 mongo::mongocxx_shared 等
# IMPORTED target 只有目录级可见性，src/ 与 tests/ 都需要引用。
# mongocxxConfig.cmake 内 find_dependency(mongoc/bsoncxx) 的嵌套
# find_package 经 <pkg>_DIR 钉到私有前缀唯一版本目录（NO_DEFAULT_PATH
# 会传播给嵌套调用使 CMAKE_PREFIX_PATH 失效，故不用 CMAKE_PREFIX_PATH）。
# 成对门禁：两个前缀必须同时给或同时不给——只给 mongocxx 前缀会让
# find_dependency(mongoc) 回落默认搜索路径，可能误链系统 libmongoc。
if(BBT_MONGOCXX_PREFIX OR BBT_MONGOC_PREFIX)
    if(NOT BBT_MONGOCXX_PREFIX OR NOT BBT_MONGOC_PREFIX)
        message(FATAL_ERROR
            "BBT_MONGOCXX_PREFIX 与 BBT_MONGOC_PREFIX 必须成对设置："
            "mongo-cxx-driver 的 package config 依赖 mongo-c-driver 前缀；"
            "只给一个会导致 find_dependency 回落系统路径。")
    endif()
endif()
if(BBT_MONGOCXX_PREFIX)
    if(BBT_MONGOC_PREFIX)
        # file(GLOB) 返回列表：多命中时 IS_DIRECTORY 对整条列表为假，旧写法
        # 会把「命中多个版本目录」误报成「未找到」。这里按命中数区分：0 命中
        # = 前缀无效，多命中 = 版本不确定；均 fail-closed，诊断必须可区分
        # （Issue #12 M3）。单命中仍须通过原有目录检查，禁止包搜索静默回退。
        file(GLOB mongoc_DIR "${BBT_MONGOC_PREFIX}/lib/cmake/mongoc-*")
        file(GLOB bson_DIR   "${BBT_MONGOC_PREFIX}/lib/cmake/bson-*")
        list(LENGTH mongoc_DIR _bbt_mongoc_n)
        list(LENGTH bson_DIR   _bbt_bson_n)
        if(_bbt_mongoc_n EQUAL 0)
            message(FATAL_ERROR
                "BBT_MONGOC_PREFIX='${BBT_MONGOC_PREFIX}' 无效：未找到 "
                "lib/cmake/mongoc-*")
        elseif(NOT _bbt_mongoc_n EQUAL 1)
            message(FATAL_ERROR
                "BBT_MONGOC_PREFIX='${BBT_MONGOC_PREFIX}' 无效：命中多个版本目录。\n"
                "lib/cmake/mongoc-* 命中 ${_bbt_mongoc_n} 个目录\n"
                "命中目录：${mongoc_DIR}")
        elseif(NOT IS_DIRECTORY "${mongoc_DIR}")
            message(FATAL_ERROR
                "BBT_MONGOC_PREFIX='${BBT_MONGOC_PREFIX}' 无效："
                "lib/cmake/mongoc-* 命中路径不是目录")
        endif()
        if(_bbt_bson_n EQUAL 0)
            message(FATAL_ERROR
                "BBT_MONGOC_PREFIX='${BBT_MONGOC_PREFIX}' 无效：未找到 "
                "lib/cmake/bson-*")
        elseif(NOT _bbt_bson_n EQUAL 1)
            message(FATAL_ERROR
                "BBT_MONGOC_PREFIX='${BBT_MONGOC_PREFIX}' 无效：命中多个版本目录。\n"
                "lib/cmake/bson-* 命中 ${_bbt_bson_n} 个目录\n"
                "命中目录：${bson_DIR}")
        elseif(NOT IS_DIRECTORY "${bson_DIR}")
            message(FATAL_ERROR
                "BBT_MONGOC_PREFIX='${BBT_MONGOC_PREFIX}' 无效："
                "lib/cmake/bson-* 命中路径不是目录")
        endif()
    endif()
    file(GLOB bsoncxx_DIR "${BBT_MONGOCXX_PREFIX}/lib/cmake/bsoncxx-*")
    list(LENGTH bsoncxx_DIR _bbt_bsoncxx_n)
    if(_bbt_bsoncxx_n EQUAL 0)
        message(FATAL_ERROR
            "BBT_MONGOCXX_PREFIX='${BBT_MONGOCXX_PREFIX}' 无效：未找到 "
            "lib/cmake/bsoncxx-*")
    elseif(NOT _bbt_bsoncxx_n EQUAL 1)
        message(FATAL_ERROR
            "BBT_MONGOCXX_PREFIX='${BBT_MONGOCXX_PREFIX}' 无效：命中多个版本目录。\n"
            "lib/cmake/bsoncxx-* 命中 ${_bbt_bsoncxx_n} 个目录\n"
            "命中目录：${bsoncxx_DIR}")
    elseif(NOT IS_DIRECTORY "${bsoncxx_DIR}")
        message(FATAL_ERROR
            "BBT_MONGOCXX_PREFIX='${BBT_MONGOCXX_PREFIX}' 无效："
            "lib/cmake/bsoncxx-* 命中路径不是目录")
    endif()
    find_package(mongocxx CONFIG REQUIRED
        PATHS "${BBT_MONGOCXX_PREFIX}" NO_DEFAULT_PATH)
endif()

# --- Protobuf 依赖接入（Issue #8，RPC wire 模块）---------------------------
# protoc 与 C++ runtime 必须同源精确版本；只允许经显式前缀接入，禁止把
# 本机私有绝对路径写进默认值或提交内容。未设置时跳过 RPC wire 模块，
# infra 其余目标仍可独立构建。
#
# 期望前缀布局（Ubuntu .deb 解包或自建安装均可满足）：
#   <pfx>/usr/bin/protoc
#   <pfx>/usr/include/google/protobuf/...
#   <pfx>/usr/lib/<triplet>/libprotobuf.so / .a
#   <pfx>/usr/lib/<triplet>/pkgconfig/protobuf.pc
# 私有前缀内允许 rpath 修正（patchelf），不得依赖系统路径兜底。
#
# 锁定版本与来源见 docs/dependencies/protobuf-3.21.12.provenance.md：
# 上游 google/protobuf v3.21.12 源码 SHA 与 Ubuntu 二进制包元数据
# 均在该文件固定；本段只负责校验前缀内 protoc/runtime 与锁定值一致。
set(BBT_PROTOBUF_PREFIX "" CACHE PATH
    "protobuf 安装前缀（含 bin/protoc、include/google/protobuf 与 libprotobuf）")

# Issue #8 锁定的精确版本（docs/dependencies/protobuf-3.21.12.provenance.md）。
# 上游 protobuf  versioning：x.y.z → x*1000000 + y*1000 + z（3.21.12→3021012）。
set(BBT_PROTOBUF_PINNED_VERSION      "3.21.12")
set(BBT_PROTOBUF_PINNED_VERSION_NUM  3021012)

if(BBT_PROTOBUF_PREFIX)
    file(GLOB _bbt_protoc "${BBT_PROTOBUF_PREFIX}/usr/bin/protoc"
                          "${BBT_PROTOBUF_PREFIX}/bin/protoc")
    if(NOT _bbt_protoc)
        message(FATAL_ERROR
            "BBT_PROTOBUF_PREFIX='${BBT_PROTOBUF_PREFIX}' 无效："
            "未找到 protoc 可执行文件（usr/bin/protoc 或 bin/protoc）")
    endif()
    # 优先静态库（.a）：消除运行时 LD_LIBRARY_PATH/rpath 依赖，保证
    # bbt_infra_rpc、测试与下游 consumer 拿到自包含产物。so 仅作兜底。
    # 禁止无序 GLOB 取第一个命中（R1 kanban_run_63 Issue-3）：候选路径显式
    # 列出并按顺序探测，命中即锁定，不存在静默「取第一个」的歧义。
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
    # GOOGLE_PROTOBUF_VERSION 一致，且二者都必须等于锁定版本
    # （3.21.12 → 3021012）。仅相等不钉死版本仍然违反 Issue #8。
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E env
                "LD_LIBRARY_PATH=${BBT_PROTOBUF_LIB_DIR}"
                "${BBT_PROTOC_EXECUTABLE}" --version
        OUTPUT_VARIABLE _bbt_protoc_ver_raw
        ERROR_VARIABLE  _bbt_protoc_ver_err
        RESULT_VARIABLE _bbt_protoc_ver_rc)
    if(NOT _bbt_protoc_ver_rc EQUAL 0)
        message(FATAL_ERROR
            "protoc 无法执行：${_bbt_protoc_ver_err}")
    endif()
    string(REGEX MATCH "[0-9]+\\.[0-9]+\\.[0-9]+"
        BBT_PROTOC_VERSION "${_bbt_protoc_ver_raw}")
    if(NOT BBT_PROTOC_VERSION)
        message(FATAL_ERROR
            "无法从 protoc --version 解析版本号：${_bbt_protoc_ver_raw}")
    endif()
    string(REGEX MATCH "^([0-9]+)\\.([0-9]+)\\.([0-9]+)$" _m
        "${BBT_PROTOC_VERSION}")
    math(EXPR BBT_PROTOC_VERSION_NUM
        "${CMAKE_MATCH_1} * 1000000 + ${CMAKE_MATCH_2} * 1000 + ${CMAKE_MATCH_3}")

    file(STRINGS "${BBT_PROTOBUF_INCLUDE_DIR}/google/protobuf/stubs/common.h"
        _bbt_runtime_ver_line
        REGEX "^#define GOOGLE_PROTOBUF_VERSION")
    string(REGEX MATCH "[0-9]+" BBT_PROTOBUF_RUNTIME_VERSION_NUM
        "${_bbt_runtime_ver_line}")
    if(NOT BBT_PROTOBUF_RUNTIME_VERSION_NUM)
        message(FATAL_ERROR
            "无法从 stubs/common.h 解析 GOOGLE_PROTOBUF_VERSION")
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

    # --- runtime 库精确来源核验（R1 kanban_run_63 Issue-3）--------------------
    # 仅校验 protoc/headers 版本不够：前缀可能混有同名异版 libprotobuf。
    # 这里把实际选中的库文件钉到显式 SHA256 清单；命中失败即 FATAL_ERROR。
    # 清单来源：docs/dependencies/protobuf-3.21.12.provenance.md（每个 .deb
    # 的官方 SHA256 + 解包后文件 SHA256，可经 apt-get download 复核）。
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

    # pkg-config 交叉核验：同前缀 protobuf.pc 的 Version 须等于 pin 版本，
    # 防止「头文件/库是 3.21.12 但 pkg-config 元数据漂移」的混装前缀。
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
        "(pinned=${BBT_PROTOBUF_PINNED_VERSION}) "
        "prefix='${BBT_PROTOBUF_PREFIX}'")

    # 从 proto 真源生成 C++；生成物落在 build 目录，不进仓库、不污染源码树。
    set(BBT_RPC_PROTO "${CMAKE_CURRENT_SOURCE_DIR}/proto/bbt/infra/rpc/v1/rpc_envelope.proto")
    set(BBT_RPC_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
    file(MAKE_DIRECTORY "${BBT_RPC_GEN_DIR}")
    add_custom_command(
        OUTPUT "${BBT_RPC_GEN_DIR}/bbt/infra/rpc/v1/rpc_envelope.pb.cc"
               "${BBT_RPC_GEN_DIR}/bbt/infra/rpc/v1/rpc_envelope.pb.h"
        COMMAND "${CMAKE_COMMAND}" -E env
                "LD_LIBRARY_PATH=${BBT_PROTOBUF_LIB_DIR}"
                "${BBT_PROTOC_EXECUTABLE}"
                --proto_path "${CMAKE_CURRENT_SOURCE_DIR}/proto"
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
endif()

add_subdirectory(src)

# Issue #27：最小真实消费示例（examples/）；不影响库目标与测试注册。
add_subdirectory(examples)

# 每个公共头单独编译 TU，验证头文件自给
set(BBT_INFRA_PUBLIC_HEADERS
    Result.hpp
    ICoObject.hpp
    ICoNetwork.hpp
    ICoCloseable.hpp
    NetworkTypes.hpp
    Codec.hpp
    HttpClient.hpp
    HttpServer.hpp
    NetworkRuntime.hpp
    TransportRuntime.hpp
    CoRedisCli.hpp
    CoMongoCli.hpp
    mongo/Client.hpp
    CoTCP.hpp
    CoUDP.hpp
    config/Value.hpp
    config/Snapshot.hpp
    config/Source.hpp
    config/MemorySource.hpp
    config/FileSource.hpp
    config/Watch.hpp)

if(TARGET bbt_infra_rpc_proto)
    list(APPEND BBT_INFRA_PUBLIC_HEADERS rpc/RpcWire.hpp)
endif()
set(_selfcheck_dir "${CMAKE_CURRENT_BINARY_DIR}/header_selfcheck")
file(MAKE_DIRECTORY "${_selfcheck_dir}")
set(_selfcheck_srcs "")
foreach(_hdr IN LISTS BBT_INFRA_PUBLIC_HEADERS)
    get_filename_component(_name "${_hdr}" NAME_WE)
    set(_tu "${_selfcheck_dir}/${_name}.cc")
    file(WRITE "${_tu}" "#include <bbt/infra/${_hdr}>\n")
    list(APPEND _selfcheck_srcs "${_tu}")
endforeach()
add_library(bbt_infra_header_selfcheck STATIC ${_selfcheck_srcs})
target_link_libraries(bbt_infra_header_selfcheck PRIVATE bbt::infra_common)
target_compile_options(bbt_infra_header_selfcheck PRIVATE -Wall -Wextra)
set_target_properties(bbt_infra_header_selfcheck PROPERTIES LINKER_LANGUAGE CXX)

include(CTest)
if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

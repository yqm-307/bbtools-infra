# src/：HTTP 模块实现（Boost.Beast + Asio）与 Redis 模块实现（hiredis
# async + 自定义 event adapter）；公开面仍在 include/bbt/infra/。

find_package(Boost REQUIRED)
find_package(Threads REQUIRED)

add_library(bbt_infra_http STATIC
    http/HttpDetail.cc
    http/HttpClientImpl.cc
    http/HttpServerImpl.cc
    http/NetworkRuntimeImpl.cc)
add_library(bbt::infra_http ALIAS bbt_infra_http)

target_include_directories(bbt_infra_http PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(bbt_infra_http
    PUBLIC  bbt::infra_common
    PRIVATE Boost::boost Threads::Threads)
target_compile_options(bbt_infra_http PRIVATE -Wall -Wextra)

# --- CoTCP 基础传输（Issue #31）----------------------------------------------
add_library(bbt_infra_tcp STATIC
    tcp/CoTCP.cc)
add_library(bbt::infra_tcp ALIAS bbt_infra_tcp)

target_include_directories(bbt_infra_tcp PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(bbt_infra_tcp
    PUBLIC bbt::infra_common)
target_compile_options(bbt_infra_tcp PRIVATE -Wall -Wextra)

# CoUDP：POSIX 非阻塞 datagram + coroutine 组合等待。
add_library(bbt_infra_udp STATIC udp/CoUDP.cc)
add_library(bbt::infra_udp ALIAS bbt_infra_udp)
target_include_directories(bbt_infra_udp PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(bbt_infra_udp PUBLIC bbt::infra_common)
target_compile_options(bbt_infra_udp PRIVATE -Wall -Wextra)

# --- P2 基础 transport owner（资源托管 + 配额）-------------------------------
# 只托管基础 TCP/UDP transport（容量名额、在途账本、受管对象强持有与物理
# 关闭收口），不含任何协议工厂；不链接 bbt_infra_http。受管 TCP/UDP 消费者
# 经本 target 即可构建，不再需要 HTTP。唯一的容量/账本实现位于
# transport/TransportRuntime.cc（自 http/NetworkRuntimeImpl 移出）。
add_library(bbt_infra_transport STATIC
    transport/TransportRuntime.cc)
add_library(bbt::infra_transport ALIAS bbt_infra_transport)

target_include_directories(bbt_infra_transport PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(bbt_infra_transport
    PUBLIC  bbt::infra_common bbt::infra_tcp bbt::infra_udp
    PRIVATE Boost::boost Threads::Threads)
target_compile_options(bbt_infra_transport PRIVATE -Wall -Wextra)

# P2 依赖门禁：把「transport 不依赖 HTTP」钉成配置期可核验事实，而不是靠
# 目录命名或注释。协议切片反向依赖 transport owner。
get_target_property(_bbt_transport_links bbt_infra_transport LINK_LIBRARIES)
if(_bbt_transport_links MATCHES "infra_http")
    message(FATAL_ERROR
        "P2 依赖方向破坏：bbt_infra_transport 不得链接 HTTP target"
        "（实际: ${_bbt_transport_links}）")
endif()

# HTTP 协议切片组合 transport owner；受管 TCP/UDP 工厂的容量与账本不再由
# 本 target 实现（CoTCP/CoUDP 符号经 transport owner 传递）。
target_link_libraries(bbt_infra_http PUBLIC bbt::infra_transport)

# --- Config 基础配置模块（Issue #35）------------------------------------------
# 只依赖 bbt::infra_common（即 coroutine）；无第三方新依赖。公开面在
# include/bbt/infra/config/，命名空间 bbt::infra::config。内存源、本地文件源、
# watch 轮询均基于协程域，不新增 I/O 线程。
add_library(bbt_infra_config STATIC
    config/MemorySource.cc
    config/FileSource.cc
    config/Watch.cc)
add_library(bbt::infra_config ALIAS bbt_infra_config)

target_include_directories(bbt_infra_config PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(bbt_infra_config
    PUBLIC bbt::infra_common
    PRIVATE Threads::Threads)
target_compile_options(bbt_infra_config PRIVATE -Wall -Wextra)

# --- Redis 模块（Issue #6）---------------------------------------------------
# hiredis 只允许经显式前缀接入：find_package 限定 NO_DEFAULT_PATH，
# 不可能误链 /usr/local 等系统位置的同名库。未给前缀时跳过本模块
# （不导出能假成功的 target），公共头仍可独立编译。
if(BBT_HIREDIS_PREFIX)
    find_package(hiredis CONFIG REQUIRED
        PATHS "${BBT_HIREDIS_PREFIX}" NO_DEFAULT_PATH)

    add_library(bbt_infra_redis STATIC
        redis/RedisDetail.cc
        redis/RedisConnection.cc
        redis/CoRedisCliImpl.cc
        # B 级 owner binding 候选（隔离实现）：hiredis 只做 RESP2 编解码，
        # 字节 I/O/等待/取消/关闭经 CoTCP；不替换上面 async+strand 生产实现。
        redis/RedisCotcpOwner.cc
        redis/CoRedisCotcpCli.cc)
    add_library(bbt::infra_redis ALIAS bbt_infra_redis)

    target_include_directories(bbt_infra_redis PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    # 候选绑定经 CoTCP 收发：redis 依赖 tcp（tcp 不依赖 http，方向门禁不破）。
    # B1 复审修正：tcp 依赖是**实现细节**，不得经 Redis 公共接口/安装接口传播，
    # 故为 PRIVATE（静态库消费者仍由 CMake 的 LINK_ONLY 传递完成最终链接）。
    target_link_libraries(bbt_infra_redis
        PUBLIC  bbt::infra_common
        PRIVATE bbt::infra_tcp Boost::boost Threads::Threads hiredis::hiredis)
    target_compile_options(bbt_infra_redis PRIVATE -Wall -Wextra)
else()
    message(STATUS
        "BBT_HIREDIS_PREFIX 未设置：跳过 bbt_infra_redis 模块。"
        "接入 Redis 客户端请传 -DBBT_HIREDIS_PREFIX=<hiredis 安装前缀>")
endif()

# --- Mongo 模块（Issue #7）--------------------------------------------------
# mongocxx 只允许经显式前缀接入：find_package 在根 CMakeLists.txt 顶层
# 作用域限定 NO_DEFAULT_PATH 执行（mongo::mongocxx_shared 等 IMPORTED
# target 只有目录级可见性，顶层解析后 src/ 与 tests/ 均可见），嵌套
# find_dependency(mongoc/bsoncxx) 经 <pkg>_DIR 钉到私有前缀。
# 未给前缀时跳过本模块（不导出能假成功的 target），公共头仍可独立编译。
if(BBT_MONGOCXX_PREFIX)
    # mongo::mongocxx_shared 等 IMPORTED target 由顶层 find_package 提供
    # （目录级可见性，见根 CMakeLists.txt 的接入注释）。
    add_library(bbt_infra_mongo STATIC
        mongo/MongoDetail.cc
        mongo/MongoProcess.cc
        mongo/MongoRuntime.cc)
    add_library(bbt::infra_mongo ALIAS bbt_infra_mongo)

    target_include_directories(bbt_infra_mongo PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(bbt_infra_mongo
        PUBLIC  bbt::infra_common
        PRIVATE Boost::boost Threads::Threads mongo::mongocxx_shared)
    target_compile_options(bbt_infra_mongo PRIVATE -Wall -Wextra)
else()
    message(STATUS
        "BBT_MONGOCXX_PREFIX 未设置：跳过 bbt_infra_mongo 模块。"
        "接入 Mongo 客户端请传 -DBBT_MONGOCXX_PREFIX=<mongo-cxx-driver 安装前缀>"
        " 与 -DBBT_MONGOC_PREFIX=<mongo-c-driver 安装前缀>")
endif()

# --- RPC wire 模块（Issue #8）------------------------------------------------
# 仅在 protoc/runtime 同源前缀给出时构建。proto 真源经 add_custom_command
# 生成到 build/generated/；本模块编译生成物 + 公开面 adapter。
if(TARGET bbt_infra_rpc_proto)
    add_library(bbt_infra_rpc STATIC
        rpc/RpcWire.cc)
    add_library(bbt::infra_rpc ALIAS bbt_infra_rpc)

    target_include_directories(bbt_infra_rpc PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(bbt_infra_rpc
        PUBLIC  bbt::infra_common
        PRIVATE bbt_infra_rpc_proto Boost::boost Threads::Threads)
    target_compile_options(bbt_infra_rpc PRIVATE -Wall -Wextra)
else()
    message(STATUS
        "BBT_PROTOBUF_PREFIX 未设置：跳过 bbt_infra_rpc 模块。"
        "接入 RPC wire profile 请传 -DBBT_PROTOBUF_PREFIX=<protobuf 前缀>")
endif()

# Config 与 Mongo 独立于网络协议栈。
find_package(Boost REQUIRED)
find_package(Threads REQUIRED)

# --- Config 基础配置模块（Issue #35）------------------------------------------
# 只依赖 bbt::infra_common（即 coroutine）；无第三方新依赖。公开面在
# bbt/infra/config/，命名空间 bbt::infra::config。内存源、本地文件源、
# watch 轮询均基于协程域，不新增 I/O 线程。
if(BBT_INFRA_CONFIG)
add_library(bbt_infra_config STATIC
    config/MemorySource.cc
    config/FileSource.cc
    config/Watch.cc
    config/Value.cc
    config/Snapshot.cc
    config/detail/CloseSupport.cc)
add_library(bbt::infra_config ALIAS bbt_infra_config)

target_include_directories(bbt_infra_config PRIVATE ${PROJECT_SOURCE_DIR})
target_link_libraries(bbt_infra_config
    PUBLIC bbt::infra_common
    PRIVATE Threads::Threads)
target_compile_options(bbt_infra_config PRIVATE -Wall -Wextra)
endif() # BBT_INFRA_CONFIG

# --- Mongo 模块（Issue #7）--------------------------------------------------
# mongocxx 只允许经显式前缀接入：find_package 在根 CMakeLists.txt 顶层
# 作用域限定 NO_DEFAULT_PATH 执行（mongo::mongocxx_shared 等 IMPORTED
# target 只有目录级可见性，顶层解析后 bbt/infra/ 与 tests/ 均可见），嵌套
# find_dependency(mongoc/bsoncxx) 经 <pkg>_DIR 钉到私有前缀。
# 未给前缀时跳过本模块（不导出能假成功的 target），公共头仍可独立编译。
if(BBT_INFRA_MONGO AND BBT_MONGOCXX_PREFIX)
    # mongo::mongocxx_shared 等 IMPORTED target 由顶层 find_package 提供
    # （目录级可见性，见根 CMakeLists.txt 的接入注释）。
    add_library(bbt_infra_mongo STATIC
        mongo/Bson.cc
        mongo/detail/MongoDetail.cc
        mongo/detail/MongoProcess.cc
        mongo/detail/MongoRuntime.cc
        mongo/Client.cc
        mongo/detail/MongoSupport.cc)
    add_library(bbt::infra_mongo ALIAS bbt_infra_mongo)

    target_include_directories(bbt_infra_mongo PRIVATE ${PROJECT_SOURCE_DIR})
    target_link_libraries(bbt_infra_mongo
        PUBLIC  bbt::infra_common bbt::infra_internal_debug
        PRIVATE Boost::boost Threads::Threads mongo::mongocxx_shared)
    target_compile_options(bbt_infra_mongo PRIVATE -Wall -Wextra)
else()
    message(STATUS
        "BBT_MONGOCXX_PREFIX 未设置：跳过 bbt_infra_mongo 模块。"
        "接入 Mongo 客户端请传 -DBBT_MONGOCXX_PREFIX=<mongo-cxx-driver 安装前缀>"
        " 与 -DBBT_MONGOC_PREFIX=<mongo-c-driver 安装前缀>")
endif()

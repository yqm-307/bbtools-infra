# src/：HTTP 模块实现（Boost.Beast + Asio），公开面仍在 include/bbt/infra/。

find_package(Boost REQUIRED)
find_package(Threads REQUIRED)

add_library(bbt_infra_http STATIC
    http/HttpDetail.cc
    http/HttpIoEngine.cc
    http/HttpClientImpl.cc
    http/HttpServerImpl.cc
    http/NetworkRuntimeImpl.cc)
add_library(bbt::infra_http ALIAS bbt_infra_http)

target_include_directories(bbt_infra_http PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(bbt_infra_http
    PUBLIC  bbt::infra_common
    PRIVATE Boost::boost Threads::Threads)
target_compile_options(bbt_infra_http PRIVATE -Wall -Wextra)

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
        mongo/CoMongoCliImpl.cc)
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

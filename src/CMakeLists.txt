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

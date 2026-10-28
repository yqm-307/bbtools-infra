# 新 net HTTP 客户端窄适配（BBT_INFRA_HTTP_NET=ON 时经 cmake/HttpNet.cmake 接入）。
# 公开头与实现同目录（对齐 bbt/infra/net、bbt/infra/redis 布局）：include 路径是
# <bbt/infra/http/...>，以 PROJECT_SOURCE_DIR 为 include root；namespace
# bbt::infra::http。detail/ 是内部部件（Http11 交换层、连接池），不公开。
# Beast 只承担 HTTP 编解码（header-only，随 Boost::headers）；网络只经
# bbt::infra_net 公开 Co 接口，不触碰 Asio socket/net detail。
add_library(bbt_infra_http_net STATIC
    Client.cc
    Server.cc
    detail/Http11.cc)
add_library(bbt::infra_http_net ALIAS bbt_infra_http_net)
target_include_directories(bbt_infra_http_net
    PUBLIC "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>"
           # 公开头仍引用旧公开头树的 NetworkTypes/Result（include/bbt/infra/）。
           "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>")
target_compile_features(bbt_infra_http_net PUBLIC cxx_std_17)
target_link_libraries(bbt_infra_http_net PUBLIC bbt::infra_net PRIVATE Boost::headers)
target_compile_options(bbt_infra_http_net PRIVATE -Wall -Wextra -Wpedantic)

# 公开头自给编译自检：公开头各自单独成 TU（detail 内部头不列入）。
set(_http_net_headers Client.hpp Server.hpp)
set(_http_net_selfcheck_srcs "")
foreach(_hdr IN LISTS _http_net_headers)
    set(_tu "${CMAKE_CURRENT_BINARY_DIR}/header_selfcheck/${_hdr}.cc")
    file(WRITE "${_tu}" "#include <bbt/infra/http/${_hdr}>\n")
    list(APPEND _http_net_selfcheck_srcs "${_tu}")
endforeach()
add_library(bbt_infra_http_net_header_selfcheck STATIC ${_http_net_selfcheck_srcs})
target_link_libraries(bbt_infra_http_net_header_selfcheck PRIVATE bbt::infra_http_net)
target_compile_options(bbt_infra_http_net_header_selfcheck PRIVATE -Wall -Wextra -Wpedantic)
set_target_properties(bbt_infra_http_net_header_selfcheck PROPERTIES LINKER_LANGUAGE CXX)

if(BUILD_TESTING)
    add_subdirectory("${PROJECT_SOURCE_DIR}/tests/http_net" http_net_tests)
endif()
add_subdirectory("${PROJECT_SOURCE_DIR}/examples/http_net" http_net_examples)

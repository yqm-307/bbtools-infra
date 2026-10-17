add_library(bbt_infra_net STATIC
    CoObject.cc Address.cc NativeSocket.cc Options.cc LoadBalancing.cc
    detail/EventLoop.cc detail/CoIoOperation.cc detail/StreamConnection.cc
    CoTcpConnection.cc detail/ConnectionBridge.cc
    Client.cc Engine.cc detail/ServerEngine.cc)
add_library(bbt::infra_net ALIAS bbt_infra_net)
target_include_directories(bbt_infra_net PUBLIC
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>")
target_compile_features(bbt_infra_net PUBLIC cxx_std_17)
target_link_libraries(bbt_infra_net PUBLIC bbt_coroutine PRIVATE Boost::headers)
target_compile_options(bbt_infra_net PRIVATE -Wall -Wextra -Wpedantic)

# 白名单单头 C++17 自检；不引用旧 include/bbt/infra/。
# Options/LB 是本包新公开头，必须进自检；detail/* 是内部部件头，
# 不得仅因自检变成公开 API，故不列入。
# Udp.hpp 是高层 UDP 的编译拒绝入口（用户 2026-10-17 裁定本轮不做高层 UDP），
# 其全部内容就是 #error，故**故意不列入** _headers：列进去只会让自检按设计失败，
# 而不是发现头缺陷。它由 net.udp_high_level_compile_reject 以正/负编译对照验证，
# 比自检更强（既证"该头确实拒绝"，也证"其它头与 datagram 原语未被连带破坏"）。
set(_headers Annotations.hpp ICoObject.hpp CoObject.hpp Address.hpp NativeSocket.hpp
    Options.hpp LoadBalancing.hpp ICoConnection.hpp CoConnectionHandler.hpp
    CoTcpConnection.hpp Action.hpp EventHandler.hpp Engine.hpp Client.hpp)
set(_selfcheck_sources "")
foreach(_header IN LISTS _headers)
    set(_source "${CMAKE_CURRENT_BINARY_DIR}/selfcheck/${_header}.cc")
    file(WRITE "${_source}" "#include <bbt/infra/net/${_header}>\n")
    list(APPEND _selfcheck_sources "${_source}")
endforeach()
add_library(bbt_infra_net_header_selfcheck STATIC ${_selfcheck_sources})
target_link_libraries(bbt_infra_net_header_selfcheck PRIVATE bbt::infra_net)
target_compile_options(bbt_infra_net_header_selfcheck PRIVATE -Wall -Wextra -Wpedantic)

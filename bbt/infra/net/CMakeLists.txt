add_library(bbt_infra_net STATIC
    CoObject.cc Address.cc NativeSocket.cc LoadBalancing.cc
    Connection.cc WriteOperation.cc CoTcpConnection.cc detail/CoInternal.cc
    Engine.cc detail/ReactorDriver.cc detail/DriverBinder.cc detail/OptionsNormalize.cc
    detail/TestAccess.cc
    Buffer.cc Error.cc ../NetError.cc ICoConnection.cc detail/SourceAddrHash.cc
    detail/ConnectionState.cc detail/EventRingBuffer.cc detail/OutboundQueue.cc
    detail/ReceiveRing.cc detail/WakeHub.cc)
add_library(bbt::infra_net ALIAS bbt_infra_net)
target_include_directories(bbt_infra_net PUBLIC
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>")
target_compile_features(bbt_infra_net PUBLIC cxx_std_17)
target_link_libraries(bbt_infra_net PUBLIC bbt_coroutine bbt::infra_common PRIVATE Boost::headers)
target_compile_options(bbt_infra_net PRIVATE -Wall -Wextra -Wpedantic)

# 白名单单头 C++17 自检；只覆盖 net 公共头。
# Options/LB/Types/Error 是值类型；ReadView、Connection、RawHandler、
# ICoConnection、CoTcpConnection 是只读视图、原始与 Co 薄封装公共头。detail/* 是内部部件
# （含 NormalizeOptions、TestAccess、EngineTestAccess），不得仅因自检变成公开 API。
# Udp.hpp 是高层 UDP 的编译拒绝入口（当前不提供高层 UDP 连接 API），
# 其全部内容就是 #error，故**故意不列入** _headers；由
# net.udp_high_level_compile_reject 做正/负编译对照验证。
set(_headers Annotations.hpp ICoObject.hpp CoObject.hpp Address.hpp NativeSocket.hpp
    Options.hpp LoadBalancing.hpp Types.hpp ReadView.hpp Connection.hpp RawHandler.hpp
    Engine.hpp Error.hpp
    Buffer.hpp WriteOperation.hpp ConnectionResult.hpp ICoConnection.hpp
    CoTcpConnection.hpp)
set(_selfcheck_sources "")
foreach(_header IN LISTS _headers)
    set(_source "${CMAKE_CURRENT_BINARY_DIR}/selfcheck/${_header}.cc")
    file(WRITE "${_source}" "#include <bbt/infra/net/${_header}>\n")
    list(APPEND _selfcheck_sources "${_source}")
endforeach()
add_library(bbt_infra_net_header_selfcheck STATIC ${_selfcheck_sources})
target_link_libraries(bbt_infra_net_header_selfcheck PRIVATE bbt::infra_net)
target_compile_options(bbt_infra_net_header_selfcheck PRIVATE -Wall -Wextra -Wpedantic)

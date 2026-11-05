# BBT_INFRA_CONSUL=ON 时的薄接线：target/测试的完整定义在
# bbt/infra/consul/CMakeLists.txt，普通模式与 NETWORK_ONLY 共用本入口。
# 依赖闭包：bbt::infra_http_net + bbt::infra_common + Boost::headers（Boost.JSON
# 源码内联，不链接 libboost_json）；不引入 Consul SDK，不新增第三方依赖。
if(NOT BBT_INFRA_HTTP_NET)
    message(FATAL_ERROR "BBT_INFRA_CONSUL=ON 需要 BBT_INFRA_HTTP_NET=ON（开关 fail-closed，不静默跳过）")
endif()
add_subdirectory("${PROJECT_SOURCE_DIR}/bbt/infra/consul" bbt_infra_consul)

# BBT_INFRA_HTTP_NET=ON 时的薄接线：无外部私有依赖（Beast 随 Boost::headers）。
# target/测试/示例的完整定义在 bbt/infra/http/CMakeLists.txt；普通模式与
# NETWORK_ONLY 共用本入口。Boost::headers 已由公共段/NetworkOnly 的
# find_package(Boost 1.90 REQUIRED) 提供。
add_subdirectory("${PROJECT_SOURCE_DIR}/bbt/infra/http" bbt_infra_http_net)

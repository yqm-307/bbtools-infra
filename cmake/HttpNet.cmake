# BBT_INFRA_HTTP_NET=ON 时的薄接线：无外部私有依赖（Beast 随 Boost::headers）。
# target/测试/示例的完整定义在 bbt/infra/http/CMakeLists.txt。
add_subdirectory("${PROJECT_SOURCE_DIR}/bbt/infra/http" http_net)

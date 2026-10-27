# Mongo 公开 target 消费示例（Mongo 适配迁移）。
# 只链接公开 target bbt::infra_mongo，只含公开头；真实后端由
# BBT_TEST_MONGO_URI 注入（未设置时示例打印 skip: 并返回 77）。
# ctest 注册见 tests/CMakeLists.txt（examples/ 早于 include(CTest) 处理）。
add_executable(mongo_client_example main.cc)
target_link_libraries(mongo_client_example PRIVATE bbt::infra_mongo)
target_compile_options(mongo_client_example PRIVATE -Wall -Wextra)

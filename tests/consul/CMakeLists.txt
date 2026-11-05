# Consul adapter 定向测试：单可执行，用例顺序固定（case-dispatch 约定见
# tests/README.md；命名与 label 约定见 docs/conventions.md §6）。
#
# 真后端用例需要 BBT_TEST_CONSUL_BIN 指向已核 SHA256 的官方 Consul 二进制
# （固定版本与校验方式见 docs/guides/consul-registry.md）。未提供时 live 测试打印
# "skip:" 并以 77 退出，由 SKIP_* 属性标为 Skipped；协议 smoke 不依赖该变量。
# 协议面（非 2xx / 坏 JSON / 缺 index / critical 过滤 / 封口）单独作为 smoke；
# 真后端另作为 live，未提供二进制只影响 live 项，不隐藏确定性契约失败。
add_executable(Test_infra_consul Test_infra_consul.cc)
target_link_libraries(Test_infra_consul
    PRIVATE bbt::infra_consul bbt::infra_http_net Boost::headers Threads::Threads)
target_compile_options(Test_infra_consul PRIVATE -Wall -Wextra -Wpedantic)
# 协议/关闭契约不依赖 Consul 二进制，必须在普通 smoke 中单独执行。
add_test(NAME infra.consul.protocol COMMAND Test_infra_consul --protocol)
set_tests_properties(infra.consul.protocol PROPERTIES TIMEOUT 60 LABELS "smoke;unit")

# 真后端闭环单独标 live；未提供二进制时只允许该项 skip。
add_test(NAME infra.consul.live COMMAND Test_infra_consul --live)
set_tests_properties(infra.consul.live PROPERTIES
    TIMEOUT 300 LABELS "live"
    SKIP_REGULAR_EXPRESSION "skip:"
    SKIP_RETURN_CODE 77)

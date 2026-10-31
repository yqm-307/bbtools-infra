# MCP Streamable HTTP 可运行示例：POST /mcp 单端点 server（loopback-only）。
# 只链接公开 target（mcp_http 带来 mcp_core 与 http_net），只消费公共头。
# BBT_INFRA_MCP_HTTP=ON（且 MCP/HTTP_NET 双开）时由 bbt/infra/mcp/CMakeLists.txt 挂接。
add_executable(Example_mcp_http_server mcp_http_server.cc)
target_link_libraries(Example_mcp_http_server PRIVATE
    bbt::infra_mcp_http Threads::Threads)
target_compile_options(Example_mcp_http_server PRIVATE -Wall -Wextra)

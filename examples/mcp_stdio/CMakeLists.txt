# MCP stdio 可运行示例：--stdio 单模式（本切片不携带 HTTP transport）。
# 只链接公开 target（mcp_core + mcp_stdio，schema 选件按开关决定是否链接）；
# 只消费公共头（bbt/infra/mcp/* + coroutine 装配头经 bbt::infra_common 传播）。
add_executable(Example_mcp_stdio_server mcp_stdio_server.cc)
target_link_libraries(Example_mcp_stdio_server PRIVATE
    bbt::infra_mcp_stdio Threads::Threads)
target_compile_options(Example_mcp_stdio_server PRIVATE -Wall -Wextra)
if(BBT_INFRA_MCP_SCHEMA AND TARGET bbt_infra_mcp_schema)
    target_link_libraries(Example_mcp_stdio_server PRIVATE bbt_infra_mcp_schema)
    target_compile_definitions(Example_mcp_stdio_server PRIVATE BBT_MCP_SCHEMA_ADAPTER)
endif()

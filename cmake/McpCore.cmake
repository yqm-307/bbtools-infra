# BBT_INFRA_MCP=ON 时的薄接线：MCP server 核心与 stdio（bbt::infra_mcp_core /
# bbt::infra_mcp_stdio）。MCP 核心不要求第三方前缀，也不依赖 HTTP/Redis/Mongo；
# 只有 schema 校验器需要前缀，见 McpSchema.cmake。target/测试/示例由实现节点
# 定义在 bbt/infra/mcp/CMakeLists.txt。目录缺失即 FATAL_ERROR，开关 ON 不静默通过。

if(NOT EXISTS "${PROJECT_SOURCE_DIR}/bbt/infra/mcp/CMakeLists.txt")
    message(FATAL_ERROR
        "BBT_INFRA_MCP=ON 需要 bbt/infra/mcp/CMakeLists.txt，但 MCP 实现节点尚未合入；"
        "开关 fail-closed，不静默跳过。")
endif()
add_subdirectory("${PROJECT_SOURCE_DIR}/bbt/infra/mcp" bbt_infra_mcp)

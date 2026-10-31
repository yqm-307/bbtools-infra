# BBT_INFRA_MCP_SCHEMA=ON 时的前缀门禁：JSON Schema 校验器私有 adapter。
# 只有 schema 校验器需要第三方前缀（架构：MCP 核心不需要）。jsoncons 与 ICU 的
# 来源/许可证尚未经 proto-schema-provenance 结论：本门禁只校验前缀已给且为目录，
# 不代表 validator 可启用；未过 provenance 时 MCP tools capability 保持 disabled。
# 依赖 BBT_INFRA_MCP=ON；target 由 bbt/infra/mcp/CMakeLists.txt 在本开关下定义。

if(NOT BBT_INFRA_MCP)
    message(FATAL_ERROR "BBT_INFRA_MCP_SCHEMA=ON 需要 BBT_INFRA_MCP=ON。")
endif()

set(BBT_JSONCONS_PREFIX "" CACHE PATH
    "jsoncons 固定私有前缀（待 proto-schema-provenance 结论）")
set(BBT_ICU_PREFIX "" CACHE PATH
    "ICU 固定私有前缀（待 proto-schema-provenance 结论）")
if(NOT BBT_JSONCONS_PREFIX OR NOT BBT_ICU_PREFIX)
    message(FATAL_ERROR
        "BBT_INFRA_MCP_SCHEMA=ON 需要显式 BBT_JSONCONS_PREFIX 与 BBT_ICU_PREFIX；"
        "缺必需前缀明确拒绝。")
endif()
if(NOT IS_DIRECTORY "${BBT_JSONCONS_PREFIX}" OR NOT IS_DIRECTORY "${BBT_ICU_PREFIX}")
    message(FATAL_ERROR
        "BBT_JSONCONS_PREFIX / BBT_ICU_PREFIX 必须是已存在的目录。")
endif()

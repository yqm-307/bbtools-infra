# MCP core + stdio 契约测试（BBT_INFRA_MCP=ON 时经 bbt/infra/mcp/CMakeLists.txt 接入）。
# 无第三方测试框架；内部实现面经 ${PROJECT_SOURCE_DIR}/bbt include 路径消费
# （detail/*.hpp、schema/*.hpp 仅各测试目标可见，不进公共 include 面）。
#
# 测试命名与矩阵：
#   mcp.json             JsonValue 值语义/精确整数/拒绝面/边界；仅链接 core。
#   mcp.protocol         JSON-RPC 帧完整性/owned id/modern _meta/version/discover
#                        构造；消费 detail/Protocol.hpp。
#   mcp.server           生命周期/准入/discover/initialize→initialized/ping/两 era
#                        方法表隔离；消费 detail/Server.hpp。
#   mcp.registry         AddResource/AddPrompt/Start/Close + 业务分派；消费
#                        detail/Server.hpp。
#   mcp.subscribe        subscriptions/listen + MRTR wire/能力门禁/retry inputs；
#                        消费 detail/Protocol.hpp + detail/Server.hpp。
#   mcp.stdio_transport  真实非 socket pipe 读写/有界输出/受管协程 dispatch/
#                        订阅 sink/cancel/EOF/诊断 stderr/子进程 loopback。
#   mcp.tools            tools 注册/分派 + EnableToolsSchemaValidation + MRTR
#                        接线；仅在 schema target 存在时注册（fail-closed 的
#                        ON 面；OFF 面由 core+server 覆盖）。
#   mcp.schema           SchemaValidator 私有 adapter 单测；仅在 schema target
#                        存在时注册。
#   mcp.teardown         request owner 唯一终态/deadline(-32603+data.kind)/cancel/
#                        Close 竞争/stdio close 返回时间与 destroy-while-alive
#                        （G4/G1 覆盖）；消费 detail/Server.hpp。

# mcp.json：只链接 core；不消费内部头。
add_executable(Test_mcp_json Test_mcp_json.cc)
target_link_libraries(Test_mcp_json PRIVATE bbt::infra_mcp_core)
target_compile_options(Test_mcp_json PRIVATE -Wall -Wextra)
add_test(NAME mcp.json COMMAND Test_mcp_json)
set_tests_properties(mcp.json PROPERTIES TIMEOUT 60)

# mcp.protocol：消费 detail/Protocol.hpp（内部实现面，仅本目标可见）。
add_executable(Test_mcp_protocol Test_mcp_protocol.cc)
target_include_directories(Test_mcp_protocol PRIVATE "${PROJECT_SOURCE_DIR}/bbt/infra")
target_link_libraries(Test_mcp_protocol PRIVATE bbt::infra_mcp_core)
target_compile_options(Test_mcp_protocol PRIVATE -Wall -Wextra)
add_test(NAME mcp.protocol COMMAND Test_mcp_protocol)
set_tests_properties(mcp.protocol PROPERTIES TIMEOUT 60)

# mcp.server：消费 detail/Server.hpp。
add_executable(Test_mcp_server Test_mcp_server.cc)
target_include_directories(Test_mcp_server PRIVATE "${PROJECT_SOURCE_DIR}/bbt/infra")
target_link_libraries(Test_mcp_server PRIVATE bbt::infra_mcp_core Threads::Threads)
target_compile_options(Test_mcp_server PRIVATE -Wall -Wextra)
add_test(NAME mcp.server COMMAND Test_mcp_server)
set_tests_properties(mcp.server PROPERTIES TIMEOUT 60)

# mcp.registry：消费 detail/Server.hpp。
add_executable(Test_mcp_registry Test_mcp_registry.cc)
target_include_directories(Test_mcp_registry PRIVATE "${PROJECT_SOURCE_DIR}/bbt/infra")
target_link_libraries(Test_mcp_registry PRIVATE bbt::infra_mcp_core Threads::Threads)
target_compile_options(Test_mcp_registry PRIVATE -Wall -Wextra)
add_test(NAME mcp.registry COMMAND Test_mcp_registry)
set_tests_properties(mcp.registry PROPERTIES TIMEOUT 60)

# mcp.subscribe：消费 detail/Protocol.hpp + detail/Server.hpp。
add_executable(Test_mcp_subscribe Test_mcp_subscribe.cc)
target_include_directories(Test_mcp_subscribe PRIVATE "${PROJECT_SOURCE_DIR}/bbt/infra")
target_link_libraries(Test_mcp_subscribe PRIVATE bbt::infra_mcp_core)
target_compile_options(Test_mcp_subscribe PRIVATE -Wall -Wextra)
add_test(NAME mcp.subscribe COMMAND Test_mcp_subscribe)
set_tests_properties(mcp.subscribe PROPERTIES TIMEOUT 60)

# mcp.stdio_transport：真实 pipe 全栈 + fork/exec 子进程 loopback（stdio 独占
# FIFO 前提；transport 不归属 fd）。链接 stdio transport + coroutine 装配头。
add_executable(Test_mcp_stdio_transport Test_mcp_stdio_transport.cc)
target_include_directories(Test_mcp_stdio_transport PRIVATE "${PROJECT_SOURCE_DIR}/bbt/infra")
target_link_libraries(Test_mcp_stdio_transport PRIVATE
    bbt::infra_mcp_stdio Threads::Threads)
target_compile_options(Test_mcp_stdio_transport PRIVATE -Wall -Wextra)
add_test(NAME mcp.stdio_transport COMMAND Test_mcp_stdio_transport)
set_tests_properties(mcp.stdio_transport PROPERTIES TIMEOUT 120)

# mcp.tools / mcp.schema：仅在私有 adapter 目标存在时注册（fail-closed 的 ON 面；
# schema OFF 时 AddTool 拒绝路径由 core+server 冒烟覆盖，不强行生成 stub 目标）。
if(TARGET bbt_infra_mcp_schema)
    add_executable(Test_mcp_tools Test_mcp_tools.cc)
    target_include_directories(Test_mcp_tools PRIVATE "${PROJECT_SOURCE_DIR}/bbt/infra")
    target_link_libraries(Test_mcp_tools PRIVATE
        bbt::infra_mcp_core bbt_infra_mcp_schema Threads::Threads)
    target_compile_options(Test_mcp_tools PRIVATE -Wall -Wextra)
    add_test(NAME mcp.tools COMMAND Test_mcp_tools)
    set_tests_properties(mcp.tools PROPERTIES TIMEOUT 60)

    add_executable(Test_mcp_schema Test_mcp_schema.cc)
    target_include_directories(Test_mcp_schema PRIVATE "${PROJECT_SOURCE_DIR}/bbt/infra")
    target_link_libraries(Test_mcp_schema PRIVATE
        bbt::infra_mcp_schema Threads::Threads)
    target_compile_options(Test_mcp_schema PRIVATE -Wall -Wextra)
    add_test(NAME mcp.schema COMMAND Test_mcp_schema)
    set_tests_properties(mcp.schema PROPERTIES TIMEOUT 120)
endif()

# mcp.teardown：消费 detail/Server.hpp（RequestOwner 终态计数）+ stdio transport。
add_executable(Test_mcp_teardown Test_mcp_teardown.cc)
target_include_directories(Test_mcp_teardown PRIVATE "${PROJECT_SOURCE_DIR}/bbt/infra")
target_link_libraries(Test_mcp_teardown PRIVATE
    bbt::infra_mcp_core bbt::infra_mcp_stdio Threads::Threads)
target_compile_options(Test_mcp_teardown PRIVATE -Wall -Wextra)
add_test(NAME mcp.teardown COMMAND Test_mcp_teardown)
set_tests_properties(mcp.teardown PROPERTIES TIMEOUT 120)

# mcp.http.*：Streamable HTTP transport 真实 TCP loopback；仅在 mcp_http target
# 存在（BBT_INFRA_MCP_HTTP=ON 且 HTTP_NET 同开）时注册。
if(TARGET bbt_infra_mcp_http)
    add_executable(Test_mcp_http_transport Test_mcp_http_transport.cc)
    target_include_directories(Test_mcp_http_transport PRIVATE "${PROJECT_SOURCE_DIR}/bbt/infra")
    target_link_libraries(Test_mcp_http_transport PRIVATE
        bbt::infra_mcp_http Threads::Threads)
    target_compile_options(Test_mcp_http_transport PRIVATE -Wall -Wextra)
    foreach(_case headers modern_headers mcp_name responses legacy bind lifecycle)
        add_test(NAME mcp.http.${_case} COMMAND Test_mcp_http_transport ${_case})
        set_tests_properties(mcp.http.${_case} PROPERTIES TIMEOUT 60 LABELS "mcp_http")
    endforeach()
    foreach(_case sse sse_queue)
        add_test(NAME mcp.stream.${_case} COMMAND Test_mcp_http_transport ${_case})
        set_tests_properties(mcp.stream.${_case} PROPERTIES TIMEOUT 60 LABELS "mcp_http")
    endforeach()
endif()

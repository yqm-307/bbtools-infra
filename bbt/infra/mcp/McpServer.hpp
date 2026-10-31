#pragma once
// MCP 公共业务接入面：用自有类型注册 resources / prompts 与 handler，Start 冻结快照。
//
// 契约来源：WFER mcp-server-foundation revision9（specs R1/R6/R10 中不依赖 Schema 与
// transport 的部分）+ 固定规范 confirmed-spec-schema.txt（2026-07-28）+ 官方 Python
// SDK（安装版 mcp 2.0.0）legacy 生命周期行为。
//
// 本切片（tasks 2-4 中不依赖 Schema 的部分）：
//   - 公共面只出现自有类型（JsonValue / McpTypes / McpServer）：无 Boost/Beast/jsoncons、
//     不需要 consumer include src/、无裸 JSON-RPC 帧入口（帧入口仍属内部 session /
//     transport 面，不因省事 promote 成公共 API）；
//   - 注册必须在 Start 前完成，Start 冻结快照；非法注册整体拒绝、不留半条记录；
//   - resources/list、resources/read、resources/templates/list、prompts/list、
//     prompts/get 的 wire 结果由库构造并校验（业务只交自有类型）；
//   - capability 只反映真实注册（未注册 resources/prompts 时不宣告）。
//
// 2-5 增量：resources 订阅有真实发布入口 PublishResourceChanged（capabilities 相应
// 宣告 resources.subscribe=true，仍不宣告 listChanged）。
// 3-3 增量：每请求 deadline 与取消栈外状态（R10 request owner：唯一终态、
// notifications/cancelled 全局路由、期限可观察拒绝、封口后晚到完成项只消费空壳）。
// 2-6 增量：tools 注册/调用（AddTool、tools/list、tools/call，R5）与业务 MRTR
// （HandlerResult<ToolResult> 的 input_required 接线）。tools 的 Schema 语义校验
// 经下方 EnableToolsSchemaValidation 显式接通（实现在私有 adapter 目标
// bbt_infra_mcp_schema）：未接通时 AddTool 一律 fail-closed 拒绝，不注册未校验工具；
// core 闭包不链接 Schema/ICU。

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/mcp/JsonValue.hpp>
#include <bbt/infra/mcp/McpTypes.hpp>

namespace bbt::infra::mcp {

class McpSession;  // 内部会话类型（定义在 bbt/infra/mcp/detail/Server.hpp），公共面无其定义

// 服务配置。默认保守（本机、单帧、有限在途、有限注册与分页）；0/空值在 Create 时严格拒绝。
struct ServerOptions {
    std::string server_name = "bbt-infra-mcp";
    std::string server_version = "0.1.0";
    std::optional<std::string> instructions;   // discover/initialize 的可选 instructions
    std::size_t max_message_bytes = 1U << 20;  // 1 MiB
    std::size_t max_depth = 32;
    std::size_t max_inflight_requests = 64;
    std::size_t max_registrations = 256;       // resources + prompts 总数上限
    std::size_t page_size = 50;                // 列表分页上界（>0）
    // 3-3：每请求处理期限（>0 严格校验）。超限请求在提交门得到可观察拒绝
    //（-32603 + data.kind="deadline_exceeded"，architecture §3.3：不向
    // -32000..-32099 分配新码）；handler 可经 RequestContext 观察期限并协作式
    // 退出，库不强杀任何栈。
    std::chrono::milliseconds request_timeout{30000};
};

// 资源回调：接已校验的 uri（与注册 uri 相同），返回由库校验的 ResourceResult。
using ResourceHandler =
    std::function<result<ResourceResult>(const RequestContext&, const std::string& uri)>;
// 提示词回调：接 params.arguments（缺省为空 object），返回由库校验的 PromptResult。
using PromptHandler =
    std::function<result<PromptResult>(const RequestContext&, const JsonValue& arguments)>;

// tools×Schema 显式装配入口（R5/S9/S10）：唯一实现在私有 adapter 目标
// bbt_infra_mcp_schema（bbt/infra/mcp/schema）。仅 Start 前可调用一次；接通后 AddTool
// 才会以真实编译 validator（官方 metaschema 全文校验、ref 策略、复杂度上限，
// fail-closed 一次成型）校验注册与调用。未接通时 AddTool 一律 fail-closed 拒绝
//（不注册未校验工具）。本声明只在公共面出现名字：core 闭包不含本符号、不链接
// Schema/ICU；只有显式链接 adapter 目标并显式调用的 consumer 启用。
class McpServer;
result<void> EnableToolsSchemaValidation(McpServer& server);

// 进程级核心：生命周期 + 注册快照 + 业务方法分派。
//
// 生命周期：Create 后注册（AddResource/AddPrompt）→ Start 一次冻结快照并开协议接纳
// → Close 幂等封口、不重开。Start 后注册被拒（快照不可变）。
class McpServer {
public:
    // 校验并创建。非法配置（0 上限/分页、空 name/version、非 UTF-8 文本）→ InvalidArgument。
    static result<std::shared_ptr<McpServer>> Create(ServerOptions options);
    ~McpServer();

    // 注册（Start 前）：失败不留半条记录（先全量校验再插入）。
    //   handler 为空、uri/name 为空或非 UTF-8、description/mime_type 非 UTF-8、
    //   prompt 参数名重复/为空 → InvalidArgument；
    //   重复 uri / 重复 prompt name → InvalidArgument（domain_code 区分）；
    //   超过 max_registrations → Overloaded；
    //   Start 之后 → InvalidContext；Close 之后 → Closed。
    result<void> AddResource(ResourceDefinition definition, ResourceHandler handler);
    result<void> AddPrompt(PromptDefinition definition, PromptHandler handler);
    // 工具注册（Start 前；失败不留半条记录）：handler 为空、name 为空或非 UTF-8、
    //   input_schema 缺失/非 object、output_schema 非 object → InvalidArgument；
    //   Schema 编译失败（非法 schema/外部 ref/超上限）→ 注册整体拒绝（S10 fail-closed，
    //   无半条记录）；重复 tool name → InvalidArgument；超 max_registrations →
    //   Overloaded；未接通 EnableToolsSchemaValidation → InvalidContext（fail-closed，
    //   不注册未校验工具）；Start 后 → InvalidContext；Close 后 → Closed。
    result<void> AddTool(ToolDefinition definition, ToolHandler handler);

    // 一次成功 Start：冻结注册快照并开协议接纳（无 socket）。重复 Start → InvalidContext；
    // Close 之后 Start → Closed（不重开）。
    result<void> Start();
    // 幂等 noexcept 封口：拒绝后续注册与准入。本切片无 transport/队列资源，
    // 故不声称已物理 teardown（属 3-3）。封口同时丢弃在册订阅（长请求名额随之
    // 释放）；per-subscription 流结束响应的 wire 发送属 transport 面（3-x）。
    void Close() noexcept;

    // 2-5：资源更新显式发布入口（规范 notifications/resources/updated）。
    //   uri 必须已注册且服务已 Start；未 Start/已 Close/未注册 → InvalidContext/Closed/
    //   InvalidArgument。只投递给**已 ack 且 accepted 过滤器包含该 uri** 的订阅；
    //   返回实际投递的订阅数。订阅通知经各 channel 的通知 sink 发出（sink 未挂的
    //   channel 计数不增、帧丢弃）——sink 挂载属 transport 面（3-x）。
    result<std::size_t> PublishResourceChanged(const std::string& uri);

    bool IsStarted() const noexcept;
    bool IsClosed() const noexcept;

    const ServerOptions& Options() const noexcept;
    // 对外声明的 revision 列表（2 个）：现代 `2026-07-28`（每请求 `_meta` 路径）与 legacy
    // `2025-11-25`（initialize 握手）。discover 的 supportedVersions 与 -32022 的 data.supported
    // 回显同一列表。与「modern 接受集」（仅 2026-07-28）不同概念；2025-06-18 不在此列。
    const std::vector<std::string>& AdvertisedVersions() const noexcept;

private:
    friend class McpSession;
    friend std::shared_ptr<McpSession> CreateSession(const std::shared_ptr<McpServer>& server);
    friend result<void> EnableToolsSchemaValidation(McpServer& server);
    struct Impl;  // 内部实现（定义在 bbt/infra/mcp/detail/Server.hpp）
    explicit McpServer(ServerOptions options);
    std::shared_ptr<Impl> m_impl;
};

// 内部装配入口（transport 面用；不属公共契约，无公共头声明）：新建一个独立会话。
std::shared_ptr<McpSession> CreateSession(const std::shared_ptr<McpServer>& server);

} // namespace bbt::infra::mcp

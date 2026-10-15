#pragma once
// MCP transport-independent 核心：生命周期 / 有界准入 / 注册快照 / 业务方法分派
// （**内部实现面**，不导出给公共消费者；公共契约只有 include/bbt/infra/mcp/）。
//
// 组织（2-4 切片）：
//   - 公共 `McpServer`（include/bbt/infra/mcp/McpServer.hpp）只持有 `Impl`；
//     真正的生命周期/注册表/分派都在本头的 `McpServer::Impl` 里，避免公共头泄漏
//     内部类型，也避免再造一层重复 core+wrapper。
//   - `McpSession`：每 channel/stream 独立会话（legacy 握手状态 + 客户端 capabilities）。
//     帧入口 `HandleFrame` **只在内部面**，不 promote 成公共 API。
//   - 注册（AddResource/AddPrompt）在 Start 前完成，Start 冻结快照；注册表锁只保护
//     表本身与 admission/state，**绝不**持锁跑用户回调。
//
// 契约来源：WFER mcp-server-foundation revision9（specs R1/R2/R3/R4/R6/R10 中不依赖
// Schema 与 transport 的部分）+ 固定规范 confirmed-spec-schema.txt（2026-07-28）+
// 官方 Python SDK（安装版 mcp 2.0.0）legacy 生命周期行为。

#include <chrono>
#include <cstddef>
#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/mcp/JsonValue.hpp>
#include <bbt/infra/mcp/McpServer.hpp>
#include <bbt/infra/mcp/McpTypes.hpp>

#include "Protocol.hpp"

namespace bbt::infra::mcp {

// 分派结果。has_response=false 表示这是合法通知（JSON-RPC 规定永不回包）。
struct DispatchOutcome {
    bool has_response = false;
    std::string response;  // has_response 为 true 时是序列化后的 JSON-RPC 响应
};

class McpSession;  // F1：owner 会话归属判据（定义在本文件后部）

// ---- 2-6：tools 的 Schema 校验接缝（owned opaque，core 不接触 Schema/ICU 类型）-----
//
// 由私有 adapter 目标（bbt_infra_mcp_schema）经公共 EnableToolsSchemaValidation
// 注入；core 只经自有 std::function 调用。闭包自持 per-tool 已编译 validator
//（owned opaque，不外泄句柄）：
//   - compile：注册期（Start 前，registry 锁内）编译并登记 input/output validator；
//     任何失败 → AddTool 整体拒绝（fail-closed，无半条记录）；
//   - validate_input / validate_output：调用期校验（Start 后；快照已冻结，表只读）。
struct ToolsSchemaHooks {
    std::function<result<void>(const ToolDefinition&)> compile;
    std::function<bool(const std::string& tool_name, const JsonValue& instance)> validate_input;
    std::function<bool(const std::string& tool_name, const JsonValue& structured)> validate_output;
    bool set = false;
};

// ---- 3-3（R10）：request 栈外 owner —— 已接纳请求的唯一终态机 ---------------------
//
// 每个接纳的 request 一个 owner，栈外持有（handler 可把 abort 视图拷到任意线程）。
// 终态恰好一次（CAS），四类触发源先到先得：
//   Completed —— 请求生命周期在提交出口正常落定（响应可含业务错误/核心拒绝）；
//   Cancelled —— notifications/cancelled 命中（会话域按 requestId 路由，S4）；
//   TimedOut  —— 提交时刻已过 ServerOptions::request_timeout 期限（S22 可观察拒绝）；
//   Closed    —— core Close 封口翻转全部在途 owner。
// 终态落定后 owner 退化为空壳：晚到完成项（handler 返回的结果）只被消费、不产生
// 任何 wire 消息；晚到 cancel/期限/Close 竞争失败方静默退出（S17「晚到 cancel 无害」）。
struct RequestOwner {
    enum class Terminal : int { kNone = 0, kCompleted, kCancelled, kTimedOut, kClosed };

    RequestId request_id;                              // 原值（期限拒绝响应回包用）
    const McpSession* session = nullptr;               // 所属会话（F1：cancel 会话域判据）
    std::chrono::steady_clock::time_point deadline{};  // epoch = 无期限
    std::shared_ptr<std::atomic<bool>> abort =         // 协作式退出观察位（cancel/期限/Close
        std::make_shared<std::atomic<bool>>(false);    //  任一终态触发时置位）

    explicit RequestOwner(RequestId id) : request_id(std::move(id)) {}

    bool TryFinish(Terminal kind) noexcept {
        int expected = static_cast<int>(Terminal::kNone);
        if (!terminal.compare_exchange_strong(expected, static_cast<int>(kind),
                                              std::memory_order_acq_rel,
                                              std::memory_order_acquire))
            return false;  // 已有终态：本触发源竞争失败，只消费空壳
        abort->store(true, std::memory_order_release);
        // 仅内测计数（非公共面）：恰好一次终态的直接观测（S21）；生产开销为每请求
        // 一次原子自增，聚合的是「终态总数 = 落定请求数」这一进程级不变项。
        switch (kind) {
            case Terminal::kCompleted: s_test_completed.fetch_add(1, std::memory_order_relaxed); break;
            case Terminal::kCancelled: s_test_cancelled.fetch_add(1, std::memory_order_relaxed); break;
            case Terminal::kTimedOut: s_test_timed_out.fetch_add(1, std::memory_order_relaxed); break;
            case Terminal::kClosed: s_test_closed.fetch_add(1, std::memory_order_relaxed); break;
            case Terminal::kNone: break;
        }
        return true;
    }
    bool DeadlinePassed() const noexcept {
        return deadline.time_since_epoch().count() != 0 &&
               std::chrono::steady_clock::now() >= deadline;
    }
    Terminal TerminalNow() const noexcept {
        return static_cast<Terminal>(terminal.load(std::memory_order_acquire));
    }

    // 仅内测（internal 面，公共头不出现）：终态计数。
    static long TestCount(Terminal kind) noexcept {
        switch (kind) {
            case Terminal::kCompleted: return s_test_completed.load(std::memory_order_relaxed);
            case Terminal::kCancelled: return s_test_cancelled.load(std::memory_order_relaxed);
            case Terminal::kTimedOut: return s_test_timed_out.load(std::memory_order_relaxed);
            case Terminal::kClosed: return s_test_closed.load(std::memory_order_relaxed);
            case Terminal::kNone: return 0;
        }
        return 0;
    }
    static void TestResetCounters() noexcept {
        s_test_completed.store(0, std::memory_order_relaxed);
        s_test_cancelled.store(0, std::memory_order_relaxed);
        s_test_timed_out.store(0, std::memory_order_relaxed);
        s_test_closed.store(0, std::memory_order_relaxed);
    }

private:
    std::atomic<int> terminal{static_cast<int>(Terminal::kNone)};
    inline static std::atomic<long> s_test_completed{0};
    inline static std::atomic<long> s_test_cancelled{0};
    inline static std::atomic<long> s_test_timed_out{0};
    inline static std::atomic<long> s_test_closed{0};
};

// 核心实现（公共 McpServer 的 pimpl）。命名在公共头里只出现为 `McpServer::Impl`。
struct McpServer::Impl {
    struct ResourceEntry {
        ResourceDefinition definition;
        ResourceHandler handler;
    };
    struct PromptEntry {
        PromptDefinition definition;
        PromptHandler handler;
    };
    struct ToolEntry {
        ToolDefinition definition;
        ToolHandler handler;
    };

    explicit Impl(ServerOptions options);

    // 生命周期。
    result<void> Start();
    void Close() noexcept;
    bool IsStarted() const noexcept;
    bool IsClosed() const noexcept;

    // 注册（Start 前；失败不留半条记录）。
    result<void> AddResource(ResourceDefinition definition, ResourceHandler handler);
    result<void> AddPrompt(PromptDefinition definition, PromptHandler handler);
    // 2-6：tools 注册（Schema 语义校验经 m_tools_hooks 编译接缝，fail-closed）。
    result<void> AddTool(ToolDefinition definition, ToolHandler handler);

    // 分派（唯一帧入口，内部面）。owner 为本帧的 request 栈外 owner（3-3）：
    // HandleFrame 在分派前登记、在唯一提交出口按其终态落定 wire 去留。
    result<DispatchOutcome> Dispatch(McpSession& session, std::string_view raw,
                                     const std::shared_ptr<struct RequestOwner>& owner);

    // 准入 RAII：任何返回路径（含异常）都归还名额。
    class Admission {
    public:
        explicit Admission(Impl& server) noexcept : m_server(&server) {}
        Admission(Admission&& other) noexcept : m_server(other.m_server) { other.m_server = nullptr; }
        Admission(const Admission&) = delete;
        Admission& operator=(const Admission&) = delete;
        Admission& operator=(Admission&&) = delete;
        ~Admission() {
            if (m_server) m_server->Release();
        }

    private:
        Impl* m_server;
    };

    result<void> Admit();
    void Release() noexcept;

    // ---- 关闭域（M3）：callback-start 门 + 结果提交门共用 m_mutex/m_closed ----------
    //
    // 线性化点：`EnterCallback()` 的 m_mutex 获取。Close 与「取得 callback-start 门」
    // 在同一门内定序，二者必有先后：
    //   - Close 先 → EnterCallback 为假，请求**不进入**用户回调；
    //   - EnterCallback 先 → 回调已开始，Close 之后其输出由提交门抑制。
    // 竞争边界（不偷称物理零窗口）：EnterCallback 返回后到用户函数首指令之间仍可能
    // 与 Close 并发（本切片不做线程强杀，也不持任何锁跨回调）；该残窗由提交门兜底，
    // 保证封口后**任何**结果（成功/业务错误/异常）都不产生响应。
    bool EnterCallback() const;
    // 结果提交门：与 Close 同域。true = 封口已发生，本次结果不得输出。
    bool Sealed() const;

    result<DispatchOutcome> DispatchModern(McpSession& session, const JsonRpcRequest& request,
                                           const std::shared_ptr<struct RequestOwner>& owner);
    result<DispatchOutcome> DispatchLegacy(McpSession& session, const JsonRpcRequest& request,
                                           const std::shared_ptr<struct RequestOwner>& owner) const;
    result<DispatchOutcome> HandleInitialize(McpSession& session,
                                             const JsonRpcRequest& request) const;
    void ApplyNotification(McpSession& session, const JsonRpcRequest& request);

    // ---- 2-5：subscriptions/listen（长请求）与资源发布 --------------------------
    //
    // 订阅是长 request：ack 与后续事件通知经会话 sink 发出（无即时 JSON-RPC 响应），
    // 在途名额由 entry.admission 持有直至订阅结束；流结束（客户端 cancel 或 Close
    // 丢弃）时释放。ack 先于该订阅的任何事件（规范 MUST）：注册后立即投递并置
    // ack_sent，发布路径跳过未 ack 订阅。
    struct SubscriptionEntry {
        RequestId listen_id;
        McpSession* session = nullptr;  // 订阅所属会话；transport 须先结束订阅再析构会话（3-x 契约）
        bool tools_list_changed = false;
        bool prompts_list_changed = false;
        bool resources_list_changed = false;
        std::vector<std::string> resource_uris;  // accepted（∩ 已注册 uri）
        bool ack_sent = false;
        Admission admission;  // 长请求在途名额（移动构造进表）
    };
    result<DispatchOutcome> HandleSubscriptionsListen(McpSession& session,
                                                      const JsonRpcRequest& request);
    // 长请求名额转移：listen 帧自身的在途名额由 HandleFrame 的 RAII 持有并将在其
    // 返回时释放；订阅登记按「允许一次同线程瞬时 +1（转移），稳态受 max 约束」
    // 接纳（m_inflight > max 时才拒绝），避免同帧双计导致 max=1 时订阅永远无法成立。
    result<void> AdmitForSubscription();
    // 客户端 notifications/cancelled 的在途命中路由（3-3 F1 起**会话域**，语义见
    // CancelInflightForSession）。命中（发起 cancel 的会话在册订阅）：结束订阅并向
    // 该会话的 sink 发流结束响应（SubscriptionsListenResult，result._meta 带原
    // subscriptionId）。未命中/已终态/跨会话：静默丢弃（S4/S17）。
    void EndSubscription(const McpSession& session, const RequestId& listen_id);
    // 3-3 F1：notifications/cancelled 的会话域在途路由（ADR 0010 决策 18）。只取消
    // **发起 cancel 的会话**登记的同 id owner（规格 S4 后半句「不跨连接误取消别人的
    // 请求」）；HTTP 跨连接 cancel 因此是静默 no-op，HTTP 订阅终止由 transport 断连
    // 驱动（EndAllSubscriptions，S20）。取消即协作式：置 abort 位让 handler 观察、
    // 终态落定 Cancelled、从在途表移除；提交门随后只消费空壳。
    void CancelInflightForSession(const McpSession& session, const RequestId& id);
    // 3-3：owner 生命周期（HandleFrame 登记 → 提交出口退休）。同会话同 id 已有在途
    // owner 时返回 nullptr（规格 S4 前半句「重复在途 id 只接纳一次」；dedup_id 仅对
    // 可检出的 wire id 生效），调用方以 -32600 拒绝重复帧、handler 不双调用。
    std::shared_ptr<RequestOwner> RegisterRequestOwner(const McpSession& session,
                                                       const RequestId& id,
                                                       std::chrono::steady_clock::time_point deadline,
                                                       bool dedup_id);
    void RetireOwner(const RequestOwner* owner);
    result<std::size_t> PublishResourceChanged(const std::string& uri);

    mutable std::mutex m_subscriptions_mutex;
    std::map<std::size_t, std::unique_ptr<SubscriptionEntry>> m_subscriptions;
    std::size_t m_next_subscription_token = 0;

    // ---- 3-3：request 栈外 owner 在途表 -----------------------------------------
    // key = RequestId::Key()（稳定可比较键，McpTypes 明言「在途去重/订阅 id 用」）。
    // multimap：跨客户端同 id 并发在途是合法常态（决策 18 更正「客户端自违约」定性），
    // 不丢条目；cancel 按会话域只命中本会话条目（S4）。同会话同 id 由准入去重排除。
    // 锁序与订阅表一致：m_mutex → m_owners_mutex；本表操作不取 m_mutex。
    mutable std::mutex m_owners_mutex;
    std::multimap<std::string, std::shared_ptr<RequestOwner>> m_request_owners;

    // 业务方法（resources/*、prompts/*）。modern 与 legacy 共用同一实现，差异只在
    // 结果外壳（modern 写 resultType/cacheScope）与 legacy 生命周期门禁。
    result<DispatchOutcome> DispatchBusiness(const RequestContext& context,
                                             const JsonRpcRequest& request, bool modern) const;

    // 真实注册能力（只含已注册的原语；resources 有显式发布入口故宣告
    // subscribe=true，listChanged 无发布机制仍不宣告——2-5 修订，ADR 0010 决策 7）。
    JsonValue BuildCapabilities() const;

    // P4：业务方法可用性 = 与 BuildCapabilities 同一冻结注册事实。
    //   resources/*（list/read/templates/list）仅在有 resource 注册时可用；
    //   prompts/*（list/get）仅在有 prompt 注册时可用；其余方法不在本 core 方法表。
    // 判定只读注册表（锁仅护表本身，绝不跨 handler），空列表不假成功。
    enum class MethodFamily { None, Resources, Prompts, Tools };
    static MethodFamily FamilyOf(std::string_view method);
    bool MethodAvailable(std::string_view method) const;

    ServerOptions m_options;
    std::vector<std::string> m_advertised;
    mutable std::mutex m_mutex;
    bool m_started = false;
    bool m_closed = false;
    std::size_t m_inflight = 0;
    // 关闭域共享只读视图的底层标志：Close 置位（m_mutex 内），RequestContext 持
    // 可 copy 的 CancellationView 观察它（协作式退出；不可重开）。
    std::shared_ptr<std::atomic<bool>> m_close_signal = std::make_shared<std::atomic<bool>>(false);

    // 仅内测同步点（非公共面，公共头不出现）：在业务回调准入门之前、锁外调用。
    // 生产不设置（空 std::function 检查，无行为差异）；用于确定性构造「封口后尚未
    // 取得 callback-start 门」的窗口。必须在并发使用前设置。
    std::function<void()> m_test_pre_callback_hook;

    // 注册表：有序 map → 稳定列表顺序（分页 cursor 可复现）。锁只保护表与冻结标志，
    // 不覆盖 handler 调用（回调在锁外执行）。
    mutable std::mutex m_registry_mutex;
    bool m_frozen = false;
    std::map<std::string, ResourceEntry> m_resources;  // key = uri
    std::map<std::string, PromptEntry> m_prompts;      // key = name
    std::map<std::string, ToolEntry> m_tools;          // key = name（2-6；与 resources/prompts
                                                       // 共用 max_registrations 有界池）
    // 2-6：Schema 编译接缝（EnableToolsSchemaValidation 注入，Start 前恰一次）。
    ToolsSchemaHooks m_tools_hooks;
};

// 每个 channel / stream 独立持有的会话状态（legacy 握手与生命周期）。
//
// 与「HTTP 无 session profile」明确区分：modern 请求**不读不写**任何会话状态
// （逐请求 `_meta` 自校验）；legacy 握手状态只属于本对象，绝不用全局 legacy flag。
class McpSession {
public:
    McpSession(const McpSession&) = delete;
    McpSession& operator=(const McpSession&) = delete;

    // 传输无关入口：喂入单帧原始文本，返回响应（合法通知 has_response=false）。
    // 协议级失败（parse/envelope/params/version/method/业务）一律以 JSON-RPC 错误响应
    // 返回，不抛异常；只有核心拒绝（未 Start / 已 Close / 超在途上限）才返回 result::err，
    // 由 transport 决定 wire 映射（如 HTTP 503）。
    result<DispatchOutcome> HandleFrame(std::string_view raw);

    // 仅内测（internal 面）：设置上述同步点。不属于公共契约。
    void SetTestPreCallbackHook(std::function<void()> hook) {
        m_core->m_test_pre_callback_hook = std::move(hook);
    }

    // 2-5 内部面（transport seam，不属公共契约）：订阅 ack/事件/流结束帧出口。
    // sink 未挂载时帧丢弃（计数照常）。锁只护拷贝，绝不持锁调用 sink。
    using NotificationSink = std::function<void(const std::string& frame)>;
    void SetNotificationSink(NotificationSink sink) {
        std::lock_guard<std::mutex> lock(m_sink_mutex);
        m_sink = std::move(sink);
    }
    // 3-1 内部面（transport teardown seam）：结束本会话全部在册订阅并经 sink 投递
    // 流结束帧（与 EndSubscription 同 wire 形状）。条目移除在 m_subscriptions_mutex
    // 内完成，sink 调用在锁外。transport 收口（Close/EOF）须先调用本方法、再析构
    // 会话（「表内条目不悬挂会话指针」C1 契约的 teardown 半边）。
    void EndAllSubscriptions();
    bool CopyNotificationSink(NotificationSink& out) const {
        std::lock_guard<std::mutex> lock(m_sink_mutex);
        out = m_sink;
        return static_cast<bool>(out);
    }
    void EmitNotificationFrame(const std::string& frame) {
        NotificationSink sink;
        {
            std::lock_guard<std::mutex> lock(m_sink_mutex);
            sink = m_sink;
        }
        if (sink) sink(frame);
    }

    // legacy 生命周期状态（只读；供定向测试与后续 transport 门禁消费）。
    //
    // 并发契约：同一 McpSession 的 HandleFrame 允许被并发调用（transport 可用读写/
    // 工作线程池驱动同一 channel 的 pipelined 帧）。下列字段及其**全部**读写由
    // m_state_mutex 保护；getter 在锁内取 owned copy，各自线性化（多次 getter 调用
    // 不是一个联合快照）。锁只覆盖字段读写，绝不覆盖 Dispatch/业务 handler。
    bool InitializeAccepted() const noexcept {
        std::lock_guard<std::mutex> lock(m_state_mutex);
        return m_initialize_accepted;
    }
    bool Initialized() const noexcept {
        std::lock_guard<std::mutex> lock(m_state_mutex);
        return m_initialized;
    }
    std::optional<std::string> NegotiatedVersion() const {
        std::lock_guard<std::mutex> lock(m_state_mutex);
        return m_negotiated_version;
    }
    // initialize 的 owned capabilities 副本（未握手 → 空 object）。
    JsonValue ClientCapabilities() const {
        std::lock_guard<std::mutex> lock(m_state_mutex);
        return m_client_capabilities;
    }

    // 内部面（HTTP legacy 无 session profile 专用 transport seam，不属公共契约；
    // 规格设计 R9/S23 显式选择 legacy HTTP 无 session）。受支持 legacy revision
    // 头已通过 transport 头门禁（ValidateHeaders 仅对两个真实 legacy 修订给出
    // Era::Legacy）时，为「本连接会话」提供 legacy era 门禁：官方 legacy 客户端
    // 逐 POST 新建 HTTP 连接，每连接会话不承载先前 initialize 状态。语义边界：
    // 不回填 clientCapabilities（不伪造任何身份）、不触发 initialized、不碰
    // modern 逐请求 `_meta` 校验、stdio 每会话 InitializeAccepted 握手语义不变；
    // 真实 initialize 已落定时不覆盖既有 negotiated 版本。
    void AdoptLegacyHeaderGate(std::string negotiated_version) {
        std::lock_guard<std::mutex> lock(m_state_mutex);
        if (m_initialize_accepted) return;
        m_initialize_accepted = true;
        m_negotiated_version = std::move(negotiated_version);
    }

private:
    friend class McpServer;
    friend std::shared_ptr<McpSession> CreateSession(const std::shared_ptr<McpServer>& server);
    explicit McpSession(std::shared_ptr<McpServer::Impl> core) : m_core(std::move(core)) {}

    std::shared_ptr<McpServer::Impl> m_core;  // 会话持 core 强引用（core 不反向持 transport）
    mutable std::mutex m_state_mutex;
    bool m_initialize_accepted = false;
    bool m_initialized = false;
    std::optional<std::string> m_negotiated_version;
    JsonValue m_client_capabilities = JsonValue::Object();
    mutable std::mutex m_sink_mutex;
    NotificationSink m_sink;
};

} // namespace bbt::infra::mcp

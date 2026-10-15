// MCP 私有 JSON Schema adapter 实现（2-3 切片）。
//
// 本 TU 是唯一接触 jsoncons / ICU 的 MCP schema 实现单元：
//   - Boost.JSON 唯一 DOM：JsonValue.Serialize()/parse 往返转码，整数词法
//     精确传递（9007199254740993 级别不降级为 double）。
//   - jsoncons 类型/异常、ICU 句柄不出本文件（SchemaValidator.hpp 为唯一边界）。
//   - pattern/patternProperties 关键词在 compile 期拦截：std::regex 可编译的
//     保留 jsoncons 原生路径；std::regex 拒绝而 ICU 接受者（如 \p{...}）重定位
//     为本文件 ICU 私有实现（IcuPattern），仅覆盖无条件实例位置；条件/组合/
//     ref/$defs 位置出现 icu-only pattern 时 compile 期 Unsupported（不静默
//     跳过，条件性沿结构向下传播，嵌套实例位置不重置回无条件）。
//   - ICU C 句柄非线程安全：IcuPattern 不持有常驻句柄，每次 Search 从不可变
//     pattern 文本重新 open（per-call 线程隔离）；运行期 UErrorCode 失败上报
//     脱敏错误，不冒充「不匹配」。
//   - UTF-8→UTF-16 用显式长度（保留 NUL，不引入尾部 NUL，转换上限 fail-closed）。
//   - 2-3 窄回修：size_t→int32 全部经显式上限检查（MF6）；ValidateImpl 运行期
//     失败以 bool 置位传播，成功/普通 mismatch 不写 *error（MF3）；AddProps
//     排除集随 kind=3 plan step 下发（MF5）；未编译对象 Validate fail-closed
//     （MF2）；Compile 裸 *error 加守卫（MF1）；"type" 接受 2020-12 数组形状
//     （MF4）。

//   - RG1：原始 schema 在进入 ICU transform/jsoncons 编译前，先过完整输入
//     字节/深度/节点预算，再按官方完整 Draft 2020-12 metaschema（jsoncons
//     内嵌 bundle，进程级编译一次的 const validator）全文校验，失败
//     InvalidSchema 脱敏。kKwTypes 手写局部切片删除；Transform 只递归真实
//     2020-12 schema 位置（definitions/dependencies 历史兼容按官方
//     metaschema 同为 schema 值位置），annotation/enum/const/未知扩展原样
//     保留（不递归、不建 obligation、不误收集 $id/网络 ref）。

#include "SchemaValidator.hpp"

// 2-6：tools×Schema 显式装配（EnableToolsSchemaValidation）。唯一实现 TU；core
// 闭包（bbt_infra_mcp_core）不含本符号，公共消费者只有显式链接本 adapter 目标
// 才能启用。经内部 Server.hpp 访问 McpServer::Impl 的注册快照。
#include <bbt/infra/mcp/McpServer.hpp>

#include "mcp/Server.hpp"

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <jsoncons_ext/jsonschema/draft202012/schema_draft202012.hpp>

#include <unicode/utypes.h>
#include <unicode/uregex.h>
#include <unicode/ustring.h>

#include <atomic>
#include <cstdint>
#include <exception>
#include <functional>
#include <limits>
#include <regex>
#include <set>
#include <stdexcept>
#include <utility>

namespace bbt::infra::mcp {
namespace {

using jsoncons::json;

// ---------- 构建期异常（本文件内部） ----------

struct BuildError : std::exception {
    SchemaErrorCode code;
    std::string msg;
    BuildError(SchemaErrorCode c, std::string m) : code(c), msg(std::move(m)) {}
    const char* what() const noexcept override { return msg.c_str(); }
};

struct ResolverRejected : std::exception {
    const char* what() const noexcept override { return "external or unknown ref rejected"; }
};

// ---------- ICU 私有 pattern 封装 ----------

// ICU C API 的长度参数为 int32_t：size_t→int32 一律先过显式上限检查（固定
// 要求，MF6），超限 fail-closed。-1 余量给 ToUtf16 目标缓冲上界（+1）留空间。
constexpr std::size_t kIcuLenMax =
    static_cast<std::size_t>(std::numeric_limits<int32_t>::max()) - 1;
constexpr const char* kIcuLenErr = "icu input length limit exceeded";
bool IcuLenOk(std::size_t n) { return n <= kIcuLenMax; }

// 显式长度 UTF-8→UTF-16：保留 NUL，成功后按 pDestLength 截断（不引入尾部 NUL）。
bool ToUtf16(std::string_view utf8, std::u16string* out, const char** err_name) {
    if (!IcuLenOk(utf8.size())) {  // MF6：转换前显式检查，不静默截断为负长度
        *err_name = kIcuLenErr;
        return false;
    }
    UErrorCode st = U_ZERO_ERROR;
    out->assign(utf8.size() + 1, u'\0');  // 上界：UTF-16 ≤ 2×UTF-8 长度
    int32_t len = 0;
    u_strFromUTF8(&(*out)[0], static_cast<int32_t>(out->size()), &len,
                  utf8.data(), static_cast<int32_t>(utf8.size()), &st);
    if (U_FAILURE(st)) {
        // 上界足够时不应溢出；任何失败（含溢出）一律 fail-closed。
        *err_name = u_errorName(st);
        return false;
    }
    out->resize(static_cast<std::size_t>(len));
    return true;
}

bool TryCompileIcu(std::string_view utf8, const char** err_name) {
    std::u16string pat;
    if (!ToUtf16(utf8, &pat, err_name)) return false;
    if (!IcuLenOk(pat.size())) {  // MF6：regex-open 入口显式检查
        *err_name = kIcuLenErr;
        return false;
    }
    UErrorCode st = U_ZERO_ERROR;
    UParseError pe{};
    URegularExpression* re =
        uregex_open(pat.data(), static_cast<int32_t>(pat.size()), 0, &pe, &st);
    if (U_FAILURE(st)) {
        *err_name = u_errorName(st);
        return false;
    }
    uregex_close(re);
    return true;
}

// 不可变 pattern 文本 + per-call open/close：无共享可变 ICU 句柄，move-only。
class IcuPattern {
public:
    explicit IcuPattern(std::string utf8) : utf8_(std::move(utf8)) {
        const char* err = nullptr;
        if (!ToUtf16(utf8_, &u16_, &err) || !IcuLenOk(u16_.size()))
            Throw(err ? err : kIcuLenErr);
    }

    // true=匹配 false=不匹配；runtime_err 非空 = ICU 运行期失败（调用方必须上报）。
    bool Search(std::string_view utf8, const char** runtime_err) const {
        *runtime_err = nullptr;
        if (!IcuLenOk(u16_.size())) {  // MF6：regex-open 入口显式检查
            *runtime_err = kIcuLenErr;
            return false;
        }
        std::u16string subj;
        if (!ToUtf16(utf8, &subj, runtime_err)) return false;
        if (!IcuLenOk(subj.size())) {  // MF6：setText 入口显式检查
            *runtime_err = kIcuLenErr;
            return false;
        }
        UErrorCode st = U_ZERO_ERROR;
        UParseError pe{};
        URegularExpression* re =
            uregex_open(u16_.data(), static_cast<int32_t>(u16_.size()), 0, &pe, &st);
        if (U_FAILURE(st)) {
            *runtime_err = u_errorName(st);
            return false;
        }
        uregex_setText(re, subj.data(), static_cast<int32_t>(subj.size()), &st);
        if (U_FAILURE(st)) {
            uregex_close(re);
            *runtime_err = u_errorName(st);
            return false;
        }
        const UBool found = uregex_find(re, 0, &st);  // 非锚定 = regex_search 语义
        if (U_FAILURE(st)) {
            uregex_close(re);
            *runtime_err = u_errorName(st);
            return false;
        }
        uregex_close(re);
        return found != 0;
    }

    IcuPattern(const IcuPattern&) = delete;
    IcuPattern& operator=(const IcuPattern&) = delete;
    IcuPattern(IcuPattern&&) = default;
    IcuPattern& operator=(IcuPattern&&) = default;

private:
    [[noreturn]] static void Throw(const char* err_name) {
        throw BuildError{SchemaErrorCode::RegexInvalid, std::string("icu regex compile failed: ") + err_name};
    }
    std::string utf8_;
    std::u16string u16_;
};

// std::regex（ECMAScript）可编译的 pattern 保留 jsoncons 原生路径。
bool StdRegexCompiles(std::string_view p) {
    try {
        std::regex r(std::string(p), std::regex::ECMAScript);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

// ---------- 编译产物 ----------

struct PlanStep {
    int kind;          // 0=Prop 1=EveryItem 2=ItemAt 3=AddProps
    std::string arg;   // Prop: 键名；ItemAt: 下标十进制
    // kind=3 专属（MF5）：同层 properties 键与 patternProperties 键 pattern
    // 下标。排除集随步骤下发（各 obligation 自带快照），不再依赖 obligation
    // 级共享字段——后者此前从未被写入，导致 additionalProperties 子 schema
    // 的 icu-only pattern 越权校验同层 properties 覆盖键。
    std::vector<std::string> excl_keys = {};
    std::vector<std::size_t> excl_patterns = {};
};

struct Obligation {
    int kind = 0;  // 0=Pattern 1=PatternProps 2=AddPropsSchema 3=AddPropsClosed
    std::size_t pattern = 0;
    std::vector<PlanStep> plan;      // schema→instance 位置映射（无条件结构路径）
    std::string schema_path;         // 展示用指针路径
    std::shared_ptr<const SchemaValidator::Impl> sub;  // PatternProps 子验证器
};

// 待回填的 AddProps 排除快照（MF5）：延迟到所在 schema 节点全部键处理完毕后
// 再快照 w->excl[spath]，避免文档键序（patternProperties 位于
// additionalProperties 之后）时 relocated 排除项漏记。
struct AddPropsPatch {
    std::size_t ob0;       // additionalProperties 子树新建 obligation 区间起点
    std::size_t ob1;       // 区间终点（不含）
    std::size_t step_idx;  // 该子树公共 plan 前缀中 kind=3 步骤的下标
    std::string spath;     // 所属节点 schema 路径
    // PP-AP1：重定位决策载荷。是否将本层 additionalProperties 转由 guard
    // obligation 承担取决于同节点 relocated PP 的存在，而 patternProperties
    // 可能位于本键之后（键序问题与 MF5 排除快照同因），决策延迟到节点处理
    // 完毕、excl 齐备处执行。
    std::vector<PlanStep> plan;  // 含 kind=3 步骤的完整 plan（guard 受众）
    jsoncons::json ap_t;         // 变换后的 AP 值（icu-only 已剥离）
    bool ap_false = false;       // AP 为 boolean false
};

// PP-AP1/R1：延迟绑定 guard 登记项。AP:schema guard 的 obligation 与排除集
// 快照在 walk 内创建，sub 编译延迟到所属资源 transformed 文档齐备：AP 子
// schema 若提前作独立根编译，$ref#/$defs 等同文档指针只能落向其自身，丢
// 外层资源引用面而误回退（XR1.e1 覆盖键误拒根因）。
struct DeferredGuard {
    std::size_t ob_idx;   // guard obligation 在 Walk::obligations 的下标
    jsoncons::json ap_t;  // 变换后的 AP 子 schema（guard 编译输入）
    std::string spath;    // 所属节点 spath（原样键段，非 RFC6901 转义形式）
};

// 编译产物完整类型（SchemaValidator::Impl）定义在匿名 ns 与外层边界之间：
// validator 成员以直初始化持有 make_json_schema 结果（prvalue 初始化，无拷贝
// /移动需求——json_schema 拷贝被删除）。

struct Walk {
    const SchemaOptions* opt = nullptr;
    std::vector<IcuPattern> patterns;
    std::vector<Obligation> obligations;
    // 文档内 $id 值集合：这些 base 属于同一文档，resolver 视为本地（仍无网络）。
    std::shared_ptr<std::set<std::string>> ids = std::make_shared<std::set<std::string>>();
    // spath → (PP 排除 pattern 下标, properties 排除键)
    std::map<std::string, std::pair<std::vector<std::size_t>, std::vector<std::string>>> excl;
    std::vector<AddPropsPatch> pending_addprops;
    std::vector<DeferredGuard> deferred_guards;
    // 含 relocated（icu-only）patternProperties 条目的节点 spath：jsoncons 对
    // 该层 additionalProperties 的覆盖判定看不到重定位键（PP-AP1 根因），该层
    // AP 需整体重定位到 obligation 执行。
    std::set<std::string> relocated_pp_nodes;
    long nodes = 0;
};
}  // namespace

// 嵌套 Impl：在外层命名空间定义（嵌套成员类不能在匿名命名空间内定义）。
// validator 以成员直初始化持有编译产物，副本不可变，IsValid 可并发调用。
struct SchemaValidator::Impl {
    jsoncons::jsonschema::json_schema<jsoncons::json> validator;
    std::vector<IcuPattern> patterns;
    std::vector<Obligation> obligations;
    SchemaOptions opt;
    Impl(const jsoncons::json& doc,
         const std::function<json(const jsoncons::uri&)>& resolver,
         const jsoncons::jsonschema::evaluation_options& options)
        : validator(jsoncons::jsonschema::make_json_schema(doc, resolver, options)) {}
    bool IsValid(const jsoncons::json& j) const { return validator.is_valid(j); }
};

namespace {

jsoncons::json TransformNode(const jsoncons::json& node, std::vector<PlanStep> plan,
                             bool positional, int depth, const std::string& spath, Walk* w);
std::shared_ptr<const SchemaValidator::Impl> CompileDoc(const jsoncons::json& doc,
                                                        const SchemaOptions& opt);
std::shared_ptr<const SchemaValidator::Impl> CompileDocCore(const jsoncons::json& doc,
                                                            const SchemaOptions& opt,
                                                            const jsoncons::json* ref_base,
                                                            const std::set<std::string>* seed_ids = nullptr);

// AddProps 的排除集预扫：同层 properties 键 + std::regex 可编译的
// patternProperties 键（relocated 键在其处理器内补记）。
void PrescanExclusions(const jsoncons::json& node, const std::string& spath, Walk* w) {
    if (node.contains("properties") && node.at("properties").is_object()) {
        for (const auto& pm : node.at("properties").object_range())
            w->excl[spath].second.push_back(std::string(pm.key()));
    }
    if (node.contains("patternProperties") && node.at("patternProperties").is_object()) {
        for (const auto& pm : node.at("patternProperties").object_range()) {
            const std::string pk(pm.key());
            if (!StdRegexCompiles(pk)) continue;  // relocated 条目由其处理器补记
            const char* err = nullptr;
            if (!TryCompileIcu(pk, &err)) continue;  // ICU 也拒的保留键无法参与排除（已知限制）
            w->patterns.emplace_back(pk);
            w->excl[spath].first.push_back(w->patterns.size() - 1);
        }
    }
}

jsoncons::json TransformNode(const jsoncons::json& node, std::vector<PlanStep> plan,
                             bool positional, int depth, const std::string& spath, Walk* w) {
    if (depth > static_cast<int>(w->opt->max_schema_depth))
        throw BuildError{SchemaErrorCode::LimitExceeded, "schema depth limit exceeded"};
    if (++w->nodes > static_cast<long>(w->opt->max_schema_nodes))
        throw BuildError{SchemaErrorCode::LimitExceeded, "schema node limit exceeded"};

    if (node.is_object()) {
        PrescanExclusions(node, spath, w);
        jsoncons::json out;
        for (const auto& m : node.object_range()) {
            const std::string key(m.key());
            const jsoncons::json& val = m.value();

            if (key == "$id") {
                if (val.is_string()) w->ids->insert(val.as_string());
            }

            if (key == "pattern") {
                if (!val.is_string()) { out[key] = val; continue; }  // 类型错误交 jsoncons 报
                const std::string pat(val.as_string());
                if (StdRegexCompiles(pat)) { out[key] = val; continue; }
                const char* err = nullptr;
                if (!TryCompileIcu(pat, &err))
                    throw BuildError{SchemaErrorCode::RegexInvalid,
                                     std::string("invalid regex pattern (icu: ") + (err ? err : "?") + ")"};
                if (!positional)
                    throw BuildError{SchemaErrorCode::Unsupported,
                                     "icu-only pattern in conditional/ref position"};
                w->patterns.emplace_back(pat);
                Obligation ob;
                ob.kind = 0;
                ob.pattern = w->patterns.size() - 1;
                ob.plan = plan;
                ob.schema_path = spath + "/pattern";
                w->obligations.push_back(std::move(ob));
                continue;  // 关键词重定位，不进入 jsoncons
            }

            if (key == "patternProperties") {
                if (!val.is_object()) { out[key] = val; continue; }
                jsoncons::json kept;
                bool any_kept = false;
                for (const auto& pm : val.object_range()) {
                    const std::string pk(pm.key());
                    const jsoncons::json& sub = pm.value();
                    if (StdRegexCompiles(pk)) {
                        kept[pk] = TransformNode(sub, plan, positional, depth + 1,
                                                 spath + "/patternProperties/" + pk, w);
                        any_kept = true;
                        continue;
                    }
                    const char* err = nullptr;
                    if (!TryCompileIcu(pk, &err))
                        throw BuildError{SchemaErrorCode::RegexInvalid,
                                         std::string("invalid patternProperties regex (icu: ") + (err ? err : "?") + ")"};
                    if (!positional)
                        throw BuildError{SchemaErrorCode::Unsupported,
                                         "icu-only patternProperties in conditional/ref position"};
                    w->patterns.emplace_back(pk);
                    const std::size_t idx = w->patterns.size() - 1;
                    // 子 schema 直接作为独立根编译（无需合成 $ref 包装）：其内部
                    // ref 依赖外层文档上下文时无法独立解析 → fail-closed Unsupported。
                    // MF7：positional 必须为 false——PP 子 schema 不在 hpp 契约的
                    // 无条件位置集内；此前空 plan + positional=true + 共享 Walk
                    // 会把子树内 ICU obligation 按文档根坐标注册到根执行空间
                    // （直属 icu pattern 静默失效、嵌套 pattern/PP 错位强制于根）。
                    // positional=false 使子树任意深度的 icu-only pattern/PP 在
                    // 门禁处抛 Unsupported（fail-closed，不静默）；std::regex 可
                    // 编译的 pattern 仍保留 jsoncons 原生路径，不受影响。
                    jsoncons::json sub_t = TransformNode(sub, {}, false, depth + 1,
                                                         spath + "/patternProperties/__sub", w);
                    std::shared_ptr<const SchemaValidator::Impl> sub_impl;
                    try {
                        sub_impl = CompileDoc(sub_t, *w->opt);
                    } catch (const ResolverRejected&) {
                        throw BuildError{SchemaErrorCode::Unsupported,
                                         "patternProperties subschema refs outside itself"};
                    } catch (const jsoncons::jsonschema::schema_error&) {
                        throw BuildError{SchemaErrorCode::Unsupported,
                                         "patternProperties subschema not self-contained"};
                    }
                    Obligation ob;
                    ob.kind = 1;
                    ob.pattern = idx;
                    ob.plan = plan;
                    ob.schema_path = spath + "/patternProperties";
                    ob.sub = std::move(sub_impl);
                    w->obligations.push_back(std::move(ob));
                    w->excl[spath].first.push_back(idx);
                    w->relocated_pp_nodes.insert(spath);
                }
                if (any_kept) out["patternProperties"] = kept;
                continue;
            }

            if (key == "properties" && val.is_object()) {
                jsoncons::json t;
                for (const auto& pm : val.object_range()) {
                    std::vector<PlanStep> cp = plan;
                    cp.push_back({0, std::string(pm.key())});
                    t[std::string(pm.key())] = TransformNode(
                        pm.value(), cp, positional, depth + 1,
                        spath + "/properties/" + std::string(pm.key()), w);
                }
                out[key] = t;
                continue;
            }
            if (key == "items") {
                std::vector<PlanStep> cp = plan;
                cp.push_back({1, ""});
                out[key] = TransformNode(val, cp, positional, depth + 1, spath + "/items", w);
                continue;
            }
            if (key == "prefixItems" && val.is_array()) {
                jsoncons::json t(jsoncons::json_array_arg);
                std::size_t i = 0;
                for (const auto& el : val.array_range()) {
                    std::vector<PlanStep> cp = plan;
                    cp.push_back({2, std::to_string(i)});
                    t.push_back(TransformNode(el, cp, positional, depth + 1,
                                              spath + "/prefixItems/" + std::to_string(i), w));
                    ++i;
                }
                out[key] = t;
                continue;
            }
            if (key == "additionalProperties") {
                // PP-AP1：本节点含 relocated patternProperties 时，jsoncons 的
                // additionalProperties 覆盖判定看不到重定位键，会把被 PP 匹配
                // 的键当额外字段（AP:false 误拒 / AP:schema 越权）。是否整体
                // 重定位到 guard obligation 取决于本节点 relocated PP 的存在，
                // 而 patternProperties 可能位于本键之后（与 MF5 排除快照同因
                // 的键序问题）：决策与载荷延迟到节点处理完毕处统一执行。
                std::vector<PlanStep> cp = plan;
                cp.push_back({3, ""});
                const std::size_t ob0 = w->obligations.size();
                jsoncons::json ap_t = TransformNode(val, cp, positional, depth + 1,
                                                    spath + "/additionalProperties", w);
                const bool ap_false = val.is_bool() && !val.as_bool();
                if (ap_false || val.is_object()) {
                    w->pending_addprops.push_back({ob0, w->obligations.size(),
                                                   cp.size() - 1, spath, std::move(cp),
                                                   std::move(ap_t), ap_false});
                } else {
                    out[key] = std::move(ap_t);  // AP:true：无语义，保持 jsoncons 原样
                }
                continue;
            }
            if (key == "allOf" && val.is_array()) {
                jsoncons::json t(jsoncons::json_array_arg);
                std::size_t i = 0;
                for (const auto& el : val.array_range()) {
                    t.push_back(TransformNode(el, plan, positional, depth + 1,
                                              spath + "/allOf/" + std::to_string(i), w));
                    ++i;
                }
                out[key] = t;
                continue;
            }

            // 独立 schema 值位置（RG1）：递归但降级为非实例位置——其中出现
            // icu-only pattern 即 Unsupported（不静默跳过）。MF7：降级沿结构
            // 向下单调传播，嵌套的 properties/items 等实例位置不再重置回
            // 无条件，避免未命中分支的 icu-only pattern 被当全局 obligation
            // 强制执行（组合语义缺陷，父级 fixture 实测复现）。
            if (key == "not" || key == "if" || key == "then" || key == "else" ||
                key == "contains" || key == "propertyNames" || key == "contentSchema" ||
                key == "unevaluatedProperties" || key == "unevaluatedItems") {
                out[key] = TransformNode(val, plan, false, depth + 1, spath + "/" + key, w);
                continue;
            }
            if (key == "anyOf" || key == "oneOf") {
                if (!val.is_array()) { out[key] = val; continue; }
                jsoncons::json t(jsoncons::json_array_arg);
                std::size_t i = 0;
                for (const auto& el : val.array_range()) {
                    t.push_back(TransformNode(el, plan, false, depth + 1,
                                              spath + "/" + key + "/" + std::to_string(i), w));
                    ++i;
                }
                out[key] = t;
                continue;
            }
            // schema 对象位置：值各为 schema。definitions/dependencies 历史兼容
            // 按官方 metaschema 同样声明为 schema 值位置（dependencies 的
            // stringArray 形态在数组/标量分支原样保留，无 obligation 副作用）。
            if ((key == "$defs" || key == "definitions" || key == "dependencies" ||
                 key == "dependentSchemas") && val.is_object()) {
                jsoncons::json t;
                for (const auto& pm : val.object_range())
                    t[std::string(pm.key())] = TransformNode(
                        pm.value(), plan, false, depth + 1,
                        spath + "/" + key + "/" + std::string(pm.key()), w);
                out[key] = t;
                continue;
            }
            // 其余键：enum/const/default/examples 等注解与未知扩展——值不是
            // schema（RG1 前置已按官方 metaschema 校验），原样保留：不递归、
            // 不创建 obligation、不误收集 $id/网络 ref。
            out[key] = val;
        }

        // 本节点全部键处理完毕：同层 properties/patternProperties 排除集已
        // 齐备（MF5），回填 additionalProperties 子树 obligation 的 kind=3
        // 步骤快照；不属于本节点的延迟项保留给对应祖先节点。PP-AP1 的
        // additionalProperties 重定位决策同在此处执行。
        if (!w->pending_addprops.empty()) {
            std::vector<AddPropsPatch> rest;
            for (auto& pp : w->pending_addprops) {
                if (pp.spath != spath) { rest.push_back(std::move(pp)); continue; }
                const auto eit = w->excl.find(spath);
                // PP-AP1 决策：本节点含 relocated patternProperties 时，该层
                // additionalProperties 的语义义务整体转由 guard obligation 承担
                // （受众=kind=3 排除快照：properties 键与全部 PP 匹配键都在额外
                // 键集合外），不再进入 jsoncons——被 PP 匹配的键不被误拒/误套，
                // 未匹配额外键与覆盖键值 schema 失败仍由各自路径拒绝。guard 的
                // 排除快照在 excl 齐备的此处直接写入。
                const bool reloc =
                    w->relocated_pp_nodes.count(spath) > 0 && eit != w->excl.end();
                bool guard_done = false;
                if (reloc && pp.ap_false) {
                    Obligation guard;
                    guard.kind = 3;  // AddPropsClosed：AP:false，排除集外一律拒绝
                    guard.plan = pp.plan;
                    guard.plan[pp.step_idx].excl_keys = eit->second.second;
                    guard.plan[pp.step_idx].excl_patterns = eit->second.first;
                    guard.schema_path = spath + "/additionalProperties";
                    w->obligations.push_back(std::move(guard));
                    guard_done = true;
                } else if (reloc && pp.ap_t.is_object()) {
                    // AP:schema：guard obligation 与排除集快照在 walk 内创建，
                    // sub 编译延迟到 CompileDocCore 收尾（PP-AP1/R1）：本地 ref
                    // 解析基必须是所属资源 transformed 文档，此处尚未成形。编译
                    // 失败（ref 超出资源/网络面）由收尾处撤销 guard 回退 jsoncons
                    // 原生路径，不新增编译期拒绝、不降级既有 $ref 面。
                    Obligation guard;
                    guard.kind = 2;  // AddPropsSchema
                    guard.plan = pp.plan;
                    guard.plan[pp.step_idx].excl_keys = eit->second.second;
                    guard.plan[pp.step_idx].excl_patterns = eit->second.first;
                    guard.schema_path = spath + "/additionalProperties";
                    w->obligations.push_back(std::move(guard));
                    w->deferred_guards.push_back(
                        {w->obligations.size() - 1, std::move(pp.ap_t), spath});
                    guard_done = true;
                }
                // 循环界只看 obligation 区间与现存容器：个别 obligation 的 plan
                // 不含该 kind=3 步骤（如 PP 子树独立根编译产生的 plan={} 短
                // obligation）时必须跳过而非提前终止——否则其后所有 obligation
                // 的排除集快照全部丢失，addProps 越权校验 properties 覆盖键
                // （MF5 回填中断回归）。guard 路径同样必须回填 [ob0, ob1) 内
                // AP 子树既有 obligation 的快照（guard 自身下标在区间外，已在
                // 上方直接快照，二者不重叠）。
                if (eit != w->excl.end()) {
                    for (std::size_t i = pp.ob0;
                         i < pp.ob1 && i < w->obligations.size(); ++i) {
                        if (pp.step_idx >= w->obligations[i].plan.size() ||
                            w->obligations[i].plan[pp.step_idx].kind != 3)
                            continue;
                        PlanStep& st = w->obligations[i].plan[pp.step_idx];
                        st.excl_keys = eit->second.second;
                        st.excl_patterns = eit->second.first;
                    }
                }
                if (guard_done) continue;  // AP 已由 guard 承担，不进入 jsoncons
                out["additionalProperties"] = std::move(pp.ap_t);  // 原生/回退路径
            }
            w->pending_addprops.swap(rest);
        }
        return out;
    }

    if (node.is_array()) {
        jsoncons::json t(jsoncons::json_array_arg);
        std::size_t i = 0;
        for (const auto& el : node.array_range()) {
            t.push_back(TransformNode(el, plan, positional, depth + 1,
                                      spath + "/" + std::to_string(i), w));
            ++i;
        }
        return t;
    }
    return node;  // 标量：值语义复制
}

// PP-AP1/R1 回退：按 walk 记录的 spath（原样键段，非 RFC6901 转义形式）在
// transformed 内逐段定位对象成员并补写叶键；段/叶均按原样键比较。
bool AddMemberAtPath(jsoncons::json& doc, const std::string& spath,
                     const std::string& leaf, const jsoncons::json& val) {
    jsoncons::json* cur = &doc;
    std::size_t beg = 0;
    while (!spath.empty()) {
        const std::size_t end = spath.find('/', beg);
        const std::string seg = spath.substr(
            beg, end == std::string::npos ? std::string::npos : end - beg);
        if (!cur->is_object() || !cur->contains(seg)) return false;
        cur = &cur->at(seg);
        if (end == std::string::npos) break;
        beg = end + 1;
    }
    if (!cur->is_object()) return false;
    (*cur)[leaf] = val;
    return true;
}

// APREF-C1：guard 编译的合成资源身份。guard 包装文档（{allOf:[AP]}）若与外层
// 资源同基（空 base）编译，其自身根指针（#/allOf/0 等）与 AP 内同文档指针同
// 表同键，ref 先自命中包装中的 AP 副本形成递归，Compile 成功而 Validate
// SIGSEGV。隔离方式（APREF-C1 根因修复，复用 jsoncons 既有 URI/registry 能力）：
// guard 根取进程内唯一 urn 私有 $id（其指针面与任何用户资源隔离）；AP 副本内
// 仅限当前资源的 fragment 引用（"#..."/""）改写为指向检索基 urn 的绝对引用，
// 检索基注入 guard walk 的 ids（resolver 既有 ids 放行分支），jsoncons 随即按
// 检索基编译并登记外层 transformed 文档的 #/ 指针与 $anchor——本地 ref 由此
// 正确指向原资源。urn 基不参与网络解析；不开放外部 URI/文件，外部 ref 默认
// 仍 ResolverRejected。
std::string SyntheticResourceBase() {
    static std::atomic<std::uint64_t> seq{0};
    return "urn:bbt:mcp:schema-apref:" + std::to_string(++seq);
}

// APREF-C1：改写 AP 编译副本内的同资源 fragment 引用为「检索基 + 原值」绝对
// 引用。只遍历真实 2020-12 schema 位置（与 TransformNode 同集，含嵌套
// additionalProperties）；遇 "$id" 资源边界即停——该子树由 jsoncons 按自身
// $id 原生登记，其引用本就指向自身资源（S3 语义），改写反而丢资源边界。
// annotation/const/enum/未知扩展与已为绝对形式的 ref 原样保留；$ref 值类型
// 错误交 jsoncons 编译期报错（fail-closed 回退路径不变）。
void RewriteFragmentRefs(jsoncons::json& node, const std::string& base) {
    if (!node.is_object()) return;
    if (node.contains("$id")) return;  // 资源边界：子树引用按自身资源解析
    for (const char* key : {"$ref", "$dynamicRef"}) {
        if (!node.contains(key)) continue;
        jsoncons::json& ref = node.at(key);
        if (!ref.is_string()) continue;
        const std::string& v = ref.as_string();
        if (v.empty() || v[0] == '#') ref = jsoncons::json(base + v);
    }
    for (const char* key : {"properties", "patternProperties", "$defs", "definitions",
                            "dependencies", "dependentSchemas"}) {
        if (!node.contains(key) || !node.at(key).is_object()) continue;
        for (auto& m : node.at(key).object_range())
            RewriteFragmentRefs(m.value(), base);
    }
    for (const char* key : {"items", "additionalProperties", "not", "if", "then", "else",
                            "contains", "propertyNames", "contentSchema",
                            "unevaluatedProperties", "unevaluatedItems"}) {
        if (node.contains(key)) RewriteFragmentRefs(node.at(key), base);
    }
    if (node.contains("prefixItems") && node.at("prefixItems").is_array()) {
        for (auto& el : node.at("prefixItems").array_range())
            RewriteFragmentRefs(el, base);
    }
    for (const char* key : {"allOf", "anyOf", "oneOf"}) {
        if (!node.contains(key) || !node.at(key).is_array()) continue;
        for (auto& el : node.at(key).array_range())
            RewriteFragmentRefs(el, base);
    }
}

std::shared_ptr<const SchemaValidator::Impl> CompileDoc(const jsoncons::json& doc,
                                                        const SchemaOptions& opt) {
    return CompileDocCore(doc, opt, nullptr);
}

// ref_base 非空 = PP-AP1/R1 guard 编译：doc 为 AP 子 schema 的 allOf 包装，
// fragment/文档内 $id 的本地 ref 解析基指向外层资源 transformed 文档（jsoncons
// 对 resolver 返回文档按检索基编译并登记 #/... 指针与 $anchor 目标——与主文档
// 兜底路径同机制），不拷贝 $defs、不注入网络解析，默认外部 ref 仍 ResolverRejected。
std::shared_ptr<const SchemaValidator::Impl> CompileDocCore(const jsoncons::json& doc,
                                                            const SchemaOptions& opt,
                                                            const jsoncons::json* ref_base,
                                                            const std::set<std::string>* seed_ids) {
    Walk w;
    w.opt = &opt;
    // APREF-C1：外层资源检索基由调用方注入（guard 编译时 resolver 的既有 ids
    // 放行分支据此返回 ref_base 文档）。
    if (seed_ids) w.ids->insert(seed_ids->begin(), seed_ids->end());
    jsoncons::json transformed = TransformNode(doc, {}, true, 0, "", &w);

    // 延迟 guard 绑定（PP-AP1/R1）：资源 transformed 文档此时已齐备。逆序处理
    // 使撤销 guard 时的 erase 不扰动更早登记的 ob_idx。编译输入用 allOf 单元素
    // 包装：验证语义恒等，且 AP 子 schema 自身的 $defs 登记位置（#/allOf/0/
    // $defs/...）不再与全文档指针（#/$defs/...）同键碰撞——同名影子不得抢注
    // 根定义（2020-12 全文档指针语义）。APREF-C1：guard 根另持独立 urn $id，
    // 包装自身根指针与外层资源隔离；AP 副本内 fragment 引用改写指向检索基，
    // 由 jsoncons 检索登记的外层 transformed 解析（#/allOf/0 命中用户原始
    // allOf[0]，不再自命中包装副本递归）。
    for (auto it = w.deferred_guards.rbegin(); it != w.deferred_guards.rend(); ++it) {
        bool bound = false;
        try {
            jsoncons::json guard_doc(jsoncons::json_object_arg);
            jsoncons::json arr(jsoncons::json_array_arg);
            jsoncons::json ap_copy = it->ap_t;  // 改写只作用于编译输入副本；回退仍用原值
            const std::string retrieval_base = SyntheticResourceBase();
            RewriteFragmentRefs(ap_copy, retrieval_base);
            arr.push_back(std::move(ap_copy));
            guard_doc["$id"] = SyntheticResourceBase();  // guard 根资源身份与外层隔离
            guard_doc["allOf"] = std::move(arr);
            const std::set<std::string> seed_ids{retrieval_base};
            w.obligations[it->ob_idx].sub =
                CompileDocCore(guard_doc, opt, &transformed, &seed_ids);
            bound = w.obligations[it->ob_idx].sub != nullptr;
        } catch (const ResolverRejected&) {
        } catch (const jsoncons::jsonschema::schema_error&) {
        }
        if (!bound) {
            // 回退：guard 撤销、AP 交还 jsoncons 原生路径（与 walk 内直填仅
            // 成员顺序差异，keyword 处理按名查找不受影响）。spath 段均由本
            // walk 创建、必然存在；定位失败属内部错误，fail-closed 不发布丢
            // 失 AP 约束的半状态。
            if (!AddMemberAtPath(transformed, it->spath, "additionalProperties", it->ap_t))
                throw BuildError{SchemaErrorCode::Internal, "deferred AP fallback lost"};
            w.obligations.erase(w.obligations.begin() +
                                static_cast<std::ptrdiff_t>(it->ob_idx));
        }
    }

    jsoncons::jsonschema::evaluation_options options;
    options.default_version(jsoncons::jsonschema::schema_version::draft202012());
    options.require_format_validation(false);
    const jsoncons::json& local_base = ref_base ? *ref_base : transformed;
    auto compiled_resolver = [&opt, &local_base, ids = w.ids](const jsoncons::uri& u) -> json {
        const std::string full = u.string();
        const std::string base = full.substr(0, full.find('#'));
        // 空 base、jsoncons 默认基与文档内 $id 均解析到本地资源文档（PP-AP1/R1：
        // guard 编译时为外层 transformed，AP 子 schema 不再充当解析根）；其余
        // （网络/文件路径等）一律拒绝（remotes 仅测试离线 fixture）。
        if (base.empty() || base == "https://jsoncons.com" || ids->count(base))
            return local_base;
        auto it = opt.remotes.find(base);
        if (it != opt.remotes.end())
            return json::parse(it->second.Serialize());
        throw ResolverRejected{};
    };
    std::function<json(const jsoncons::uri&)> resolver = compiled_resolver;
    auto impl = std::make_shared<SchemaValidator::Impl>(transformed, resolver, options);
    impl->patterns = std::move(w.patterns);
    impl->obligations = std::move(w.obligations);
    impl->opt = opt;
    return impl;
}

// ---------- 验证 ----------

bool CountInstance(const jsoncons::json& n, int depth, const SchemaOptions& o, long* nodes) {
    if (++*nodes > static_cast<long>(o.max_instance_nodes)) return false;
    if (depth > static_cast<int>(o.max_instance_depth)) return false;
    if (n.is_array()) {
        for (const auto& e : n.array_range())
            if (!CountInstance(e, depth + 1, o, nodes)) return false;
    } else if (n.is_object()) {
        for (const auto& m : n.object_range())
            if (!CountInstance(m.value(), depth + 1, o, nodes)) return false;
    }
    return true;
}

// runtime_failed（MF3）：仅 ICU/UErrorCode 级运行期失败置位并写 *err；成功与
// 普通 schema mismatch 不触碰 error，避免默认 Internal 值污染成功路径。
ValidationResult ValidateImpl(const SchemaValidator::Impl& impl, const jsoncons::json& root,
                              SchemaError* err, bool* runtime_failed) {
    ValidationResult r;
    bool ok = true;
    auto fail_internal = [&](const char* what) {
        if (err) *err = SchemaError{SchemaErrorCode::Internal, what};
        *runtime_failed = true;
    };
    try {
        ok = impl.IsValid(root);
    } catch (const std::exception&) {
        fail_internal("validation failed");
        r.ok = false;
        r.issues.push_back({"internal", "/", ""});
        return r;
    }
    if (!ok) r.issues.push_back({"jsonschema", "/", ""});

    for (const auto& ob : impl.obligations) {
        std::vector<std::pair<const jsoncons::json*, std::string>> cur{{&root, ""}};
        for (const auto& st : ob.plan) {
            std::vector<std::pair<const jsoncons::json*, std::string>> next;
            for (const auto& p : cur) {
                const jsoncons::json* n = p.first;
                if (st.kind == 0) {
                    if (n->is_object() && n->contains(st.arg))
                        next.emplace_back(&n->at(st.arg), p.second + "/" + st.arg);
                } else if (st.kind == 1) {
                    if (n->is_array()) {
                        std::size_t i = 0;
                        for (const auto& el : n->array_range())
                            next.emplace_back(&el, p.second + "/" + std::to_string(i++));
                    }
                } else if (st.kind == 2) {
                    if (n->is_array()) {
                        const long idx = std::stol(st.arg);
                        if (idx >= 0 && static_cast<std::size_t>(idx) < n->size())
                            next.emplace_back(&n->at(static_cast<std::size_t>(idx)),
                                              p.second + "/" + st.arg);
                    }
                } else {  // AddProps：排除同层 properties 键与全部 PP 键匹配者（MF5：用步骤快照）
                    if (!n->is_object()) continue;
                    for (const auto& pm : n->object_range()) {
                        const std::string k(pm.key());
                        bool excl = false;
                        for (const auto& ek : st.excl_keys)
                            if (ek == k) { excl = true; break; }
                        if (!excl) {
                            for (std::size_t pi : st.excl_patterns) {
                                const char* rte = nullptr;
                                if (impl.patterns[pi].Search(k, &rte)) { excl = true; break; }
                                if (rte) {
                                    fail_internal("icu pattern evaluation failed");
                                    r.issues.push_back({"pattern_runtime", ob.schema_path, p.second});
                                    r.ok = false;
                                    excl = true;
                                }
                            }
                        }
                        if (!excl) next.emplace_back(&pm.value(), p.second + "/" + k);
                    }
                }
            }
            cur = std::move(next);
        }

        bool extra_reported = false;  // kind=3 guard 每 obligation 只报一条（脱敏）
        for (const auto& p : cur) {
            if (ob.kind == 0) {
                if (!p.first->is_string()) continue;  // pattern 仅作用于 string
                const char* rte = nullptr;
                const bool hit = impl.patterns[ob.pattern].Search(p.first->as_string(), &rte);
                if (rte) {
                    fail_internal("icu pattern evaluation failed");
                    r.issues.push_back({"pattern_runtime", ob.schema_path, p.second});
                    r.ok = false;
                    continue;
                }
                if (!hit) {
                    r.issues.push_back({"pattern", ob.schema_path, p.second});
                    r.ok = false;
                }
            } else if (ob.kind == 1) {
                if (!p.first->is_object()) continue;
                for (const auto& pm : p.first->object_range()) {
                    const std::string k(pm.key());
                    const char* rte = nullptr;
                    const bool hit = impl.patterns[ob.pattern].Search(k, &rte);
                    if (rte) {
                        fail_internal("icu pattern evaluation failed");
                        r.issues.push_back({"pattern_runtime", ob.schema_path, p.second});
                        r.ok = false;
                        continue;
                    }
                    if (!hit) continue;
                    ValidationResult subr = ValidateImpl(*ob.sub, pm.value(), err, runtime_failed);
                    if (!subr.ok) {
                        r.ok = false;
                        for (auto& iss : subr.issues) {
                            iss.instance_path = p.second + "/" + k + iss.instance_path;
                            r.issues.push_back(std::move(iss));
                        }
                    }
                }
            } else if (ob.kind == 2) {
                // PP-AP1：AP:schema guard——只校验 properties/PP 覆盖集之外的值，
                // 覆盖键不误套 additionalProperties。
                ValidationResult subr = ValidateImpl(*ob.sub, *p.first, err, runtime_failed);
                if (!subr.ok) {
                    r.ok = false;
                    for (auto& iss : subr.issues) {
                        iss.instance_path = p.second + iss.instance_path;
                        r.issues.push_back(std::move(iss));
                    }
                }
            } else {  // kind == 3：AP:false guard——额外键集合非空即拒绝（脱敏单条）
                if (!extra_reported) {
                    extra_reported = true;
                    r.issues.push_back({"additionalProperties", ob.schema_path, p.second});
                    r.ok = false;
                }
            }
        }
    }
    r.ok = r.ok && ok;
    return r;
}

// ---------- RG1 前置：完整输入预算 + 官方 metaschema 全文校验 ----------

// 完整输入预算（字节上限已在 Compile 入口、先于 parse）：进入任何
// transform/编译前对完整 doc 计数，超限先拒（LimitExceeded 优先于
// InvalidSchema，不以非法形状短路预算门禁）。
void CountSchemaBudget(const jsoncons::json& n, int depth, const SchemaOptions& o, long* nodes) {
    if (++*nodes > static_cast<long>(o.max_schema_nodes))
        throw BuildError{SchemaErrorCode::LimitExceeded, "schema node limit exceeded"};
    if (depth > static_cast<int>(o.max_schema_depth))
        throw BuildError{SchemaErrorCode::LimitExceeded, "schema depth limit exceeded"};
    if (n.is_array()) {
        for (const auto& e : n.array_range())
            CountSchemaBudget(e, depth + 1, o, nodes);
    } else if (n.is_object()) {
        for (const auto& m : n.object_range())
            CountSchemaBudget(m.value(), depth + 1, o, nodes);
    }
}

// 官方完整 Draft 2020-12 metaschema validator：进程内编译一次的不可变 const
// 对象（函数局部 static，C++11 起初始化线程安全；is_valid 为 const 只读，
// 与既有 compiled validator 并发 Validate 同一机制，不新增线程）。固定依赖
// jsoncons v1.10.0 内嵌官方 root+7 vocabulary allOf bundle；兜底 resolver 只
// 服务 *.json-schema.org/draft/2020-12/*（同一 bundle，离线、无网络），与用户
// schema 的外部 ref resolver（CompileDoc，默认全拒）完全隔离。
const jsoncons::jsonschema::json_schema<jsoncons::json>& MetaValidator() {
    jsoncons::jsonschema::evaluation_options options;
    options.default_version(jsoncons::jsonschema::schema_version::draft202012());
    options.require_format_validation(false);
    static const jsoncons::jsonschema::json_schema<jsoncons::json> meta(
        jsoncons::jsonschema::make_json_schema(
            jsoncons::jsonschema::draft202012::schema_draft202012<jsoncons::json>::get_schema(),
            [](const jsoncons::uri& u) -> jsoncons::json {
                const std::string base = u.base().string();
                if (base.rfind("https://json-schema.org/draft/2020-12/", 0) == 0)
                    return jsoncons::jsonschema::draft202012::
                        schema_draft202012<jsoncons::json>::get_schema();
                return jsoncons::json::null();
            },
            options));
    return meta;
}

// 原始 schema 进入 Transform/jsoncons 编译前的全文 metaschema 校验（RG1）。
// 失败 InvalidSchema 脱敏（固定短语，不回显 schema 内容）；校验引擎自身异常
// 上报脱敏 Internal，不冒充「schema 非法」。
void ValidateMetaSchema(const jsoncons::json& doc) {
    try {
        if (!MetaValidator().is_valid(doc))
            throw BuildError{SchemaErrorCode::InvalidSchema,
                             "schema rejected by draft 2020-12 metaschema validation"};
    } catch (const BuildError&) {
        throw;
    } catch (const std::exception&) {
        throw BuildError{SchemaErrorCode::Internal, "metaschema validation failed"};
    }
}

}  // namespace

// ---------- 公共 API ----------

bool SchemaValidator::Compile(const JsonValue& schema, const SchemaOptions& options,
                              SchemaValidator* out, SchemaError* error) {
    // 契约：失败路径全部显式写 *error；成功不写（调用方原值保留，与 Validate
    // 同一语义，回修 2-3）。out 为空是调用方 bug，报脱敏 Internal。
    if (!out) {
        if (error) *error = SchemaError{SchemaErrorCode::Internal, "null output argument"};
        return false;
    }
    try {
        // Boost.JSON → 文本 → jsoncons：整数词法精确传递（两库均 int64/uint64）。
        const std::string text = schema.Serialize();
        if (text.size() > options.max_schema_bytes) {
            if (error) *error = SchemaError{SchemaErrorCode::LimitExceeded, "schema byte limit exceeded"};  // MF1：*error 判空，error=nullptr 不崩
            return false;
        }
        const jsoncons::json doc = jsoncons::json::parse(text);
        // RG1 前置：完整输入预算（字节已在上）先拒，再按官方完整 metaschema
        // 全文校验原始 schema，之后才进入 ICU transform 与 jsoncons 编译。
        long schema_nodes = 0;
        CountSchemaBudget(doc, 0, options, &schema_nodes);
        ValidateMetaSchema(doc);
        out->impl_ = CompileDoc(doc, options);  // 全部构建完成后一次性发布：无半注册状态
        return true;
    } catch (const BuildError& e) {
        if (error) *error = SchemaError{e.code, e.msg};
    } catch (const ResolverRejected&) {
        if (error) *error = SchemaError{SchemaErrorCode::ExternalRef, "external ref rejected"};
    } catch (const jsoncons::jsonschema::schema_error&) {
        if (error) *error = SchemaError{SchemaErrorCode::InvalidSchema,
                                        "schema rejected by draft 2020-12 compilation"};
    } catch (const std::regex_error&) {
        if (error) *error = SchemaError{SchemaErrorCode::RegexInvalid, "invalid regex"};
    } catch (const std::exception&) {
        if (error) *error = SchemaError{SchemaErrorCode::Internal, "schema compilation failed"};
    }
    return false;
}

ValidationResult SchemaValidator::Validate(const JsonValue& instance, SchemaError* error) const {
    ValidationResult r;
    // MF2：默认构造 / Compile 失败对象无实现，fail-closed 而非空指针解引用。
    if (!impl_) {
        if (error) *error = SchemaError{SchemaErrorCode::Internal, "validator not compiled"};
        r.ok = false;
        r.issues.push_back({"internal", "/", ""});
        return r;
    }
    try {
        const std::string text = instance.Serialize();
        if (text.size() > impl_->opt.max_instance_bytes) {
            if (error) *error = SchemaError{SchemaErrorCode::LimitExceeded,
                                            "instance byte limit exceeded"};
            r.ok = false;
            return r;
        }
        const jsoncons::json parsed = jsoncons::json::parse(text);
        long nodes = 0;
        if (!CountInstance(parsed, 0, impl_->opt, &nodes)) {
            if (error) *error = SchemaError{SchemaErrorCode::LimitExceeded,
                                            "instance depth/node limit exceeded"};
            r.ok = false;
            return r;
        }
        SchemaError verr;
        bool runtime_failed = false;
        ValidationResult rr = ValidateImpl(*impl_, parsed, &verr, &runtime_failed);
        if (error && runtime_failed) *error = verr;  // MF3：仅运行期失败传播，成功/mismatch 不污染
        return rr;
    } catch (const std::exception&) {
        if (error) *error = SchemaError{SchemaErrorCode::Internal, "validation failed"};
        r.ok = false;
        return r;
    }
}

// ---- 2-6：tools×Schema 显式装配（公共 EnableToolsSchemaValidation 唯一实现）------
//
// compile：注册期在 registry 锁内对 input_schema（必需）与 output_schema（声明则）
// 做真实 Compile（官方 metaschema 全文校验 + ref 策略 + 复杂度上限，fail-closed
// 一次成型）；per-tool validator 由闭包自持（owned shared_ptr，不外泄句柄）。
// validate_input / validate_output：调用期对冻结快照按 tool 名查找已编译 validator
//（快照冻结 → 表只读；SchemaValidator 不可变且线程安全可并发）。未登记名（不应
// 发生：注册与校验同表）→ 拒绝（fail-closed）。
namespace {

Error MakeToolSchemaError(ErrorCode code, const char* domain_code, const char* message) {
    Error e;
    e.code = code;
    e.domain_code = domain_code;
    e.message = message;
    return e;
}

struct ToolsSchemaState {
    std::map<std::string, std::shared_ptr<const SchemaValidator>> input_validators;
    std::map<std::string, std::shared_ptr<const SchemaValidator>> output_validators;
};

}  // namespace

result<void> EnableToolsSchemaValidation(McpServer& server) {
    auto state = std::make_shared<ToolsSchemaState>();
    McpServer::Impl& impl = *server.m_impl;
    std::lock_guard<std::mutex> lock(impl.m_mutex);
    if (impl.m_closed)
        return result<void>::err(
            MakeToolSchemaError(ErrorCode::Closed, "mcp.server.closed", "server is closed"));
    if (impl.m_started)
        return result<void>::err(MakeToolSchemaError(
            ErrorCode::InvalidContext, "mcp.tools.frozen", "registration snapshot is frozen"));
    if (impl.m_tools_hooks.set)
        return result<void>::err(MakeToolSchemaError(ErrorCode::InvalidContext,
                                                     "mcp.tools.already_enabled",
                                                     "tools schema validation already enabled"));
    ToolsSchemaHooks hooks;
    hooks.compile = [state](const ToolDefinition& definition) -> result<void> {
        auto compiled = std::make_shared<SchemaValidator>();
        SchemaError error;
        if (!SchemaValidator::Compile(definition.input_schema, SchemaOptions{}, compiled.get(),
                                      &error)) {
            std::string message = "tool input schema rejected";
            if (error.code == SchemaErrorCode::ExternalRef) message = "tool input schema external ref rejected";
            else if (error.code == SchemaErrorCode::LimitExceeded) message = "tool input schema limits exceeded";
            else if (error.code == SchemaErrorCode::Unsupported) message = "tool input schema unsupported construct";
            return result<void>::err(MakeToolSchemaError(ErrorCode::InvalidArgument,
                                                         "mcp.tools.schema", message.c_str()));
        }
        state->input_validators[definition.name] = std::move(compiled);
        if (definition.output_schema) {
            auto out_compiled = std::make_shared<SchemaValidator>();
            SchemaError out_error;
            if (!SchemaValidator::Compile(*definition.output_schema, SchemaOptions{},
                                          out_compiled.get(), &out_error)) {
                state->input_validators.erase(definition.name);  // 无半条记录
                std::string message = "tool output schema rejected";
                if (out_error.code == SchemaErrorCode::ExternalRef) message = "tool output schema external ref rejected";
                else if (out_error.code == SchemaErrorCode::LimitExceeded) message = "tool output schema limits exceeded";
                else if (out_error.code == SchemaErrorCode::Unsupported) message = "tool output schema unsupported construct";
                return result<void>::err(MakeToolSchemaError(ErrorCode::InvalidArgument,
                                                             "mcp.tools.schema", message.c_str()));
            }
            state->output_validators[definition.name] = std::move(out_compiled);
        }
        return result<void>::ok();
    };
    hooks.validate_input = [state](const std::string& tool_name, const JsonValue& instance) -> bool {
        auto found = state->input_validators.find(tool_name);
        if (found == state->input_validators.end()) return false;  // fail-closed
        SchemaError ignored;
        return found->second->Validate(instance, &ignored).ok;
    };
    hooks.validate_output = [state](const std::string& tool_name, const JsonValue& structured) -> bool {
        auto found = state->output_validators.find(tool_name);
        if (found == state->output_validators.end()) return false;  // 未声明 output schema
        SchemaError ignored;
        return found->second->Validate(structured, &ignored).ok;
    };
    hooks.set = true;
    impl.m_tools_hooks = std::move(hooks);
    return result<void>::ok();
}

}  // namespace bbt::infra::mcp

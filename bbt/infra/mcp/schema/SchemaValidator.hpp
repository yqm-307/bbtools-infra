#pragma once

/**
 * @file SchemaValidator.hpp
 * @brief MCP 私有 JSON Schema adapter。
 *
 * 边界（第三方依赖策略见 docs/conventions.md §3）：
 *   - 本头是唯一消费面（内部 API，不安装、不进 include/ 公共目录）：输入输出
 *     只用自有 JsonValue 与标准库类型；jsoncons / ICU / Boost 的类型、异常与
 *     句柄不出 bbt/infra/mcp/schema/（实现 TU 唯一接触第三方）。
 *   - compile 全有或全无：任何失败不产生半注册状态。
 *   - 默认 resolver 拒绝一切网络/文件外部 ref；仅内部 $defs（或测试经
 *     SchemaOptions::remotes 显式提供的离线 fixture）可解析。
 *   - Schema 与 instance 的字节/深度/节点上限显式 fail-closed；不做隐式
 *     cast/default/delete。
 *   - draft 2020-12：编译前按官方完整 metaschema（内嵌 bundle，进程级编译
 *     一次的 const validator）做全文校验（失败 InvalidSchema 脱敏）；
 *     再由 jsoncons 编译（结构非法即拒）。
 *   - pattern/patternProperties：std::regex 可编译的保持 jsoncons 原生路径；
 *     std::regex 拒绝而 ICU 接受者（如 \\p{...}）重定位到本 adapter 的 ICU
 *     私有实现，仅覆盖无条件位置（root/properties/items/prefixItems/
 *     additionalProperties/allOf）；条件或 $ref/$defs 位置需要 ICU 的 pattern
 *     在 compile 期拒绝（Unsupported，不静默跳过）。
 *   - 线程安全：compiled 实例不可变；ICU C 句柄非线程安全，因此每次匹配从
 *     不可变 pattern 文本重新 open（per-call 隔离），无共享可变句柄。
 *   - 脱敏：ValidationIssue 只含 keyword 与路径，不回显 schema/实例值；ICU
 *     运行期失败上报为脱敏错误，不当「不匹配」吞掉。
 *
 * 未覆盖（不在本包宣称）：tools/handler 输入输出接线、注册表聚合、
 * 格式关键字（format）Unicode 语义、完整 JSON Schema Test Suite。
 */
#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <bbt/infra/mcp/JsonValue.hpp>

namespace bbt::infra::mcp {

/**
 * @brief 复杂度与依赖上限；默认值可在 Compile 时显式覆盖，必须为正。
 */
struct SchemaOptions {

    /**
     * schema 字节上限
     */
    std::size_t max_schema_bytes = 256u * 1024u;

    /**
     * schema 嵌套深度上限
     */
    std::size_t max_schema_depth = 64;

    /**
     * schema 节点数上限
     */
    std::size_t max_schema_nodes = 10000;

    /**
     * instance 字节上限
     */
    std::size_t max_instance_bytes = 1024u * 1024u;

    /**
     * instance 嵌套深度上限
     */
    std::size_t max_instance_depth = 128;

    /**
     * instance 节点数上限
     */
    std::size_t max_instance_nodes = 50000;

    /**
     * 测试专用离线 remote fixture：base-url（无 fragment）→ schema。默认空，即一切外部
     * ref（http/https/file/绝对路径等）在 compile 期拒绝。
     */
    std::map<std::string, JsonValue> remotes;
};

/**
 * @brief Schema 编译/验证错误码。
 */
enum class SchemaErrorCode {

    /**
     * 结构非法 / draft 2020-12 编译拒绝
     */
    InvalidSchema,

    /**
     * 外部或未知 ref 被默认 resolver 拒绝
     */
    ExternalRef,

    /**
     * schema 或 instance 超出显式字节/深度/节点上限
     */
    LimitExceeded,

    /**
     * pattern 无法编译（std::regex 与 ICU 均拒绝）
     */
    RegexInvalid,

    /**
     * ICU 路径不覆盖的条件/ref 位置（compile 期拒绝，不静默跳过）
     */
    Unsupported,

    /**
     * 未预期内部错误（脱敏，不含输入内容）
     */
    Internal,
};

/**
 * @brief Schema 错误：码 + 脱敏描述。
 */
struct SchemaError {

    /**
     * 错误码
     */
    SchemaErrorCode code = SchemaErrorCode::Internal;

    /**
     * 固定短语级描述；不回显 schema/实例/敏感值
     */
    std::string message;
};

/**
 * @brief 单条验证失败：只含 keyword 与 schema/instance 指针路径（无值）。
 */
struct ValidationIssue {

    /**
     * 触发的关键字
     */
    std::string keyword;

    /**
     * schema 内路径
     */
    std::string schema_path;

    /**
     * instance 内路径
     */
    std::string instance_path;
};

/**
 * @brief 验证结果：是否通过及失败项列表。
 */
struct ValidationResult {

    /**
     * 是否通过
     */
    bool ok = true;

    /**
     * 失败项（ok=false 时非空）
     */
    std::vector<ValidationIssue> issues;
};

/**
 * @brief 已编译 schema：不可变，可被多线程并发 Validate。
 */
class SchemaValidator {
public:
    /**
     * @brief 不透明编译产物：完整类型定义在实现 TU（PIMPL）。
     *
     * 公开仅使内部帮助结构能以 shared_ptr 持有；不泄露任何第三方头。
     */
    struct Impl;

    /**
     * @brief 全有或全无编译（含官方完整 metaschema 全文校验、ref 策略、复杂度上限、
     *        pattern 分类与 ICU 编译）。
     *
     * 成功时填充 *out；失败时填充 *error（可空），不产生半注册状态。
     * @param schema 待编译 schema
     * @param options 复杂度与依赖上限
     * @param out 成功时填充的编译产物
     * @param error 失败时填充的错误（可空）
     * @return 是否编译成功
     */
    static bool
    Compile(const JsonValue& schema, const SchemaOptions& options, SchemaValidator* out, SchemaError* error);

    /**
     * @brief 验证 instance（线程安全，可并发调用）。
     *
     * 实例超出上限时返回 ok=false 的 LimitExceeded 错误（经 *error，可空）；ICU 运行期失败
     * 返回 ok=false 且 *error 为脱敏 Internal（不冒充「不匹配」）。成功或普通 schema mismatch
     * 不写 *error（调用方原值保留）。未编译对象（默认构造或 Compile 失败）Validate 返回
     * ok=false 的脱敏 Internal，不崩溃。
     * @param instance 待验证实例
     * @param error 超限/内部失败时填充的错误（可空）
     * @return 验证结果；超限或内部失败为 ok=false
     */
    ValidationResult Validate(const JsonValue& instance, SchemaError* error) const;

private:
    std::shared_ptr<const Impl> impl_;
};

// clang-format off
}
// clang-format on

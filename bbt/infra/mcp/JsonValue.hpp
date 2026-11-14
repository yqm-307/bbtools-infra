#pragma once

/**
 * @file JsonValue.hpp
 * @brief MCP 协议核：Boost.JSON 的轻值语义封装。
 *
 * 契约来源：官方 MCP 规范 2026-07-28。
 *   - 公共头不出现任何 Boost/第三方类型；唯一实现单元 bbt/infra/mcp/JsonValue.cc 是
 *     全仓唯一包含 boost/json/src.hpp 的 TU（Boost.JSON 以源码方式内联进本库，
 *     不需要 libboost_json、不需要额外系统安装）。
 *   - 不造第二套 JSON parser/DOM：解析、序列化、整数/浮点词法全部交给 Boost.JSON。
 *   - 拒绝面：非法 UTF-8、重复对象键、不可表示数值、超深/超大帧、单一完整文档
 *     之外的尾随数据。不可表示数值 = 非有限（NaN/±Inf 字面量、1e309/1e400）或
 *     超出 int64/uint64 可表示范围的整数词法（不静默降级为 double）；统一映射
 *     Error::domain_code = "mcp.json.number"。
 *   - 有限浮点词法（含 '.'/'e'）按 IEEE 754 最近舍入：正常范围无损；下溢到 0
 *     （如 1e-999 → 0）保留 IEEE 语义、不属拒绝面；本包不宣称任意十进制浮点无损。
 *   - 值语义：copy 深复制（互不影响），move 后源对象为 null 且仍可用。
 *   - 错误文本固定，不回显输入；机器可读区分见 Error::domain_code（"mcp.json.*"）。
 */
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <bbt/infra/Result.hpp>

namespace bbt::infra::mcp {

/**
 * @brief 值的 kind。Int64/UInt64 分开，保证整数精确性不被 double 吞掉。
 */
enum class JsonKind { Null, Bool, Int64, UInt64, Double, String, Array, Object };

/**
 * @brief 自有 JSON 值，默认构造为 null（不分配）。
 *
 * 值语义：copy 深复制，move 后源对象为 null 且仍可用。构造字符串时严格校验
 * UTF-8，非法序列抛 std::invalid_argument。
 */
class JsonValue {
public:
    /**
     * @brief null
     */
    JsonValue() noexcept;

    /**
     * @brief 显式 null。
     */
    JsonValue(std::nullptr_t) noexcept;

    /**
     * @brief 布尔值。
     */
    JsonValue(bool value);

    /**
     * @brief int 字面量专用重载：按 int64 存储。
     *
     * 避免 JsonValue(0) 在 bool/int64/uint64/double/nullptr 之间产生歧义（尤其不能被
     * 静默解释成 null）。
     */
    JsonValue(int value);

    /**
     * @brief 有符号 64 位整数。
     */
    JsonValue(std::int64_t value);

    /**
     * @brief 无符号 64 位整数。
     */
    JsonValue(std::uint64_t value);

    /**
     * @brief 浮点值；非有限值（NaN/±Inf）抛 std::invalid_argument（JSON 无法表达）。
     */
    JsonValue(double value);

    /**
     * @brief 字符串值；非法 UTF-8 抛 std::invalid_argument。
     *
     * 严格校验复用 bbt::infra::detail::IsValidUtf8，不把坏字节静默写进值。
     */
    JsonValue(std::string_view value);

    /**
     * @brief 字符串值（C 字符串）；非法 UTF-8 抛 std::invalid_argument。
     */
    JsonValue(const char* value);

    /**
     * @brief copy 构造：深复制，与源互不影响。
     */
    JsonValue(const JsonValue& other);

    /**
     * @brief move 构造：转移内部值，源对象变为 null 且仍可用。
     */
    JsonValue(JsonValue&& other) noexcept;

    /**
     * @brief copy 赋值：深复制，与源互不影响。
     */
    JsonValue& operator=(const JsonValue& other);

    /**
     * @brief move 赋值：转移内部值，源对象变为 null 且仍可用。
     */
    JsonValue& operator=(JsonValue&& other) noexcept;
    ~JsonValue();

    /**
     * @brief 构造空 object。
     */
    static JsonValue Object();

    /**
     * @brief 构造空 array。
     */
    static JsonValue Array();

    /**
     * @brief 严格解析单一完整 JSON 文档。
     *
     *   max_bytes/max_depth 必须为正，否则 InvalidArgument；
     *   text.size() > max_bytes → ProtocolError("mcp.json.max_bytes")；
     *   深度超限 → ProtocolError("mcp.json.max_depth")；
     *   语法/编码/尾随数据 → ProtocolError("mcp.json.syntax")；
     *   重复对象键 → ProtocolError("mcp.json.duplicate_key")；
     *   不可表示数值（非有限或超范围整数）→ ProtocolError("mcp.json.number")。
     * @param text 待解析文本
     * @param max_bytes 输入字节上限
     * @param max_depth 嵌套深度上限
     * @return 解析出的值或协议错误
     */
    static result<JsonValue> Parse(std::string_view text, std::size_t max_bytes, std::size_t max_depth);

    /**
     * @brief 当前值的 kind。
     */
    JsonKind Kind() const noexcept;

    /**
     * @name kind 判定系
     * @brief 判定当前值的 kind；null 与 move 之后的源对象同样可判定。
     *
     * 共享语义：只读、不分配、不改值；下列判定互斥，恰好一个为真。
     * @{
     */

    /**
     * @brief 是否为 null。
     */
    bool IsNull() const noexcept;

    /**
     * @brief 是否为 bool。
     */
    bool IsBool() const noexcept;

    /**
     * @brief 是否为有符号整数。
     */
    bool IsInt64() const noexcept;

    /**
     * @brief 是否为无符号整数。
     */
    bool IsUInt64() const noexcept;

    /**
     * @brief 是否为浮点。
     */
    bool IsDouble() const noexcept;

    /**
     * @brief 是否为字符串。
     */
    bool IsString() const noexcept;

    /**
     * @brief 是否为 array。
     */
    bool IsArray() const noexcept;

    /**
     * @brief 是否为 object。
     * @}
     */
    bool IsObject() const noexcept;

    /**
     * @brief object/array 元素数；标量返回 0。
     */
    std::size_t Size() const noexcept;

    /**
     * @brief 按键取值（拥有型 copy，返回值与本源对象互不影响）。
     * @param key 键名
     * @return 值的副本；非 object → InvalidArgument("mcp.json.not_object")，
     *         键不存在 → NotFound
     */
    result<JsonValue> Find(std::string_view key) const;

    /**
     * @brief 按下标取值（拥有型 copy）。
     * @param index 元素下标
     * @return 值的副本；非 array → InvalidArgument("mcp.json.not_array")，
     *         越界 → NotFound
     */
    result<JsonValue> At(std::size_t index) const;

    /**
     * @brief object 的键列表（owned copy，按对象插入顺序）。
     *
     * 调用者可独立修改返回值。非 object（含 null / move 之后的源对象）→
     * InvalidArgument("mcp.json.not_object")；空 object → 空 vector。这是唯一
     * 的键枚举入口，不引入 DOM/visitor。
     * @return 键列表或类型错误
     */
    result<std::vector<std::string>> Keys() const;

    /**
     * @brief 写入 object 成员；目标不是 object 或键非法 UTF-8 时失败且不修改本对象
     * （校验先于写入，不存在半份修改）。
     *
     * @param key 键名
     * @param value 值
     * @return 成功或类型/编码错误
     */
    result<void> Set(std::string_view key, JsonValue value);

    /**
     * @brief 追加 array 元素；目标不是 array 时失败且不修改本对象。
     *
     * @param value 值
     * @return 成功或类型错误
     */
    result<void> Push(JsonValue value);

    /**
     * @name 取值系
     * @brief 按 kind 取出有类型的原值；不改变本对象。
     *
     * 共享语义：不丢精度、不回绕；各转换函数仅接受其成员文档列出的安全数值转换，
     * 其它类型不匹配返回 TypeMismatch。
     * @{
     */

    /**
     * @brief 取 bool 值；类型不符 → TypeMismatch。
     */
    result<bool> AsBool() const;

    /**
     * @brief 取 int64 值；UInt64 超出 int64 可表示范围 → TypeMismatch（不静默回绕）。
     */
    result<std::int64_t> AsInt64() const;

    /**
     * @brief 取 uint64 值；负 Int64 → TypeMismatch。
     */
    result<std::uint64_t> AsUInt64() const;

    /**
     * @brief 取 double 值；仅 Double，或可精确表示为 double 的整数（|v| ≤ 2^53）；
     * 超出 → TypeMismatch("mcp.json.precision")，不静默丢精度。
     */
    result<double> AsDouble() const;

    /**
     * @brief 取字符串值；类型不符 → TypeMismatch。
     * @}
     */
    result<std::string> AsString() const;

    /**
     * @brief 紧凑序列化（UTF-8）。null 值序列化为 "null"。
     */
    std::string Serialize() const;

private:
    /**
     * @brief 实现细节：包装 boost::json::value，定义在 bbt/infra/mcp/JsonValue.cc。
     * 采用类内嵌套（而非 bbt::infra::mcp::detail）以保持 namespace 深度 ≤ 3；
     * 公共头只出现前置声明，不传播 Boost 类型。
     */
    struct Impl;

    /**
     * @brief nullptr 即 null 值：默认构造/move 之后零分配、仍可用。
     */
    explicit JsonValue(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> m_impl;
};

// clang-format off
}
// clang-format on

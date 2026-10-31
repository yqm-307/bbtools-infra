#pragma once
// MCP 协议核：Boost.JSON 的轻值语义封装。
//
// 契约来源：WFER mcp-server-foundation revision7（specs R2 / design「JsonValue」段）。
//   - 公共头不出现任何 Boost/第三方类型；唯一实现单元 bbt/infra/mcp/JsonValue.cc 是
//     全仓唯一包含 boost/json/src.hpp 的 TU（Boost.JSON 以源码方式内联进本库，
//     不需要 libboost_json、不需要额外系统安装）。
//   - 不造第二套 JSON parser/DOM：解析、序列化、整数/浮点词法全部交给 Boost.JSON。
//   - 拒绝面：非法 UTF-8、重复对象键、不可表示数值、超深/超大帧、单一完整文档
//     之外的尾随数据。不可表示数值 = 非有限（NaN/±Inf 字面量、1e309/1e400）或
//     超出 int64/uint64 可表示范围的整数词法（不静默降级为 double）；统一映射
//     Error::domain_code = "mcp.json.number"。
//   - 有限浮点词法（含 '.'/'e'）按 IEEE 754 最近舍入：正常范围无损；下溢到 0
//     （如 1e-999 → 0）保留 IEEE 语义、不属拒绝面；本包不宣称任意十进制浮点无损。
//   - 值语义：copy 深复制（互不影响），move 后源对象为 null 且仍可用。
//   - 错误文本固定，不回显输入；机器可读区分见 Error::domain_code（"mcp.json.*"）。

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <bbt/infra/Result.hpp>

namespace bbt::infra::mcp {

// 值的 kind。Int64/UInt64 分开，保证整数精确性不被 double 吞掉。
enum class JsonKind { Null, Bool, Int64, UInt64, Double, String, Array, Object };

// 自有 JSON 值。默认构造为 null（不分配）。
class JsonValue {
public:
    JsonValue() noexcept;                        // null
    JsonValue(std::nullptr_t) noexcept;          // null
    JsonValue(bool value);
    // int 字面量专用重载：避免 JsonValue(0) 在 bool/int64/uint64/double/nullptr
    // 之间产生歧义（尤其不能被静默解释成 null）。按 int64 存储。
    JsonValue(int value);
    JsonValue(std::int64_t value);
    JsonValue(std::uint64_t value);
    // 非有限值（NaN/±Inf）拒绝：JSON 无法表达，抛 std::invalid_argument。
    JsonValue(double value);
    // UTF-8 严格校验（复用 bbt::infra::detail::IsValidUtf8）；非法序列抛
    // std::invalid_argument，不把坏字节静默写进值。
    JsonValue(std::string_view value);
    JsonValue(const char* value);

    JsonValue(const JsonValue& other);
    JsonValue(JsonValue&& other) noexcept;
    JsonValue& operator=(const JsonValue& other);
    JsonValue& operator=(JsonValue&& other) noexcept;
    ~JsonValue();

    // 空 object / 空 array。
    static JsonValue Object();
    static JsonValue Array();

    // 严格解析单一完整 JSON 文档。
    //   max_bytes/max_depth 必须为正，否则 InvalidArgument；
    //   text.size() > max_bytes → ProtocolError("mcp.json.max_bytes")；
    //   深度超限 → ProtocolError("mcp.json.max_depth")；
    //   语法/编码/尾随数据 → ProtocolError("mcp.json.syntax")；
    //   重复对象键 → ProtocolError("mcp.json.duplicate_key")；
    //   不可表示数值（非有限或超范围整数）→ ProtocolError("mcp.json.number")。
    static result<JsonValue> Parse(std::string_view text, std::size_t max_bytes,
                                   std::size_t max_depth);

    JsonKind Kind() const noexcept;
    bool IsNull() const noexcept;
    bool IsBool() const noexcept;
    bool IsInt64() const noexcept;
    bool IsUInt64() const noexcept;
    bool IsDouble() const noexcept;
    bool IsString() const noexcept;
    bool IsArray() const noexcept;
    bool IsObject() const noexcept;

    // object/array 元素数；标量返回 0。
    std::size_t Size() const noexcept;

    // 拥有型 copy：返回值与本源对象互不影响（调用者可独立修改）。
    // 非 object → InvalidArgument("mcp.json.not_object")；键不存在 → NotFound。
    result<JsonValue> Find(std::string_view key) const;
    // 非 array → InvalidArgument("mcp.json.not_array")；越界 → NotFound。
    result<JsonValue> At(std::size_t index) const;

    // object 的键列表（owned copy，按对象插入顺序）。调用者可独立修改返回值。
    // 非 object（含 null / move 之后的源对象）→ InvalidArgument("mcp.json.not_object")；
    // 空 object → 空 vector。仅此一处键枚举，不引入 DOM/visitor。
    result<std::vector<std::string>> Keys() const;

    // 目标不是 object/array（或键非法 UTF-8）时失败且不修改本对象；
    // 校验先于写入，不存在半份修改。
    result<void> Set(std::string_view key, JsonValue value);
    result<void> Push(JsonValue value);

    result<bool> AsBool() const;
    // UInt64 超出 int64 可表示范围 → TypeMismatch（不静默回绕）。
    result<std::int64_t> AsInt64() const;
    // 负 Int64 → TypeMismatch。
    result<std::uint64_t> AsUInt64() const;
    // 仅 Double，或可精确表示为 double 的整数（|v| ≤ 2^53）；超出 →
    // TypeMismatch("mcp.json.precision")，不静默丢精度。
    result<double> AsDouble() const;
    result<std::string> AsString() const;

    // 紧凑序列化（UTF-8）。null 值序列化为 "null"。
    std::string Serialize() const;

private:
    // 实现细节：包装 boost::json::value，定义在 bbt/infra/mcp/JsonValue.cc。
    // 采用类内嵌套（而非 bbt::infra::mcp::detail）以保持 namespace 深度 ≤ 3；
    // 公共头只出现前置声明，不传播 Boost 类型。
    struct Impl;
    // nullptr 即 null 值：默认构造/move 之后零分配、仍可用。
    explicit JsonValue(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> m_impl;
};

} // namespace bbt::infra::mcp

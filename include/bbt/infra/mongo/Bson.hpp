#pragma once
// mongo BSON 公共值类型（bbtools-infra Mongo 适配迁移）：BSON 文档的 owning
// 字节载体 + JSON 构造/读取的最薄入口。bsoncxx 类型不出现在本头——构造走
// libbson 的 JSON 解析（不手写编解码），读取走 Extended JSON 序列化 + 4 个
// 顶层标量 getter。
//
// 定位（与旧 CoMongoCli.hpp 的关系）：MongoDocument / MongoUpdateResult 的
// **唯一**定义在本头，namespace 为 bbt::infra::mongo；旧头
// <bbt/infra/CoMongoCli.hpp> 用 using 别名指回此处，历史代码与
// `MongoDocument{{0x05, ...}}` 聚合直写仍然合法（bytes 依旧是唯一真源）。
//
// 数值保真（实测 libbson 2.5.4，见 tests/Test_mongo_unit.cc 的 mongo_bson 套件）：
//   - 构造侧的裸整数字面量 `7` 只会得到 int32（JSON 字面量没有更细的类型信息）；
//     需要 int64 必须写 {"$numberLong":"7"}；需要 double 写 7.0 或
//     {"$numberDouble":"7.0"}；超 int64 的整数 → InvalidArgument（不静默转 double）。
//   - ToJson 默认 canonical（类型保真）：int32→{"$numberInt"}、int64→
//     {"$numberLong"}、double→{"$numberDouble"}。relaxed 便于人读，但会把
//     int64 输出成裸数字、回环后按 int32 解析（类型信息丢失），故不作为默认。
//
// 读取边界（明确 ceiling，本轮不做）：只支持**顶层**字段——无嵌套路径、无数组
// 索引、无 _id 特殊化。GetInt64 是唯一放宽的 getter（int32/int64 统一读成
// int64）；GetDouble/GetBool/GetString 只接受同名 BSON 类型，避免隐式截断。

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <bbt/infra/Result.hpp>

namespace bbt::infra::mongo {

// ToJson 的 Extended JSON 形态。
enum class MongoJsonMode {
    k_canonical = 0,  // 默认：保留 BSON 类型（$numberInt/$numberLong/$numberDouble）
    k_relaxed,        // 人读：int64 塌成裸数字，类型信息丢失
};

// 一份完整 BSON 文档的 owning 字节载体（首部 int32 长度 + 元素序列 + 结尾
// 0x00），二进制安全。聚合初始化 `MongoDocument{{...}}` 仍可直接写字节；
// 合法性（结构良构）在提交/读取时校验，非法输入返回 InvalidArgument。
struct MongoDocument {
    std::vector<std::uint8_t> bytes;

    // JSON（含 Extended JSON）→ 完整 BSON。空串/仅空白/顶层非对象/畸形/
    // 多份对象或尾部多余内容/超 int64 的整数 → InvalidArgument，不接触
    // driver。解析复用 bsoncxx 的 JSON 前端，不自写编码器；只有库实际报错
    // 时才带 backend_category="bsoncxx" 与 backend_code，本组件自己的前置
    // 与完整性检查不带后端元数据。
    static result<MongoDocument> FromJson(std::string_view json);

    // 完整 BSON → Extended JSON。字节非法（空/不良构）→ InvalidArgument；
    // 序列化失败 → InternalError。
    result<std::string> ToJson(
        MongoJsonMode mode = MongoJsonMode::k_canonical) const;

    // 顶层字段是否存在（bytes 非法时返回 false：不存在的文档没有字段）。
    bool Has(std::string_view field) const;

    // 顶层字段读取：字段缺失 → NotFound；类型不符 → TypeMismatch；
    // bytes 非法 → InvalidArgument。GetInt64 接受 int32/int64，其余 getter
    // 只接受同名类型。
    result<std::string>  GetString(std::string_view field) const;
    result<std::int64_t> GetInt64(std::string_view field) const;
    result<double>       GetDouble(std::string_view field) const;
    result<bool>         GetBool(std::string_view field) const;
};

// UpdateOne 的写回执（acknowledged write concern 下由驱动给出）。
struct MongoUpdateResult {
    std::int64_t matched  = 0;
    std::int64_t modified = 0;
    std::int64_t upserted = 0;
};

} // namespace bbt::infra::mongo

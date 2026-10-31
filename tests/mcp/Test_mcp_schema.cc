// tests/Test_mcp_schema.cc — 2-3 私有 JSON Schema adapter 最小行为检查。
// 覆盖：compile 拒绝面（InvalidSchema/ExternalRef/RegexInvalid/Unsupported/
// 复杂度 LimitExceeded）、整数精确、ICU pattern/patternProperties（Unicode/
// NUL）、AddProps 排除、脱敏、并发 Validate、离线 remote fixture。
// 不覆盖（OPEN）：tools/handler 接线、注册表聚合、完整 JSON Schema Test Suite。

#include "mcp/schema/SchemaValidator.hpp"  // 私有实现头：仅测试目标可见（bbt/infra/mcp/schema/）

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using bbt::infra::mcp::JsonValue;
namespace sc = bbt::infra::mcp;

static int g_fail = 0;
#define CHECK(cond)                                        \
    do {                                                   \
        if (!(cond)) {                                     \
            std::printf("FAIL %d: %s\n", __LINE__, #cond); \
            ++g_fail;                                      \
        }                                                  \
    } while (0)

static JsonValue V(std::string_view text) {
    auto parsed = JsonValue::Parse(text, 1u << 20, 128);
    if (!parsed) {
        std::printf("FATAL fixture parse: %s\n", parsed.error().message.c_str());
        std::exit(2);
    }
    return std::move(parsed).value();
}

int main() {
    // 1. 结构非法 schema：draft 2020-12 编译拒绝，错误不回显 schema 内容。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        const bool ok = sc::SchemaValidator::Compile(
            V("{\"description\":\"ZZMARKER\",\"type\":123}"), sc::SchemaOptions{}, &v, &err);
        CHECK(!ok);
        CHECK(err.code == sc::SchemaErrorCode::InvalidSchema);
        CHECK(err.message.find("ZZMARKER") == std::string::npos);
    }
    // 2. 外部 ref 默认拒绝；失败后仍可正常 compile（无半注册状态）。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        CHECK(!sc::SchemaValidator::Compile(V("{\"$ref\":\"https://evil.test/x\"}"),
                                            sc::SchemaOptions{}, &v, &err));
        CHECK(err.code == sc::SchemaErrorCode::ExternalRef);
        CHECK(!sc::SchemaValidator::Compile(V("{\"$ref\":\"file:///etc/passwd\"}"),
                                            sc::SchemaOptions{}, &v, &err));
        CHECK(err.code == sc::SchemaErrorCode::ExternalRef);
        CHECK(sc::SchemaValidator::Compile(V("{\"type\":\"string\"}"), sc::SchemaOptions{}, &v, &err));
        auto r = v.Validate(V("\"ok\""), &err);
        CHECK(r.ok);
    }
    // 3. 离线 remote fixture（测试专用）与未知 remote。
    {
        sc::SchemaOptions opt;
        opt.remotes.emplace("https://offline.test/a", V("{\"type\":\"integer\"}"));
        sc::SchemaValidator v;
        sc::SchemaError err;
        CHECK(sc::SchemaValidator::Compile(V("{\"$ref\":\"https://offline.test/a\"}"), opt, &v, &err));
        CHECK(v.Validate(V("5"), &err).ok);
        CHECK(!v.Validate(V("\"x\""), &err).ok);
        sc::SchemaValidator v2;
        CHECK(!sc::SchemaValidator::Compile(V("{\"$ref\":\"https://offline.test/missing\"}"),
                                            sc::SchemaOptions{}, &v2, &err));
        CHECK(err.code == sc::SchemaErrorCode::ExternalRef);
    }
    // 4. 复杂度上限 fail-closed（schema 与 instance，显式配置）。
    {
        sc::SchemaOptions opt;
        opt.max_schema_nodes = 50;
        sc::SchemaValidator v;
        sc::SchemaError err;
        std::string big = "[";
        for (int i = 0; i < 100; ++i) big += "null,";
        big += "null]";
        CHECK(!sc::SchemaValidator::Compile(V(big.c_str()), opt, &v, &err));
        CHECK(err.code == sc::SchemaErrorCode::LimitExceeded);

        sc::SchemaOptions opt2;
        opt2.max_instance_nodes = 10;
        CHECK(sc::SchemaValidator::Compile(V("{\"type\":\"array\"}"), opt2, &v, &err));
        auto r = v.Validate(V(big.c_str()), &err);
        CHECK(!r.ok);
        CHECK(err.code == sc::SchemaErrorCode::LimitExceeded);
    }
    // 5. 整数精确：2^53+1 边界不被 double 吞掉。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        CHECK(sc::SchemaValidator::Compile(
            V("{\"type\":\"integer\",\"exclusiveMaximum\":9007199254740993}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("9007199254740992"), &err).ok);
        CHECK(!v.Validate(V("9007199254740993"), &err).ok);
        CHECK(sc::SchemaValidator::Compile(V("{\"type\":\"integer\",\"maximum\":9007199254740993}"),
                                           sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("9007199254740993"), &err).ok);
    }
    // 6. ICU pattern：Unicode property（std::regex 盲区）走私有 ICU 路径。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        CHECK(sc::SchemaValidator::Compile(V("{\"pattern\":\"^\\\\p{Letter}+$\"}"),
                                           sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("\"π\""), &err).ok);
        CHECK(v.Validate(V("\"École\""), &err).ok);
        auto bad = v.Validate(V("\"123\""), &err);
        CHECK(!bad.ok);
        CHECK(bad.issues.size() == 1);
        CHECK(bad.issues[0].keyword == "pattern");
        CHECK(bad.issues[0].instance_path == "");
        // 脱敏：issue 不含实例值。
        CHECK(bad.issues[0].schema_path == "/pattern");
    }
    // 7. ASCII pattern 保持 jsoncons 原生（std::regex）路径。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        CHECK(sc::SchemaValidator::Compile(V("{\"pattern\":\"^[a-z]+$\"}"), sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("\"abc\""), &err).ok);
        CHECK(!v.Validate(V("\"AB1\""), &err).ok);
    }
    // 8. 两侧都拒的 pattern → RegexInvalid。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        CHECK(!sc::SchemaValidator::Compile(V("{\"pattern\":\"\\\\p{NoSuchProperty}\"}"),
                                            sc::SchemaOptions{}, &v, &err));
        CHECK(err.code == sc::SchemaErrorCode::RegexInvalid);
    }
    // 9. icu-only pattern 出现在条件位置 → Unsupported（不静默跳过）。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        CHECK(!sc::SchemaValidator::Compile(V("{\"anyOf\":[{\"pattern\":\"^\\\\p{Letter}+$\"}]}"),
                                            sc::SchemaOptions{}, &v, &err));
        CHECK(err.code == sc::SchemaErrorCode::Unsupported);
    }
    // 11. patternProperties 语义：保留（std::regex 可编译）与 ICU 重定位组合。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        // 全保留 + additionalProperties:false：jsoncons 原生路径。
        CHECK(sc::SchemaValidator::Compile(
            V("{\"patternProperties\":{\"^[0-9]+$\":{\"type\":\"integer\"}},"
              "\"additionalProperties\":false}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("{\"1\":2}"), &err).ok);
        CHECK(!v.Validate(V("{\"1\":true}"), &err).ok);
        CHECK(!v.Validate(V("{\"%\":1}"), &err).ok);  // 未被任何键 pattern 覆盖 → additional 拒绝
        // 仅 ICU 重定位（无 additionalProperties）：键名匹配走 ICU，值交子验证器。
        bool pp_ok = sc::SchemaValidator::Compile(
            V("{\"patternProperties\":{\"^\\\\p{Letter}+$\":{\"type\":\"string\"}}}"),
            sc::SchemaOptions{}, &v, &err);
        if (!pp_ok)
            std::printf("DIAG pp-only: code=%d msg=%s\n", static_cast<int>(err.code),
                        err.message.c_str());
        CHECK(pp_ok);
        CHECK(v.Validate(V("{\"π\":\"ok\"}"), &err).ok);
        CHECK(!v.Validate(V("{\"π\":1}"), &err).ok);
        CHECK(v.Validate(V("{\"123\":1}"), &err).ok);
        // PP-AP1 回修：relocated patternProperties 与 additionalProperties:false
        // 组合按标准语义执行——被 pattern 匹配的键不再是额外字段，未被匹配的
        // 额外键仍拒绝，PP 值 schema 失败仍拒绝。
        bool pp2_ok = sc::SchemaValidator::Compile(
            V("{\"patternProperties\":{\"^\\\\p{Letter}+$\":{\"type\":\"string\"}},"
              "\"additionalProperties\":false}"),
            sc::SchemaOptions{}, &v, &err);
        if (!pp2_ok)
            std::printf("DIAG pp-mixed: code=%d msg=%s\n", static_cast<int>(err.code),
                        err.message.c_str());
        CHECK(pp2_ok);
        CHECK(v.Validate(V("{\"π\":\"ok\"}"), &err).ok);   // PP 覆盖键不再当额外字段
        CHECK(!v.Validate(V("{\"π\":1}"), &err).ok);       // PP 值 schema 失败仍拒绝
        CHECK(!v.Validate(V("{\"zz\":1}"), &err).ok);      // 未匹配额外键仍拒绝
        CHECK(v.Validate(V("{}"), &err).ok);               // 空对象合法
    }
    // 12. NUL 保留：icu-only 语法（\x{0} 为字面 NUL）+ 实例嵌入 NUL。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        CHECK(sc::SchemaValidator::Compile(V("{\"pattern\":\"a\\\\x{0}b\"}"), sc::SchemaOptions{}, &v, &err));
        const std::string with_nul = "\"xa\\u0000bx\"";
        CHECK(v.Validate(V(with_nul), &err).ok);
        CHECK(!v.Validate(V("\"xa?bx\""), &err).ok);
    }
    // 13. 条件关键词在 ASCII pattern 下原生可用（if/then）。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        CHECK(sc::SchemaValidator::Compile(
            V("{\"if\":{\"properties\":{\"a\":{\"const\":1}}},"
              "\"then\":{\"properties\":{\"b\":{\"pattern\":\"^[a-z]+$\"}}}}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(!v.Validate(V("{\"a\":1,\"b\":\"AB\"}"), &err).ok);
        CHECK(v.Validate(V("{\"a\":2,\"b\":\"AB\"}"), &err).ok);
    }
    // 14. 并发 Validate：同一 compiled（含 ICU pattern）多线程结果一致。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        CHECK(sc::SchemaValidator::Compile(
            V("{\"properties\":{\"x\":{\"pattern\":\"^\\\\p{Letter}+$\"},"
              "\"y\":{\"type\":\"integer\"}}}"),
            sc::SchemaOptions{}, &v, &err));
        std::atomic<int> mismatches{0};
        std::vector<std::thread> ts;
        for (int t = 0; t < 4; ++t) {
            ts.emplace_back([&v, &mismatches] {
                for (int i = 0; i < 300; ++i) {
                    sc::SchemaError e;
                    if (!v.Validate(V("{\"x\":\"π\",\"y\":1}"), &e).ok) ++mismatches;
                    if (v.Validate(V("{\"x\":\"123\",\"y\":1}"), &e).ok) ++mismatches;
                    if (v.Validate(V("{\"x\":\"π\",\"y\":\"s\"}"), &e).ok) ++mismatches;
                }
            });
        }
        for (auto& th : ts) th.join();
        CHECK(mismatches.load() == 0);
    }
    // 15. 内部 $defs/$ref 正常解析（默认 resolver 不干预内部引用）。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        CHECK(sc::SchemaValidator::Compile(
            V("{\"$defs\":{\"s\":{\"type\":\"string\"}},\"$ref\":\"#/$defs/s\"}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("\"a\""), &err).ok);
        CHECK(!v.Validate(V("3"), &err).ok);
        sc::SchemaValidator v2;
        CHECK(!sc::SchemaValidator::Compile(V("{\"$ref\":\"#/missing\"}"), sc::SchemaOptions{}, &v2, &err));
        CHECK(err.code == sc::SchemaErrorCode::InvalidSchema);
    }

    // 16. 回修 MF1/MF2：error=nullptr 时超限 Compile 不崩；未编译对象 Validate fail-closed。
    {
        sc::SchemaOptions opt;
        opt.max_schema_bytes = 10;
        sc::SchemaValidator v;
        CHECK(!sc::SchemaValidator::Compile(V("{\"type\":\"string\"}"), opt, &v, nullptr));
        sc::SchemaError err;
        auto r = v.Validate(V("\"x\""), &err);  // Compile 失败后 impl_ 未发布
        CHECK(!r.ok);
        CHECK(err.code == sc::SchemaErrorCode::Internal);
        CHECK(v.Validate(V("\"x\""), nullptr).ok == false);  // error=nullptr 同样不崩
        sc::SchemaValidator fresh;  // 默认构造
        auto r2 = fresh.Validate(V("\"x\""), &err);
        CHECK(!r2.ok);
        CHECK(!r2.issues.empty() && r2.issues[0].keyword == "internal");
        CHECK(err.code == sc::SchemaErrorCode::Internal);
    }
    // 17. 回修 MF3：成功与普通 mismatch 不污染 *error（sentinel 原值保留）。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        err = sc::SchemaError{sc::SchemaErrorCode::RegexInvalid, "sentinel"};
        CHECK(sc::SchemaValidator::Compile(V("{\"type\":\"string\"}"), sc::SchemaOptions{}, &v, &err));
        CHECK(err.code == sc::SchemaErrorCode::RegexInvalid && err.message == "sentinel");
        CHECK(v.Validate(V("\"ok\""), &err).ok);
        CHECK(err.code == sc::SchemaErrorCode::RegexInvalid && err.message == "sentinel");
        CHECK(!v.Validate(V("3"), &err).ok);  // 普通 schema mismatch 不是 Internal
        CHECK(err.code == sc::SchemaErrorCode::RegexInvalid && err.message == "sentinel");
    }
    // 18. 回修 MF4：draft 2020-12 "type" 接受非空全 string 数组。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        CHECK(sc::SchemaValidator::Compile(V("{\"type\":[\"string\",\"null\"]}"),
                                           sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("\"s\""), &err).ok);
        CHECK(v.Validate(V("null"), &err).ok);
        CHECK(!v.Validate(V("1"), &err).ok);
        CHECK(sc::SchemaValidator::Compile(V("{\"type\":[\"integer\"]}"), sc::SchemaOptions{}, &v, &err));
        CHECK(!sc::SchemaValidator::Compile(V("{\"type\":[]}"), sc::SchemaOptions{}, &v, &err));
        CHECK(err.code == sc::SchemaErrorCode::InvalidSchema);
        CHECK(!sc::SchemaValidator::Compile(V("{\"type\":[\"string\",123]}"),
                                            sc::SchemaOptions{}, &v, &err));
        CHECK(err.code == sc::SchemaErrorCode::InvalidSchema);
    }
    // 19. 回修 MF5：additionalProperties 子 schema 的 icu-only pattern 不越权
    // 校验同层 properties 键；排除集含 relocated patternProperties（键序无关）。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        CHECK(sc::SchemaValidator::Compile(
            V("{\"properties\":{\"name\":{\"type\":\"string\"}},"
              "\"additionalProperties\":{\"pattern\":\"^\\\\p{Letter}+$\"}}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("{\"name\":\"123\"}"), &err).ok);     // name 由 properties 覆盖
        CHECK(v.Validate(V("{\"other\":\"abc\"}"), &err).ok);
        CHECK(!v.Validate(V("{\"other\":\"123\"}"), &err).ok);   // 未知键仍按 pattern 校验
        CHECK(!v.Validate(V("{\"name\":\"a\",\"zz9\":\"123\"}"), &err).ok);  // 未知键值不匹配
        // additionalProperties 在前：relocated patternProperties 排除项延迟快照。
        CHECK(sc::SchemaValidator::Compile(
            V("{\"additionalProperties\":{\"pattern\":\"^\\\\p{Nd}+$\"},"
              "\"patternProperties\":{\"^\\\\p{Letter}+$\":{\"type\":\"string\"}}}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("{\"π\":\"ok\",\"9\":7}"), &err).ok);  // π 走 PP；9 走 addProps
        CHECK(!v.Validate(V("{\"π\":\"ok\",\"z\":1}"), &err).ok);  // z 命中 PP 子 type:string
        CHECK(!v.Validate(V("{\"π\":\"ok\",\"!\":1}"), &err).ok == false);  // pattern 不作用于非 string 值
        CHECK(!v.Validate(V("{\"π\":\"ok\",\"5\":\"x\"}"), &err).ok);  // 未知 string 值仅 addProps pattern 管辖
    }
    // 20. 回修 MF7：条件/组合/$defs 位置的 icu-only pattern → compile 期
    // Unsupported（降级向下传播，不静默移除也不强制执行未选分支）。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        CHECK(!sc::SchemaValidator::Compile(
            V("{\"anyOf\":[{\"properties\":{\"x\":{\"pattern\":\"^\\\\p{Letter}+$\"}}},"
              "{\"properties\":{\"y\":{\"type\":\"integer\"}}}]}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(err.code == sc::SchemaErrorCode::Unsupported);
        CHECK(!sc::SchemaValidator::Compile(
            V("{\"if\":{\"properties\":{\"kind\":{\"const\":\"letters\"}},\"required\":[\"kind\"]},"
              "\"then\":{\"properties\":{\"text\":{\"pattern\":\"^\\\\p{Letter}+$\"}}}}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(err.code == sc::SchemaErrorCode::Unsupported);
        CHECK(!sc::SchemaValidator::Compile(
            V("{\"anyOf\":[{\"items\":{\"pattern\":\"^\\\\p{Letter}+$\"}}]}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(err.code == sc::SchemaErrorCode::Unsupported);
        CHECK(!sc::SchemaValidator::Compile(
            V("{\"$defs\":{\"s\":{\"pattern\":\"^\\\\p{Letter}+$\"}},\"$ref\":\"#/$defs/s\"}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(err.code == sc::SchemaErrorCode::Unsupported);
    }

    // 21. 回修 MF5 负回归（probe G2 形状）：additionalProperties 子树内
    // patternProperties 子 schema 直属 icu-only pattern——独立根编译产生
    // plan={} obligation，此前使排除集回填循环提前终止，properties 覆盖键
    // 实例被误拒。该位置在 hpp 无条件位置契约之外，修复后 compile 期
    // fail-closed Unsupported（MF7 门禁），不再误编译误拒。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        CHECK(!sc::SchemaValidator::Compile(
            V("{\"properties\":{\"keep\":{}},\"additionalProperties\":"
              "{\"patternProperties\":{\"^\\\\p{Letter}+$\":"
              "{\"pattern\":\"^\\\\p{Nd}+$\",\"required\":[\"tag\"]}}}}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(err.code == sc::SchemaErrorCode::Unsupported);
        // 同形状但子 schema 无 icu-only pattern：回填语义保持——properties
        // 覆盖键不被误拒（原 G2 实例），未知键约束不丢失。
        sc::SchemaValidator v2;
        CHECK(sc::SchemaValidator::Compile(
            V("{\"properties\":{\"keep\":{}},\"additionalProperties\":"
              "{\"patternProperties\":{\"^\\\\p{Letter}+$\":{\"required\":[\"tag\"]}}}}"),
            sc::SchemaOptions{}, &v2, &err));
        CHECK(v2.Validate(V("{\"keep\":{\"zzz\":{}}}"), &err).ok);    // keep 由 properties 覆盖
        CHECK(v2.Validate(V("{\"keep\":{}}"), &err).ok);
        CHECK(!v2.Validate(V("{\"other\":{\"zzz\":{}}}"), &err).ok);  // 未知键值仍受子 schema required
        CHECK(v2.Validate(V("{\"other\":{}}"), &err).ok);             // 子 PP 未命中键合法
    }
    // 22. 回修 MF7：patternProperties 子 schema 内（契约位置集之外）任意深度
    // 的 icu-only pattern / patternProperties → compile 期 Unsupported
    // fail-closed，不再错位注册到文档根执行空间；std::regex 可编译 pattern
    // 的子树原生路径保持可用，根层键匹配不受子 schema pattern 错位影响。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        // A：外层 icu PP 子 schema 内 properties 值的 icu-only pattern。
        CHECK(!sc::SchemaValidator::Compile(
            V("{\"patternProperties\":{\"^\\\\p{Nd}+$\":"
              "{\"properties\":{\"a\":{\"pattern\":\"^\\\\p{Nd}+$\"}}}}}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(err.code == sc::SchemaErrorCode::Unsupported);
        // B：外层 icu PP 子 schema 内嵌套 icu-only patternProperties（根数值
        // 键本应合法的形状，语义在契约外 → 安全预期 compile Unsupported）。
        CHECK(!sc::SchemaValidator::Compile(
            V("{\"patternProperties\":{\"^\\\\p{Letter}+$\":"
              "{\"patternProperties\":{\"^\\\\p{Nd}+$\":{\"type\":\"string\"}}}}}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(err.code == sc::SchemaErrorCode::Unsupported);
        // 原生路径保留 + 根键不错位：子内 ASCII pattern 正确作用于子值。
        sc::SchemaValidator v2;
        CHECK(sc::SchemaValidator::Compile(
            V("{\"patternProperties\":{\"^\\\\p{Nd}+$\":"
              "{\"properties\":{\"a\":{\"pattern\":\"^[A-Z]+$\"}}}}}"),
            sc::SchemaOptions{}, &v2, &err));
        CHECK(v2.Validate(V("{\"9\":1}"), &err).ok);                 // 数值键命中外层 PP；值非 object
        CHECK(v2.Validate(V("{\"a\":\"999\"}"), &err).ok);           // 非数字键不适用外层 PP（不误拒）
        CHECK(!v2.Validate(V("{\"9\":{\"a\":\"999\"}}"), &err).ok);  // 子 pattern 正确作用于子值
        CHECK(v2.Validate(V("{\"9\":{\"a\":\"ABC\"}}"), &err).ok);
    }

    // 23. RG1：官方 metaschema 全文负例（逐 vocabulary 规则命中，含嵌套
    // schema 位置的 dynamicRef 递归与历史兼容位置）。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        const char* negs[] = {
            "{\"minLength\":-1}",
            "{\"minLength\":\"5\"}",
            "{\"enum\":\"x\"}",
            "{\"type\":[\"string\",\"string\"]}",
            "{\"type\":[\"string\",\"integr\"]}",
            "{\"type\":{}}",
            "{\"required\":[\"a\",\"a\"]}",
            "{\"required\":\"a\"}",
            "{\"dependentRequired\":{\"a\":\"b\"}}",
            "{\"properties\":{\"x\":{\"minLength\":-1}}}",
            "{\"items\":{\"type\":123}}",
            "{\"$defs\":{\"d\":{\"required\":true}}}",
            "{\"definitions\":{\"d\":{\"enum\":{}}}}",
            "{\"contentSchema\":5}",
            "{\"$recursiveAnchor\":true}",
            "\"not-a-schema\"",
            "{\"multipleOf\":0}",
            "{\"maximum\":\"3\"}",
            "{\"patternProperties\":{\"^a$\":{\"dependentSchemas\":[]}}}",
        };
        for (const char* n : negs) {
            const bool ok = sc::SchemaValidator::Compile(V(n), sc::SchemaOptions{}, &v, &err);
            if (ok || err.code != sc::SchemaErrorCode::InvalidSchema) {
                std::printf("FAIL %d: %s -> ok=%d code=%d msg=%s\n", __LINE__, n,
                            ok ? 1 : 0, static_cast<int>(err.code), err.message.c_str());
                ++g_fail;
            }
        }
    }
    // 24. RG1：metaschema 合法形状不误拒——annotation/未知扩展携带 schema 形
    // 状值、空 enum（合法、实例永不 valid）、布尔 schema、合法联合、历史兼容
    // definitions/dependencies、meta 拒绝脱敏。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        CHECK(sc::SchemaValidator::Compile(
            V("{\"default\":{\"type\":123,\"pattern\":\"\\\\p{...}\"},"
              "\"examples\":[{\"$ref\":\"https://x.test/a\"}],"
              "\"const\":{\"type\":[]},"
              "\"enum\":[{\"required\":7}],"
              "\"x-ext\":{\"$ref\":\"http://evil.test\"}}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(sc::SchemaValidator::Compile(V("{\"enum\":[]}"), sc::SchemaOptions{}, &v, &err));
        CHECK(!v.Validate(V("\"anything\""), &err).ok);
        CHECK(!v.Validate(V("5"), &err).ok);
        CHECK(!v.Validate(V("null"), &err).ok);
        sc::SchemaValidator vt;
        CHECK(sc::SchemaValidator::Compile(V("true"), sc::SchemaOptions{}, &vt, &err));
        CHECK(vt.Validate(V("{\"any\":[1,2,3]}"), &err).ok);
        sc::SchemaValidator vf;
        CHECK(sc::SchemaValidator::Compile(V("false"), sc::SchemaOptions{}, &vf, &err));
        CHECK(!vf.Validate(V("1"), &err).ok);
        sc::SchemaValidator vu;
        CHECK(sc::SchemaValidator::Compile(V("{\"type\":[\"string\",\"null\"]}"),
                                           sc::SchemaOptions{}, &vu, &err));
        CHECK(vu.Validate(V("\"s\""), &err).ok);
        CHECK(vu.Validate(V("null"), &err).ok);
        CHECK(!vu.Validate(V("1"), &err).ok);
        // definitions 历史兼容位置：metaschema 合法且作为 schema 位置被走查
        // （非法内容见用例 23 n13）。注意 "#/definitions/..." 指针 ref 解析是
        // 固定 jsoncons 依赖的既有限制（基线 probe 同样拒绝，见 REPORT），
        // 本包不改第三方树；指针支持路径为 "#/$defs/..."。
        sc::SchemaValidator vd;
        CHECK(sc::SchemaValidator::Compile(
            V("{\"definitions\":{\"s\":{\"type\":\"string\",\"minLength\":2}},"
              "\"properties\":{\"a\":{\"type\":\"string\"}}}"),
            sc::SchemaOptions{}, &vd, &err));
        CHECK(vd.Validate(V("{\"a\":\"ok\"}"), &err).ok);
        CHECK(!vd.Validate(V("{\"a\":3}"), &err).ok);
        CHECK(sc::SchemaValidator::Compile(V("{\"dependencies\":{\"a\":[\"b\"]}}"),
                                           sc::SchemaOptions{}, &v, &err));
        sc::SchemaValidator vm;
        CHECK(!sc::SchemaValidator::Compile(V("{\"description\":\"ZZMETA\",\"minLength\":-1}"),
                                            sc::SchemaOptions{}, &vm, &err));
        CHECK(err.code == sc::SchemaErrorCode::InvalidSchema);
        CHECK(err.message.find("ZZMETA") == std::string::npos);
    }
    // 25. RG1：预算先于 metaschema 判定（完整输入先守卫）；meta 合法但产品
    // policy 拒绝的码值区分（RegexInvalid/ExternalRef，非 InvalidSchema）。
    {
        sc::SchemaOptions opt;
        opt.max_schema_nodes = 10;
        sc::SchemaValidator v;
        sc::SchemaError err;
        std::string big = "[";  // 根形状非法（数组）但预算门禁必须先命中
        for (int i = 0; i < 40; ++i) big += "null,";
        big += "null]";
        CHECK(!sc::SchemaValidator::Compile(V(big.c_str()), opt, &v, &err));
        CHECK(err.code == sc::SchemaErrorCode::LimitExceeded);
        CHECK(!sc::SchemaValidator::Compile(V("{\"pattern\":\"\\\\p{NoSuchProperty}\"}"),
                                            sc::SchemaOptions{}, &v, &err));
        CHECK(err.code == sc::SchemaErrorCode::RegexInvalid);
        CHECK(!sc::SchemaValidator::Compile(V("{\"$ref\":\"https://evil.test/x\"}"),
                                            sc::SchemaOptions{}, &v, &err));
        CHECK(err.code == sc::SchemaErrorCode::ExternalRef);
    }

    // 26. PP-AP1 回修回归：ICU relocated patternProperties × additionalProperties
    // 组合（父级 11-case probe 的产品缺陷面：官方 5 个 optional 失败 + 自有
    // 正例）。AP:false 不再误拒被 PP 匹配的键；未匹配额外键、覆盖键值 schema
    // 失败仍拒绝；AP:schema 不越权、AP:true 不跳 PP；properties+PP 同键多
    // 约束并存；文档键序（AP 前置/后置）不影响语义。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        // 官方 G1：\p{Letter}cole + AP:false（ascii/字面 é/\u 转义三种键写法）。
        CHECK(sc::SchemaValidator::Compile(
            V("{\"type\":\"object\",\"patternProperties\":{\"\\\\p{Letter}cole\":true},"
              "\"additionalProperties\":false}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("{\"l'ecole\":\"pas de vraie vie\"}"), &err).ok);
        CHECK(v.Validate(V("{\"l'école\":\"pas de vraie vie\"}"), &err).ok);
        CHECK(v.Validate(V("{\"l'\\u00e9cole\":\"pas de vraie vie\"}"), &err).ok);
        CHECK(!v.Validate(V("{\"l'9cole\":1}"), &err).ok);  // 正控反例：未匹配额外键拒绝
        // 官方 G2：^\\p{digit}+$ + AP:false（含非 ASCII 孟加拉数字键）。
        CHECK(sc::SchemaValidator::Compile(
            V("{\"type\":\"object\",\"patternProperties\":{\"^\\\\p{digit}+$\":true},"
              "\"additionalProperties\":false}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("{\"42\":\"life\"}"), &err).ok);
        CHECK(v.Validate(V("{\"৪২\":\"khajit\"}"), &err).ok);
        CHECK(!v.Validate(V("{\"x\":1}"), &err).ok);        // 正控反例
        // 父级自有正例：^\\p{Lu}+$ + AP:false。
        CHECK(sc::SchemaValidator::Compile(
            V("{\"type\":\"object\",\"patternProperties\":{\"^\\\\p{Lu}+$\":true},"
              "\"additionalProperties\":false}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("{\"ABC\":\"ok\"}"), &err).ok);
        CHECK(v.Validate(V("{}"), &err).ok);
        CHECK(!v.Validate(V("{\"abc\":\"ok\"}"), &err).ok);  // 正控反例
        // ICU/原生 PP 混合 + AP:false：两类匹配键都排除在额外键集合外。
        CHECK(sc::SchemaValidator::Compile(
            V("{\"patternProperties\":{\"^[a-z]+$\":{\"type\":\"integer\"},"
              "\"^\\\\p{Lu}+$\":{\"type\":\"string\"}},\"additionalProperties\":false}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("{\"a\":1,\"ABC\":\"ok\"}"), &err).ok);
        CHECK(!v.Validate(V("{\"a\":\"x\"}"), &err).ok);      // 原生 PP 值 schema 失败
        CHECK(!v.Validate(V("{\"ABC\":1}"), &err).ok);        // relocated PP 值 schema 失败
        CHECK(!v.Validate(V("{\"$\":1}"), &err).ok);          // 两类 pattern 均未命中 → 拒绝
        // AP:schema 不越权：覆盖键不套 AP 子 schema，未覆盖键仍受 AP 约束。
        CHECK(sc::SchemaValidator::Compile(
            V("{\"patternProperties\":{\"^\\\\p{Lu}+$\":{\"type\":\"string\"}},"
              "\"additionalProperties\":{\"type\":\"integer\"}}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("{\"ABC\":\"ok\"}"), &err).ok);    // 覆盖键不受 AP:integer 影响
        CHECK(!v.Validate(V("{\"ABC\":1}"), &err).ok);        // PP 值 schema 仍拒绝
        CHECK(v.Validate(V("{\"zz\":5}"), &err).ok);          // 未覆盖键走 AP
        CHECK(!v.Validate(V("{\"zz\":\"s\"}"), &err).ok);     // 未覆盖键 AP 拒绝非 integer
        // AP:true：额外键放行，PP 子 schema 不跳过。
        CHECK(sc::SchemaValidator::Compile(
            V("{\"patternProperties\":{\"^\\\\p{Lu}+$\":{\"type\":\"string\"}},"
              "\"additionalProperties\":true}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("{\"ABC\":\"ok\",\"x\":1}"), &err).ok);
        CHECK(!v.Validate(V("{\"ABC\":1}"), &err).ok);
        // properties+PP 同键多约束 + AP:false；PP 与 properties 键序互换同语义。
        CHECK(sc::SchemaValidator::Compile(
            V("{\"patternProperties\":{\"^\\\\p{Lu}+$\":{\"type\":\"string\"}},"
              "\"properties\":{\"ABC\":{\"minLength\":5}},\"additionalProperties\":false}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("{\"ABC\":\"12345\"}"), &err).ok);
        CHECK(!v.Validate(V("{\"ABC\":\"ab\"}"), &err).ok);   // properties minLength 仍生效
        CHECK(!v.Validate(V("{\"ABC\":1}"), &err).ok);        // PP type 仍生效
        CHECK(sc::SchemaValidator::Compile(
            V("{\"properties\":{\"ABC\":{\"minLength\":5}},"
              "\"patternProperties\":{\"^\\\\p{Lu}+$\":{\"type\":\"string\"}},"
              "\"additionalProperties\":false}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("{\"ABC\":\"12345\"}"), &err).ok);
        CHECK(!v.Validate(V("{\"ABC\":\"ab\"}"), &err).ok);
        // AP 在 PP 之前（MF5 延迟快照路径）：重定位决策同样延迟生效。
        CHECK(sc::SchemaValidator::Compile(
            V("{\"additionalProperties\":false,"
              "\"patternProperties\":{\"^\\\\p{Lu}+$\":{\"type\":\"string\"}}}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("{\"ABC\":\"ok\"}"), &err).ok);
        CHECK(!v.Validate(V("{\"zz\":1}"), &err).ok);
    }

    // 27. PP-AP1/R1 回修回归：relocated PP × AP 子 schema 本地 $ref 指向根
    // $defs（独审 XR1.e1）。guard 延迟绑定后：覆盖键不误套 AP；未覆盖键经
    // guard 走全文档指针解析的 $defs 定义，约束保留。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        CHECK(sc::SchemaValidator::Compile(
            V("{\"patternProperties\":{\"^\\\\p{Lu}+$\":{\"type\":\"string\"}},"
              "\"additionalProperties\":{\"$ref\":\"#/$defs/posint\"},"
              "\"$defs\":{\"posint\":{\"type\":\"integer\",\"minimum\":1}}}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("{\"ABC\":\"ok\"}"), &err).ok);   // 覆盖键不受 AP 约束
        CHECK(v.Validate(V("{\"zz\":2}"), &err).ok);         // 未覆盖键过 $ref
        CHECK(!v.Validate(V("{\"zz\":0}"), &err).ok);        // minimum 仍拒绝
        CHECK(!v.Validate(V("{\"zz\":\"s\"}"), &err).ok);    // type 仍拒绝
    }
    // 28. R1 指针语义对立回归：AP 子 schema 自带同名影子 $defs。#/$defs/...
    // 是全文档指针，必须指向根定义（integer）；若误以 AP 子 schema 为解析根
    // 则影子 string 定义抢注，以下两条反向断言互换。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        CHECK(sc::SchemaValidator::Compile(
            V("{\"patternProperties\":{\"^\\\\p{Lu}+$\":{\"type\":\"string\"}},"
              "\"additionalProperties\":"
              "{\"$defs\":{\"posint\":{\"type\":\"string\"}},"
              "\"$ref\":\"#/$defs/posint\"},"
              "\"$defs\":{\"posint\":{\"type\":\"integer\",\"minimum\":1}}}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("{\"zz\":2}"), &err).ok);         // 根 integer 定义生效
        CHECK(!v.Validate(V("{\"zz\":\"s\"}"), &err).ok);    // 影子 string 不生效
    }

    // 29. APREF-C1 回归：guard 包装文档不得抢注本地 JSON Pointer。AP 内
    // "#/allOf/0" 是全文档指针，必须指向根 allOf[0]（用户原始 schema），而非
    // guard 包装中的 AP 副本自身——此前自命中造成 Compile 成功、Validate
    // SIGSEGV。minLength 仅约束 string，外层 object 无该约束。
    {
        sc::SchemaValidator v;
        sc::SchemaError err;
        CHECK(sc::SchemaValidator::Compile(
            V("{\"allOf\":[{\"minLength\":2}],"
              "\"patternProperties\":{\"^\\\\p{Lu}+$\":{\"type\":\"string\"}},"
              "\"additionalProperties\":{\"$ref\":\"#/allOf/0\"}}"),
            sc::SchemaOptions{}, &v, &err));
        CHECK(v.Validate(V("{\"zz\":\"ok\"}"), &err).ok);    // AP 值长度 2
        CHECK(!v.Validate(V("{\"zz\":\"x\"}"), &err).ok);    // AP 值长度 1
        CHECK(v.Validate(V("{\"zz\":2}"), &err).ok);         // minLength 不约束 number
        // 原始对照（根 allOf[0]=type:string 约束整个实例）：对象一律 false，
        // 且必须无崩溃（此前同样在 Validate SIGSEGV）。
        sc::SchemaValidator v2;
        CHECK(sc::SchemaValidator::Compile(
            V("{\"allOf\":[{\"type\":\"string\"}],"
              "\"patternProperties\":{\"^\\\\p{Lu}+$\":{\"type\":\"string\"}},"
              "\"additionalProperties\":{\"$ref\":\"#/allOf/0\"}}"),
            sc::SchemaOptions{}, &v2, &err));
        CHECK(!v2.Validate(V("{\"zz\":\"s\"}"), &err).ok);
        CHECK(!v2.Validate(V("{\"zz\":2}"), &err).ok);
    }

    if (g_fail == 0) std::printf("mcp.schema: all checks passed\n");
    else std::printf("mcp.schema: %d check(s) failed\n", g_fail);
    return g_fail == 0 ? 0 : 1;
}

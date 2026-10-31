// 前置：infra 通用位置参数 payload codec 契约测试。
//
// 证据口径（不把「新 encoder → 新 decoder」当唯一证据）：
// 1. golden 期望字节是从旧 framework 源 CoRpc.hpp（bd9c4905 的 rpc_detail）
// 的字节规则独立写出的 literal；本测试只做 encode(值) == literal。
// 2. decode 侧用同一 literal 独立解回值，证明解码不依赖新编码器。
// 3. 旧源/新源字节级等价另有一次性交叉验证程序（不进仓）：
// scratch:infra-status-ab50534/rpc-payload-golden/old_vs_new.cc
// —— 直接编译旧 framework 头与新 infra 头对同批值逐字节比对。
//
// 旧源 pin（内容寻址，可经 git cat-file / sha256sum 复核）：
// rev bd9c490541ccad10cf32e15b91bb40104aa86118
// blob 08d1c954553f625ef396e534d898d6a2697ac232
// sha256 ae37d61fdcfe604f0c08ba518fffd3721a2d56f51202b471177ae19b071cf0d4
//
// 无第三方测试框架依赖；只链接 bbt::infra_common（纯 header-only codec）。

#include <cstdint>
#include <cstdio>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/rpc/RpcPayload.hpp>

namespace {

int g_checks   = 0;
int g_failures = 0;

void Check(bool cond, const char* expr, const char* file, int line) {
    ++g_checks;
    if (!cond) {
        ++g_failures;
        std::fprintf(stderr, "FAIL %s:%d: %s\n", file, line, expr);
    }
}

#define CHECK(cond) Check(static_cast<bool>(cond), #cond, __FILE__, __LINE__)

using namespace bbt::infra;
using namespace bbt::infra::rpc;

// --- 基线来源 pin（内容寻址常量，禁止被清空） -------------------------------
inline constexpr const char* kBaselineFrameworkRev =
    "bd9c490541ccad10cf32e15b91bb40104aa86118";
inline constexpr const char* kBaselineBlobOid =
    "08d1c954553f625ef396e534d898d6a2697ac232";
inline constexpr const char* kBaselineSourceSha256 =
    "ae37d61fdcfe604f0c08ba518fffd3721a2d56f51202b471177ae19b071cf0d4";

std::vector<std::uint8_t> Vec(std::initializer_list<int> bytes) {
    std::vector<std::uint8_t> out;
    out.reserve(bytes.size());
    for (int b : bytes) out.push_back(static_cast<std::uint8_t>(b));
    return out;
}

template <class... Ts>
result<std::vector<std::uint8_t>> Enc(const std::tuple<Ts...>& t) {
    return EncodeTuple(t, std::index_sequence_for<Ts...>{});
}

template <class... Ts>
result<std::tuple<Ts...>> Dec(const std::vector<std::uint8_t>& bytes) {
    return DecodeTuple<Ts...>(bytes, std::index_sequence_for<Ts...>{});
}

// 断言 encode 结果与旧源 literal 完全一致，并记录失败位置。
void CheckBytes(const result<std::vector<std::uint8_t>>& got,
                const std::vector<std::uint8_t>& want, const char* what,
                const char* file, int line) {
    ++g_checks;
    if (!got) {
        ++g_failures;
        std::fprintf(stderr, "FAIL %s:%d: %s encode err: %s\n", file, line, what,
                     got.error().message.c_str());
        return;
    }
    if (got.value() != want) {
        ++g_failures;
        std::fprintf(stderr, "FAIL %s:%d: %s bytes mismatch\n", file, line, what);
        std::fprintf(stderr, "  want:");
        for (auto b : want) std::fprintf(stderr, " %02x", b);
        std::fprintf(stderr, "\n  got :");
        for (auto b : got.value()) std::fprintf(stderr, " %02x", b);
        std::fprintf(stderr, "\n");
    }
}

#define CHECK_BYTES(got, want, what) \
    CheckBytes((got), (want), (what), __FILE__, __LINE__)

// --- 1. golden：旧源 literal ↔ 新 encode 逐字节相等 -------------------------
// 布局：count(u32 LE) + 逐字段 [tag(u8)][len(u32 LE)][payload]。
// tag: Bool=1 Int=2 Uint=3 Float=4 String=5 Bytes=6。

void TestGoldenEncode() {
    // int32 -1 → tag Int(2), len 4, 补码全 FF
    CHECK_BYTES(Enc(std::tuple<std::int32_t>{-1}),
                Vec({0x01, 0x00, 0x00, 0x00,
                     0x02, 0x04, 0x00, 0x00, 0x00,
                     0xFF, 0xFF, 0xFF, 0xFF}),
                "int32(-1)");

    // int64 -2 → tag Int(2), len 8, 0xFFFFFFFFFFFFFFFE
    CHECK_BYTES(Enc(std::tuple<std::int64_t>{-2}),
                Vec({0x01, 0x00, 0x00, 0x00,
                     0x02, 0x08, 0x00, 0x00, 0x00,
                     0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}),
                "int64(-2)");

    // uint64 0x0102030405060708 → tag Uint(3), len 8, 小端
    CHECK_BYTES(Enc(std::tuple<std::uint64_t>{0x0102030405060708ull}),
                Vec({0x01, 0x00, 0x00, 0x00,
                     0x03, 0x08, 0x00, 0x00, 0x00,
                     0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}),
                "uint64(LE)");

    // uint64 上界
    CHECK_BYTES(Enc(std::tuple<std::uint64_t>{0xFFFFFFFFFFFFFFFFull}),
                Vec({0x01, 0x00, 0x00, 0x00,
                     0x03, 0x08, 0x00, 0x00, 0x00,
                     0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}),
                "uint64(max)");

    // bool true → tag Bool(1), len 1, 0x01
    CHECK_BYTES(Enc(std::tuple<bool>{true}),
                Vec({0x01, 0x00, 0x00, 0x00,
                     0x01, 0x01, 0x00, 0x00, 0x00,
                     0x01}),
                "bool(true)");
    CHECK_BYTES(Enc(std::tuple<bool>{false}),
                Vec({0x01, 0x00, 0x00, 0x00,
                     0x01, 0x01, 0x00, 0x00, 0x00,
                     0x00}),
                "bool(false)");

    // string "hi" → tag String(5), len 2
    CHECK_BYTES(Enc(std::tuple<std::string>{"hi"}),
                Vec({0x01, 0x00, 0x00, 0x00,
                     0x05, 0x02, 0x00, 0x00, 0x00,
                     0x68, 0x69}),
                "string(hi)");

    // 空 string → 零长合法
    CHECK_BYTES(Enc(std::tuple<std::string>{""}),
                Vec({0x01, 0x00, 0x00, 0x00,
                     0x05, 0x00, 0x00, 0x00, 0x00}),
                "string(empty)");

    // bytes {0xDE,0xAD} → tag Bytes(6)
    CHECK_BYTES(Enc(std::tuple<std::vector<std::uint8_t>>{{0xDE, 0xAD}}),
                Vec({0x01, 0x00, 0x00, 0x00,
                     0x06, 0x02, 0x00, 0x00, 0x00,
                     0xDE, 0xAD}),
                "bytes(2)");

    // 空 bytes → 零长合法
    CHECK_BYTES(Enc(std::tuple<std::vector<std::uint8_t>>{{}}),
                Vec({0x01, 0x00, 0x00, 0x00,
                     0x06, 0x00, 0x00, 0x00, 0x00}),
                "bytes(empty)");

    // float 1.0f → tag Float(4), len 4, 0x3F800000 小端
    CHECK_BYTES(Enc(std::tuple<float>{1.0f}),
                Vec({0x01, 0x00, 0x00, 0x00,
                     0x04, 0x04, 0x00, 0x00, 0x00,
                     0x00, 0x00, 0x80, 0x3F}),
                "float(1.0f)");

    // double 1.0 → tag Float(4), len 8, 0x3FF0000000000000 小端
    CHECK_BYTES(Enc(std::tuple<double>{1.0}),
                Vec({0x01, 0x00, 0x00, 0x00,
                     0x04, 0x08, 0x00, 0x00, 0x00,
                     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F}),
                "double(1.0)");

    // 多参 tuple<int32,string,bool>{7,"ab",false} → count 3，字段顺序保持
    CHECK_BYTES(Enc(std::tuple<std::int32_t, std::string, bool>{7, "ab", false}),
                Vec({0x03, 0x00, 0x00, 0x00,
                     0x02, 0x04, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00,
                     0x05, 0x02, 0x00, 0x00, 0x00, 0x61, 0x62,
                     0x01, 0x01, 0x00, 0x00, 0x00, 0x00}),
                "tuple<int32,string,bool>");
}

// --- 2. decode 侧用旧源 literal 独立解回 -------------------------------------

void TestGoldenDecode() {
    auto t = Dec<std::int32_t, std::string, bool>(
        Vec({0x03, 0x00, 0x00, 0x00,
             0x02, 0x04, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00,
             0x05, 0x02, 0x00, 0x00, 0x00, 0x61, 0x62,
             0x01, 0x01, 0x00, 0x00, 0x00, 0x00}));
    CHECK(t);
    if (t) {
        CHECK(std::get<0>(t.value()) == 7);
        CHECK(std::get<1>(t.value()) == "ab");
        CHECK(std::get<2>(t.value()) == false);
    }

    // int32 -1 从 literal 解回，验证符号扩展
    auto i = DecodeTuple<std::int32_t>(
        Vec({0x01, 0x00, 0x00, 0x00, 0x02, 0x04, 0x00, 0x00, 0x00,
             0xFF, 0xFF, 0xFF, 0xFF}),
        std::index_sequence<0>{});
    CHECK(i);
    if (i) CHECK(std::get<0>(i.value()) == -1);

    // 空 string / 空 bytes 从零长 literal 解回
    auto s = DecodeTuple<std::string>(
        Vec({0x01, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00}),
        std::index_sequence<0>{});
    CHECK(s);
    if (s) CHECK(std::get<0>(s.value()).empty());

    auto by = DecodeTuple<std::vector<std::uint8_t>>(
        Vec({0x01, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00}),
        std::index_sequence<0>{});
    CHECK(by);
    if (by) CHECK(std::get<0>(by.value()).empty());

    // float 1.0f 从 literal 解回
    auto f = DecodeTuple<float>(
        Vec({0x01, 0x00, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x00,
             0x00, 0x00, 0x80, 0x3F}),
        std::index_sequence<0>{});
    CHECK(f);
    if (f) CHECK(std::get<0>(f.value()) == 1.0f);
}

// --- 3. DecodeAt 现有语义与错误码 --------------------------------------------

void TestDecodeAt() {
    const auto two = Vec({0x02, 0x00, 0x00, 0x00,
                          0x02, 0x04, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00,
                          0x05, 0x02, 0x00, 0x00, 0x00, 0x78, 0x79});
    auto a = DecodeAt<std::int32_t>(two, 0);
    CHECK(a);
    if (a) CHECK(a.value() == 5);
    auto b = DecodeAt<std::string>(two, 1);
    CHECK(b);
    if (b) CHECK(b.value() == "xy");

    // 序号越界：InvalidArgument（不是 ProtocolError）
    auto oor = DecodeAt<std::int32_t>(two, 2);
    CHECK(!oor);
    if (!oor) CHECK(oor.error().code == ErrorCode::InvalidArgument);

    // 空输入 / header 截断：ProtocolError
    auto empty = DecodeAt<std::int32_t>({}, 0);
    CHECK(!empty);
    if (!empty) CHECK(empty.error().code == ErrorCode::ProtocolError);
    auto short_hdr = DecodeAt<std::int32_t>(Vec({0x01, 0x00, 0x00}), 0);
    CHECK(!short_hdr);
    if (!short_hdr) CHECK(short_hdr.error().code == ErrorCode::ProtocolError);

    // count=1 但字段缺失
    auto miss = DecodeAt<std::int32_t>(Vec({0x01, 0x00, 0x00, 0x00}), 0);
    CHECK(!miss);
    if (!miss) CHECK(miss.error().code == ErrorCode::ProtocolError);

    // 字段长度声明超过剩余字节（truncated argument）
    auto trunc = DecodeAt<std::int32_t>(
        Vec({0x01, 0x00, 0x00, 0x00, 0x02, 0x04, 0x00, 0x00, 0x00, 0x05}), 0);
    CHECK(!trunc);
    if (!trunc) CHECK(trunc.error().code == ErrorCode::ProtocolError);

    // 长度字段本身截断（TakeU32 剩余 < 4）
    auto trunc_len = DecodeAt<std::int32_t>(
        Vec({0x01, 0x00, 0x00, 0x00, 0x02, 0x04, 0x00}), 0);
    CHECK(!trunc_len);
    if (!trunc_len) CHECK(trunc_len.error().code == ErrorCode::ProtocolError);

    // trailing garbage：合法单 bool 后追加一字节
    auto trail = DecodeAt<bool>(
        Vec({0x01, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00}),
        0);
    CHECK(!trail);
    if (!trail) CHECK(trail.error().code == ErrorCode::ProtocolError);

    // 类型不匹配：int32 字节流按 uint32 解 → TypeMismatch
    auto tmis = DecodeAt<std::uint32_t>(
        Vec({0x01, 0x00, 0x00, 0x00, 0x02, 0x04, 0x00, 0x00, 0x00,
             0x01, 0x00, 0x00, 0x00}),
        0);
    CHECK(!tmis);
    if (!tmis) CHECK(tmis.error().code == ErrorCode::TypeMismatch);

    // 未知 tag（7）按 int32 解 → TypeMismatch
    auto unk = DecodeAt<std::int32_t>(
        Vec({0x01, 0x00, 0x00, 0x00, 0x07, 0x01, 0x00, 0x00, 0x00, 0x2A}), 0);
    CHECK(!unk);
    if (!unk) CHECK(unk.error().code == ErrorCode::TypeMismatch);
}

// --- 4. 标量边界拒绝 ---------------------------------------------------------

void TestScalarBoundaries() {
    // bool 非法载荷（>1）
    auto badbool = DecodeAt<bool>(
        Vec({0x01, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x02}), 0);
    CHECK(!badbool);
    if (!badbool) CHECK(badbool.error().code == ErrorCode::ProtocolError);

    // bool 长度不为 1
    auto boollen = DecodeAt<bool>(
        Vec({0x01, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00}),
        0);
    CHECK(!boollen);
    if (!boollen) CHECK(boollen.error().code == ErrorCode::ProtocolError);

    // 整数长度与标量宽度不符（int32 声明 len=2）
    auto intlen = DecodeAt<std::int32_t>(
        Vec({0x01, 0x00, 0x00, 0x00, 0x02, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00}),
        0);
    CHECK(!intlen);
    if (!intlen) CHECK(intlen.error().code == ErrorCode::ProtocolError);

    // 浮点长度与标量宽度不符（double 声明 len=4）
    auto flen = DecodeAt<double>(
        Vec({0x01, 0x00, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x00,
             0x00, 0x00, 0x80, 0x3F}),
        0);
    CHECK(!flen);
    if (!flen) CHECK(flen.error().code == ErrorCode::ProtocolError);

    // 32 位长度截断显式检查：边界可放入，越 1 字节即拒绝。
    CHECK(FitsU32(static_cast<std::size_t>(kMaxFieldBytes)));
    CHECK(!FitsU32(static_cast<std::size_t>(kMaxFieldBytes) + 1));
}

// --- 5. DecodeTuple count 严格匹配 ------------------------------------------

void TestTupleCountMismatch() {
    // 单字段流按两元素 tuple 解 → count 不匹配 InvalidArgument
    auto m = DecodeTuple<std::int32_t, std::int32_t>(
        Vec({0x01, 0x00, 0x00, 0x00, 0x02, 0x04, 0x00, 0x00, 0x00,
             0x01, 0x00, 0x00, 0x00}),
        std::index_sequence<0, 1>{});
    CHECK(!m);
    if (!m) CHECK(m.error().code == ErrorCode::InvalidArgument);

    // 编码两字段流按单元素 tuple 解 → 同样拒绝
    auto two = Enc(std::tuple<std::int32_t, std::int32_t>{1, 2});
    CHECK(two);
    if (two) {
        auto one = Dec<std::int32_t>(two.value());
        CHECK(!one);
        if (!one) CHECK(one.error().code == ErrorCode::InvalidArgument);
    }
}

// --- 6. schema 常量与 ProtoCodec seam ---------------------------------------

void TestSchemaAndSeam() {
    // schema 常量：与旧 framework kCoRpcPositionalSchema 值一致
    static_assert(std::is_same_v<decltype(kRpcPositionalSchema),
                                 const std::string_view>);
    CHECK(kRpcPositionalSchema == "bbt.rpc.positional/v1");

    // 默认 trait：未特化类型 kDefined=false（infra 不认识业务 proto）
    static_assert(!is_proto_codec_v<int>);
    static_assert(!is_proto_codec_v<std::string>);

    // 基线来源 pin：字符串常量结构可核（禁止被清空/替换成非摘要）
    CHECK(std::string(kBaselineFrameworkRev).size() == 40);
    CHECK(std::string(kBaselineBlobOid).size() == 40);
    CHECK(std::string(kBaselineSourceSha256).size() == 64);
}

// framework 侧薄兼容形态：继承 infra 默认 trait 再对业务类型显式特化。
// 验证「旧用户特化不因换 namespace 失效」可经继承保留，且默认值随继承生效。
template <class T>
struct FrameworkProtoCodec : bbt::infra::rpc::ProtoCodec<T> {};

struct BizMsg {
    int value = 0;
};

template <>
struct FrameworkProtoCodec<BizMsg> {
    static constexpr bool kDefined = true;
    static result<std::vector<std::uint8_t>> Encode(const BizMsg& m) {
        return result<std::vector<std::uint8_t>>::ok(
            std::vector<std::uint8_t>{static_cast<std::uint8_t>(m.value)});
    }
    static result<BizMsg> Decode(const std::vector<std::uint8_t>& b) {
        if (b.size() != 1)
            return result<BizMsg>::err(MakeError(ErrorCode::ProtocolError,
                                                 "biz msg length"));
        BizMsg m;
        m.value = b[0];
        return result<BizMsg>::ok(m);
    }
};

void TestProtoCodecSeam() {
    // 未特化：继承到默认 kDefined=false
    static_assert(!FrameworkProtoCodec<int>::kDefined);
    // 业务特化：kDefined=true，且 Encode/Decode 可用
    static_assert(FrameworkProtoCodec<BizMsg>::kDefined);

    BizMsg in;
    in.value = 0x2A;
    auto enc = FrameworkProtoCodec<BizMsg>::Encode(in);
    CHECK(enc);
    if (enc) {
        CHECK(enc.value() == Vec({0x2A}));
        auto dec = FrameworkProtoCodec<BizMsg>::Decode(enc.value());
        CHECK(dec);
        if (dec) CHECK(dec.value().value == 0x2A);
    }

    // infra 侧默认 trait 对业务类型仍为 false（业务特化不下沉 infra）
    static_assert(!is_proto_codec_v<BizMsg>);
}

} // namespace

int main() {
    TestGoldenEncode();
    TestGoldenDecode();
    TestDecodeAt();
    TestScalarBoundaries();
    TestTupleCountMismatch();
    TestSchemaAndSeam();
    TestProtoCodecSeam();

    std::printf("rpc.payload: %d checks, %d failures\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}

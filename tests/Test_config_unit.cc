#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <cstdlib>
#include <fstream>
#include <initializer_list>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include <bbt/infra/config/FileSource.hpp>
#include <bbt/infra/config/MemorySource.hpp>
#include <bbt/infra/config/Snapshot.hpp>
#include <bbt/infra/config/Value.hpp>

using namespace bbt::infra::config;
using bbt::infra::ErrorCode;

namespace {

Value Map(std::initializer_list<std::pair<std::string, std::string>> kv) {
    Value::Map m;
    for (const auto& p : kv) m.emplace(p.first, p.second);
    return Value::FromMap(std::move(m));
}

// 临时文件统一落在 TMPDIR（进程 scratch），不放系统 /tmp。
std::string MakeTempPath(const char* name) {
    const char* t = std::getenv("TMPDIR");
    std::string dir = (t && *t) ? t : ".";
    std::string tmpl = dir + "/" + name + "_XXXXXX";
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');
    const int fd = ::mkstemp(buf.data());
    BOOST_REQUIRE(fd >= 0);
    ::close(fd);
    return std::string(buf.data());
}

void WriteFile(const std::string& path, const std::string& content) {
    std::ofstream f(path, std::ios::trunc);
    f << content;
    BOOST_REQUIRE(f.good());
}

std::string MakeTempFile(const char* name, const std::string& content) {
    const std::string path = MakeTempPath(name);
    WriteFile(path, content);
    return path;
}

BOOST_AUTO_TEST_CASE(value_typed_getters) {
    auto v = Map({{"server.host", "127.0.0.1"},
                  {"server.port", "8080"},
                  {"enabled", "true"},
                  {"ratio", "0.5"},
                  {"empty", ""}});
    BOOST_CHECK(v.Has("server.host"));
    BOOST_CHECK(!v.Has("nope"));
    BOOST_CHECK_EQUAL(v.Size(), 5u);

    auto host = v.GetString("server.host");
    BOOST_REQUIRE(host);
    BOOST_CHECK_EQUAL(host.value(), "127.0.0.1");

    auto port = v.GetInt64("server.port");
    BOOST_REQUIRE(port);
    BOOST_CHECK_EQUAL(port.value(), 8080);

    auto enabled = v.GetBool("enabled");
    BOOST_REQUIRE(enabled);
    BOOST_CHECK(enabled.value());

    auto ratio = v.GetDouble("ratio");
    BOOST_REQUIRE(ratio);
    BOOST_CHECK_CLOSE(ratio.value(), 0.5, 1e-9);

    // 缺失键 → NotFound
    auto missing = v.GetString("nope");
    BOOST_CHECK(!missing);
    BOOST_CHECK(static_cast<int>(missing.error().code) ==
                static_cast<int>(ErrorCode::NotFound));

    // 空串是「存在且为空」，与缺失区分
    auto empty = v.GetString("empty");
    BOOST_REQUIRE(empty);
    BOOST_CHECK_EQUAL(empty.value(), "");

    // 类型不匹配 → TypeMismatch
    BOOST_CHECK(!v.GetInt64("server.host"));
    BOOST_CHECK(static_cast<int>(v.GetInt64("server.host").error().code) ==
                static_cast<int>(ErrorCode::TypeMismatch));
    BOOST_CHECK(!v.GetBool("server.host"));
    BOOST_CHECK(!v.GetDouble("server.host"));

    // bool 全真/全假集合
    auto b = Map({{"a", "1"}, {"b", "0"}, {"c", "yes"}, {"d", "no"},
                  {"e", "on"}, {"f", "off"}, {"g", "TRUE"}, {"h", "False"}});
    BOOST_CHECK(b.GetBool("a").value());
    BOOST_CHECK(!b.GetBool("b").value());
    BOOST_CHECK(b.GetBool("c").value());
    BOOST_CHECK(!b.GetBool("d").value());
    BOOST_CHECK(b.GetBool("e").value());
    BOOST_CHECK(!b.GetBool("f").value());
    BOOST_CHECK(b.GetBool("g").value());
    BOOST_CHECK(!b.GetBool("h").value());
}

BOOST_AUTO_TEST_CASE(value_fingerprint_stable) {
    const auto v1 = Map({{"a", "1"}, {"b", "2"}});
    const auto v2 = Map({{"a", "1"}, {"b", "2"}});
    const auto v3 = Map({{"b", "2"}, {"a", "1"}}); // 乱序构造，映射排序后一致
    const auto v4 = Map({{"a", "1"}, {"b", "3"}});
    BOOST_CHECK_EQUAL(v1.Fingerprint(), v2.Fingerprint());
    BOOST_CHECK_EQUAL(v1.Fingerprint(), v3.Fingerprint());
    BOOST_CHECK(v1.Fingerprint() != v4.Fingerprint());
    BOOST_CHECK_EQUAL(v1.Fingerprint().size(), 16u);
}

BOOST_AUTO_TEST_CASE(memory_source_versions) {
    MemorySource src("app", "server");

    const auto s0 = src.Current();
    BOOST_CHECK_EQUAL(s0.version, 0u);
    BOOST_CHECK(s0.value.Empty());
    BOOST_CHECK_EQUAL(s0.ns, "app");
    BOOST_CHECK_EQUAL(s0.key, "server");
    BOOST_CHECK(s0.source == "memory:app/server");

    const auto s1 = src.Set(Map({{"a", "1"}}));
    BOOST_CHECK_EQUAL(s1.version, 1u);
    BOOST_CHECK(!s1.revision.empty());
    BOOST_CHECK(s1.observed_at.time_since_epoch().count() > 0);

    auto r = src.Read();
    BOOST_REQUIRE(r);
    BOOST_CHECK_EQUAL(r.value().version, 1u);

    // 相同内容 → 版本仍递增，revision 不变（内容指纹）
    const auto s2 = src.Set(Map({{"a", "1"}}));
    BOOST_CHECK_EQUAL(s2.version, 2u);
    BOOST_CHECK(s2.revision == s1.revision);

    // 不同内容 → revision 变化
    const auto s3 = src.Set(Map({{"a", "2"}}));
    BOOST_CHECK_EQUAL(s3.version, 3u);
    BOOST_CHECK(s3.revision != s2.revision);

    // 显式版本必须严格递增
    const auto bad = src.Set(Map({{"a", "3"}}), 3);
    BOOST_CHECK(!bad);
    BOOST_CHECK(static_cast<int>(bad.error().code) ==
                static_cast<int>(ErrorCode::InvalidArgument));

    const auto s4 = src.Set(Map({{"a", "4"}}), 10);
    BOOST_REQUIRE(s4);
    BOOST_CHECK_EQUAL(s4.value().version, 10u);
}

BOOST_AUTO_TEST_CASE(file_source_read_and_update) {
    const std::string path = MakeTempFile(
        "cfg_unit", "# comment\nversion = 5\nserver.host = 127.0.0.1\nserver.port = 8080\n");
    auto src = FileSource::Create(path, "app", "server");
    BOOST_REQUIRE(src);

    auto r = src.value()->Read();
    BOOST_REQUIRE(r);
    BOOST_CHECK_EQUAL(r.value().version, 5u);
    BOOST_CHECK_EQUAL(r.value().ns, "app");
    BOOST_CHECK_EQUAL(r.value().key, "server");
    BOOST_CHECK(r.value().source == "file:" + path);
    auto port = r.value().value.GetInt64("server.port");
    BOOST_REQUIRE(port);
    BOOST_CHECK_EQUAL(port.value(), 8080);

    // 更新文件 → 版本与 revision 均变
    WriteFile(path, "version = 6\nserver.port = 9090\n");
    auto r2 = src.value()->Read();
    BOOST_REQUIRE(r2);
    BOOST_CHECK_EQUAL(r2.value().version, 6u);
    BOOST_CHECK(r2.value().revision != r.value().revision);

    // 同内容重读 → SameVersionAs
    auto r3 = src.value()->Read();
    BOOST_REQUIRE(r3);
    BOOST_CHECK(r3.value().SameVersionAs(r2.value()));

    ::unlink(path.c_str());
}

BOOST_AUTO_TEST_CASE(file_source_malformed_no_pseudo_success) {
    // version 非数字
    {
        const std::string p = MakeTempFile("cfg_badver", "version = abc\nk = v\n");
        auto src = FileSource::Create(p, "app", "k");
        BOOST_REQUIRE(src);
        auto r = src.value()->Read();
        BOOST_CHECK(!r);
        BOOST_CHECK(static_cast<int>(r.error().code) ==
                    static_cast<int>(ErrorCode::ProtocolError));
        ::unlink(p.c_str());
    }
    // 重复键
    {
        const std::string p = MakeTempFile("cfg_dup", "k = 1\nk = 2\n");
        auto src = FileSource::Create(p, "app", "k");
        BOOST_REQUIRE(src);
        auto r = src.value()->Read();
        BOOST_CHECK(!r);
        BOOST_CHECK(static_cast<int>(r.error().code) ==
                    static_cast<int>(ErrorCode::ProtocolError));
        ::unlink(p.c_str());
    }
    // 无 '='
    {
        const std::string p = MakeTempFile("cfg_noeq", "novalue\n");
        auto src = FileSource::Create(p, "app", "k");
        BOOST_REQUIRE(src);
        auto r = src.value()->Read();
        BOOST_CHECK(!r);
        BOOST_CHECK(static_cast<int>(r.error().code) ==
                    static_cast<int>(ErrorCode::ProtocolError));
        ::unlink(p.c_str());
    }
    // 空键
    {
        const std::string p = MakeTempFile("cfg_emptykey", " = 1\n");
        auto src = FileSource::Create(p, "app", "k");
        BOOST_REQUIRE(src);
        BOOST_CHECK(!src.value()->Read());
        ::unlink(p.c_str());
    }
}

BOOST_AUTO_TEST_CASE(file_source_missing) {
    auto src = FileSource::Create("/nonexistent/definitely/missing.cfg", "app", "k");
    BOOST_REQUIRE(src);
    auto r = src.value()->Read();
    BOOST_CHECK(!r);
    BOOST_CHECK(static_cast<int>(r.error().code) ==
                static_cast<int>(ErrorCode::NotFound));
}

BOOST_AUTO_TEST_CASE(file_source_directory_and_empty) {
    // 目录路径：open 成功但读取失败 → InternalError，不得发布伪成功空快照
    const char* t = std::getenv("TMPDIR");
    std::string dir = (t && *t) ? t : ".";
    std::string tmpl = dir + "/cfg_dir_XXXXXX";
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');
    BOOST_REQUIRE(::mkdtemp(buf.data()) != nullptr);
    const std::string dirpath(buf.data());
    {
        auto src = FileSource::Create(dirpath, "app", "k");
        BOOST_REQUIRE(src);
        auto r = src.value()->Read();
        BOOST_CHECK(!r);
        BOOST_CHECK(static_cast<int>(r.error().code) ==
                    static_cast<int>(ErrorCode::InternalError));
    }
    ::rmdir(dirpath.c_str());

    // 空文件：合法空配置，不是读取失败
    const std::string empty = MakeTempPath("cfg_empty");
    {
        auto src = FileSource::Create(empty, "app", "k");
        BOOST_REQUIRE(src);
        auto r = src.value()->Read();
        BOOST_REQUIRE(r);
        BOOST_CHECK(r.value().value.Empty());
        BOOST_CHECK_EQUAL(r.value().version, 0u);
    }
    ::unlink(empty.c_str());
}

} // namespace

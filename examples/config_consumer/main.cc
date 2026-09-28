// Issue #35 独立消费验证（随候选保存的可重现入口，见同目录 README.md）：
// 只 include bbt/infra/config 公共头，只链接 bbt::infra_config target。
// 证明最小公共契约与模块 target 可被独立程序消费，不泄漏 framework 或第三方类型。
//
// 运行：$TMPDIR 或当前目录用于临时文件；退出码 0 表示全部检查通过。

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <unistd.h>
#include <vector>

#include <bbt/infra/config/FileSource.hpp>
#include <bbt/infra/config/MemorySource.hpp>
#include <bbt/infra/config/Snapshot.hpp>
#include <bbt/infra/config/Value.hpp>

using namespace bbt::infra::config;
using bbt::infra::ErrorCode;

namespace {

std::string MakeTempDir() {
    const char* t = std::getenv("TMPDIR");
    std::string dir = (t && *t) ? t : ".";
    std::string tmpl = dir + "/consumer_dir_XXXXXX";
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');
    if (::mkdtemp(buf.data()) == nullptr) return {};
    return std::string(buf.data());
}

std::string MakeTempFile(const char* name, const std::string& content) {
    const char* t = std::getenv("TMPDIR");
    std::string dir = (t && *t) ? t : ".";
    std::string tmpl = dir + "/" + name + "_XXXXXX";
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');
    const int fd = ::mkstemp(buf.data());
    if (fd < 0) return {};
    ::close(fd);
    std::ofstream f(buf.data(), std::ios::trunc);
    f << content;
    f.close();
    return std::string(buf.data());
}

} // namespace

int main() {
    int failures = 0;
    auto check = [&failures](bool ok, const char* what) {
        std::printf("  [%s] %s\n", ok ? "ok" : "FAIL", what);
        if (!ok) ++failures;
    };

    std::printf("== 内存源 ==\n");
    MemorySource mem("app", "server");
    const Snapshot s1 = mem.Set(Value::FromMap(Value::Map{{"server.port", "8080"}}));
    check(s1.version == 1, "version 单调递增为 1");
    check(!s1.revision.empty(), "revision 为内容指纹");
    check(s1.ns == "app" && s1.key == "server", "namespace/key 元数据");
    auto got = s1.value.GetInt64("server.port");
    check(got && got.value() == 8080, "类型化读取 GetInt64");
    check(!s1.value.GetInt64("missing"), "缺失键返回错误，不返回 0");

    std::printf("== 本地文件源 ==\n");
    const std::string path = MakeTempFile(
        "consumer_cfg", "version = 3\nserver.host = 127.0.0.1\nserver.port = 9090\n");
    check(!path.empty(), "临时文件创建");
    auto fsrc = FileSource::Create(path, "app", "server");
    check(static_cast<bool>(fsrc), "FileSource::Create");
    auto r = fsrc.value()->Read();
    check(static_cast<bool>(r), "FileSource::Read");
    if (r) {
        check(r.value().version == 3, "文件 version 字段");
        auto host = r.value().value.GetString("server.host");
        check(host && host.value() == "127.0.0.1", "文件源字符串读取");
        check(r.value().source == "file:" + path, "source 标识");
    }

    // 格式错误不发布伪成功
    std::ofstream(path, std::ios::trunc) << "version = abc\n";
    auto bad = fsrc.value()->Read();
    check(!bad, "格式错误不发布伪成功快照");
    check(static_cast<bool>(bad) == false &&
              static_cast<int>(bad.error().code) ==
                  static_cast<int>(ErrorCode::ProtocolError),
          "格式错误映射 ProtocolError");

    // 目录路径：读取失败，不得发布伪成功空快照
    const std::string dir = MakeTempDir();
    check(!dir.empty(), "临时目录创建");
    auto dsrc = FileSource::Create(dir, "app", "server");
    check(static_cast<bool>(dsrc), "FileSource::Create(目录)");
    auto dr = dsrc.value()->Read();
    check(!dr, "目录路径读取失败，不发布伪成功空快照");

    ::unlink(path.c_str());
    ::rmdir(dir.c_str());
    std::printf("\nconsumer: %s\n", failures == 0 ? "ALL OK" : "FAILURES PRESENT");
    return failures == 0 ? 0 : 1;
}

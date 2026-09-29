// Issue #35：config 首切片最小真实消费示例。
// 演示：本地文件快照读取 → 上层 schema 校验 → 接受/拒绝。
// 明确未交付：远程 provider 适配/重连、业务 schema 归属、资源热切换。

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include <bbt/infra/config/FileSource.hpp>
#include <bbt/infra/config/MemorySource.hpp>
#include <bbt/infra/config/Snapshot.hpp>
#include <bbt/infra/config/Value.hpp>

using namespace bbt::infra::config;

namespace {

std::string MakeTempPath(const char* name) {
    const char* t = std::getenv("TMPDIR");
    std::string dir = (t && *t) ? t : ".";
    std::string tmpl = dir + "/" + name + "_XXXXXX";
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');
    const int fd = ::mkstemp(buf.data());
    if (fd < 0) {
        std::fprintf(stderr, "mkstemp failed\n");
        std::exit(1);
    }
    ::close(fd);
    return std::string(buf.data());
}

void WriteFile(const std::string& path, const std::string& content) {
    std::ofstream f(path, std::ios::trunc);
    f << content;
}

// 上层 schema 校验（示例规则，非 infra 契约）：必须含 server.port 且为
// [1, 65535] 整数。真实业务 schema 归 framework。
struct Validation {
    bool accepted;
    std::string reason;
};

Validation Validate(const Snapshot& s) {
    auto port = s.value.GetInt64("server.port");
    if (!port) return {false, "server.port 缺失或非整数"};
    if (port.value() < 1 || port.value() > 65535)
        return {false, "server.port 越界"};
    return {true, ""};
}

void Run(const char* label, const Snapshot& s) {
    const Validation v = Validate(s);
    std::printf("[%s] version=%llu revision=%.16s -> %s (%s)\n", label,
                static_cast<unsigned long long>(s.version),
                s.revision.c_str(),
                v.accepted ? "ACCEPT" : "REJECT",
                v.accepted ? "port ok" : v.reason.c_str());
}

} // namespace

int main() {
    // 1. 本地文件源：有效配置 → 读取 → 校验 → 接受
    const std::string path = MakeTempPath("cfg_example");
    WriteFile(path, R"(version = 1
server.port = 8080
)");
    auto fsrc = FileSource::Create(path, "app", "server");
    if (!fsrc) {
        std::fprintf(stderr, "create source failed: %s\n",
                     fsrc.error().message.c_str());
        return 1;
    }
    auto r = fsrc.value()->Read();
    if (!r) {
        std::fprintf(stderr, "read failed: %s\n", r.error().message.c_str());
        return 1;
    }
    Run("valid", r.value());

    // 2. 覆盖为越界配置 → 读取成功，但上层校验拒绝
    WriteFile(path, R"(version = 2
server.port = 99999
)");
    auto r2 = fsrc.value()->Read();
    if (!r2) return 1;
    Run("invalid", r2.value());

    // 3. 覆盖为格式错误 → 读取失败，不发布伪成功快照
    WriteFile(path, "version = abc\n");
    auto r3 = fsrc.value()->Read();
    if (r3) {
        std::fprintf(stderr, "expected read failure\n");
        return 1;
    }
    std::printf("[malformed] read failed (code=%d): %s\n",
                static_cast<int>(r3.error().code), r3.error().message.c_str());

    // 4. 内存源：同一版本化快照语义（version 单调 + revision 内容指纹）
    MemorySource mem("app", "server");
    mem.Set(Value::FromMap(Value::Map{{"server.port", "9090"}}));
    Run("memory", mem.Current());

    ::unlink(path.c_str());
    std::printf("\n未交付（framework 或后续切片承担）：\n");
    std::printf("  - 远程 provider 适配与真实重连（Issue #35 仅本地源）\n");
    std::printf("  - 业务 schema 校验（本示例规则仅为演示，属上层）\n");
    std::printf("  - Redis/Mongo 客户端资源热切换（framework Resource Registry）\n");
    return 0;
}

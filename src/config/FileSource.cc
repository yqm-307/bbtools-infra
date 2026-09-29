#include <bbt/infra/config/FileSource.hpp>

#include <charconv>
#include <chrono>
#include <fstream>
#include <sstream>
#include <string_view>
#include <system_error>
#include <utility>

namespace bbt::infra::config {
namespace {

std::string_view Trim(std::string_view s) noexcept {
    std::size_t b = 0;
    std::size_t e = s.size();
    while (b < e && (s[b] == ' ' || s[b] == '\t' || s[b] == '\r')) ++b;
    while (e > b && (s[e - 1] == ' ' || s[e - 1] == '\t' || s[e - 1] == '\r')) --e;
    return s.substr(b, e - b);
}

struct ParseResult {
    bool ok{true};
    Error error;
    std::uint64_t version{0};
    Value value;
};

ParseResult Parse(std::string_view content) {
    ParseResult out;
    Value::Map entries;
    std::istringstream iss{std::string(content)};
    std::string line;
    std::size_t lineno = 0;
    while (std::getline(iss, line)) {
        ++lineno;
        const std::string_view trimmed = Trim(line);
        if (trimmed.empty() || trimmed.front() == '#') continue;
        const std::size_t eq = trimmed.find('=');
        if (eq == std::string_view::npos) {
            out.ok = false;
            out.error = MakeError(ErrorCode::ProtocolError,
                "config file parse: line " + std::to_string(lineno) + " has no '='");
            return out;
        }
        std::string key(Trim(trimmed.substr(0, eq)));
        std::string value(Trim(trimmed.substr(eq + 1)));
        if (key.empty()) {
            out.ok = false;
            out.error = MakeError(ErrorCode::ProtocolError,
                "config file parse: empty key at line " + std::to_string(lineno));
            return out;
        }
        if (key == "version") {
            std::uint64_t v = 0;
            const char* begin = value.data();
            const char* end = begin + value.size();
            const auto [ptr, ec] = std::from_chars(begin, end, v);
            if (ec != std::errc() || ptr != end) {
                out.ok = false;
                out.error = MakeError(ErrorCode::ProtocolError,
                    "config file parse: version not a uint64 at line " +
                        std::to_string(lineno));
                return out;
            }
            out.version = v;
            continue;
        }
        if (entries.find(key) != entries.end()) {
            out.ok = false;
            out.error = MakeError(ErrorCode::ProtocolError,
                "config file parse: duplicate key '" + key + "' at line " +
                    std::to_string(lineno));
            return out;
        }
        entries.emplace(std::move(key), std::move(value));
    }
    out.value = Value::FromMap(std::move(entries));
    return out;
}

} // namespace

FileSource::FileSource(std::string path, std::string ns, std::string key)
    : m_path(std::move(path)), m_ns(std::move(ns)), m_key(std::move(key)),
      m_source("file:" + m_path) {}

result<FileSource::SPtr> FileSource::Create(std::string path, std::string ns,
                                            std::string key) {
    if (path.empty())
        return result<SPtr>::err(MakeError(ErrorCode::InvalidArgument,
            "config file source: empty path"));
    if (ns.empty() || key.empty())
        return result<SPtr>::err(MakeError(ErrorCode::InvalidArgument,
            "config file source: ns/key must not be empty"));
    return result<SPtr>::ok(
        SPtr(new FileSource(std::move(path), std::move(ns), std::move(key))));
}

result<Snapshot> FileSource::Read() {
    std::ifstream in(m_path, std::ios::binary);
    if (!in.is_open())
        return result<Snapshot>::err(MakeError(ErrorCode::NotFound,
            "config file source: cannot open " + m_path));
    // 目录路径 open 成功但非普通文件：peek() 使其进入读取路径，目录读取置 bad；
    // 空文件则只置 eof、bad 保持 0——两者必须区分（目录不得发布伪成功空快照）。
    in.peek();
    std::ostringstream ss;
    ss << in.rdbuf();
    if (in.bad())
        return result<Snapshot>::err(MakeError(ErrorCode::InternalError,
            "config file source: read failed " + m_path));

    ParseResult parsed = Parse(ss.str());
    if (!parsed.ok)
        return result<Snapshot>::err(std::move(parsed.error));

    Snapshot snapshot;
    snapshot.ns = m_ns;
    snapshot.key = m_key;
    snapshot.version = parsed.version;
    snapshot.revision = parsed.value.Fingerprint();
    snapshot.observed_at = std::chrono::system_clock::now();
    snapshot.source = m_source;
    snapshot.value = std::move(parsed.value);
    return result<Snapshot>::ok(std::move(snapshot));
}

std::string FileSource::SourceId() const noexcept { return m_source; }

const std::string& FileSource::Path() const noexcept { return m_path; }

} // namespace bbt::infra::config

#include <bbt/infra/config/MemorySource.hpp>

#include <chrono>
#include <utility>

namespace bbt::infra::config {

MemorySource::MemorySource(std::string ns, std::string key)
    : m_ns(std::move(ns)), m_key(std::move(key)),
      m_source("memory:" + m_ns + "/" + m_key) {
    m_current.ns = m_ns;
    m_current.key = m_key;
    m_current.source = m_source;
    m_current.observed_at = std::chrono::system_clock::now();
}

Snapshot MemorySource::Set(Value value) {
    std::lock_guard<std::mutex> lock(m_mtx);
    Snapshot next = m_current;
    next.version = m_current.version + 1;
    next.revision = value.Fingerprint();
    next.observed_at = std::chrono::system_clock::now();
    next.value = std::move(value);
    m_current = next;
    return next;
}

result<Snapshot> MemorySource::Set(Value value, std::uint64_t version) {
    std::lock_guard<std::mutex> lock(m_mtx);
    if (version <= m_current.version)
        return result<Snapshot>::err(MakeError(ErrorCode::InvalidArgument,
            "config memory source: version must be strictly increasing"));
    Snapshot next = m_current;
    next.version = version;
    next.revision = value.Fingerprint();
    next.observed_at = std::chrono::system_clock::now();
    next.value = std::move(value);
    m_current = next;
    return result<Snapshot>::ok(next);
}

result<Snapshot> MemorySource::Read() {
    std::lock_guard<std::mutex> lock(m_mtx);
    return result<Snapshot>::ok(m_current);
}

std::string MemorySource::SourceId() const noexcept { return m_source; }

Snapshot MemorySource::Current() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_current;
}

} // namespace bbt::infra::config

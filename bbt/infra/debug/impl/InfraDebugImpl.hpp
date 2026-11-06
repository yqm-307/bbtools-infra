#pragma once
// 父头末尾包含的实现；依赖父头已完成类型与声明。

#ifdef BBT_INFRA_STRINGENT_DEBUG

namespace bbt::infra::debug {

inline void MongoRuntimeObservation::Record(MongoThreadEvidence evidence)
{
    std::lock_guard<std::mutex> lk(evidence_mtx);
    last_evidence = std::move(evidence);
}

inline std::optional<MongoThreadEvidence> MongoRuntimeObservation::LastEvidence() const
{
    std::lock_guard<std::mutex> lk(evidence_mtx);
    return last_evidence;
}

} // namespace bbt::infra::debug

#endif // BBT_INFRA_STRINGENT_DEBUG

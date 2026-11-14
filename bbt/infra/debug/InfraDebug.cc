#include <bbt/infra/debug/InfraDebug.hpp>

#ifdef BBT_INFRA_STRINGENT_DEBUG

namespace bbt::infra::debug {

void MongoRuntimeObservation::Record(MongoThreadEvidence evidence)
{
    std::lock_guard<std::mutex> lk(evidence_mtx);
    last_evidence = std::move(evidence);
}

std::optional<MongoThreadEvidence> MongoRuntimeObservation::LastEvidence() const
{
    std::lock_guard<std::mutex> lk(evidence_mtx);
    return last_evidence;
}

// clang-format off
}
// clang-format on

#endif

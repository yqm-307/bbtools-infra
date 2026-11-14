#include <bbt/infra/config/Snapshot.hpp>

namespace bbt::infra::config {

std::string Snapshot::FullKey() const
{
    return ns + "/" + key;
}

// clang-format off
}
// clang-format on

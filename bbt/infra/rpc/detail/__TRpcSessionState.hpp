#pragma once

namespace bbt::infra::rpc {

template <class Table, class Pending>
bool ErasePending(Table& table, const std::string& key, const std::shared_ptr<Pending>& operation)
{
    const auto it = table.find(key);
    if (it != table.end() && it->second == operation) {
        table.erase(it);
        return true;
    }
    return false;
}

// clang-format off
}
// clang-format on
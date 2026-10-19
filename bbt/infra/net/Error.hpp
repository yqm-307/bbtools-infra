#pragma once

// net 自有错误码（公共值类型，不传播第三方/系统类别）：区分 EOF、WouldBlock、
// Busy、写结算与显式拒绝，避免把 Busy/EOF 伪装成系统 errno。

#include <string>
#include <system_error>

namespace bbt::infra::net {

enum class NetErrc {
    Eof = 1,
    WouldBlock,
    Busy,
    WriteSettling,
    Rejected,
    Closed,
};

inline const std::error_category& NetErrorCategory() {
    static const struct Category final : std::error_category {
        const char* name() const noexcept override { return "bbt.net"; }
        std::string message(int value) const override {
            switch (static_cast<NetErrc>(value)) {
                case NetErrc::Eof: return "eof";
                case NetErrc::WouldBlock: return "would block";
                case NetErrc::Busy: return "ring busy";
                case NetErrc::WriteSettling: return "write settling";
                case NetErrc::Rejected: return "rejected (not submitted)";
                case NetErrc::Closed: return "closed";
            }
            return "unknown net error";
        }
    } category;
    return category;
}

inline std::error_code make_error_code(NetErrc value) noexcept {
    return {static_cast<int>(value), NetErrorCategory()};
}

} // namespace bbt::infra::net

namespace std {
template <>
struct is_error_code_enum<bbt::infra::net::NetErrc> : true_type {};
} // namespace std

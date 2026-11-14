#include <bbt/infra/net/Error.hpp>

namespace bbt::infra::net {

const std::error_category& NetErrorCategory()
{
    static const struct Category final : std::error_category {
        const char* name() const noexcept override { return "bbt.net"; }

        std::string message(int value) const override
        {
            switch (static_cast<NetErrc>(value)) {
            case NetErrc::Eof:
                return "eof";
            case NetErrc::WouldBlock:
                return "would block";
            case NetErrc::Busy:
                return "ring busy";
            case NetErrc::WriteSettling:
                return "write settling";
            case NetErrc::Rejected:
                return "rejected (not submitted)";
            case NetErrc::Closed:
                return "closed";
            }
            return "unknown net error";
        }
    } category;

    return category;
}

std::error_code make_error_code(NetErrc value) noexcept
{
    return {static_cast<int>(value), NetErrorCategory()};
}

// clang-format off
}
// clang-format on

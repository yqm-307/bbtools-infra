#pragma once

namespace bbt::infra::mcp {

template <typename Complete> inline HandlerResult<Complete> HandlerResult<Complete>::Completed(Complete value)
{
    HandlerResult out;
    out.m_value = std::move(value);
    return out;
}

template <typename Complete> inline HandlerResult<Complete> HandlerResult<Complete>::Deferred(InputRequired required)
{
    HandlerResult out;
    out.m_value = std::move(required);
    return out;
}

template <typename Complete> inline bool HandlerResult<Complete>::IsCompleted() const noexcept
{
    return m_value.index() == 0;
}

template <typename Complete> inline bool HandlerResult<Complete>::IsInputRequired() const noexcept
{
    return m_value.index() == 1;
}

template <typename Complete> inline const Complete& HandlerResult<Complete>::Value() const
{
    return std::get<0>(m_value);
}

template <typename Complete> inline const InputRequired& HandlerResult<Complete>::Required() const
{
    return std::get<1>(m_value);
}

// clang-format off
}
// clang-format on

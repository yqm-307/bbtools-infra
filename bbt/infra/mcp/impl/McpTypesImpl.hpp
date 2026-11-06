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

} // namespace bbt::infra::mcp

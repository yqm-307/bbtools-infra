#pragma once

namespace bbt::infra {

template <class T> inline result<T> result<T>::ok(T value)
{
    return result(OkTag{}, std::move(value));
}

template <class T> inline result<T> result<T>::err(Error error)
{
    return result(ErrTag{}, std::move(error));
}

template <class T> inline T& result<T>::value() &
{
    RequireOk();
    return std::get<0>(m_data);
}

template <class T> inline const T& result<T>::value() const&
{
    RequireOk();
    return std::get<0>(m_data);
}

template <class T> inline T&& result<T>::value() &&
{
    RequireOk();
    return std::move(std::get<0>(m_data));
}

template <class T> inline Error& result<T>::error() &
{
    RequireErr();
    return std::get<1>(m_data);
}

template <class T> inline const Error& result<T>::error() const&
{
    RequireErr();
    return std::get<1>(m_data);
}

template <class T> inline result<T>::result(OkTag, T value) : m_data(std::in_place_index<0>, std::move(value))
{
}

template <class T> inline result<T>::result(ErrTag, Error error) : m_data(std::in_place_index<1>, std::move(error))
{
}

template <class T> inline void result<T>::RequireOk() const
{
    if (m_data.index() != 0)
        throw std::logic_error("bbt::infra::result: value() on error result");
}

template <class T> inline void result<T>::RequireErr() const
{
    if (m_data.index() != 1)
        throw std::logic_error("bbt::infra::result: error() on ok result");
}

} // namespace bbt::infra

#include <bbt/infra/net/CoObject.hpp>

#include <utility>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/object/CoObject.hpp>

namespace bbt::infra::net {

CoEnvironment ICoObject::GetEnvironment() const {
    return {bbt::coroutine::GetLocalCoroutineId()};
}

std::error_code ICoObject::RequireCoroutine() const {
    return GetEnvironment().CanSuspend() ? std::error_code{} : std::make_error_code(std::errc::operation_not_permitted);
}

CoObject::CoObject(std::string kind, std::string name)
    : m_info(bbt::coroutine::CreateObjectInfo(std::move(kind), std::move(name))) {}

bbt::coroutine::CoObjectInfo CoObject::GetObjectInfo() const { return m_info; }

} // namespace bbt::infra::net

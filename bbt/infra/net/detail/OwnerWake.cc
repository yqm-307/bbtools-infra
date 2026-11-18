#include <bbt/infra/net/detail/OwnerWake.hpp>

#include <stdexcept>
#include <utility>
#include <bbt/infra/net/detail/Reactor.hpp>

namespace bbt::infra::net {

void OwnerWake::_Attach(std::weak_ptr<Reactor> reactor, std::weak_ptr<ConnCore> core)
{
    if (m_attached) {
        throw std::logic_error("owner wake already attached");
    }
    m_reactor = std::move(reactor);
    m_core = std::move(core);
    m_attached = true;
}

void OwnerWake::Wake() const
{
    if (const auto reactor = m_reactor.lock()) {
        reactor->MarkDirty(m_core);
    }
}

// clang-format off
}
// clang-format on

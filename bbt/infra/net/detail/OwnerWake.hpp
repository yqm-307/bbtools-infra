#pragma once

#include <memory>

namespace bbt::infra::net {

class ConnectionState;
class ConnCore;
class Reactor;

/**
 * 连接发布前绑定一次。残留的连接句柄只借弱引用请求 owner 推进。
 */
class OwnerWake {
public:
    void Wake() const;

    bool Attached() const noexcept { return m_attached; }

private:
    friend class ConnectionState;
    void _Attach(std::weak_ptr<Reactor> reactor, std::weak_ptr<ConnCore> core);

    std::weak_ptr<Reactor> m_reactor;
    std::weak_ptr<ConnCore> m_core;
    bool m_attached{false};
};

// clang-format off
}
// clang-format on

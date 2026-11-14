#pragma once

/**
 * 成员实现由声明头末尾包含，保留调用点可见性与 inline。
 */
namespace bbt::infra::net {

template <class... Args> inline std::shared_ptr<Engine> Engine::Create(Args&&... args)
{
    return bbt::coroutine::MakeCoObject<Engine>(
        "infra.net.Engine", "", [&] { return std::shared_ptr<Engine>(new Engine(std::forward<Args>(args)...)); });
}

// clang-format off
}
// clang-format on

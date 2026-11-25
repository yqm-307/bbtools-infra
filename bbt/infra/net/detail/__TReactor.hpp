#pragma once

#include <utility>
#include <boost/asio/post.hpp>

namespace bbt::infra::net {

template <class Callback> void Reactor::SafeHandler(Callback&& callback)
{
    try {
        callback();
    } catch (...) {
        DetailNoteHandlerError(m_impl);
    }
}

template <class Handler> bool Reactor::TryPostInternal(Handler&& handler)
{
    /**
     * 提交资格与 driver 租约同一窄门：持锁跨 post，ReleaseDriver 只能在
     * post 完成后 reset，已接纳命令必进入封口前的 context 队列。
     */
    std::lock_guard<std::mutex> lock(m_driver_mutex);
    if (!m_driver) {
        return false;
    }
    try {
        asio::post(m_driver->Context(), std::forward<Handler>(handler));
        return true;
    } catch (...) {
        return false;
    }
}

template <class Handler> bool Reactor::TryPost(Handler&& handler)
{
    /**
     * 提交门：持 driver 租约跨 sealed 检查与 post，使「资格→入队」与
     * ReleaseDriver 的 reset 线性化。hold 只覆盖一次短 post（Asio do_run_one
     * 在 complete 前解锁，不构成 scheduler 反序死锁），
     * 不会跨同步解绑：ReleaseDriver 等该锁，post 返回即释放。
     */
    std::lock_guard<std::mutex> lock(m_driver_mutex);
    if (m_sealed.load() || m_impl->IsSealed() || !m_driver) {
        /**
         * 封口后/已解绑：零接纳
         */
        return false;
    }
    InvokeHook(HookPoint::BeforeDriverPost);

    try {
        asio::post(m_driver->Context(), std::forward<Handler>(handler));
        return true;
    } catch (...) {
        return false;
    }
}

// clang-format off
}
// clang-format on
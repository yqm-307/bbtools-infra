#pragma once

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/ICoNetwork.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra::tcp {

class CoTCPListener;

class CoTCP final : public ICoNetwork, public ICoCloseable,
                    public std::enable_shared_from_this<CoTCP> {
public:
    using SPtr = std::shared_ptr<CoTCP>;

    static result<SPtr> DialTCP(std::string host, std::uint16_t port,
                                const CallOptions& options);

    ~CoTCP() override;

    bbt::coroutine::CoObjectInfo GetObjectInfo() const override;

    CoTCP(const CoTCP&) = delete;
    CoTCP& operator=(const CoTCP&) = delete;

    IoResult TryReadSome(MutableBytes dst);
    IoResult TryWriteSome(ConstBytes src);
    IoResult ReadSome(MutableBytes dst, const CallOptions& options);
    IoResult WriteSome(ConstBytes src, const CallOptions& options);
    IoResult WriteAll(ConstBytes src, const CallOptions& options);

    // Compatibility with the first transport slice; prefer IoResult overloads
    // when EOF must be distinguished from a zero-length successful read.
    result<std::size_t> ReadSome(void* buffer, std::size_t size,
                                 const CallOptions& options);
    result<std::size_t> WriteSome(const void* buffer, std::size_t size,
                                  const CallOptions& options);
    result<std::size_t> WriteAll(const void* buffer, std::size_t size,
                                 const CallOptions& options);

    void RequestClose() noexcept override;
    bool IsClosed() const noexcept override;
    CloseStatus WaitClosed(bbt::coroutine::Deadline deadline,
                           bbt::coroutine::CancellationToken cancel) override;

    // Runtime 托管登记（co-io-adapter/v1 §4.0.1.2）：工厂在交付前注册，
    // 物理关闭落定（m_closed_source 触发处）时通知 Runtime 释放容量。
    void SetClosedHook(std::function<void()> hook) noexcept {
        m_closed_hook = std::move(hook);
    }

private:
    friend class CoTCPListener;
    explicit CoTCP(int fd) noexcept;

    result<std::size_t> _ReadSome(void* buffer, std::size_t size,
                                  const CallOptions& options);
    result<std::size_t> _WriteSome(const void* buffer, std::size_t size,
                                   const CallOptions& options);
    result<void> _Wait(bool readable, const CallOptions& options);
    void _CloseFd() noexcept; // m_mtx held
    void _EndIo() noexcept;

    int m_fd{-1};
    bool m_closed{false};
    bool m_closing{false};
    int m_inflight{0};
    mutable std::mutex m_mtx;
    std::atomic_bool m_close_waiting{false};
    std::shared_ptr<bbt::coroutine::CancellationSource> m_close_source{
        std::make_shared<bbt::coroutine::CancellationSource>()};
    std::shared_ptr<bbt::coroutine::CancellationSource> m_closed_source{
        std::make_shared<bbt::coroutine::CancellationSource>()};
    bbt::coroutine::CoObjectInfo m_info;
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> m_waiter;
    std::function<void()> m_closed_hook;   // 物理关闭落定通知（Runtime 托管记账）
};

class CoTCPListener final : public ICoNetwork, public ICoCloseable,
                            public std::enable_shared_from_this<CoTCPListener> {
public:
    using SPtr = std::shared_ptr<CoTCPListener>;

    static result<SPtr> ListenTCP(std::string host, std::uint16_t port,
                                  int backlog = 128);

    ~CoTCPListener() override;

    bbt::coroutine::CoObjectInfo GetObjectInfo() const override;

    CoTCPListener(const CoTCPListener&) = delete;
    CoTCPListener& operator=(const CoTCPListener&) = delete;

    result<std::shared_ptr<CoTCP>> Accept(const CallOptions& options);
    SocketAddress LocalAddress() const;
    void SetAcceptHooks(std::function<bool()> admit,
                        std::function<void()> release,
                        std::function<bool(std::shared_ptr<CoTCP>)> adopt) noexcept {
        m_accept_admit = std::move(admit);
        m_accept_release = std::move(release);
        m_accept_adopt = std::move(adopt);
    }

    // Runtime 托管登记（co-io-adapter/v1 §4.0.1.2）：工厂在交付前注册，
    // 物理关闭落定时通知 Runtime 释放容量。
    void SetClosedHook(std::function<void()> hook) noexcept {
        m_closed_hook = std::move(hook);
    }

    void RequestClose() noexcept override;
    bool IsClosed() const noexcept override;
    CloseStatus WaitClosed(bbt::coroutine::Deadline deadline,
                           bbt::coroutine::CancellationToken cancel) override;

private:
    explicit CoTCPListener(int fd) noexcept;
    void _CloseFd() noexcept; // m_mtx held
    void _EndIo() noexcept;

    int m_fd{-1};
    bool m_closed{false};
    bool m_closing{false};
    int m_inflight{0};
    mutable std::mutex m_mtx;
    std::atomic_bool m_close_waiting{false};
    std::shared_ptr<bbt::coroutine::CancellationSource> m_close_source{
        std::make_shared<bbt::coroutine::CancellationSource>()};
    std::shared_ptr<bbt::coroutine::CancellationSource> m_closed_source{
        std::make_shared<bbt::coroutine::CancellationSource>()};
    bbt::coroutine::CoObjectInfo m_info;
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> m_waiter;
    std::function<void()> m_closed_hook;   // 物理关闭落定通知（Runtime 托管记账）
    std::function<bool()> m_accept_admit;
    std::function<void()> m_accept_release;
    std::function<bool(std::shared_ptr<CoTCP>)> m_accept_adopt;
};

} // namespace bbt::infra::tcp

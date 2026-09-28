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

// Issue #32 F2：受管 DialTCP 的等待段接缝（仅 NetworkRuntime 托管路径
// 装配；未托管静态入口以默认构造 DialWaitOptions 调用，零语义变化）。
//   extra_cancel           —— Runtime 级关闭源合成的取消令牌：runtime
//                            关闭时唤醒挂起在 DNS/connect 等待的 dial
//                            协程走正常归还路径（先于硬销毁兜底）。
//   dns_on_registered      —— 非空时，DNS 等待段在协程真正挂起（await
//                            event 已注册入 parked 表）后回调一次。
//   connect_on_registered  —— 非空时，connect 等待段（EINPROGRESS 的
//                            fd 可写等待）在协程挂起后回调一次。
// 约定：回调 noexcept、不得取锁/阻塞——其在 scheduler 恢复路径上执行，
// 由调用方保证只用于测试同步或登记簿落账。
struct DialWaitOptions {
    bbt::coroutine::CancellationToken extra_cancel{};
    std::function<void()>             dns_on_registered{};
    std::function<void()>             connect_on_registered{};
};

// Issue #32 F2：受管 DialTCP 等待段的「未完成 dial」归还凭据（堆上持有，
// 不依赖协程栈展开）。等待段名额在 Runtime 账本入账后由本凭据担保归还：
//   - 协程正常返回（成功/失败/取消/超时）：调用方 OnSuccess 后统一归还，
//     凭据失效；
//   - runtime->RequestClose()：Runtime 经 extra_cancel 唤醒挂起的 dial
//     协程走正常返回路径归还；
//   - Scheduler::Stop 硬销毁挂起协程（不展开栈）：凭据析构 Fail 兜底归还。
// 两路径经 released 原子标志互斥，不重复归还。
struct DialWaitPermit {
    std::function<void()> release;
    std::atomic_bool      released{false};

    explicit DialWaitPermit(std::function<void()> r) : release(std::move(r)) {}
    ~DialWaitPermit() { Fail(); }
    void Fail() noexcept {
        if (release && !released.exchange(true))
            release();
    }
    void OnSuccess() noexcept { released.store(true); }
};

class CoTCP final : public ICoNetwork, public ICoCloseable,
                    public std::enable_shared_from_this<CoTCP> {
public:
    using SPtr = std::shared_ptr<CoTCP>;

    static result<SPtr> DialTCP(std::string host, std::uint16_t port,
                                const CallOptions& options,
                                DialWaitOptions dial_wait = {});

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

    // §4.0.1.7：Runtime 级在途配额注入（仅受管对象；未托管入口不装钩，
    // 未托管入口亦无限额语义）。admit 在 _CheckEntry（参数/协程上下文/
    // 代际）通过、计入 m_inflight 之前调用——false 即 Overloaded，立即
    // 返回不挂起；admit 通过的操作在 m_mtx 内登记 m_quota_held++，op
    // 结束归还 1，RequestClose/析构经 DrainQuota 归还剩余——Stop 不
    // 展开挂起栈时由对象侧兜底归还，不依赖协程栈 RAII。quota_scope
    // 强持有 Runtime 共享账本，保证归还发生时账本仍存活。
    void SetInflightHooks(std::function<bool()> admit,
                          std::function<void()> release,
                          std::shared_ptr<void> quota_scope) noexcept {
        m_inflight_admit = std::move(admit);
        m_inflight_release = std::move(release);
        m_inflight_scope = std::move(quota_scope);
    }

    // 测试接缝（Issue #32）：可等待 op 在参数/上下文/代际检查与名额登记
    // （m_quota_held++）完成后、进入首次等待循环之前在协程内触发一次；
    // 生产路径不安装，为空零开销。约定：noexcept、不得回调本对象/取 m_mtx/
    // 阻塞——持锁线程与同 scheduler 上其他协程依赖它快速返回。
    void SetWaitEntryGateForTest(std::function<void()> gate) noexcept {
        m_wait_entry_gate_for_test = std::move(gate);
    }

    // 测试接缝（Issue #32 F3）：ReadSome/WriteSome 数据路径每次进入
    // fd 等待（_WaitFd 内 CoWaiter::Wait 的 on_registered，即协程真正
    // 挂起、事件已登记）后回调一次——与 wait-entry gate（等待循环入口）
    // 区分，本接缝证明「op 已真实挂起在 fd 等待」。生产路径不安装。
    // 约定同 SetWaitEntryGateForTest：noexcept、不取锁、不阻塞。
    void SetIoWaitRegisteredGateForTest(std::function<void()> gate) noexcept {
        m_io_wait_registered_gate_for_test = std::move(gate);
    }

    // 测试接缝（Issue #32 F3）：返回底层 fd 供测试调整 socket 选项
    // （例如缩小 SO_SNDBUF 以确定性触发 EAGAIN/suspended write）。不持有
    // m_mtx，返回的是对象生命周期内的原始 fd；测试须只在连接建立后、
    // 关闭前使用，不得关闭/改向。生产路径不调用。
    int NativeFdForTest() const noexcept { return m_fd; }

private:
    friend class CoTCPListener;
    explicit CoTCP(int fd) noexcept;

    result<void> _CheckEntry(bool in_coroutine_only) const;

    result<std::size_t> _ReadSome(void* buffer, std::size_t size,
                                  const CallOptions& options);
    result<std::size_t> _WriteSome(const void* buffer, std::size_t size,
                                   const CallOptions& options);
    result<void> _Wait(bool readable, const CallOptions& options);
    void _CloseFd() noexcept; // m_mtx held
    void _EndIo() noexcept;
    // §4.0.1.7：归还本对象仍持有的在途名额（m_mtx held；幂等）。
    // op 正常结束经 _EndIo 归还 1；Stop 致挂起协程不展开栈时，由
    // RequestClose/析构的 DrainQuota 兜底归还剩余名额。
    void _DrainQuotaLocked() noexcept {
        if (m_inflight_release && m_quota_held > 0) {
            const auto n = m_quota_held;
            m_quota_held = 0;
            for (std::size_t i = 0; i < n; ++i)
                m_inflight_release();
        }
    }

    int m_fd{-1};
    bool m_closed{false};
    bool m_closing{false};
    int m_inflight{0};
    // §4.0.1.7：本对象已 admit 未归还的 Runtime 在途名额（m_mtx 保护）。
    std::size_t m_quota_held{0};
    mutable std::mutex m_mtx;
    std::atomic_bool m_close_waiting{false};
    std::shared_ptr<bbt::coroutine::CancellationSource> m_close_source{
        std::make_shared<bbt::coroutine::CancellationSource>()};
    std::shared_ptr<bbt::coroutine::CancellationSource> m_closed_source{
        std::make_shared<bbt::coroutine::CancellationSource>()};
    bbt::coroutine::CoObjectInfo m_info;
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> m_waiter;
    std::function<void()> m_closed_hook;   // 物理关闭落定通知（Runtime 托管记账）
    // §4.0.1.7 在途配额（仅受管对象注入）：admit/release 经 shared_ptr
    // 账本与 Runtime 配对；scope 保活账本，Stop 不展开栈时由对象侧
    // teardown/析构归还，不依赖协程栈 RAII。
    std::function<bool()> m_inflight_admit;
    std::function<void()> m_inflight_release;
    std::shared_ptr<void> m_inflight_scope;
    // §4.0.1.7 测试接缝：见 SetWaitEntryGateForTest。
    std::function<void()> m_wait_entry_gate_for_test;
    // §4.0.1.7 F3 测试接缝：见 SetIoWaitRegisteredGateForTest。挂起落定
    // 后回调（_WaitFd on_registered），证明 op 真实挂在 fd 等待上。
    std::function<void()> m_io_wait_registered_gate_for_test;
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

    // §4.0.1.7：同 CoTCP::SetInflightHooks——Accept 的 admit 先于既有
    // m_accept_admit（连接容量），在挂起等待前判定 Overloaded。
    void SetInflightHooks(std::function<bool()> admit,
                          std::function<void()> release,
                          std::shared_ptr<void> quota_scope) noexcept {
        m_inflight_admit = std::move(admit);
        m_inflight_release = std::move(release);
        m_inflight_scope = std::move(quota_scope);
    }

    // 测试接缝（Issue #32）：同 CoTCP::SetWaitEntryGateForTest——Accept
    // 在名额登记完成、首次 accept4/等待前于协程内触发一次。
    void SetWaitEntryGateForTest(std::function<void()> gate) noexcept {
        m_wait_entry_gate_for_test = std::move(gate);
    }

    result<void> _CheckEntry() const;

    void RequestClose() noexcept override;
    bool IsClosed() const noexcept override;
    CloseStatus WaitClosed(bbt::coroutine::Deadline deadline,
                           bbt::coroutine::CancellationToken cancel) override;

private:
    explicit CoTCPListener(int fd) noexcept;
    void _CloseFd() noexcept; // m_mtx held
    void _EndIo() noexcept;
    // §4.0.1.7：归还本 listener 仍持有的在途名额（m_mtx held；幂等）。
    // op 结束归还 1；Stop 致挂起协程不展开栈时由 RequestClose/析构兜底。
    void _DrainQuotaLocked() noexcept {
        if (m_inflight_release && m_quota_held > 0) {
            const auto n = m_quota_held;
            m_quota_held = 0;
            for (std::size_t i = 0; i < n; ++i)
                m_inflight_release();
        }
    }

    int m_fd{-1};
    bool m_closed{false};
    bool m_closing{false};
    int m_inflight{0};
    // §4.0.1.7：已 admit 未归还的 Runtime 在途名额（m_mtx 保护）。
    std::size_t m_quota_held{0};
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
    // §4.0.1.7 在途配额（语义同 CoTCP 同名成员）：Accept 的 admit 先于
    // m_accept_admit 判定，未托管 listener 不装钩（Unlimited 语义）。
    std::function<bool()> m_inflight_admit;
    std::function<void()> m_inflight_release;
    std::shared_ptr<void> m_inflight_scope;
    // §4.0.1.7 测试接缝：见 SetWaitEntryGateForTest。
    std::function<void()> m_wait_entry_gate_for_test;
};

} // namespace bbt::infra::tcp

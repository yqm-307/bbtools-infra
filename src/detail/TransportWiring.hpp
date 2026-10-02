#pragma once
// src 内部装配面（owner 装配 + 测试接缝）。
//
// 为什么在这里：本文件里的入口只有两类消费者——src 内的装配者
// （src/transport 与协议 owner src/http）与同仓测试；协议消费者（后续
// binding、协议 Cli）永不调用它们。把它们放在公共头等于把 owner 实现细节
// 与 ABI（新增 std::function 成员、虚方法槽位）钉进对外契约，而
// co-io-adapter/v1 §4/§5 的冻结面并不含任何 Set*Hooks 或测试接缝。
//
// 约束：
//   - 本头不安装、不进 INSTALL_INTERFACE；只由 src/ 内 target 与 tests
//     经 PRIVATE include（${CMAKE_CURRENT_SOURCE_DIR}）包含。
//   - 行为约束（谁在什么终态归还一次名额、admit 的门禁位置、关闭落定的
//     唯一信号）留在 0005 §4.0.1.7 与各实现注释里；装配入口不再是公共 API。
//   - 全部函数 noexcept 且不得阻塞：其调用点在数据路径等待段、scheduler
//     恢复路径与关闭路径上。
//
// 访问方式：TransportWiring 是 CoTCP/CoTCPListener/CoUDP/TransportRuntime
// 的 friend，直接读写其私有装配状态（不额外增加公共/受保护成员）。

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include <bbt/infra/CoTCP.hpp>
#include <bbt/infra/CoUDP.hpp>
#include <bbt/infra/TransportRuntime.hpp>

#include "detail/IoSupport.hpp"   // detail::CloseWaiters（受管 dial 等待段的关闭唤醒登记）

namespace bbt::infra::detail {

// Issue #32 F2：受管 DialTCP 的等待段接缝（仅 TransportRuntime 托管路径
// 装配；未托管静态入口以默认构造调用，零语义变化）。
//   close_waiters         —— owner 级关闭唤醒登记：受管 dial 的 connect
//                           等待段在事件登记成功后挂上它，owner->Close()
//                           经 CloseAndWakeAll 唤醒该等待段，使挂起的 dial
//                           尽快走正常返回路径归还容量（不再有取消令牌）。
//   dns_on_registered     —— 非空时，DNS 等待段在协程真正挂起（await
//                           event 已注册入 parked 表）后回调一次。
//   connect_on_registered —— 非空时，connect 等待段（EINPROGRESS 的
//                           fd 可写等待）在协程挂起后回调一次。
// 约定：回调 noexcept、不得取锁/阻塞——其在 scheduler 恢复路径上执行，由调用
// 方保证只用于测试同步或登记簿落账。
//
// 已知边界（新语义下的残余）：上游 AwaitBounded 内部自建 waiter，外部无法
// Notify，因此 DNS 等待段不挂 close_waiters——该段的封口响应只由调用方
// CallOptions::deadline 界定。owner 的在途账本/连接名额在该 dial 返回时归还。
struct DialWaitOptions {
    std::shared_ptr<detail::CloseWaiters> close_waiters{};
    std::function<void()>                 dns_on_registered{};
    std::function<void()>                 connect_on_registered{};
};

// 内部装配面：owner↔对象的装配接缝与测试探针。
struct TransportWiring {
    // —— 受管 DialTCP：等待段接缝入口（dst 的公共静态入口只走默认语义）——
    static result<tcp::CoTCP::SPtr> DialTCP(std::string host, std::uint16_t port,
                                           const CallOptions& options,
                                           const DialWaitOptions& dial_wait) {
        return tcp::CoTCP::_DialTcp(std::move(host), port, options, dial_wait);
    }

    // —— CoTCP：Runtime 托管登记与在途账本装配 ——
    // 物理关闭落定（对象侧收口完成处）时通知 Runtime 释放容量。
    static void SetClosedHook(tcp::CoTCP& owner, std::function<void()> hook) noexcept {
        owner.m_closed_hook = std::move(hook);
    }
    // §4.0.1.7：Runtime 级在途配额注入（仅受管对象；未托管入口不装钩，
    // 亦无限额语义）。admit 在 _CheckEntry（参数/协程上下文/运行时可用性）
    // 通过、计入 m_inflight 之前调用——false 即 Overloaded，立即返回不挂起；
    // admit 通过的操作在 m_mtx 内登记 m_quota_held++，op 结束归还 1，
    // Close/析构经 DrainQuota 归还剩余名额。quota_scope 强持有 Runtime 共享
    // 账本，保证归还发生时账本仍存活。
    static void SetInflightHooks(tcp::CoTCP& owner,
                                 std::function<bool()> admit,
                                 std::function<void()> release,
                                 std::shared_ptr<void> quota_scope) noexcept {
        owner.m_inflight_admit = std::move(admit);
        owner.m_inflight_release = std::move(release);
        owner.m_inflight_scope = std::move(quota_scope);
    }

    // —— CoTCP：测试接缝（生产路径不安装，空钩子零开销）——
    // 可等待 op 在参数/上下文/运行时检查与名额登记（m_quota_held++）完成后、
    // 进入首次等待循环之前在协程内触发一次。约定：noexcept、不得回调本
    // 对象/取 m_mtx/阻塞——持锁线程与同 scheduler 上其他协程依赖它快速返回。
    static void SetWaitEntryGateForTest(tcp::CoTCP& owner,
                                        std::function<void()> gate) noexcept {
        owner.m_wait_entry_gate_for_test = std::move(gate);
    }
    // F3：ReadSome/WriteSome 数据路径每次进入 fd 等待（_WaitFd 内
    // CoWaiter::Wait 的 on_registered，即协程真正挂起、事件已登记）后回调
    // 一次——与 wait-entry gate（等待循环入口）区分，本接缝证明「op 已真实
    // 挂起在 fd 等待」。约定同上。
    static void SetIoWaitRegisteredGateForTest(tcp::CoTCP& owner,
                                               std::function<void()> gate) noexcept {
        owner.m_io_wait_registered_gate_for_test = std::move(gate);
    }
    // F3：返回底层 fd 供测试调整 socket 选项（例如缩小 SO_SNDBUF 以确定性
    // 触发 EAGAIN/suspended write）。不持有 m_mtx，返回的是对象生命周期内
    // 的原始 fd；测试须只在连接建立后、关闭前使用，不得关闭/改向。
    static int NativeFdForTest(const tcp::CoTCP& owner) noexcept {
        return owner.m_fd;
    }

    // —— CoTCPListener：连接容量 + 接纳收养、关闭登记、账本与测试 gate ——
    // admit（连接容量）与 adopt（接纳收养：Runtime 侧登记并装各钩子，
    // false 表示投递期已封口）在 Runtime 托管路径安装；unmanaged listener
    // 不装钩（Unlimited 语义）。Accept 的 InflightHooks.admit 先于
    // m_accept_admit 判定，在挂起等待前判 Overloaded。
    static void SetAcceptHooks(tcp::CoTCPListener& owner,
                               std::function<bool()> admit,
                               std::function<void()> release,
                               std::function<bool(std::shared_ptr<tcp::CoTCP>)> adopt) noexcept {
        owner.m_accept_admit = std::move(admit);
        owner.m_accept_release = std::move(release);
        owner.m_accept_adopt = std::move(adopt);
    }
    static void SetClosedHook(tcp::CoTCPListener& owner,
                              std::function<void()> hook) noexcept {
        owner.m_closed_hook = std::move(hook);
    }
    static void SetInflightHooks(tcp::CoTCPListener& owner,
                                 std::function<bool()> admit,
                                 std::function<void()> release,
                                 std::shared_ptr<void> quota_scope) noexcept {
        owner.m_inflight_admit = std::move(admit);
        owner.m_inflight_release = std::move(release);
        owner.m_inflight_scope = std::move(quota_scope);
    }
    // Accept 在名额登记完成、首次 accept4/等待前于协程内触发一次。
    static void SetWaitEntryGateForTest(tcp::CoTCPListener& owner,
                                        std::function<void()> gate) noexcept {
        owner.m_wait_entry_gate_for_test = std::move(gate);
    }

    // —— CoUDP：关闭登记、在途账本与测试 gate ——
    static void SetClosedHook(udp::CoUDP& owner, std::function<void()> hook) noexcept {
        owner.m_closed_hook = std::move(hook);
    }
    // §4.0.1.7：admit 在 _CheckEntry 通过、计入 m_inflight 之前调用——
    // 返回 false 即 Overloaded，立即返回不挂起；quota_scope 保活 Runtime
    // 共享账本，对象侧 Close/析构归还，不依赖协程栈 RAII。
    static void SetInflightHooks(udp::CoUDP& owner,
                                 std::function<bool()> admit,
                                 std::function<void()> release,
                                 std::shared_ptr<void> quota_scope) noexcept {
        owner.m_inflight_admit = std::move(admit);
        owner.m_inflight_release = std::move(release);
        owner.m_inflight_scope = std::move(quota_scope);
    }
    static void SetWaitEntryGateForTest(udp::CoUDP& owner,
                                        std::function<void()> gate) noexcept {
        owner.m_wait_entry_gate_for_test = std::move(gate);
    }

    // —— TransportRuntime（owner）：协议 owner 的落定接线 ——
    // 本 owner 的受管 transport 全部物理关闭落定（IsClosed 观察点）后回调
    // 一次：协议 owner 用它把 transport 落定并入自身 finalize 门控；发布前
    // 一次性安装，生产路径至多一个装配者。约定：noexcept、不取本 owner 的
    // 锁、不阻塞。
    static void SetClosedHook(TransportRuntime& owner,
                              std::function<void()> hook) noexcept {
        owner.m_closed_hook = std::move(hook);
    }
    // 测试探针：本 owner 当前记账的受管 transport 名额（收敛判据：对象级
    // closed_hook 与名额归还一一配对后该值必须归零）。生产路径不使用。
    static std::size_t TransportsHeldForTest(TransportRuntime& owner) noexcept;

    // —— TransportRuntime：测试探针 ——
    // 直读共享在途账本当前名额数，用于跨对象/跨 op 的确定性断言；不替代
    // 公开语义，生产路径不使用。实现定义在 src/transport/TransportRuntime.cc
    // （唯一实现是 transport_detail::TransportRuntimeImpl，本头不暴露实现
    // 类型）。
    static std::size_t InflightQuotaHeldForTest(TransportRuntime& owner) noexcept;
    // R1 探针：owner 在途工厂装配计数（容量已预留、child 尚未交接/自行关闭）。
    static std::size_t PendingFactoriesForTest(TransportRuntime& owner) noexcept;
    // R1 探针：owner 级关闭唤醒登记是否已封口。受控交错回归据此在「Close 已
    // 封口、尚未返回」处确定性放行被暂停的工厂线程。
    static bool CloseSealedForTest(TransportRuntime& owner) noexcept;
    // 受管 DialTCP 的 DNS/connect 等待段在协程真正挂起后回调一次，用于确定性
    // 观察「dial 已进入等待」；生产路径不安装。约定：noexcept、不取锁不阻塞、
    // 不得回调本对象。
    static void SetDialWaitEntryGateForTest(TransportRuntime& owner,
                                            std::function<void()> gate) noexcept;
};

} // namespace bbt::infra::detail

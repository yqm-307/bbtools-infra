// Issue #31 独立消费验证：只 include bbt/infra 公共头与 coroutine/core 基础头，
// 只链接 bbt::infra_transport（不链接 HTTP/Redis/Mongo，也不 include 任何
// src 内部装配头），证明受管 TCP/UDP 消费者可从仓外独立构建并运行。
//
// 运行：退出码 0 且打印 `transport_consumer: ALL OK` 表示全部检查通过。
// 用法见同目录 README.md。

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

#include <bbt/infra/CoTCP.hpp>
#include <bbt/infra/CoUDP.hpp>
#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/TransportRuntime.hpp>

// 注意：本文件不做 `using namespace bbt::infra;`。`bbt::infra` 下有名为 `udp`
// 的命名空间（CoUDP 的实现命名空间），在文件作用域引入整套名字会与局部变量名
// 冲突（`auto udp = runtime->BindUDP(...)` 的初始化器里 `udp::CoUDP` 解析歧义）。
// 因此全部名字显式限定。
namespace {

int g_failures = 0;

void Check(bool ok, const char* what) {
    if (ok) {
        std::printf("ok: %s\n", what);
    } else {
        ++g_failures;
        std::printf("FAIL: %s\n", what);
    }
}

bbt::infra::NetworkLimits Limits() {
    bbt::infra::NetworkLimits limits{};
    limits.max_connections  = 4;
    limits.max_inflight     = 4;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = 64 * 1024;
    limits.incoming_timeout = std::chrono::milliseconds{1000};
    return limits;
}

} // namespace

int main() {
    using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;
    using bbt::infra::CloseStatus;
    using bbt::infra::ErrorCode;
    using bbt::infra::SocketAddress;
    using bbt::infra::TransportRuntime;

    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    if (!scheduler->IsRunning()) {
        std::printf("FAIL: scheduler did not start\n");
        return 1;
    }

    auto created = TransportRuntime::Create(Limits());
    Check(static_cast<bool>(created), "TransportRuntime::Create");
    if (!created) {
        scheduler->Stop();
        return 1;
    }
    auto runtime = std::move(created).value();
    Check(static_cast<bool>(runtime->Start()), "TransportRuntime::Start");

    // 控制线程配置面：数值地址 listener + UDP socket 均占用受管容量。
    auto listener = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    Check(static_cast<bool>(listener), "ListenTCP numeric loopback");
    if (listener)
        Check(listener.value()->LocalAddress().port != 0,
              "listener LocalAddress returns bound port");

    auto udp_socket = runtime->BindUDP(SocketAddress{"127.0.0.1", 0});
    Check(static_cast<bool>(udp_socket), "BindUDP numeric loopback");

    // 未 Start 的 Runtime 必须拒绝工厂（不产出半成品对象）。
    auto pre_start = TransportRuntime::Create(Limits());
    if (pre_start) {
        auto rejected =
            pre_start.value()->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
        Check(!rejected &&
                  rejected.error().code == ErrorCode::RuntimeUnavailable,
              "factory rejected before Start (RuntimeUnavailable)");
    }

    // 关闭落定只能在协程内等待。
    auto settled = std::make_shared<bbt::core::thread::CountDownLatch>(1);
    auto status  = std::make_shared<std::atomic_int>(-1);
    runtime->RequestClose();
    bool registered = false;
    scheduler->RegistCoroutineTask(
        [runtime, settled, status]() {
            const auto deadline = std::chrono::steady_clock::now() +
                                  std::chrono::milliseconds{3000};
            status->store(static_cast<int>(runtime->WaitClosed(deadline, {})));
            settled->Down();
        },
        registered);
    Check(registered, "RegistCoroutineTask accepted the waiter");
    Check(settled->WaitTimeout(5000) == 0, "WaitClosed settled within budget");
    Check(status->load() == static_cast<int>(CloseStatus::Closed),
          "runtime WaitClosed == Closed");
    if (listener)
        Check(listener.value()->IsClosed(), "listener IsClosed after RequestClose");
    if (udp_socket)
        Check(udp_socket.value()->IsClosed(), "udp IsClosed after RequestClose");

    scheduler->Stop();

    if (g_failures == 0) {
        std::printf("transport_consumer: ALL OK\n");
        return 0;
    }
    std::printf("transport_consumer: FAILED (%d checks)\n", g_failures);
    return 1;
}

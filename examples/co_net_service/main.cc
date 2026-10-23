// R2 协程网络最小示例（只消费公开 API：bbt::infra_net + bbtools-coroutine）。
//
// 真实 TCP 回环：Engine server（main + 1 sub owner，各自绑定已登记 Processer
// driver 槽，不新建网络线程）→ CoAccept 取连接 → CoRead/CoWrite 回显。
// client 用 NativeSocket 直连服务端实际绑定地址（port 0 由内核分配）。
//
// 退出约定：runtime 无 Stop/优雅停机，本进程业务收口后 flush 并以 std::_Exit
// 结束，不展开栈；这不是 runtime 资源回收证据。

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <poll.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/infra/net/CoConnection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/RawHandler.hpp>

namespace net = bbt::infra::net;
using namespace std::chrono_literals;

namespace {

bbt::coroutine::WaitOptions Until(std::chrono::milliseconds duration) {
    return {std::chrono::steady_clock::now() + duration};
}

bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}

} // namespace

int main() {
    std::atomic<int> failures{0};
    auto check = [&failures](bool value, const char* name) {
        if (!value) {
            std::cerr << "CHECK failed: " << name << '\n';
            failures.fetch_add(1);
        }
    };

    g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
    g_bbt_coroutine_config->m_cfg_stack_size = 65536;
    g_scheduler->Start();

    net::Engine engine(std::make_shared<net::RawHandler>());
    check(!engine.EnableAcceptHandoff(4), "enable accept handoff");

    std::error_code run_error;
    std::thread runner([&] {
        net::EngineConfig config;
        std::error_code error;
        config.listen_addresses = {net::Address::Ip("127.0.0.1", 0, error)};
        config.runtime_progress = net::RuntimeProgress::Independent;
        config.options.num_event_loop = 1;
        run_error = engine.Run(config);
    });
    check(WaitUntil([&] { return engine.Phase() == net::EnginePhase::Running; }, 3000ms),
          "engine running");

    std::vector<net::Address> addresses;
    check(!engine.LocalAddresses(addresses) && addresses.size() == 1, "local addresses");

    std::atomic<int> echo_ok{0};
    std::atomic<int> echo_drained{0};   // 尾部读空后的守卫状态断言
    auto server_task = [&] {
        net::Connection connection;
        if (net::CoAccept(engine, connection, Until(3000ms))) {
            return;
        }
        std::string data;
        if (net::CoRead(connection, data, Until(3000ms))) {
            return;
        }
        net::WriteOutcome outcome;
        net::WriteOperation operation;
        if (net::CoWrite(connection, data, Until(3000ms), outcome, operation)) {
            return;
        }
        // Done 只表示该次发送已到终态，成功还须 error 为空。
        if (outcome.phase == net::WritePhase::Done && !outcome.error && data == "hello") {
            echo_ok.store(1);
        }
        // 公开读取守卫：读空后立即再取一次保护应得到 WouldBlock（不是 Busy）；
        // 守卫持连接状态强引用，释放 connection 之后仍可安全 Size/析构。
        auto drained = connection.TryAccess();
        if (!drained && drained.WouldBlock()) {
            echo_drained.store(1);
        }
        connection.RequestClose();
    };
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    g_scheduler->RegistCoroutineTask(65536, [promise, server_task] {
        server_task();
        promise->set_value();
    }, "example.echo");

    std::error_code error;
    auto client = net::NativeSocket::Open(net::AddressFamily::IPv4, net::SocketType::Stream, error);
    check(!error, "client open");
    (void)client.Connect(addresses[0]);
    pollfd descriptor{client.Fd(), POLLOUT, 0};
    check(::poll(&descriptor, 1, 3000) > 0 && !client.PendingError(), "client connect");
    check(client.Send("hello", 5).bytes == 5, "client send");

    std::string echoed;
    char buffer[64];
    check(WaitUntil([&] {
        const auto result = client.Receive(buffer, sizeof(buffer));
        if (result.bytes > 0) {
            echoed.append(buffer, result.bytes);
        }
        return echoed.size() >= 5;
    }, 3000ms), "client recv");
    check(echoed == "hello", "echo content");
    if (future.wait_for(5s) == std::future_status::ready) {
        future.get();
    } else {
        check(false, "server task completed");
    }

    (void)client.Close();
    engine.RequestClose();
    runner.join();
    check(!run_error, "engine run clean");

    check(echo_drained.load() == 1, "read guard would_block after drain");
    std::cout << "co_net_service echo=" << (echo_ok.load() ? "ok" : "bad")
              << " guard_wouldblock_after_drain=" << (echo_drained.load() ? "ok" : "bad")
              << " failures=" << failures.load() << '\n';
    const bool clean = failures.load() == 0;
    std::cout.flush();
    std::cerr.flush();
    std::fflush(nullptr);
    std::_Exit(clean ? 0 : 1);
}

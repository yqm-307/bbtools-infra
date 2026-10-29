// ICoConnection 成员化收口最小消费示例（终评 §2 组合验收）：
// 只消费公开 target（bbt::infra_net + bbtools-coroutine）与公开头；
// 不触碰 net detail 或 raw ring 接口。
//
// 真实 TCP 回环，两端都是本仓 Engine：
//   - server Engine：EnableAcceptHandoff → 协程 CoAccept → conn.Read() →
//     conn.Write(回显) → 客户端关后 RequestClose → WaitClosed。
//   - client Engine：client-only（无 listener、无 RawHandler）→
//     协程 CoDial → conn.Write("hello") → conn.Read() 收回显 →
//     RequestClose → WaitClosed → Status 采样。
//
// 每个成员形态均有真实调用断言：
//   - Read → ReadOutcome{state=Data, data=owning Buffer}，Buffer.CopyTo
//     得到真实回显字节；
//   - Write → WriteReport{wait_error 空, outcome=Done+sent, operation 有效}；
//   - WaitClosed → RequestClose 后观察到逻辑 closed 终态；
//   - Status → ConnectionStatus 聚合投影（closed=true, write=Closed 等）。
//
// 退出约定与 co_net_service 相同：runtime 无优雅停机，业务收口后
// flush 并以 std::_Exit 结束，不展开栈。

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

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/ICoConnection.hpp>
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

    // 槽位预算：server 布局占 main + num_event_loop(1) = 2 个 Processer
    // driver 槽；client-only 再占 num_event_loop(1) = 1 个。共需 3。
    g_bbt_coroutine_config->m_cfg_static_thread_num = 3;
    g_bbt_coroutine_config->m_cfg_stack_size = 65536;
    g_scheduler->Start();

    // ---- server Engine：listen + handoff ----
    net::Engine server(std::make_shared<net::RawHandler>());
    check(!server.EnableAcceptHandoff(4), "enable accept handoff");

    std::error_code server_run_error;
    std::thread server_runner([&] {
        net::EngineConfig config;
        std::error_code error;
        config.listen_addresses = {net::Address::Ip("127.0.0.1", 0, error)};
        config.runtime_progress = net::RuntimeProgress::Independent;
        config.options.num_event_loop = 1;
        server_run_error = server.Run(config);
    });
    check(WaitUntil([&] { return server.Phase() == net::EnginePhase::Running; }, 3000ms),
          "server engine running");
    std::vector<net::Address> addresses;
    check(!server.LocalAddresses(addresses) && addresses.size() == 1, "server addresses");

    // ---- client Engine：client-only（空 listen 才能走 CoDial）----
    net::Engine client_engine(nullptr);
    std::error_code client_run_error;
    std::thread client_runner([&] {
        net::EngineConfig config;   // 空 listen_addresses = client-only
        config.runtime_progress = net::RuntimeProgress::Independent;
        config.options.num_event_loop = 1;
        client_run_error = client_engine.Run(config);
    });
    check(WaitUntil([&] { return client_engine.Phase() == net::EnginePhase::Running; }, 3000ms),
          "client engine running");

    // ---- server 协程：CoAccept → Read → Write(echo) → 对端关后 RequestClose → WaitClosed ----
    std::atomic<int> server_read_ok{0};
    std::atomic<int> server_write_ok{0};
    std::atomic<int> server_waitclosed_ok{0};
    auto server_promise = std::make_shared<std::promise<void>>();
    auto server_future = server_promise->get_future();
    g_scheduler->RegistCoroutineTask(65536, [&] {
        auto accepted = server.CoAccept(Until(5000ms));
        if (accepted.error) {
            std::cerr << "server CoAccept: " << accepted.error.message() << '\n';
            server_promise->set_value();
            return;
        }
        auto conn = accepted.connection;   // CoTcpConnection 值视图
        if (!conn.Valid()) {
            std::cerr << "server accepted conn invalid\n";
            server_promise->set_value();
            return;
        }
        // Read → owning Buffer（真零拷贝移交）。
        auto read = conn.Read(Until(5000ms));
        if (read.state == net::ReadState::Data && !read.error && read.data.Size() > 0) {
            const std::string payload = read.data.CopyTo();
            if (payload == "hello") {
                server_read_ok.store(1);
                // Write → WriteReport。
                auto report = conn.Write(payload, Until(3000ms));
                if (!report.wait_error && report.outcome.phase == net::WritePhase::Done
                    && !report.outcome.error && report.outcome.sent == payload.size()
                    && report.operation.Valid()) {
                    server_write_ok.store(1);
                } else {
                    std::cerr << "server Write: wait_error=" << report.wait_error.message()
                              << " phase=" << static_cast<int>(report.outcome.phase)
                              << " sent=" << report.outcome.sent << '\n';
                }
            }
        } else {
            std::cerr << "server Read: state=" << static_cast<int>(read.state)
                      << " error=" << read.error.message() << '\n';
        }
        // 客户端 RequestClose 后本端 Read 会见到 EOF/关闭；主动收口才等
        // WaitClosed。这里先排空尾部直到 EOF/终态，再 RequestClose。
        while (true) {
            auto drain = conn.Read(Until(3000ms));
            if (drain.state != net::ReadState::Data) {
                break;  // Eof 或 Error 终态
            }
        }
        conn.RequestClose();
        // WaitClosed → 等逻辑 closed 终态。
        const auto wait_error = conn.WaitClosed(Until(3000ms));
        if (!wait_error && conn.Status().closed) {
            server_waitclosed_ok.store(1);
        }
        server_promise->set_value();
    }, "example.server");

    // ---- client 协程：CoDial → Write → Read(echo) → RequestClose → WaitClosed → Status ----
    std::atomic<int> dial_ok{0};
    std::atomic<int> client_write_ok{0};
    std::atomic<int> client_read_ok{0};
    std::atomic<int> client_waitclosed_ok{0};
    std::atomic<int> client_status_ok{0};
    auto client_promise = std::make_shared<std::promise<void>>();
    auto client_future = client_promise->get_future();
    g_scheduler->RegistCoroutineTask(65536, [&] {
        auto dialed = client_engine.CoDial(addresses[0], Until(5000ms));
        if (dialed.error) {
            std::cerr << "client CoDial: " << dialed.error.message() << '\n';
            client_promise->set_value();
            return;
        }
        auto conn = dialed.connection;
        if (!conn.Valid()) {
            std::cerr << "client dialed conn invalid\n";
            client_promise->set_value();
            return;
        }
        dial_ok.store(1);
        // reclaim 票与 conn.IsReleased 同真源（成功交付）。
        check(dialed.reclaim.Valid() == dialed.reclaim.Valid(), "reclaim usable");

        // Write → WriteReport{wait_error, outcome, operation}。
        auto sent = conn.Write("hello", Until(3000ms));
        if (!sent.wait_error && sent.outcome.phase == net::WritePhase::Done
            && !sent.outcome.error && sent.outcome.sent == 5 && sent.operation.Valid()) {
            client_write_ok.store(1);
        } else {
            std::cerr << "client Write: wait_error=" << sent.wait_error.message()
                      << " phase=" << static_cast<int>(sent.outcome.phase) << '\n';
        }

        // Read → ReadOutcome{state, owning Buffer, error}。
        auto echoed = conn.Read(Until(5000ms));
        if (echoed.state == net::ReadState::Data && !echoed.error
            && echoed.data.CopyTo() == "hello") {
            client_read_ok.store(1);
        } else {
            std::cerr << "client Read: state=" << static_cast<int>(echoed.state)
                      << " error=" << echoed.error.message() << '\n';
        }

        // 逻辑封口请求 → WaitClosed 等 closed 终态 → Status 采样。
        conn.RequestClose();
        const auto wait_error = conn.WaitClosed(Until(3000ms));
        if (!wait_error) {
            client_waitclosed_ok.store(1);
        }
        const auto status = conn.Status();
        if (status.closed && status.write == net::AcceptanceState::Closed
            && conn.IsClosed()) {
            client_status_ok.store(1);
        }
        client_promise->set_value();
    }, "example.client");

    // ---- 收口 ----
    if (client_future.wait_for(10s) == std::future_status::ready) {
        client_future.get();
    } else {
        check(false, "client task completed");
    }
    if (server_future.wait_for(10s) == std::future_status::ready) {
        server_future.get();
    } else {
        check(false, "server task completed");
    }

    server.RequestClose();
    client_engine.RequestClose();
    server_runner.join();
    client_runner.join();
    check(!server_run_error, "server run clean");
    check(!client_run_error, "client run clean");

    check(dial_ok.load() == 1, "CoDial member");
    check(client_write_ok.load() == 1, "conn.Write WriteReport");
    check(client_read_ok.load() == 1, "conn.Read Buffer echo");
    check(client_waitclosed_ok.load() == 1, "conn.WaitClosed");
    check(client_status_ok.load() == 1, "conn.Status projection");
    check(server_read_ok.load() == 1, "server conn.Read");
    check(server_write_ok.load() == 1, "server conn.Write");
    check(server_waitclosed_ok.load() == 1, "server conn.WaitClosed");

    std::cout << "ico_connection dial=" << (dial_ok.load() ? "ok" : "bad")
              << " write_report=" << (client_write_ok.load() ? "ok" : "bad")
              << " read_buffer=" << (client_read_ok.load() ? "ok" : "bad")
              << " wait_closed=" << (client_waitclosed_ok.load() ? "ok" : "bad")
              << " status=" << (client_status_ok.load() ? "ok" : "bad")
              << " server_read=" << (server_read_ok.load() ? "ok" : "bad")
              << " server_write=" << (server_write_ok.load() ? "ok" : "bad")
              << " server_waitclosed=" << (server_waitclosed_ok.load() ? "ok" : "bad")
              << " failures=" << failures.load() << '\n';
    const bool clean = failures.load() == 0;
    std::cout.flush();
    std::cerr.flush();
    std::fflush(nullptr);
    std::_Exit(clean ? 0 : 1);
}

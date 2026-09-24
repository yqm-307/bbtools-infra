#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <cstring>
#include <string>
#include <thread>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>
#include <bbt/infra/CoUDP.hpp>

using bbt::infra::CallOptions;
using bbt::infra::ConstBytes;
using bbt::infra::DatagramRead;
using bbt::infra::ErrorCode;
using bbt::infra::IoState;
using bbt::infra::MutableBytes;
using bbt::infra::SocketAddress;
using bbt::infra::udp::CoUDP;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

namespace {

// 动态端口：bind port=0 后经 LocalAddress 取实际端口，避免同机多 agent
// 与重复运行时的固定端口冲突（AGENTS.md 测试规约）。
SocketAddress Loopback(std::uint16_t port) {
    SocketAddress addr;
    addr.ip   = "127.0.0.1";
    addr.port = port;
    return addr;
}

CallOptions Options(int timeout_ms = 5000) {
    CallOptions options;
    options.deadline = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(timeout_ms);
    return options;
}

bool SameCode(const bbt::infra::Error& error, ErrorCode expected) {
    return static_cast<int>(error.code) == static_cast<int>(expected);
}

SocketAddress LocalOf(const CoUDP::SPtr& sock) {  // 传 .value() 解包后的 SPtr
    auto local = sock->LocalAddress();
    BOOST_REQUIRE(!local.ip.empty());
    return local;
}

} // namespace

// §5 真实双端 loopback：datagram 边界、peer 地址、回发、LocalAddress。
BOOST_AUTO_TEST_CASE(t_udp_real_loopback) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto receiver = CoUDP::BindUDP(Loopback(0));
    BOOST_REQUIRE(receiver);
    auto sender = CoUDP::BindUDP(Loopback(0));
    BOOST_REQUIRE(sender);

    const auto recv_addr = LocalOf(receiver.value());
    const auto send_addr = LocalOf(sender.value());
    BOOST_CHECK_EQUAL(recv_addr.port != 0, true);
    BOOST_CHECK_EQUAL(send_addr.port != 0, true);

    bbt::core::thread::CountDownLatch done{2};
    std::atomic_bool recv_ok{false};
    std::atomic_bool send_ok{false};

    bbtco [rx = receiver.value(), recv_addr, &recv_ok, &done]() {
        char buffer[64]{};
        auto got = rx->Receive(
            MutableBytes{buffer, sizeof(buffer)}, Options());
        if (!got || got.value().state != IoState::Ok ||
            got.value().bytes != 5 || std::string(buffer, 5) != "hello" ||
            got.value().peer.ip != "127.0.0.1" || got.value().peer.port == 0 ||
            got.value().truncated) {
            done.Down();
            return;
        }
        const char response[] = "world";
        auto back = rx->Send(
            ConstBytes{response, 5}, got.value().peer, Options());
        recv_ok.store(back && back.value().state == IoState::Ok &&
                      back.value().bytes == 5);
        done.Down();
    };

    bbtco [sx = sender.value(), send_addr, recv_addr, &send_ok, &done]() {
        const char request[] = "hello";
        auto sent = sx->Send(ConstBytes{request, 5}, recv_addr, Options());
        if (!sent || sent.value().state != IoState::Ok ||
            sent.value().bytes != 5) {
            done.Down();
            return;
        }
        char buffer[64]{};
        auto got = sx->Receive(
            MutableBytes{buffer, sizeof(buffer)}, Options());
        send_ok.store(got && got.value().state == IoState::Ok &&
                      got.value().bytes == 5 &&
                      std::string(buffer, 5) == "world" &&
                      got.value().peer.ip == "127.0.0.1" &&
                      got.value().peer.port == recv_addr.port);
        done.Down();
    };

    BOOST_REQUIRE_EQUAL(done.WaitTimeout(10000), 0);
    BOOST_CHECK(recv_ok.load());
    BOOST_CHECK(send_ok.load());

    receiver.value()->RequestClose();
    receiver.value()->RequestClose(); // 幂等
    BOOST_CHECK(receiver.value()->IsClosed());
    sender.value()->RequestClose();
    scheduler->Stop();
}

// §5 截断语义：大报文 + 小 buffer → bytes=容量、truncated=true；
// 恰好等于容量 → truncated=false；size=0 buffer 读到报文 → truncated=true。
BOOST_AUTO_TEST_CASE(t_udp_truncation) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto receiver = CoUDP::BindUDP(Loopback(0));
    BOOST_REQUIRE(receiver);
    auto sender = CoUDP::BindUDP(Loopback(0));
    BOOST_REQUIRE(sender);
    const auto recv_addr = LocalOf(receiver.value());

    // 发送 200 字节报文
    std::string big(200, 'x');
    auto sent = sender.value()->TrySend(ConstBytes{big.data(), big.size()}, recv_addr);
    BOOST_REQUIRE(sent && sent.value().bytes == 200);

    bbt::core::thread::CountDownLatch done{1};
    std::atomic_int truncated_bytes{-1};
    std::atomic_bool truncated_flag{false};
    bbtco [receiver, &done, &truncated_bytes, &truncated_flag]() {
        char buffer[100]{}; // 容量 100 < 200
        auto got = receiver.value()->Receive(
            MutableBytes{buffer, sizeof(buffer)}, Options());
        if (got && got.value().state == IoState::Ok) {
            truncated_bytes.store(static_cast<int>(got.value().bytes));
            truncated_flag.store(got.value().truncated);
        }
        done.Down();
    };
    BOOST_REQUIRE_EQUAL(done.WaitTimeout(5000), 0);
    BOOST_CHECK_EQUAL(truncated_bytes.load(), 100);
    BOOST_CHECK_EQUAL(truncated_flag.load(), true); // 超出部分被内核丢弃

    // 恰好等于容量：不截断
    auto sent2 = sender.value()->TrySend(ConstBytes{big.data(), 100}, recv_addr);
    BOOST_REQUIRE(sent2 && sent2.value().bytes == 100);
    char buffer_exact[100]{};
    auto exact = receiver.value()->TryReceive(MutableBytes{buffer_exact, 100});
    BOOST_REQUIRE(exact && exact.value().state == IoState::Ok);
    BOOST_CHECK_EQUAL(exact.value().bytes, 100u);
    BOOST_CHECK_EQUAL(exact.value().truncated, false);

    // size=0 buffer + 有报文：bytes=0、truncated=true
    auto sent3 = sender.value()->TrySend(ConstBytes{big.data(), 50}, recv_addr);
    BOOST_REQUIRE(sent3 && sent3.value().bytes == 50);
    char sink = 0;
    auto zero = receiver.value()->TryReceive(MutableBytes{&sink, 0});
    BOOST_REQUIRE(zero && zero.value().state == IoState::Ok);
    BOOST_CHECK_EQUAL(zero.value().bytes, 0u);
    BOOST_CHECK_EQUAL(zero.value().truncated, true);

    sender.value()->RequestClose();
    receiver.value()->RequestClose();
    scheduler->Stop();
}

// 零长度 datagram（合法、非 EOF）与无数据 deadline 到点。
BOOST_AUTO_TEST_CASE(t_udp_zero_length_datagram_and_timeout) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto receiver = CoUDP::BindUDP(Loopback(0));
    BOOST_REQUIRE(receiver);
    auto sender = CoUDP::BindUDP(Loopback(0));
    BOOST_REQUIRE(sender);
    const auto recv_addr = LocalOf(receiver.value());

    // 零长度 datagram：send size=0，recv 返回 Ok/bytes=0/truncated=false
    // 且 peer 有效。
    auto sent = sender.value()->TrySend(ConstBytes{nullptr, 0}, recv_addr);
    BOOST_REQUIRE(sent && sent.value().bytes == 0);
    bbt::core::thread::CountDownLatch zero_done{1};
    std::atomic_bool zero_ok{false};
    bbtco [rx = receiver.value(), &zero_done, &zero_ok]() {
        char buffer[8]{};
        auto got = rx->Receive(
            MutableBytes{buffer, sizeof(buffer)}, Options());
        zero_ok.store(got && got.value().state == IoState::Ok &&
                      got.value().bytes == 0 &&
                      !got.value().truncated &&
                      got.value().peer.port != 0 &&
                      !got.value().peer.ip.empty());
        zero_done.Down();
    };
    BOOST_CHECK_EQUAL(zero_done.WaitTimeout(5000), 0);
    BOOST_CHECK(zero_ok.load());

    // 无数据时 deadline 到点：Receive 返回 TimedOut（协程内等待）。
    bbt::core::thread::CountDownLatch timeout_done{1};
    std::atomic_int timeout_code{-1};
    bbtco [rx = receiver.value(), &timeout_code, &timeout_done]() {
        char buffer[8]{};
        auto timed_out = rx->Receive(
            MutableBytes{buffer, sizeof(buffer)}, Options(200));
        timeout_code.store(timed_out
            ? -1 : static_cast<int>(timed_out.error().code));
        timeout_done.Down();
    };
    BOOST_CHECK_EQUAL(timeout_done.WaitTimeout(5000), 0);
    BOOST_CHECK_EQUAL(timeout_code.load(), static_cast<int>(ErrorCode::TimedOut));

    sender.value()->RequestClose();
    receiver.value()->RequestClose();
    scheduler->Stop();
}

// §5 Try*：无数据 WouldBlock、有数据立即成功、TrySend 正常；Try* 不挂起。
BOOST_AUTO_TEST_CASE(t_udp_try_operations) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto receiver = CoUDP::BindUDP(Loopback(0));
    BOOST_REQUIRE(receiver);
    auto sender = CoUDP::BindUDP(Loopback(0));
    BOOST_REQUIRE(sender);
    const auto recv_addr = LocalOf(receiver.value());

    // 无数据：TryReceive WouldBlock，不挂起（协程外也可调用）。
    char buffer[16]{};
    auto empty = receiver.value()->TryReceive(MutableBytes{buffer, sizeof(buffer)});
    BOOST_REQUIRE(empty && empty.value().state == IoState::WouldBlock);
    BOOST_CHECK_EQUAL(empty.value().bytes, 0u);

    // TrySend 成功后 TryReceive 立即取到。
    const char payload[] = "ping";
    auto pushed = sender.value()->TrySend(
        ConstBytes{payload, 4}, recv_addr);
    BOOST_REQUIRE(pushed && pushed.value().state == IoState::Ok &&
                  pushed.value().bytes == 4);
    auto got = receiver.value()->TryReceive(MutableBytes{buffer, sizeof(buffer)});
    BOOST_REQUIRE(got && got.value().state == IoState::Ok);
    BOOST_CHECK_EQUAL(got.value().bytes, 4u);
    BOOST_CHECK(std::string(buffer, 4) == "ping");

    // 参数非法：nullptr + size>0 → InvalidArgument，立即返回。
    auto bad_recv = receiver.value()->TryReceive(MutableBytes{nullptr, 8});
    BOOST_REQUIRE(!bad_recv);
    BOOST_CHECK(SameCode(bad_recv.error(), ErrorCode::InvalidArgument));
    auto bad_send = sender.value()->TrySend(ConstBytes{nullptr, 8}, recv_addr);
    BOOST_REQUIRE(!bad_send);
    BOOST_CHECK(SameCode(bad_send.error(), ErrorCode::InvalidArgument));
    auto bad_addr = sender.value()->TrySend(ConstBytes{payload, 4},
                                    SocketAddress{"no-such-host.invalid", 1});
    BOOST_REQUIRE(!bad_addr);
    BOOST_CHECK(SameCode(bad_addr.error(), ErrorCode::InvalidArgument));

    sender.value()->RequestClose();
    receiver.value()->RequestClose();
    scheduler->Stop();
}

// §6.4 关闭协议：跨线程 RequestClose 唤醒在途 Receive（Closed）、
// WaitClosed 真实等待清理完成、封口后新调用立即 Closed、FD 复用防护。
BOOST_AUTO_TEST_CASE(t_udp_close_lifecycle) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto victim = CoUDP::BindUDP(Loopback(0));
    BOOST_REQUIRE(victim);
    const auto victim_port = LocalOf(victim.value()).port;

    // 在途 Receive 由控制线程 close 唤醒：返回 Closed 而非悬挂到 deadline。
    bbt::core::thread::CountDownLatch inflight_done{1};
    std::atomic_int inflight_code{-1};
    bbtco [vx = victim.value(), &inflight_code, &inflight_done]() {
        char buffer[8]{};
        auto got = vx->Receive(MutableBytes{buffer, sizeof(buffer)},
                                   Options(10000)); // 长 deadline：靠 close 唤醒
        inflight_code.store(got ? 0 : static_cast<int>(got.error().code));
        inflight_done.Down();
    };

    // WaitClosed 在另一协程等待真实清理；close 由控制线程发起。
    bbt::core::thread::CountDownLatch wait_done{1};
    std::atomic_int wait_result{-1};
    bbtco [vx = victim.value(), &wait_result, &wait_done]() {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(5000);
        wait_result.store(static_cast<int>(
            vx->WaitClosed(deadline, /*cancel=*/{})));
        wait_done.Down();
    };

    // 控制线程（非协程线程）跨线程 RequestClose。
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    victim.value()->RequestClose();

    BOOST_CHECK_EQUAL(inflight_done.WaitTimeout(5000), 0);
    BOOST_CHECK_EQUAL(inflight_code.load(),
                      static_cast<int>(ErrorCode::Closed));
    BOOST_CHECK_EQUAL(wait_done.WaitTimeout(5000), 0);
    BOOST_CHECK_EQUAL(wait_result.load(),
                      static_cast<int>(bbt::infra::CloseStatus::Closed));
    BOOST_CHECK(victim.value()->IsClosed());

    // 控制线程的可挂起调用先被协程上下文门禁拒绝。
    char buffer[8]{};
    auto after = victim.value()->Receive(MutableBytes{buffer, sizeof(buffer)}, Options());
    BOOST_REQUIRE(!after);
    BOOST_CHECK(SameCode(after.error(), ErrorCode::InvalidContext));
    auto after_send = victim.value()->Send(ConstBytes{"x", 1}, Loopback(victim_port),
                                   Options());
    BOOST_REQUIRE(!after_send);
    BOOST_CHECK(SameCode(after_send.error(), ErrorCode::InvalidContext));
    auto closed_try = victim.value()->TryReceive(MutableBytes{buffer, sizeof(buffer)});
    BOOST_REQUIRE(!closed_try);
    BOOST_CHECK(SameCode(closed_try.error(), ErrorCode::Closed));

    // FD 数字复用防护：close 后立即新建 socket（大概率复用同一 fd 数字），
    // 旧对象的等待者已全部退出（上文 WaitClosed=Closed），新对象可正常收发。
    auto recycled = CoUDP::BindUDP(Loopback(victim_port));
    BOOST_REQUIRE(recycled); // 同端口重绑成功即旧 FD 已真实关闭
    char probe[8]{};
    auto idle = recycled.value()->TryReceive(MutableBytes{probe, sizeof(probe)});
    BOOST_REQUIRE(idle && idle.value().state == IoState::WouldBlock);

    // 已关闭旧对象：等待者与新事件不再触达新 FD（TryReceive 拒绝）。
    auto stale = victim.value()->TryReceive(MutableBytes{probe, sizeof(probe)});
    BOOST_REQUIRE(!stale);
    BOOST_CHECK(SameCode(stale.error(), ErrorCode::Closed));

    recycled.value()->RequestClose();
    scheduler->Stop();
}

// cancel 令牌唤醒在途 Receive；CloseStatus 语义（未关闭对象上
// WaitClosed 被 cancel 唤醒 → Cancelled）。
BOOST_AUTO_TEST_CASE(t_udp_cancel_wakeup) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto receiver = CoUDP::BindUDP(Loopback(0));
    BOOST_REQUIRE(receiver);

    bbt::coroutine::CancellationSource source;
    bbt::core::thread::CountDownLatch cancel_done{1};
    std::atomic_int cancel_code{-1};
    bbtco [rx = receiver.value(), source, &cancel_code, &cancel_done]() {
        char buffer[8]{};
        CallOptions options;
        options.deadline = std::chrono::steady_clock::now() +
                           std::chrono::milliseconds(10000);
        options.cancel = source.Token();
        auto got = rx->Receive(MutableBytes{buffer, sizeof(buffer)},
                                     options);
        cancel_code.store(got ? 0 : static_cast<int>(got.error().code));
        cancel_done.Down();
    };

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    source.RequestCancel();
    BOOST_CHECK_EQUAL(cancel_done.WaitTimeout(5000), 0);
    BOOST_CHECK_EQUAL(cancel_code.load(), static_cast<int>(ErrorCode::Cancelled));

    // 未关闭对象：对象仍可用。
    char probe[8]{};
    auto idle = receiver.value()->TryReceive(MutableBytes{probe, sizeof(probe)});
    BOOST_REQUIRE(idle && idle.value().state == IoState::WouldBlock);

    receiver.value()->RequestClose();
    scheduler->Stop();
}

// BindUDP 参数校验：空 ip / 非数值 ip 拒绝；Scheduler 未 Start 拒绝。
BOOST_AUTO_TEST_CASE(t_udp_bind_validation) {
    // 空 ip：通配必须显式。
    auto empty = CoUDP::BindUDP(SocketAddress{"", 0});
    BOOST_REQUIRE(!empty);
    BOOST_CHECK(SameCode(empty.error(), ErrorCode::InvalidArgument));
    // 非数值主机名：只接受数值地址。
    auto hostname = CoUDP::BindUDP(SocketAddress{"no-such-host.invalid", 0});
    BOOST_REQUIRE(!hostname);
    BOOST_CHECK(SameCode(hostname.error(), ErrorCode::InvalidArgument));
}

// §4 入口代际检查：Runtime 未 Start 或对象归属其他运行代时，Try* 与协程
// 方法都必须立即 RuntimeUnavailable——不挂起、不下发 syscall 到旧 fd。
// Try* 不要求协程上下文（非协程线程可照常调用），但同样受代际门禁
// （§4「Try* 只检查参数、上下文、代际和关闭状态」）。
BOOST_AUTO_TEST_CASE(t_udp_generation_guard) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto socket = CoUDP::BindUDP(Loopback(0));
    BOOST_REQUIRE(socket);
    const auto addr = LocalOf(socket.value());

    // 运行时代际归 0：Try* 在控制线程立即 RuntimeUnavailable。
    scheduler->Stop();
    BOOST_CHECK(!scheduler->IsRunning());
    char buffer[8]{};
    auto after_stop = socket.value()->TryReceive(MutableBytes{buffer, sizeof(buffer)});
    BOOST_REQUIRE(!after_stop);
    BOOST_CHECK(SameCode(after_stop.error(), ErrorCode::RuntimeUnavailable));
    auto after_stop_send = socket.value()->TrySend(ConstBytes{"x", 1}, addr);
    BOOST_REQUIRE(!after_stop_send);
    BOOST_CHECK(SameCode(after_stop_send.error(), ErrorCode::RuntimeUnavailable));

    // 重启进入新代际：旧对象在协程内同样被拒（不触碰可能已复用的旧 fd）。
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());
    bbt::core::thread::CountDownLatch done{1};
    std::atomic_int code{-1};
    bbtco [sx = socket.value(), addr, &done, &code]() {
        char buf[8]{};
        auto got = sx->Receive(MutableBytes{buf, sizeof(buf)}, Options());
        code.store(got ? -2 : static_cast<int>(got.error().code));
        auto sent = sx->Send(ConstBytes{"y", 1}, addr, Options());
        if (sent)
            code.store(-3);
        done.Down();
    };
    BOOST_REQUIRE_EQUAL(done.WaitTimeout(5000), 0);
    BOOST_CHECK_EQUAL(code.load(), static_cast<int>(ErrorCode::RuntimeUnavailable));

    // 代际门禁只拒绝操作、不改关闭状态：对象仍可正常收口，不泄漏 FD。
    BOOST_CHECK(!socket.value()->IsClosed());
    socket.value()->RequestClose();
    BOOST_CHECK(socket.value()->IsClosed());
    scheduler->Stop();
}

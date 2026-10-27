// 由 Test_redis_net_contract.cc 在匿名 namespace 内包含；复用真实 TCP/宿主 fixture。
using PoolAccess = redis::PoolTestAccess;

std::shared_ptr<redis::CoRedisPool> PeerPool(Host& host, net::Address address,
                                         std::size_t size = 1, std::size_t waiters = 1) {
    auto created = redis::CoRedisPool::Create(host.engine, address, {size, waiters});
    CHECK(created);
    auto pool = std::move(created).value();
    CHECK(host.Call([&] { return bbt_co_await pool->Connect(Until()); }));
    return pool;
}

// 完整命令已到对端才提交等待者；两个槽都占满，FIFO、排队上限与归还容量。
void PoolStages() {
    Endpoint listener(true);
    std::atomic<unsigned> received{0};
    std::atomic<bool> release{false};
    auto server = std::async(std::launch::async, [&] {
        std::vector<std::future<void>> workers;
        for (unsigned i = 0; i < 2; ++i) {
            pollfd pfd{listener.fd, POLLIN, 0};
            CHECK(::poll(&pfd, 1, 3000) == 1);
            const int fd = ::accept4(listener.fd, nullptr, nullptr, SOCK_CLOEXEC);
            CHECK(fd >= 0);
            workers.push_back(std::async(std::launch::async, [&, fd] {
                struct CloseFd { int fd; ~CloseFd() { ::close(fd); } } close{fd};
                CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"PING"}));
                ++received;
                CHECK(WaitUntil([&] { return release.load(); }));
                CHECK(Peer::Send(fd, "+PONG\r\n"));
                // 后续命令在 pool Close 前由 host 逐个提交。
                for (;;) {
                    pollfd ready{fd, POLLIN, 0};
                    CHECK(::poll(&ready, 1, 3000) == 1);
                    char byte;
                    if (::recv(fd, &byte, 1, MSG_PEEK) == 0) { break; }
                    CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"PING"}));
                    CHECK(Peer::Send(fd, "+PONG\r\n"));
                }
            }));
        }
        for (auto& worker : workers) { Get(worker); }
    });
    Host host;
    auto pool = PeerPool(host, listener.address, 2, 2);
    auto a = host.Submit([&] { return bbt_co_await pool->Ping(Until()); });
    auto b = host.Submit([&] { return bbt_co_await pool->Ping(Until()); });
    CHECK(WaitUntil([&] { return received.load() == 2; }));
    std::vector<int> order;
    auto c = host.Submit([&] { auto r = bbt_co_await pool->Ping(Until()); order.push_back(1); return r; });
    CHECK(WaitUntil([&] { return PoolAccess::Queued(*pool) == 1; }));
    auto d = host.Submit([&] { auto r = bbt_co_await pool->Ping(Until()); order.push_back(2); return r; });
    CHECK(WaitUntil([&] { return PoolAccess::Queued(*pool) == 2; }));
    ExpectCode(host.Call([&] { return bbt_co_await pool->Ping(Until()); }), ErrorCode::Overloaded);
    release.store(true);
    CHECK(Get(a)); CHECK(Get(b)); CHECK(Get(c)); CHECK(Get(d));
    // 两连接的完成顺序不必 FIFO；FIFO 由单槽用例另外断言。
    CHECK(order.size() == 2 && PoolAccess::Queued(*pool) == 0);
    CHECK(host.Call([&] { return bbt_co_await pool->Ping(Until()); }));
    pool->Close(); pool.reset(); Get(server); host.Finish();
    std::cout << "pool_stages complete_commands=2 occupied=2 queue_peak=2 excess=Overloaded capacity_returned=1\n";
}

// 单槽 FIFO；超时/取消/Close 均在真实入队后触发；0 拒绝不登记。
void PoolWaitExits() {
    for (const std::string mode : {"fifo", "timeout", "cancel", "close", "zero", "deadline"}) {
        std::atomic<bool> received{false}, release{false};
        Peer peer([&](int fd) {
            CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"PING"}));
            received.store(true);
            CHECK(WaitUntil([&] { return release.load(); }));
            if (mode == "close") { return; }
            CHECK(Peer::Send(fd, "+PONG\r\n"));
            const int more = mode == "fifo" ? 3 : 1;
            for (int i = 0; i < more; ++i) {
                CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"PING"}));
                if (mode == "deadline") {
                    char byte;
                    CHECK(::recv(fd, &byte, 1, 0) == 0); // 等待+命令共用期限，命令超时退役。
                    return;
                }
                CHECK(Peer::Send(fd, "+PONG\r\n"));
            }
        });
        Host host;
        auto pool = PeerPool(host, peer.address, 1, mode == "zero" ? 0 : 2);
        auto a = host.Submit([&] { return bbt_co_await pool->Ping(Until()); });
        CHECK(WaitUntil([&] { return received.load(); }));
        std::promise<bbt::coroutine::detail::Coroutine*> coroutine;
        auto task = coroutine.get_future();
        const auto options = Until(mode == "timeout" || mode == "deadline" ? 200ms : 2s);
        std::vector<int> order;
        auto b = host.Submit([&] {
            coroutine.set_value(g_bbt_tls_coroutine_co);
            auto value = bbt_co_await pool->Ping(options);
            order.push_back(1);
            return value;
        });
        auto* current = Get(task);
        if (mode != "zero") { CHECK(WaitUntil([&] { return PoolAccess::Queued(*pool) == 1; })); }
        std::future<result<void>> c;
        if (mode == "fifo") {
            c = host.Submit([&] { auto value = bbt_co_await pool->Ping(Until()); order.push_back(2); return value; });
            CHECK(WaitUntil([&] { return PoolAccess::Queued(*pool) == 2; }));
        } else if (mode == "cancel") { current->RequestCancel(); }
        else if (mode == "close") { pool->Close(); }
        if (mode == "timeout" || mode == "cancel" || mode == "close" || mode == "zero") {
            auto value = Get(b);
            ExpectCode(value, mode == "timeout" ? ErrorCode::TimedOut : mode == "cancel" ? ErrorCode::Cancelled :
                             mode == "close" ? ErrorCode::Closed : ErrorCode::Overloaded);
            CHECK(PoolAccess::Queued(*pool) == 0);
        }
        if (mode == "deadline") {
            // 已确认 B 入队，扣留 A 消耗大部分预算；重置期限会越过下方界限。
            CHECK(WaitUntil([&] { return std::chrono::steady_clock::now() >= options.deadline - 60ms; }));
        }
        release.store(true);
        auto first = Get(a);
        if (mode != "close") { CHECK(first); }
        if (mode == "fifo") {
            CHECK(Get(b)); CHECK(Get(c)); CHECK(order == std::vector<int>({1, 2}));
        } else if (mode == "deadline") {
            auto value = Get(b); CHECK(!value); CheckUnknown(value.error(), "TimedOut");
            CHECK(std::chrono::steady_clock::now() < options.deadline + 80ms);
        }
        if (mode != "close" && mode != "deadline") {
            CHECK(host.Call([&] { return bbt_co_await pool->Ping(Until()); }));
        }
        if (mode == "close") { CHECK(host.engine->Phase() == net::EnginePhase::Running); }
        pool->Close(); pool.reset(); peer.Join(); host.Finish();
        std::cout << "pool_wait_exit mode=" << mode << " stage_proven=1 settled_once=1\n";
    }
}

// 登记前的旧丢通知窗口：归还与 Close 都发生在 B 尚未登记时。
// 取消先胜但归还已交接的窗口：出账必须把槽交给 C。
void PoolWaitWindows() {
    for (const std::string mode : {"return_before_register", "close_before_register", "throw_registered"}) {
        std::atomic<bool> received{false}, release{false};
        Peer peer([&](int fd) {
            CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"PING"}));
            received.store(true);
            CHECK(WaitUntil([&] { return release.load(); }));
            if (mode == "close_before_register") { return; }
            CHECK(Peer::Send(fd, "+PONG\r\n"));
            // throw/cancel 两例由 C 使用归还容量，return 由 B 使用。
            CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"PING"}));
            CHECK(Peer::Send(fd, "+PONG\r\n"));
        });
        Host host;
        auto pool = PeerPool(host, peer.address);
        auto a = host.Submit([&] { return bbt_co_await pool->Ping(Until()); });
        CHECK(WaitUntil([&] { return received.load(); }));
        Gate paused;
        PoolAccess::SetWaitHook(*pool, [&](int phase) {
            if (phase == 0 && mode.find("before_register") != std::string::npos) { paused.Pause(); }
            if (phase == 1 && mode == "throw_registered") { throw std::runtime_error("registered injected failure"); }
        });
        std::promise<bbt::coroutine::detail::Coroutine*> coroutine;
        auto task = coroutine.get_future();
        auto b = host.Submit([&] { coroutine.set_value(g_bbt_tls_coroutine_co); return bbt_co_await pool->Ping(Until()); });
        auto* current = Get(task);
        (void)current;
        if (mode != "throw_registered") { CHECK(WaitUntil([&] { return paused.entered.load(); })); }
        if (mode == "close_before_register") { pool->Close(); }
        release.store(true);
        auto first = Get(a);
        if (mode != "close_before_register") { CHECK(first); }
        paused.Release();
        if (mode == "throw_registered") {
            bool threw = false;
            try { (void)Get(b); } catch (const std::runtime_error& error) { threw = std::string(error.what()) == "registered injected failure"; }
            CHECK(threw);
        } else {
            auto value = Get(b);
            if (mode == "return_before_register") { CHECK(value); }
            else { ExpectCode(value, mode == "cancel_handoff" ? ErrorCode::Cancelled : ErrorCode::Closed); }
        }
        CHECK(PoolAccess::Queued(*pool) == 0);
        if (mode == "throw_registered") {
            CHECK(host.Call([&] { return bbt_co_await pool->Ping(Until()); }));
        }
        PoolAccess::SetWaitHook(*pool, {});
        pool->Close(); pool.reset(); peer.Join(); host.Finish();
        std::cout << "pool_window mode=" << mode << " no_lost_wake_slot=1\n";
    }
}

void PoolCancelHandoff() {
    Peer peer([](int fd) {
        CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"PING"}));
        CHECK(Peer::Send(fd, "+PONG\r\n"));
    });
    Host host;
    auto pool = PeerPool(host, peer.address);
    auto taken = host.Call([&] { return bbt_co_await PoolAccess::Take(*pool, Until()); });
    CHECK(taken);
    std::optional<redis::CoRedisPool::Borrow> held(std::move(taken).value());
    NativeBarrier paused;
    PoolAccess::SetWaitHook(*pool, [&](int phase) { if (phase == 2) { paused.Pause(); } });
    std::promise<bbt::coroutine::detail::Coroutine*> coroutine;
    auto task = coroutine.get_future();
    auto b = host.Submit([&] { coroutine.set_value(g_bbt_tls_coroutine_co); return bbt_co_await pool->Ping(Until()); });
    auto* current = Get(task);
    CHECK(WaitUntil([&] { return PoolAccess::Queued(*pool) == 1; }));
    current->RequestCancel();
    CHECK(WaitUntil([&] { return paused.entered.load(); }));
    held.reset(); // B 已取消，尚未出账；native 归还仍把槽 handoff 给 B。
    CHECK(PoolAccess::Queued(*pool) == 0);
    paused.Release();
    ExpectCode(Get(b), ErrorCode::Cancelled);
    PoolAccess::SetWaitHook(*pool, {});
    CHECK(host.Call([&] { return bbt_co_await pool->Ping(Until()); }));
    pool->Close(); pool.reset(); peer.Join(); host.Finish();
    std::cout << "pool_cancel_handoff cancelled_before_return=1 handed_before_cleanup=1 slot_recovered=1\n";
}

void PoolRepairWaiter() {
    std::atomic<bool> received{false}, fail{false};
    unsigned sessions = 0;
    Peer peer([&](int fd) {
        if (++sessions == 1) {
            CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"PING"}));
            received.store(true);
            CHECK(WaitUntil([&] { return fail.load(); }));
            return;
        }
        CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"PING"}));
        CHECK(Peer::Send(fd, "+PONG\r\n"));
    }, 2);
    Host host;
    auto pool = PeerPool(host, peer.address);
    auto a = host.Submit([&] { return bbt_co_await pool->Ping(Until()); });
    CHECK(WaitUntil([&] { return received.load(); }));
    auto b = host.Submit([&] { return bbt_co_await pool->Ping(Until()); });
    CHECK(WaitUntil([&] { return PoolAccess::Queued(*pool) == 1; }));
    fail.store(true);
    auto failed = Get(a); CHECK(!failed); CheckUnknown(failed.error(), "TransportError");
    CHECK(host.Call([&] { return bbt_co_await pool->Connect(Until()); }));
    CHECK(Get(b)); // 不发送第三个命令，显式恢复必须主动服务 B。
    CHECK(PoolAccess::Queued(*pool) == 0);
    pool->Close(); pool.reset(); peer.Join(); host.Finish();
    CHECK(sessions == 2);
    std::cout << "pool_repair queued_before_failure=1 explicit_repair_wakes_without_new_command=1 no_replay=1\n";
}

void PoolConnectRollback() {
    Host host;
    Endpoint listener(true);
    unsigned dials = 0;
    net::CoTestAccess::SetDialBarrier([&] {
        if (++dials == 2) { g_bbt_tls_coroutine_co->RequestCancel(); }
    });
    auto created = redis::CoRedisPool::Create(host.engine, listener.address, {2, 1}); CHECK(created);
    auto pool = std::move(created).value();
    auto connected = host.Call([&] { return bbt_co_await pool->Connect(Until()); });
    ExpectCode(connected, ErrorCode::Cancelled);
    CHECK(dials == 2); // 第二次拨号前第一条已成功；取消只作用于第二次。
    net::CoTestAccess::SetDialBarrier({});
    ExpectCode(host.Call([&] { return bbt_co_await pool->Ping(Until()); }), ErrorCode::RuntimeUnavailable);
    CHECK(WaitUntil([&] { return host.engine->CountConnections() == 0 && Access::ConnectTableSize(*host.engine) == 0; }));
    CHECK(host.Call([&] { return bbt_co_await pool->Connect(Until()); }));
    CHECK(host.engine->CountConnections() == 2);
    pool->Close(); pool.reset(); host.Finish();
    std::cout << "pool_rollback first_connected_second_cancelled=1 all_rolled_back=1 explicit_retry_ready=1\n";
}

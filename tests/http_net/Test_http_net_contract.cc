// http_net 契约测试：GET/POST 连接复用、池排队/超时/过载、关闭与取消竞争、
// 坏流退役、keepalive 过期。全部走自有进程动态端口的真实 loopback HTTP 对端，
// 目标阶段用真实请求到达/池状态屏障证明，不用固定 sleep 冒充时序。
#include "TestSupport.hpp"
#include "../net/ProcessRuntimeTestMain.hpp"

#include <bbt/coroutine/detail/Coroutine.hpp>

#include <cctype>

using namespace http_net_test;
namespace http = bbt::infra::http;

namespace {

bool EqualsIgnoreCase(std::string_view left, std::string_view right) {
    if (left.size() != right.size()) { return false; }
    for (std::size_t i = 0; i < left.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(left[i])) !=
            std::tolower(static_cast<unsigned char>(right[i]))) {
            return false;
        }
    }
    return true;
}

bool HasHeader(const http::Headers& headers, const std::string& name,
               const std::string& value) {
    for (const auto& header : headers) {
        if (EqualsIgnoreCase(header.first, name) && header.second == value) { return true; }
    }
    return false;
}

http::ClientOptions ClientOptionsFor(const HttpPeer& peer, http::PoolLimits limits) {
    http::ClientOptions options;
    options.base_url = peer.base_url();
    options.headers = {{"X-Default", "d1"}};
    options.params = {{"client", "1"}};
    options.limits = limits;
    return options;
}

// GET/POST/base_url/默认头/params 编码/连接复用（服务端 accept 总数与单连接
// 请求数为真实证据，不是 client 自述）。
void GetPostReuse() {
    Watchdog watchdog;
    HttpPeer peer;
    Host host;
    const auto sockets_base = SocketCount();
    auto created = http::Client::Create(host.engine, ClientOptionsFor(peer, {}));
    CHECK(created);
    auto client = std::move(created).value();

    auto first = host.Call([&] {
        http::RequestOptions options;
        options.params = {{"page", "1"}, {"q", "hello world"}};
        options.headers = {{"X-Request", "r1"}};
        return bbt_co_await client->Get("/items", options, Until());
    });
    CHECK(first);
    CHECK(first.value().status_code == 200);
    CHECK(first.value().Ok());
    CHECK(first.value().body == "peer-default");
    CHECK(HasHeader(first.value().headers, "Content-Length", "12"));

    auto second = host.Call([&] {
        http::RequestOptions options;
        options.params = {{"page", "2"}};
        return bbt_co_await client->Get("/items", options, Until());
    });
    CHECK(second);
    CHECK(second.value().body == "peer-default");

    // 复用证据：一次 accept、两个请求在同一条连接上。
    CHECK(peer.accept_total() == 1);
    CHECK(peer.max_requests_on_one_connection() == 2);
    CHECK(peer.requests_total() == 2);

    auto requests = peer.requests();
    CHECK(requests.size() == 2);
    CHECK(requests[0].method == "GET");
    // params merge：client 在前，请求级追加（对齐 httpx merge_queryparams 顺序）；
    // 空格 %20 编码。
    CHECK(requests[0].target == "/items?client=1&page=1&q=hello%20world");
    CHECK(HasHeader(requests[0].headers, "X-Default", "d1"));
    CHECK(HasHeader(requests[0].headers, "X-Request", "r1"));
    CHECK(HasHeader(requests[0].headers, "Host", peer.base_url().substr(7)));
    CHECK(requests[1].target == "/items?client=1&page=2");
    CHECK(HasHeader(requests[1].headers, "X-Request", "r1") == false);

    // POST：服务端校验收到的字节后回显。
    peer.set_canned([](const HttpPeer::Recorded& request) {
        return HttpPeer::HttpResponse(201, "Created", "echo:" + request.body);
    });
    auto posted = host.Call([&] {
        http::RequestOptions options;
        http::RequestBody body;
        body.content = "raw-bytes-\x01\x02";
        body.content_type = "text/plain";
        return bbt_co_await client->Post("/submit", body, options, Until());
    });
    CHECK(posted);
    CHECK(posted.value().status_code == 201);
    CHECK(posted.value().body == "echo:raw-bytes-\x01\x02");
    requests = peer.requests();
    CHECK(requests.size() == 3);
    CHECK(requests[2].method == "POST");
    CHECK(requests[2].target == "/submit?client=1");
    CHECK(requests[2].body == std::string("raw-bytes-\x01\x02"));
    CHECK(HasHeader(requests[2].headers, "Content-Type", "text/plain"));

    const auto stats = client->Stats();
    CHECK(stats.idle == 1);
    CHECK(stats.active == 0);
    CHECK(stats.budget == 1);

    client->Close();
    peer.Stop();
    host.Finish();
    CHECK(WaitUntil([&] { return SocketCount() <= sockets_base; }));
    std::cout << "get_post_reuse accepts=1 single_conn=2(post=3) params_encoded=1\n";
}

// 池 max 满→排队→过载→pool 超时；真实屏障：服务端扣留首个响应，请求字节
// 已到对端才提交排队者。
void PoolQueueTimeout() {
    Watchdog watchdog;
    HttpPeer peer;
    Host host;
    const auto sockets_base = SocketCount();
    http::PoolLimits limits;
    limits.max_connections = 1;
    limits.max_keepalive_connections = 1;
    limits.max_queue = 1;
    limits.keepalive_expiry = std::chrono::milliseconds{30000};
    auto created = http::Client::Create(host.engine, ClientOptionsFor(peer, limits));
    CHECK(created);
    auto client = std::move(created).value();

    std::atomic<bool> release{false};
    std::atomic<bool> first_seen{false};
    peer.PushConnectionHook([&](int fd) {
        auto request = HttpPeer::ReadRequestForHook(fd);
        CHECK(request);
        CHECK(request->target == "/slow?client=1");
        first_seen.store(true);
        while (!release.load()) { std::this_thread::sleep_for(1ms); }
        CHECK(HttpPeer::SendForHook(fd, HttpPeer::HttpResponse(200, "OK", "slow")));
        // 响应后保持连接可复用（继续服务后续请求），保持 keepalive 语义。
        for (;;) {
            auto next = HttpPeer::ReadRequestForHook(fd);
            if (!next) { break; }
            CHECK(HttpPeer::SendForHook(fd, HttpPeer::HttpResponse(200, "OK", "fast")));
        }
    });

    auto a = host.Submit([&] {
        return bbt_co_await client->Get("/slow", http::RequestOptions{}, Until(5s));
    });
    // 阶段证据：请求字节已到对端且池 active=1，才提交排队者。
    CHECK(WaitUntil([&] { return first_seen.load(); }));
    CHECK(WaitUntil([&] { return client->Stats().active == 1; }));

    CallOptions short_pool;
    short_pool.deadline = std::chrono::steady_clock::now() + 5s;
    http::RequestOptions b_options;
    b_options.timeout = http::Timeout{};
    b_options.timeout->pool = http::PhaseTimeout::Milliseconds(std::chrono::milliseconds{300});
    auto b = host.Submit([&] {
        return bbt_co_await client->Get("/queued", b_options, short_pool);
    });
    CHECK(WaitUntil([&] { return client->Stats().waiting == 1; }));

    // 排队已满：第三个立即 Overloaded，不排队。
    ExpectCode(host.Call([&] {
        return bbt_co_await client->Get("/overload", http::RequestOptions{}, Until(5s));
    }), ErrorCode::Overloaded);

    // b 在 300ms pool 档到期 → TimedOut（不是 Closed/Overloaded）。
    ExpectCode(Get(b), ErrorCode::TimedOut);
    CHECK(WaitUntil([&] { return client->Stats().waiting == 0; }));

    // 释放屏障：a 拿到响应；连接完整回池。
    release.store(true);
    auto a_result = Get(a);
    CHECK(a_result);
    CHECK(a_result.value().body == "slow");
    CHECK(WaitUntil([&] { return client->Stats().idle == 1; }));

    // 空闲连接复用：不再新建（accept 仍为 1；hook 续接服务返回 "fast"）。
    auto again = host.Call([&] {
        return bbt_co_await client->Get("/fast", http::RequestOptions{}, Until());
    });
    CHECK(again);
    CHECK(again.value().body == "fast");
    CHECK(peer.accept_total() == 1);

    client->Close();
    peer.Stop();
    host.Finish();
    CHECK(WaitUntil([&] { return SocketCount() <= sockets_base; }));
    std::cout << "pool_queue_timeout overload=1 pool_timeout=1 reuse_after=1\n";
}

// 关闭/取消与在飞、排队请求的竞争：结果唯一、等待者全部出账、Engine 收口。
void CloseCancelRace() {
    Watchdog watchdog;
    HttpPeer peer;
    Host host;
    const auto sockets_base = SocketCount();
    http::PoolLimits limits;
    limits.max_connections = 1;
    limits.max_keepalive_connections = 1;
    limits.max_queue = 2;
    limits.keepalive_expiry = std::nullopt;
    auto created = http::Client::Create(host.engine, ClientOptionsFor(peer, limits));
    CHECK(created);
    auto client = std::move(created).value();

    std::atomic<bool> release{false};
    std::atomic<bool> first_seen{false};
    peer.PushConnectionHook([&](int fd) {
        auto request = HttpPeer::ReadRequestForHook(fd);
        CHECK(request);
        first_seen.store(true);
        while (!release.load()) { std::this_thread::sleep_for(1ms); }
        CHECK(HttpPeer::SendForHook(fd, HttpPeer::HttpResponse(200, "OK", "late")));
    });

    // a 占池；请求字节已到对端。
    auto a = host.Submit([&] {
        return bbt_co_await client->Get("/inflight", http::RequestOptions{}, Until(5s));
    });
    CHECK(WaitUntil([&] { return first_seen.load(); }));
    CHECK(WaitUntil([&] { return client->Stats().active == 1; }));

    // b 排队；拿到协程句柄后外部取消。
    std::promise<bbt::coroutine::detail::Coroutine*> co_promise;
    auto co_future = co_promise.get_future();
    auto b = host.Submit([&] {
        co_promise.set_value(g_bbt_tls_coroutine_co);
        return bbt_co_await client->Get("/queued-cancel", http::RequestOptions{}, Until(5s));
    });
    CHECK(WaitUntil([&] { return client->Stats().waiting == 1; }));
    auto* current = Get(co_future);
    CHECK(current != nullptr);
    current->RequestCancel();
    ExpectCode(Get(b), ErrorCode::Cancelled);
    CHECK(WaitUntil([&] { return client->Stats().waiting == 0; }));

    // c 排队后 Client 封口：Closed 一次出账，不悬挂。
    auto c = host.Submit([&] {
        return bbt_co_await client->Get("/queued-close", http::RequestOptions{}, Until(5s));
    });
    CHECK(WaitUntil([&] { return client->Stats().waiting == 1; }));
    client->Close();
    ExpectCode(Get(c), ErrorCode::Closed);
    CHECK(client->IsClosed());

    // 在飞 a 持借用完成（封口不抢在飞请求）；服务端正常响应。
    release.store(true);
    auto a_result = Get(a);
    CHECK(a_result);
    CHECK(a_result.value().body == "late");

    // 封口后新请求立即 Closed；重复 Close 幂等。
    ExpectCode(host.Call([&] {
        return bbt_co_await client->Get("/after-close", http::RequestOptions{}, Until(5s));
    }), ErrorCode::Closed);
    client->Close();

    peer.Stop();
    host.Finish();
    CHECK(WaitUntil([&] { return SocketCount() <= sockets_base; }));
    std::cout << "close_cancel_race cancelled=1 closed_waiter=1 inflight_completed=1\n";
}

// 坏流退役：非 HTTP 字节→ProtocolError→连接退役不回池；下一请求新建连接成功。
void BadStreamRetire() {
    Watchdog watchdog;
    HttpPeer peer;
    Host host;
    const auto sockets_base = SocketCount();
    http::PoolLimits limits;
    limits.max_connections = 2;
    limits.max_keepalive_connections = 1;
    limits.keepalive_expiry = std::nullopt;
    auto created = http::Client::Create(host.engine, ClientOptionsFor(peer, limits));
    CHECK(created);
    auto client = std::move(created).value();

    peer.PushConnectionHook([](int fd) {
        // 坏流：合法 HTTP 头前面是垃圾字节，Beast 解析必须拒绝。
        (void)HttpPeer::SendForHook(fd, "GARBAGE NOT HTTP\r\n\r\n");
        ::shutdown(fd, SHUT_RDWR);
    });

    auto bad = host.Call([&] {
        return bbt_co_await client->Get("/poisoned", http::RequestOptions{}, Until());
    });
    ExpectCode(bad, ErrorCode::ProtocolError);
    // 坏流不能回池。owner 可已完成回收，故不强求异步 retiring 瞬态仍可见；
    // 若尚未回收，唯一连接必须仍记账，完成则为零。
    const auto retired = client->Stats();
    CHECK(retired.idle == 0 && retired.active == 0 && retired.dialing == 0);
    CHECK(retired.retiring <= 1 && retired.budget == retired.retiring);

    // 退役后新建连接成功；退役连接没有被复用（第一次 accept 是坏流连接）。
    auto good = host.Call([&] {
        return bbt_co_await client->Get("/fresh", http::RequestOptions{}, Until());
    });
    CHECK(good);
    CHECK(good.value().body == "peer-default");
    CHECK(peer.accept_total() == 2);
    CHECK(peer.requests_total() == 1);
    CHECK(peer.max_requests_on_one_connection() == 1);
    CHECK(WaitUntil([&] { return client->Stats().idle == 1; }));
    // 退役连接物理收口观察后出账：预算只剩新连接。
    CHECK(WaitUntil([&] { return client->Stats().budget == 1; }, 5s));

    client->Close();
    peer.Stop();
    host.Finish();
    CHECK(WaitUntil([&] { return SocketCount() <= sockets_base; }));
    std::cout << "bad_stream_retire protocol_error=1 retired=1 redial=1\n";
}

// F1：省略最终 CallOptions 走真实成功请求（Deadline::max() 缺省），且显式
// CallOptions{}（epoch 期限）在池/I/O 前拒绝、对端零请求。两种形态分别证明
// 「省略参数」与「显式过期」是不同语义。
void OmittedCallOptionsDeadline() {
    Watchdog watchdog;
    HttpPeer peer;
    Host host;
    const auto sockets_base = SocketCount();
    auto created = http::Client::Create(host.engine, ClientOptionsFor(peer, {}));
    CHECK(created);
    auto client = std::move(created).value();

    // 省略 call：Get 双参形态，真实成功（先前缺省 epoch 必 TimedOut）。
    auto omitted_get = host.Call([&] {
        return bbt_co_await client->Get("/omitted", http::RequestOptions{});
    });
    CHECK(omitted_get);
    CHECK(omitted_get.value().status_code == 200);
    CHECK(omitted_get.value().body == "peer-default");

    // 省略 call：Send 直接形态（BuildRequest 产物复用同一路径）。
    auto built = host.Call([&] {
        auto prepared = client->BuildRequest("GET", "/omitted-send", http::RequestOptions{});
        CHECK(prepared);
        return bbt_co_await client->Send(prepared.value());
    });
    CHECK(built);
    CHECK(built.value().body == "peer-default");
    CHECK(peer.accept_total() == 1);
    CHECK(peer.requests_total() == 2);

    // 显式 CallOptions{}：deadline=epoch 已过期，CheckCall 在池借用与序列化
    // 发送之前拒绝；对端观察零新请求（accept/requests 不变）。
    const auto accepts_before = peer.accept_total();
    const auto requests_before = peer.requests_total();
    auto expired = host.Call([&] {
        return bbt_co_await client->Get("/expired", http::RequestOptions{}, CallOptions{});
    });
    ExpectCode(expired, ErrorCode::TimedOut);
    CHECK(peer.accept_total() == accepts_before);
    CHECK(peer.requests_total() == requests_before);

    // Send 路径同样显式过期拒绝。
    auto expired_send = host.Call([&] {
        auto prepared = client->BuildRequest("GET", "/expired-send", http::RequestOptions{});
        CHECK(prepared);
        return bbt_co_await client->Send(prepared.value(), CallOptions{});
    });
    ExpectCode(expired_send, ErrorCode::TimedOut);
    CHECK(peer.requests_total() == requests_before);

    client->Close();
    peer.Stop();
    host.Finish();
    CHECK(WaitUntil([&] { return SocketCount() <= sockets_base; }));
    std::cout << "omitted_call_options get_ok=1 send_ok=1 expired_rejected=2 peer_zero_new=1\n";
}

// keepalive 过期：expiry=0 → 响应后连接立即过期，下一请求必须新建连接
// （httpcore immediate_expiry 的等价验收，走真实 sweep 而不是 mock）。
void KeepaliveExpiry() {
    Watchdog watchdog;
    HttpPeer peer;
    Host host;
    const auto sockets_base = SocketCount();
    http::PoolLimits limits;
    limits.max_connections = 4;
    limits.max_keepalive_connections = 2;
    limits.keepalive_expiry = std::chrono::milliseconds{0};
    auto created = http::Client::Create(host.engine, ClientOptionsFor(peer, limits));
    CHECK(created);
    auto client = std::move(created).value();

    auto first = host.Call([&] {
        return bbt_co_await client->Get("/one", http::RequestOptions{}, Until());
    });
    CHECK(first);
    // 读尽即过期：下一次分配 sweep 必然退役，不会复用。
    auto second = host.Call([&] {
        return bbt_co_await client->Get("/two", http::RequestOptions{}, Until());
    });
    CHECK(second);
    CHECK(peer.accept_total() == 2);
    CHECK(peer.requests_total() == 2);
    CHECK(peer.max_requests_on_one_connection() == 1);
    // expiry=0：归还即过期，观察面永不出现 idle（sweep 退役），也不会复用。
    CHECK(client->Stats().idle == 0);

    client->Close();
    peer.Stop();
    host.Finish();
    CHECK(WaitUntil([&] { return SocketCount() <= sockets_base; }));
    std::cout << "keepalive_expiry immediate=1 dials=2\n";
}

int Run(int argc, char** argv) {
    Watchdog watchdog;
    try {
        CHECK(argc == 2);
        g_bbt_coroutine_config->m_cfg_static_thread_num = 1;
        g_bbt_coroutine_config->m_cfg_stack_size = 128 * 1024;
        g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
        g_scheduler->Start();
        {
            // 延迟初始化后建立稳定基线（对齐 redis_net：warm host 先推进一次）。
            Host warm;
            CHECK(warm.Call([] {
                auto waiter = bbt::coroutine::sync::CoWaiter::Create();
                CHECK(waiter);
                const auto status = waiter->WaitWithCallback(
                    {std::chrono::steady_clock::now() + 5ms}, [] { return true; });
                CHECK(status == bbt::coroutine::WaitStatus::TimedOut);
                return true;
            }));
        }
        const std::string test_case = argv[1];
        if (test_case == "get_post_reuse") { GetPostReuse(); }
        else if (test_case == "pool_queue_timeout") { PoolQueueTimeout(); }
        else if (test_case == "close_cancel_race") { CloseCancelRace(); }
        else if (test_case == "bad_stream_retire") { BadStreamRetire(); }
        else if (test_case == "keepalive_expiry") { KeepaliveExpiry(); }
        else if (test_case == "omitted_call_options") { OmittedCallOptionsDeadline(); }
        else { CHECK(false); }
    } catch (const std::exception& error) {
        std::cerr << "http_net_test exception: " << error.what() << '\n';
        return 1;
    }
    std::cout << "http_net_test case=" << argv[1] << " PASS\n";
    return 0;
}

} // namespace

int main(int argc, char** argv) {
    return bbt_net_test::RunTestProcessExit([&] { return Run(argc, argv); });
}

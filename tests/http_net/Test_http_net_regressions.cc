// 审查发现的协议、输入、期限及物理预算回归；只操作真实动态端口对端。
#include "TestSupport.hpp"
#include "../net/ProcessRuntimeTestMain.hpp"
#include <bbt/coroutine/detail/Coroutine.hpp>

using namespace http_net_test;
namespace http = bbt::infra::http;

namespace {

struct Fixture {
    Host host;
    HttpPeer peer;
    std::shared_ptr<http::Client> client;
    std::size_t baseline{SocketCount()};

    explicit Fixture(http::ClientOptions options = {}, unsigned maximum = 1) {
        options.base_url = peer.base_url();
        options.limits.max_connections = maximum;
        options.limits.max_keepalive_connections = 1;
        options.limits.keepalive_expiry = std::nullopt;
        auto made = http::Client::Create(host.engine, options);
        CHECK(made);
        client = made.value();
    }
    auto GetPath(const std::string& path = "/test") {
        return host.Call([&] { return bbt_co_await client->Get(path, {}, Until()); });
    }
    void Finish() {
        client->Close();
        peer.Stop();
        host.Finish();
        CHECK(WaitUntil([&] { return SocketCount() <= baseline; }));
    }
};

void Protocol(const std::string& mode) {
    Fixture f;
    if (mode == "fragment_header" || mode == "fragment_body") {
        std::promise<void> release;
        auto ready = release.get_future().share();
        std::atomic<bool> first_sent{false};
        f.peer.PushConnectionHook([&, ready](int fd) {
            CHECK(HttpPeer::ReadRequestForHook(fd));
            const bool header = mode == "fragment_header";
            CHECK(HttpPeer::SendForHook(fd, header ? "HTTP/1.1 200 O"
                : "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nAB"));
            first_sent = true;
            ready.wait();
            CHECK(HttpPeer::SendForHook(fd, header ? "K\r\nContent-Length: 4\r\n\r\nABCD" : "CD"));
        });
        auto call = f.host.Submit([&] { return bbt_co_await f.client->Get("/fragment", {}, Until()); });
        CHECK(WaitUntil([&] { return first_sent.load(); }));
        // 已收完整请求且首片已发；扣住后片时请求应仍待完成，而不是 need_more 错误。
        CHECK(call.wait_for(30ms) == std::future_status::timeout);
        release.set_value();
        auto response = Get(call);
        CHECK(response);
        CHECK(response.value().status_code == 200 && response.value().body == "ABCD");
    } else if (mode == "chunked") {
        f.peer.set_canned([](const HttpPeer::Recorded&) {
            std::string bytes = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
            for (unsigned i = 0; i != 64; ++i) { bytes += "1\r\nx\r\n"; }
            return bytes + "0\r\nX-Trailer: yes\r\n\r\n";
        });
        for (unsigned i = 0; i != 2; ++i) {
            auto response = f.GetPath();
            CHECK(response && response.value().body == std::string(64, 'x'));
        }
        CHECK(f.peer.accept_total() == 1);
    } else if (mode == "head_interim") {
        f.peer.set_canned([](const HttpPeer::Recorded& request) {
            if (request.method == "HEAD") {
                return std::string("HTTP/1.1 200 OK\r\nContent-Length: 123\r\n\r\n");
            }
            return "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\n\r\n" +
                HttpPeer::HttpResponse(200, "OK", request.target);
        });
        auto head = f.host.Call([&] { return bbt_co_await f.client->Request("HEAD", "/head", {}, Until()); });
        CHECK(head && head.value().status_code == 200 && head.value().body.empty());
        for (const auto& target : {"/one", "/two"}) {
            auto response = f.GetPath(target);
            CHECK(response && response.value().status_code == 200 && response.value().body == target);
        }
        CHECK(f.peer.accept_total() == 1 && f.peer.requests_total() == 3);
    } else if (mode == "eof_truncated") {
        f.peer.PushConnectionHook([](int fd) {
            CHECK(HttpPeer::ReadRequestForHook(fd));
            CHECK(HttpPeer::SendForHook(fd,
                "HTTP/1.0 200 OK\r\nConnection: close\r\n\r\nEOF-BODY"));
        });
        auto eof = f.GetPath();
        CHECK(eof && eof.value().body == "EOF-BODY");
        f.peer.PushConnectionHook([](int fd) {
            CHECK(HttpPeer::ReadRequestForHook(fd));
            CHECK(HttpPeer::SendForHook(fd,
                "HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nshort"));
        });
        auto partial = f.GetPath();
        ExpectCode(partial, ErrorCode::OutcomeUnknown);
        CHECK(partial.error().request_phase == RequestPhase::RequestCommitted);
        auto again = f.GetPath();
        CHECK(again && again.value().body == "peer-default");
        CHECK(f.peer.accept_total() == 3);
    } else if (mode == "body_limit") {
        http::ClientOptions options;
        options.base_url = f.peer.base_url();
        options.max_body_bytes = 3;
        auto small = http::Client::Create(f.host.engine, options);
        CHECK(small);
        auto rejected = f.host.Call([&] { return bbt_co_await small.value()->Get("/large", {}, Until()); });
        ExpectCode(rejected, ErrorCode::ProtocolError);
        f.peer.set_canned([](const HttpPeer::Recorded&) { return HttpPeer::HttpResponse(200, "OK", "ok"); });
        auto good = f.host.Call([&] { return bbt_co_await small.value()->Get("/small", {}, Until()); });
        CHECK(good && good.value().body == "ok");
        small.value()->Close();
    } else { CHECK(false); }
    f.Finish();
}

void InputBoundary() {
    Fixture f;
    http::ClientOptions bad_defaults;
    bad_defaults.headers = {{"X-Test", "safe\r\nInjected: yes"}};
    CHECK(!http::Client::Create(f.host.engine, bad_defaults));
    CHECK(!f.client->BuildRequest("BAD METHOD", "/", {}));
    CHECK(!f.client->BuildRequest("GET", "http://127.0.0.1:80garbage/", {}));
    CHECK(!f.client->BuildRequest("GET", "/with space", {}));
    CHECK(!f.client->BuildRequest("GET", "https://127.0.0.1/", {}));
    const std::vector<http::Headers> invalid{
        {{"X-Test", "safe\r\nInjected: yes"}}, {{"Bad Name", "value"}},
        {{"X-Nul", std::string("a\0b", 3)}}, {{"X-Huge", std::string(70000, 'x')}},
        {{"Content-Length", "100"}}, {{"Transfer-Encoding", "chunked"}}
    };
    for (const auto& headers : invalid) {
        http::RequestOptions options;
        options.headers = headers;
        CHECK(!f.client->BuildRequest("GET", "/invalid", options));
        auto built = f.client->BuildRequest("GET", "/valid", {});
        CHECK(built);
        built.value().headers = headers; // Send 也验证，不信任可变的 BuildRequest 产物。
        auto rejected = f.host.Call([&] { return bbt_co_await f.client->Send(built.value(), Until()); });
        ExpectCode(rejected, ErrorCode::InvalidArgument);
    }
    auto changed = f.client->BuildRequest("GET", "/valid", {});
    CHECK(changed);
    changed.value().target_for_wire = "/bad\r\nInjected: yes";
    ExpectCode(f.host.Call([&] { return bbt_co_await f.client->Send(changed.value(), Until()); }),
               ErrorCode::InvalidArgument);
    auto bad_type = f.host.Call([&] {
        return bbt_co_await f.client->Post("/bad", {"body", "x\r\nInjected: yes"}, {}, Until());
    });
    ExpectCode(bad_type, ErrorCode::InvalidArgument);
    CHECK(f.client->Stats().budget == 0 && f.peer.accept_total() == 0);
    CHECK(f.GetPath());
    CHECK(f.GetPath());
    CHECK(f.client->Stats().active == 0 && f.client->Stats().idle == 1);
    CHECK(f.peer.accept_total() == 1 && f.peer.requests_total() == 2);
    f.Finish();
}

void PreexpiredCancelled() {
    Fixture f;
    auto reject = [&](bool cancel) {
        return f.host.Call([&] {
            if (cancel) { g_bbt_tls_coroutine_co->RequestCancel(); }
            const auto call = cancel ? Until() : CallOptions{std::chrono::steady_clock::now() - 1s};
            return bbt_co_await f.client->Post("/must-not-send", {"side-effect", ""}, {}, call);
        });
    };
    ExpectCode(reject(false), ErrorCode::TimedOut);
    ExpectCode(reject(true), ErrorCode::Cancelled);
    CHECK(f.peer.accept_total() == 0 && f.client->Stats().budget == 0);
    CHECK(Access::ConnectTableSize(*f.host.engine) == 0);
    CHECK(f.GetPath("/warm"));
    ExpectCode(reject(false), ErrorCode::TimedOut);
    ExpectCode(reject(true), ErrorCode::Cancelled);
    CHECK(f.GetPath("/after-rejections"));
    CHECK(f.peer.accept_total() == 1 && f.peer.requests_total() == 2);
    for (const auto& request : f.peer.requests()) { CHECK(request.method == "GET"); }
    f.Finish();
}

void PhaseTimeouts() {
    Fixture f;
    std::promise<void> release;
    auto ready = release.get_future().share();
    std::atomic<bool> first_seen{false};
    f.peer.PushConnectionHook([&, ready](int fd) {
        CHECK(HttpPeer::ReadRequestForHook(fd));
        first_seen = true;
        ready.wait();
        CHECK(HttpPeer::SendForHook(fd, HttpPeer::HttpResponse(200, "OK", "first")));
        CHECK(HttpPeer::ReadRequestForHook(fd));
        CHECK(HttpPeer::SendForHook(fd, HttpPeer::HttpResponse(200, "OK", "queued")));
    });
    auto first = f.host.Submit([&] { return bbt_co_await f.client->Get("/first", {}, Until()); });
    CHECK(WaitUntil([&] { return first_seen.load(); }));
    http::RequestOptions options;
    options.timeout = http::Timeout::Uniform(http::PhaseTimeout::Milliseconds(60ms));
    options.timeout->pool = http::PhaseTimeout::Milliseconds(1s);
    auto queued = f.host.Submit([&] { return bbt_co_await f.client->Get("/queued", options, Until()); });
    CHECK(WaitUntil([&] { return f.client->Stats().waiting == 1; }));
    // 排队阶段已成立；刻意持池超过后续阶段档，后续 write/read 仍各有 60ms。
    CHECK(queued.wait_for(160ms) == std::future_status::timeout);
    release.set_value();
    CHECK(Get(first));
    auto response = Get(queued);
    CHECK(response && response.value().body == "queued");
    CHECK(f.peer.accept_total() == 1);
    f.Finish();
}

void PerReadAndTotal() {
    Fixture f;
    std::atomic<bool> request_seen{false};
    f.peer.PushConnectionHook([&](int fd) {
        CHECK(HttpPeer::ReadRequestForHook(fd));
        request_seen = true;
        CHECK(HttpPeer::SendForHook(fd, "HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\n"));
        // 这里的间隔是被测输入（慢分片），不是用 sleep 冒充进入请求阶段。
        for (unsigned i = 0; i != 6; ++i) {
            std::this_thread::sleep_for(60ms);
            CHECK(HttpPeer::SendForHook(fd, "x"));
        }
    });
    http::RequestOptions options;
    options.timeout = http::Timeout{};
    options.timeout->read = http::PhaseTimeout::Milliseconds(200ms);
    auto reading = f.host.Submit([&] { return bbt_co_await f.client->Get("/slow-chunks", options, Until()); });
    CHECK(WaitUntil([&] { return request_seen.load(); }));
    auto response = Get(reading);
    CHECK(response && response.value().body == "xxxxxx");
    // 总期限在真正提交后到期，必须保留 RequestCommitted/OutcomeUnknown，不能重试。
    std::promise<void> release;
    auto ready = release.get_future().share();
    std::atomic<bool> committed{false};
    f.peer.PushConnectionHook([&, ready](int fd) {
        CHECK(HttpPeer::ReadRequestForHook(fd));
        committed = true;
        ready.wait();
    });
    auto total = f.host.Submit([&] {
        return bbt_co_await f.client->Post("/total", {"effect", ""}, {}, Until(150ms));
    });
    CHECK(WaitUntil([&] { return committed.load(); }));
    auto expired = Get(total);
    ExpectCode(expired, ErrorCode::OutcomeUnknown);
    CHECK(expired.error().request_phase == RequestPhase::RequestCommitted);
    release.set_value();
    f.Finish();
}

void CrossOrigin() {
    Fixture f;
    {
        HttpPeer other;
        CHECK(f.GetPath("/a"));
        auto b = f.GetPath(other.base_url() + "/b");
        CHECK(b && b.value().body == "peer-default");
        CHECK(f.GetPath("/a-again"));
        CHECK(f.peer.accept_total() == 2 && other.accept_total() == 1);
        CHECK(f.client->Stats().budget <= 1);
        other.Stop();
    }
    f.Finish();
}

void HandoffConcurrency() {
    Fixture f;
    std::promise<void> release;
    auto ready = release.get_future().share();
    std::atomic<bool> first_seen{false};
    f.peer.PushConnectionHook([&, ready](int fd) {
        CHECK(HttpPeer::ReadRequestForHook(fd));
        first_seen = true;
        ready.wait();
        CHECK(HttpPeer::SendForHook(fd, HttpPeer::HttpResponse(200, "OK", "ok")));
        for (unsigned i = 0; i != 8; ++i) {
            CHECK(HttpPeer::ReadRequestForHook(fd));
            CHECK(HttpPeer::SendForHook(fd, HttpPeer::HttpResponse(200, "OK", "ok")));
        }
    });
    auto first = f.host.Submit([&] { return bbt_co_await f.client->Get("/first", {}, Until()); });
    CHECK(WaitUntil([&] { return first_seen.load(); }));
    std::vector<std::future<result<http::Response>>> waiting;
    for (unsigned i = 0; i != 8; ++i) {
        waiting.push_back(f.host.Submit([&] { return bbt_co_await f.client->Get("/handoff", {}, Until()); }));
    }
    CHECK(WaitUntil([&] { return f.client->Stats().waiting == waiting.size(); }));
    release.set_value();
    CHECK(Get(first));
    for (auto& future : waiting) { auto value = Get(future); CHECK(value && value.value().body == "ok"); }
    CHECK(f.peer.accept_total() == 1);
    CHECK(f.client->Stats().active == 0 && f.client->Stats().waiting == 0);
    f.Finish();
}

void PhysicalBudget(unsigned maximum) {
    Fixture f({}, maximum);
    Access::SetConnectDeliveryDelay(*f.host.engine, 400ms);
    http::RequestOptions options;
    options.timeout = http::Timeout{};
    options.timeout->connect = http::PhaseTimeout::Milliseconds(20ms);
    options.timeout->pool = http::PhaseTimeout::Milliseconds(40ms);
    for (unsigned i = 0; i != maximum; ++i) {
        auto dialing = f.host.Submit([&] { return bbt_co_await f.client->Get("/late", options, Until()); });
        CHECK(WaitUntil([&] { return f.peer.accept_total() == i + 1; }));
        ExpectCode(Get(dialing), ErrorCode::TimedOut);
    }
    CHECK(f.client->Stats().budget == maximum && f.client->Stats().retiring == maximum);
    for (unsigned i = 0; i != 3; ++i) {
        auto blocked = f.host.Call([&] { return bbt_co_await f.client->Get("/blocked", options, Until()); });
        ExpectCode(blocked, ErrorCode::TimedOut);
        CHECK(f.peer.accept_total() == maximum && f.client->Stats().budget == maximum);
        CHECK(SocketCount() <= f.baseline + 2 * maximum);
    }
    Access::SetConnectDeliveryDelay(*f.host.engine, 0ms);
    CHECK(WaitUntil([&] { return f.client->Stats().budget == 0; }));
    CHECK(f.GetPath("/recovered"));
    CHECK(f.GetPath("/reused"));
    CHECK(f.peer.accept_total() == maximum + 1 && f.peer.requests_total() == 2);
    f.Finish();
}

void PhysicalCancel() {
    Fixture f;
    Access::SetConnectDeliveryDelay(*f.host.engine, 400ms);
    // 已提交且真实对端已 accept，才在同一协程置取消；不是入口预取消。
    net::CoTestAccess::SetDialBarrier([&] {
        CHECK(WaitUntil([&] { return f.peer.accept_total() == 1; }));
        g_bbt_tls_coroutine_co->RequestCancel();
    });
    auto cancelled = f.host.Call([&] { return bbt_co_await f.client->Get("/cancel-dial", {}, Until()); });
    net::CoTestAccess::SetDialBarrier({});
    ExpectCode(cancelled, ErrorCode::Cancelled);
    CHECK(f.client->Stats().budget <= 1);
    Access::SetConnectDeliveryDelay(*f.host.engine, 0ms);
    CHECK(WaitUntil([&] { return f.client->Stats().budget == 0; }));
    CHECK(f.GetPath("/after-cancel"));
    CHECK(f.peer.accept_total() == 2 && f.peer.requests_total() == 1);
    f.Finish();
}

int Run(int argc, char** argv) {
    Watchdog watchdog;
    try {
        CHECK(argc == 2);
        g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
        g_bbt_coroutine_config->m_cfg_stack_size = 128 * 1024;
        g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
        g_scheduler->Start();
        // 复用既有运行时暖机，使 fd 基线不含一次性初始化抖动。
        { Host warm; CHECK(warm.Call([] { return true; })); warm.Finish(); }
        const std::string mode = argv[1];
        if (mode == "fragment_header" || mode == "fragment_body" || mode == "chunked" ||
            mode == "head_interim" || mode == "eof_truncated" || mode == "body_limit") { Protocol(mode); }
        else if (mode == "input_boundary") { InputBoundary(); }
        else if (mode == "preexpired_cancelled") { PreexpiredCancelled(); }
        else if (mode == "phase_timeouts") { PhaseTimeouts(); }
        else if (mode == "per_read_total") { PerReadAndTotal(); }
        else if (mode == "cross_origin") { CrossOrigin(); }
        else if (mode == "handoff_concurrency") { HandoffConcurrency(); }
        else if (mode == "physical_budget") { PhysicalBudget(1); }
        else if (mode == "physical_budget_two") { PhysicalBudget(2); }
        else if (mode == "physical_cancel") { PhysicalCancel(); }
        else { CHECK(false); }
        std::cout << "http regression " << mode << " PASS\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "http regression FAIL: " << error.what() << '\n';
        return 1;
    }
}

} // namespace

int main(int argc, char** argv) {
    return bbt_net_test::RunTestProcessExit([&] { return Run(argc, argv); });
}

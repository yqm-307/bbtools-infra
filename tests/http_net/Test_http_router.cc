/**
 * @file Test_http_router.cc
 * @brief bbt::infra::http::Router 契约测试（Boost.Test + 自包 main 退出边界）。
 *
 * 除 round_trip 外直接驱动 Router::Build 产出的 ServerHandler，不起 Engine；
 * round_trip 用真实 loopback Server + 原生 socket 证明 Build 产物能被
 * Server::Create 消费并走通一次完整请求/响应。
 */
#define BOOST_TEST_NO_MAIN
#define BOOST_TEST_ALTERNATIVE_INIT_API
#include <boost/test/included/unit_test.hpp>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/infra/http/Router.hpp>
#include <bbt/infra/net/Engine.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace bbt::infra;
using namespace std::chrono_literals;
namespace http = bbt::infra::http;
namespace net = bbt::infra::net;

namespace {

IncomingCallContext CallContext()
{
    return {std::chrono::steady_clock::now() + 30s, {}};
}

/**
 * 直接驱动 Build 产出的 ServerHandler：匹配/参数/中间件顺序/配置错误等契约
 * 不需要真实 socket，直接断言 Response 比绕网络更精确。
 */
result<http::Response>
Dispatch(const http::ServerHandler& handler, std::string method, std::string target, std::string body = {})
{
    http::IncomingRequest request;
    request.method = std::move(method);
    request.target = std::move(target);
    request.body = std::move(body);
    return handler(CallContext(), std::move(request));
}

struct ParsedResponse {
    int status{0};
    http::Headers headers;
    std::string body;
};

bool ParseResponse(const std::string& bytes, ParsedResponse& out)
{
    const auto head_end = bytes.find("\r\n\r\n");
    if (head_end == std::string::npos) {
        return false;
    }
    const std::string head = bytes.substr(0, head_end);
    const auto line_end = head.find("\r\n");
    const std::string status_line = head.substr(0, line_end);
    const auto sp = status_line.find(' ');
    if (sp == std::string::npos) {
        return false;
    }
    out.status = std::atoi(status_line.c_str() + sp + 1);
    std::size_t length = 0;
    bool has_length = false;
    std::size_t pos = line_end == std::string::npos ? head.size() : line_end + 2;
    while (pos < head.size()) {
        const auto eol = head.find("\r\n", pos);
        const std::string line = head.substr(pos, eol == std::string::npos ? std::string::npos : eol - pos);
        const auto colon = line.find(':');
        if (colon != std::string::npos) {
            const std::string name = line.substr(0, colon);
            std::string value = line.substr(colon + 1);
            const auto first = value.find_first_not_of(' ');
            if (first != std::string::npos) {
                value.erase(0, first);
            }
            out.headers.emplace_back(name, value);
            std::string lower = name;
            std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) {
                return static_cast<char>(std::tolower(c));
            });
            if (lower == "content-length") {
                length = static_cast<std::size_t>(std::atoll(value.c_str()));
                has_length = true;
            }
        }
        if (eol == std::string::npos) {
            break;
        }
        pos = eol + 2;
    }
    if (!has_length || bytes.size() < head_end + 4 + length) {
        return false;
    }
    out.body = bytes.substr(head_end + 4, length);
    return true;
}

std::optional<std::string> HeaderValue(const http::Headers& headers, const std::string& name)
{
    for (const auto& header : headers) {
        if (header.first == name) {
            return header.second;
        }
    }
    return std::nullopt;
}

bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout = 3s)
{
    const auto end = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < end) {
        if (predicate()) {
            return true;
        }
        std::this_thread::yield();
    }
    return predicate();
}

/**
 * 真实 loopback Server 宿主：Engine 独立 Run 线程 + 动态端口 + 收口断言，
 * 与 Test_http_net_server.cc 的 ServerHost 同形态，仅保留单次请求所需最小面。
 */
struct ServerHost {
    std::shared_ptr<net::Engine> engine{net::Engine::Create(std::shared_ptr<net::RawHandler>{})};
    std::shared_ptr<http::Server> server;
    std::thread runner;
    std::error_code run_error;
    net::Address address;

    explicit ServerHost(http::ServerHandler handler)
    {
        auto made = http::Server::Create(engine, std::move(handler));
        BOOST_REQUIRE(made);
        server = std::move(made).value();
        BOOST_REQUIRE(engine->Phase() == net::EnginePhase::Idle);
        net::EngineConfig config;
        config.runtime_progress = net::RuntimeProgress::Independent;
        config.options.num_event_loop = 1;
        std::error_code error;
        config.listen_addresses = {net::Address::Ip("127.0.0.1", 0, error)};
        BOOST_REQUIRE(!error);
        runner = std::thread([this, config] { run_error = engine->Run(config); });
        BOOST_REQUIRE(WaitUntil([this] { return engine->Phase() == net::EnginePhase::Running; }));
        std::vector<net::Address> addresses;
        BOOST_REQUIRE(!engine->LocalAddresses(addresses) && addresses.size() == 1);
        address = addresses[0];
        BOOST_REQUIRE(server->Start());
    }

    void Finish()
    {
        BOOST_REQUIRE(
            server->Close(CloseOptions{CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + 3s}));
        const auto stats = server->Stats();
        BOOST_REQUIRE(stats.tasks == 0 && stats.sessions == 0 && stats.inflight == 0);
        BOOST_REQUIRE(engine->AcceptingStopped());
        (void)engine->Close();
        runner.join();
        BOOST_REQUIRE(!run_error && engine->Phase() == net::EnginePhase::Stopped);
        BOOST_REQUIRE(engine->CountConnections() == 0);
    }
};

/**
 * 原生 socket 客户端：发送原始请求字节并读回完整响应（header + Content-Length body）。
 */
struct RawPeer {
    int fd{-1};

    explicit RawPeer(const net::Address& address)
    {
        fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        BOOST_REQUIRE(fd >= 0);
        timeval timeout{5, 0};
        BOOST_REQUIRE(::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);
        sockaddr_in target{};
        target.sin_family = AF_INET;
        target.sin_port = htons(address.Port());
        target.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        BOOST_REQUIRE(::connect(fd, reinterpret_cast<sockaddr*>(&target), sizeof(target)) == 0);
    }

    ~RawPeer()
    {
        if (fd >= 0) {
            ::close(fd);
        }
    }

    ParsedResponse Exchange(const std::string& bytes)
    {
        std::size_t sent = 0;
        while (sent < bytes.size()) {
            const auto n = ::send(fd, bytes.data() + sent, bytes.size() - sent, MSG_NOSIGNAL);
            BOOST_REQUIRE(n > 0);
            sent += static_cast<std::size_t>(n);
        }
        std::string received;
        char chunk[8192];
        ParsedResponse parsed;
        while (!ParseResponse(received, parsed)) {
            const auto n = ::recv(fd, chunk, sizeof(chunk), 0);
            BOOST_REQUIRE(n > 0);
            received.append(chunk, static_cast<std::size_t>(n));
        }
        return parsed;
    }
};

http::Handler ReturnBody(std::string body)
{
    return [body = std::move(body)](http::Request&) {
        return http::Text(200, body);
    };
}

std::string Join(const std::vector<std::string>& items)
{
    std::string joined;
    for (const auto& item : items) {
        joined += (joined.empty() ? "" : ",") + item;
    }
    return joined;
}

// clang-format off
}
// clang-format on

BOOST_AUTO_TEST_SUITE(http_router)

/**
 * 静态/参数匹配、静态段优先、404、405 + Allow。
 * 判别错误实现：参数路由抢在静态前匹配；405 不输出 Allow；404 落到参数兜底。
 */
BOOST_AUTO_TEST_CASE(matching)
{
    http::Router router;
    router.Get("/health")(ReturnBody("ok"));
    router.Get("/users/{id}")(
        [](http::Request& request) { return http::Text(200, "user:" + request.params.at("id")); });
    router.Get("/users/me")(ReturnBody("me"));

    auto built = router.Build();
    BOOST_REQUIRE(built);

    auto reply = Dispatch(built.value(), "GET", "/health");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 200);
    BOOST_CHECK_EQUAL(reply.value().body, "ok");

    /**
     * 静态段优先于参数段：/users/me 不得落到 {id}=me 的参数路由。
     */
    reply = Dispatch(built.value(), "GET", "/users/me");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().body, "me");

    reply = Dispatch(built.value(), "GET", "/users/42");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().body, "user:42");

    reply = Dispatch(built.value(), "GET", "/missing");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 404);

    /**
     * 路径命中、方法不命中 → 405，且 Allow 只含该形状允许的方法。
     */
    reply = Dispatch(built.value(), "DELETE", "/users/me");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 405);
    const auto allow = HeaderValue(reply.value().headers, "Allow");
    BOOST_REQUIRE(allow.has_value());
    BOOST_CHECK_EQUAL(*allow, "GET");
}

/**
 * 更宽泛的参数路由支持该方法、更具体的静态路由不支持 → 必须 405，禁止
 * 回退到参数路由。判别错误实现：先按方法过滤候选再选路径（会错误命中参数路由）。
 */
BOOST_AUTO_TEST_CASE(method_fallback)
{
    http::Router router;
    router.Get("/users/{id}")([](http::Request&) { return http::Text(200, "param"); });
    router.Get("/users/me")(ReturnBody("static"));

    auto built = router.Build();
    BOOST_REQUIRE(built);

    auto reply = Dispatch(built.value(), "POST", "/users/me");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 405);
    const auto allow = HeaderValue(reply.value().headers, "Allow");
    BOOST_REQUIRE(allow.has_value());
    BOOST_CHECK_EQUAL(*allow, "GET");

    /**
     * 参数路由本身仍可命中其它路径。
     */
    reply = Dispatch(built.value(), "GET", "/users/7");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().body, "param");
}

/**
 * 同形状不同方法各自声明参数名：GET /u/{id} 与 POST /u/{name}，选中方法后
 * 参数名必须来自选中的路由。判别错误实现（原型缺口）：保存首个最具体候选的
 * 参数名，POST handler 会看到 id 而非 name。
 */
BOOST_AUTO_TEST_CASE(params_per_method)
{
    http::Router router;
    router.Get("/u/{id}")([](http::Request& request) {
        return http::Text(200, "get:" + request.params.at("id") + ":" + std::to_string(request.params.count("name")));
    });
    router.Post("/u/{name}")([](http::Request& request) {
        return http::Text(200, "post:" + request.params.at("name") + ":" + std::to_string(request.params.count("id")));
    });

    auto built = router.Build();
    BOOST_REQUIRE(built);

    auto reply = Dispatch(built.value(), "GET", "/u/abc");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().body, "get:abc:0");

    reply = Dispatch(built.value(), "POST", "/u/abc");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().body, "post:abc:0");
}

/**
 * target 规则：尾斜杠严格匹配、重复斜杠按字面空段、查询串剥离、百分号编码/
 * 反斜杠/点段 → 400。判别错误实现：折叠斜杠、自动重定向尾斜杠、未剥离查询串、
 * 漏校验编码。
 */
BOOST_AUTO_TEST_CASE(target_rules)
{
    http::Router router;
    router.Get("/health")(ReturnBody("ok"));
    router.Get("/a/")(ReturnBody("trailing"));
    router.Get("/u/{id}")([](http::Request& request) { return http::Text(200, "u:" + request.params.at("id")); });

    auto built = router.Build();
    BOOST_REQUIRE(built);

    auto reply = Dispatch(built.value(), "GET", "/a/");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 200);
    BOOST_CHECK_EQUAL(reply.value().body, "trailing");

    /**
     * 严格尾斜杠：/a 不得匹配 /a/；未注册的 /health/ 不得重定向。
     */
    reply = Dispatch(built.value(), "GET", "/a");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 404);
    reply = Dispatch(built.value(), "GET", "/health/");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 404);

    /**
     * 重复斜杠不折叠：字面空段不匹配参数，也不等同单斜杠。
     */
    reply = Dispatch(built.value(), "GET", "/u//x");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 404);
    reply = Dispatch(built.value(), "GET", "//health");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 404);

    /**
     * 查询串剥离：不参与匹配，也不进入参数值。
     */
    reply = Dispatch(built.value(), "GET", "/u/42?x=1&y=2");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().body, "u:42");

    for (const std::string target : {"/u/%41", "/u/a%20b", "/u/a\\b", "/u/./x", "/u/../x", "/bad%Q0"}) {
        reply = Dispatch(built.value(), "GET", target);
        BOOST_REQUIRE(reply);
        BOOST_CHECK_MESSAGE(reply.value().status_code == 400, "expected 400 for " << target);
    }

    /**
     * 方法大小写敏感：get 不匹配已注册的 GET。
     */
    reply = Dispatch(built.value(), "get", "/health");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 405);
}

/**
 * 中间件：全局先注册先进入/逆序退出；组中间件在路径匹配之后、只包裹选中的
 * 组内 handler（不覆盖组前缀下的 404）；提前返回短路；全局中间件覆盖 Router
 * 产生的 404。判别错误实现：包裹顺序装反（B 先进）、组中间件包在匹配外、
 * 把 Router 的 404 绕出全局链。
 */
BOOST_AUTO_TEST_CASE(middleware)
{
    std::vector<std::string> trace;

    http::Router app;
    app.Use([&trace](http::Request& request, const http::Next& next) -> http::Reply {
        trace.push_back("A-in");
        auto reply = next(request);
        trace.push_back("A-out");
        if (reply) {
            reply.value().headers.emplace_back("X-Trace", Join(trace));
        }
        return reply;
    });
    app.Use([&trace](http::Request& request, const http::Next& next) -> http::Reply {
        trace.push_back("B-in");
        auto reply = next(request);
        trace.push_back("B-out");
        return reply;
    });
    app.Get("/hit")([&trace](http::Request&) {
        trace.push_back("handler");
        return http::Text(200, "hit");
    });

    /**
     * 组中间件：只包裹组内选中的 handler。
     */
    http::Router group;
    group.Use([&trace](http::Request& request, const http::Next& next) -> http::Reply {
        trace.push_back("group-in");
        auto reply = next(request);
        trace.push_back("group-out");
        return reply;
    });
    group.Get("/inside")([&trace](http::Request&) {
        trace.push_back("group-handler");
        return http::Text(200, "inside");
    });
    app.Include("/g", group);

    auto built = app.Build();
    BOOST_REQUIRE(built);

    /**
     * 全局 A→B 进入、逆序退出，组中间件只在选中 handler 外层。
     */
    trace.clear();
    auto reply = Dispatch(built.value(), "GET", "/hit");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(Join(trace), "A-in,B-in,handler,B-out,A-out");

    trace.clear();
    reply = Dispatch(built.value(), "GET", "/g/inside");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().body, "inside");
    BOOST_CHECK_EQUAL(Join(trace), "A-in,B-in,group-in,group-handler,group-out,B-out,A-out");

    /**
     * 组前缀下 404：全局中间件仍覆盖，组中间件不得运行（组链只包裹选中 handler）。
     */
    trace.clear();
    reply = Dispatch(built.value(), "GET", "/g/missing");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 404);
    BOOST_CHECK_EQUAL(Join(trace), "A-in,B-in,B-out,A-out");
    const auto echoed = HeaderValue(reply.value().headers, "X-Trace");
    BOOST_REQUIRE(echoed.has_value());
    BOOST_CHECK_EQUAL(*echoed, "A-in,B-in,B-out,A-out");

    /**
     * 提前返回：改路由为中间件短路，下游 handler 不得执行。
     */
    http::Router shorted;
    shorted.Use([](http::Request&, const http::Next&) { return http::Text(403, "blocked"); });
    shorted.Get("/x")([](http::Request&) {
        BOOST_FAIL("short-circuited handler must not run");
        return http::Text(200, "");
    });
    auto shorted_built = shorted.Build();
    BOOST_REQUIRE(shorted_built);
    reply = Dispatch(shorted_built.value(), "GET", "/x");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 403);
    BOOST_CHECK_EQUAL(reply.value().body, "blocked");
}

/**
 * Build 快照：产出 handler 后修改 Router（继续注册）或销毁 Router 都不改变
 * 已发出的快照。判别错误实现：快照与 Router 共享可变状态（指针/引用）。
 */
BOOST_AUTO_TEST_CASE(build_snapshot)
{
    http::ServerHandler handler;
    {
        http::Router router;
        router.Get("/a")(ReturnBody("a"));
        auto built = router.Build();
        BOOST_REQUIRE(built);
        handler = std::move(built).value();

        /**
         * Build 已冻结快照，后续装配不改变在服务的路由表。
         */
        router.Get("/late")(ReturnBody("late"));
    }

    auto reply = Dispatch(handler, "GET", "/a");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().body, "a");

    reply = Dispatch(handler, "GET", "/late");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 404);
}

/**
 * 配置错误集中在 Build：重复形状、重复参数名、空 handler、空中间件、非法
 * 方法 token、自包含、非法前缀。判别错误实现：静默覆盖前一路由、接受 "GE T"
 * 这类非法方法、Include 自包含死循环。
 */
BOOST_AUTO_TEST_CASE(config_errors)
{

    /**
     * 同形状等价重复（{id} 与 {name} 同形状同方法）→ Build 拒绝。
     */
    {
        http::Router router;
        router.Get("/users/{id}")(ReturnBody("first"));
        router.Get("/users/{name}")(ReturnBody("second"));
        BOOST_CHECK(!router.Build());
    }

    /**
     * 同路径形状不同方法：允许。
     */
    {
        http::Router router;
        router.Get("/users/{id}")(ReturnBody("first"));
        router.Post("/users/{id}")(ReturnBody("second"));
        BOOST_CHECK(router.Build());
    }

    /**
     * 同一路径内参数名重复。
     */
    {
        http::Router router;
        router.Get("/users/{id}/{id}")(ReturnBody("dup"));
        BOOST_CHECK(!router.Build());
    }

    /**
     * 空 handler / 空 middleware。
     */
    {
        http::Router router;
        router.Get("/")(http::Handler{});
        BOOST_CHECK(!router.Build());
        http::Router with_mw;
        with_mw.Use(http::Middleware{});
        BOOST_CHECK(!with_mw.Build());
        http::Router with_route_mw;
        with_route_mw.Get("/").Use(http::Middleware{}).Handle(ReturnBody("never"));
        BOOST_CHECK(!with_route_mw.Build());
    }

    /**
     * 非法方法 token：带空格/分隔符拒绝；合法 tchar 方法接受。
     */
    {
        http::Router router;
        router.AddRoute("GE T", "/a", ReturnBody("a"));
        BOOST_CHECK(!router.Build());
        http::Router slash;
        slash.AddRoute("GE/T", "/a", ReturnBody("a"));
        BOOST_CHECK(!slash.Build());
        http::Router empty;
        empty.AddRoute("", "/a", ReturnBody("a"));
        BOOST_CHECK(!empty.Build());
        http::Router legal;
        legal.AddRoute("M-SEARCH", "/a", ReturnBody("a"));
        legal.AddRoute("PURGE!", "/b", ReturnBody("b"));
        BOOST_CHECK(legal.Build());
    }

    /**
     * 自包含：在副作用前拒绝（不挂死、Build 报错）。
     */
    {
        http::Router router;
        router.Get("/a")(ReturnBody("a"));
        router.Include("/g", router);
        BOOST_CHECK(!router.Build());
        auto built = router.Build();
        BOOST_CHECK(!built);
    }

    /**
     * 非法前缀：不以 '/' 开头、以 '/' 结尾、含非法字符。
     */
    {
        http::Router group;
        group.Get("/x")(ReturnBody("x"));
        http::Router bad1;
        bad1.Include("g", group);
        BOOST_CHECK(!bad1.Build());
        http::Router bad2;
        bad2.Include("/g/", group);
        BOOST_CHECK(!bad2.Build());
        http::Router bad3;
        bad3.Include("/g x", group);
        BOOST_CHECK(!bad3.Build());
    }

    /**
     * 组错误随 Include 传播。
     */
    {
        http::Router bad_group;
        bad_group.Get("/x")(http::Handler{});
        http::Router app;
        app.Include("/g", bad_group);
        BOOST_CHECK(!app.Build());
    }
}

/**
 * Next 只在当前调用栈内有效且最多调用一次：第二次调用返回错误（经全局
 * 中间件链如实上抛）。判别错误实现：next 可重复调用而静默重入下游。
 */
BOOST_AUTO_TEST_CASE(next_twice)
{
    http::Router router;
    router.Use([](http::Request& request, const http::Next& next) -> http::Reply {
        auto first = next(request);
        BOOST_REQUIRE(first);
        auto second = next(request);
        BOOST_REQUIRE(!second);
        BOOST_CHECK(second.error().code == ErrorCode::InvalidContext);
        return http::Text(209, "second-was-error");
    });
    router.Get("/x")(ReturnBody("x"));

    auto built = router.Build();
    BOOST_REQUIRE(built);

    auto reply = Dispatch(built.value(), "GET", "/x");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 209);
}

/**
 * 单路由链式装配：Use 装饰器先于 Depend/Handle 进入；依赖可声明子依赖，
 * 同一请求按节点身份复用，失败由外层统一响应中间件转换，异常也由其收口。
 */
BOOST_AUTO_TEST_CASE(route_decorator_and_dependencies)
{
    struct Database {
        int id;
    };

    struct User {
        std::string name;
        int database_id;
    };

    int database_calls = 0;
    int user_calls = 0;
    int business_calls = 0;
    int remaining = 2;
    const auto database = http::Dependencies<>().Provide<Database>([&](http::Request&) {
        ++database_calls;
        return result<Database>::ok(Database{7});
    });
    const auto user =
        http::Dependencies<>().Depend(database).Provide<User>([&](http::Request& request, const Database& db) {
            ++user_calls;
            return result<User>::ok(User{request.params.at("name"), db.id});
        });

    http::Router router;
    router.Get("/users/{name}")
        .Use([&](http::Request& request, const http::Next& next) -> http::Reply {
            if (remaining == 0) {
                return http::Text(429, "rate limited");
            }
            --remaining;
            return next(request);
        })
        .Depend(user)
        .Depend(database)
        .Handle([&](const User& current, const Database& db) {
            ++business_calls;
            BOOST_CHECK_EQUAL(current.database_id, db.id);
            return http::Text(200, current.name + ":" + std::to_string(db.id));
        });

    auto built = router.Build();
    BOOST_REQUIRE(built);
    auto reply = Dispatch(built.value(), "GET", "/users/alice");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 200);
    BOOST_CHECK_EQUAL(reply.value().body, "alice:7");
    BOOST_CHECK_EQUAL(database_calls, 1);
    BOOST_CHECK_EQUAL(user_calls, 1);
    BOOST_CHECK_EQUAL(business_calls, 1);

    /**
     * 依赖声明在每次请求建立独立 Scope；同一个声明下次请求重新解析。
     */
    reply = Dispatch(built.value(), "GET", "/users/bob");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 200);
    BOOST_CHECK_EQUAL(database_calls, 2);
    BOOST_CHECK_EQUAL(user_calls, 2);

    /**
     * 装饰器短路发生在依赖解析之前。
     */
    reply = Dispatch(built.value(), "GET", "/users/carol");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 429);
    BOOST_CHECK_EQUAL(database_calls, 2);
    BOOST_CHECK_EQUAL(user_calls, 2);
    BOOST_CHECK_EQUAL(business_calls, 2);
}

/**
 * 统一响应中间件：Reply::err 与下游异常都由最外层映射为 HTTP 响应，业务 handler
 * 只表达业务结果；已经生成的 401/404/429 等正常 HTTP 响应保持原状态码。
 */
BOOST_AUTO_TEST_CASE(unified_error_middleware)
{
    struct AuthenticatedUser {};

    int handler_calls = 0;
    http::Router router;
    router.Use([](http::Request& request, const http::Next& next) -> http::Reply {
        try {
            auto reply = next(request);
            if (!reply) {
                return http::Text(503, "service unavailable");
            }
            return reply;
        } catch (const std::exception&) {
            return http::Text(500, "internal error");
        } catch (...) {
            return http::Text(500, "internal error");
        }
    });

    const auto rejected = http::Dependencies<>().Provide<AuthenticatedUser>([](http::Request&) {
        return result<AuthenticatedUser>::err(MakeError(ErrorCode::Unavailable, "auth backend unavailable"));
    });
    router.Get("/rejected").Depend(rejected).Handle([&](const AuthenticatedUser&) {
        ++handler_calls;
        return http::Text(200, "must not run");
    });
    router.Get("/throws").Handle(
        [&](http::Request&) -> http::Reply { throw std::runtime_error("secret implementation detail"); });
    router.Get("/limited")
        .Use([](http::Request&, const http::Next&) { return http::Text(429, "too many"); })
        .Handle([&](http::Request&) {
            ++handler_calls;
            return http::Text(200, "must not run");
        });

    auto built = router.Build();
    BOOST_REQUIRE(built);
    auto reply = Dispatch(built.value(), "GET", "/rejected");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 503);
    reply = Dispatch(built.value(), "GET", "/throws");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 500);
    reply = Dispatch(built.value(), "GET", "/limited");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 429);
    BOOST_CHECK_EQUAL(handler_calls, 0);
}

/**
 * framework 接入接缝演练：外层 Router Use 业务门（未就绪/停止中 503），
 * Include("", 用户路由)。判别错误实现：Include 丢弃外层中间件（门不再覆盖
 * 组路由）、全局中间件不覆盖 Router 生成的 404（未就绪时泄漏 404）、Router
 * 不可按值转移（Options.routes 无法持有）或 Build 快照依赖原 Router。
 */
BOOST_AUTO_TEST_CASE(framework_wiring)
{
    const auto ready = std::make_shared<std::atomic<bool>>(false);

    /**
     * 用户路由组：模拟受管 Service 分发入口。
     */
    http::Router user;
    user.Get("/svc/ping")(ReturnBody("pong"));
    user.Get("/svc/echo/{name}")(
        [](http::Request& request) { return http::Text(200, "echo:" + request.params.at("name")); });

    /**
     * 外层 Router：业务门在路由匹配之前运行——未 ready 时用户路由与 404 统一 503。
     */
    http::Router routes;
    routes.Use([ready](http::Request& request, const http::Next& next) -> http::Reply {
        if (!ready->load()) {
            return http::Text(503, "not ready");
        }
        return next(request);
    });
    routes.Include("", user);

    /**
     * Options.routes 按值持有 Router：移入 owner 后 Build，在线 handler 不依赖原对象。
     */
    http::Router owner(std::move(routes));
    auto built = owner.Build();
    BOOST_REQUIRE(built);

    /**
     * 未就绪：已注册路由与未注册路径都被门挡在 503。
     */
    auto reply = Dispatch(built.value(), "GET", "/svc/ping");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 503);
    reply = Dispatch(built.value(), "GET", "/no/such/route");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 503);

    /**
     * 就绪后：用户路由与路径参数正常放行，404 语义恢复。
     */
    ready->store(true);
    reply = Dispatch(built.value(), "GET", "/svc/ping");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().body, "pong");
    reply = Dispatch(built.value(), "GET", "/svc/echo/alice");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().body, "echo:alice");
    reply = Dispatch(built.value(), "GET", "/no/such/route");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 404);
}

/**
 * 组路由带路径参数、经 Include 前缀拼接后参数仍按最终选中路由提取。
 * 判别错误实现：Include 拼前缀后按未拼接 pattern 取段（参数名/值错位）、
 * 或把拼接路径中的参数段当作静态段处理（{id} 不再捕获）。
 */
BOOST_AUTO_TEST_CASE(group_param)
{
    http::Router group;
    group.Get("/{id}")([](http::Request& request) { return http::Text(200, "id:" + request.params.at("id")); });
    group.Post("/{id}/sub/{action}")([](http::Request& request) {
        return http::Text(200, request.params.at("id") + ":" + request.params.at("action"));
    });

    http::Router app;
    app.Include("/users", group);

    auto built = app.Build();
    BOOST_REQUIRE(built);

    auto reply = Dispatch(built.value(), "GET", "/users/42");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().body, "id:42");

    reply = Dispatch(built.value(), "POST", "/users/7/sub/kick");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().body, "7:kick");

    /**
     * 拼接后的形状仍按方法维度区分：POST /users/42 命中 GET 形状 → 405。
     */
    reply = Dispatch(built.value(), "POST", "/users/42");
    BOOST_REQUIRE(reply);
    BOOST_CHECK_EQUAL(reply.value().status_code, 405);
}

/**
 * 真实 loopback 一次往返：Router::Build 产物交给 Server::Create，原生 socket
 * 发请求、读响应、核对服务端收口凭据。判别错误实现：Build 产物不能直接作为
 * ServerHandler 消费（类型/语义错配）。
 */
BOOST_AUTO_TEST_CASE(round_trip)
{
    http::Router router;
    router.Get("/hello/{name}")(
        [](http::Request& request) { return http::Text(200, "Hello, " + request.params.at("name") + "!"); });

    auto built = router.Build();
    BOOST_REQUIRE(built);

    ServerHost host(std::move(built).value());
    {
        RawPeer peer(host.address);
        const auto response = peer.Exchange("GET /hello/router HTTP/1.1\r\nHost: localhost\r\n\r\n");
        BOOST_CHECK_EQUAL(response.status, 200);
        BOOST_CHECK_EQUAL(response.body, "Hello, router!");
    }
    host.Finish();
}

BOOST_AUTO_TEST_SUITE_END()

/**
 * 替代 Boost.Test 自动 main：启动 scheduler 后运行套件，全部局部对象离开
 * 作用域后经 _Exit 收口（退出边界见 tests/net/ProcessRuntimeTestMain.hpp）。
 */
bool init_unit_test()
{
    return true;
}

int main(int argc, char* argv[])
{
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 128 * 1024;
    g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
    g_scheduler->Start();

    const int code = boost::unit_test::unit_test_main(&init_unit_test, argc, argv);
    std::cout.flush();
    std::cerr.flush();
    int final_code = code;
    if (std::fflush(nullptr) != 0 || std::ferror(stdout) != 0 || std::ferror(stderr) != 0) {
        final_code = 1;
    }
    std::_Exit(final_code);
}

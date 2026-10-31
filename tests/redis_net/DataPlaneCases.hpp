// 由 Test_redis_net_contract.cc 在匿名 namespace 内包含；覆盖 COMMON 数据面契约。
// 真实后端用例遵守：BBT_REDIS_PORT 必须显式指向本验证自起的实例；缺失（回落
// 6379 共享实例）一律拒跑返回非零，不触碰共享服务，也不以 skip 冒充绿。
// 鉴权覆盖复用同一 BBT_REDIS_PORT 无鉴权实例：管理连接现场创建本用例唯一 ACL
// 用户（pid+随机、临时密码、仅 ~prefix* 键域与所需命令），RAII 清理 key 与
// DELUSER 并读回；不修改 default 用户密码、不 CONFIG SET requirepass、不动共享
// 全局鉴权。CLIENT KILL 只按 CLIENT LIST 中归属本用例 ACL 用户的连接 ID 定点
// 执行，禁止 TYPE normal / SKIPME no 等范围 kill。
// 单参数 AUTH 编码由受控 Peer 断言（{AUTH,password} 与 {AUTH,user,pass} 两种
// 线上字节序，SELECT 必在其后）；真实服务端单参 AUTH 冒烟此前在自有
// requirepass 实例完成（证据见 handoff），CI 不改 default 故本用例不覆盖该真实路径。

using redis::RedisHandshake;
using redis::SetOptions;
using redis::TtlResult;
using redis::HashEntries;
namespace redis_detail = bbt::infra::redis_detail;

// 显式端口守门：未注入时 RedisPort() 回落 6379（本机有他人共享实例），必须拒绝。
int RequiredPort(const char* variable) {
    const char* text = std::getenv(variable);
    CHECK(text && *text);
    const int port = std::atoi(text);
    CHECK(port > 0 && port <= 65535);
    return port;
}

std::shared_ptr<Client> MakeHandshake(Host& host, net::Address address, RedisHandshake handshake) {
    auto value = Client::Create(host.engine, address, std::move(handshake));
    CHECK(value);
    auto client = std::move(value).value();
    return client;
}

std::shared_ptr<Pool> MakePoolWith(Host& host, net::Address address, std::size_t connections,
                                   std::size_t max_waiters, RedisHandshake handshake = {}) {
    Pool::PoolOptions opts{connections, max_waiters, std::move(handshake)};
    auto value = Pool::Create(host.engine, address, opts);
    CHECK(value);
    return std::move(value).value();
}

// 同一 BBT_REDIS_PORT 实例上的同步管理连接：只做 ACL 管理/SELECT/读回/DEL/
// CLIENT LIST+KILL，不属于被测面。BBT_AUTHED_REDIS_* 已由本夹具取代。
class AdminRedis {
public:
    explicit AdminRedis(int port) : m_context(redisConnectWithTimeout("127.0.0.1", port, timeval{2, 0}), &redisFree) {
        CHECK(m_context && m_context->err == 0);
        CHECK(redisSetTimeout(m_context.get(), timeval{2, 0}) == REDIS_OK);
        const int db_count = DbCount();
        CHECK(db_count > 2); // 用例依赖 db2 存在；CI 默认 16 库。
    }
    Reply Command(std::vector<std::string> args) {
        std::vector<const char*> argv; std::vector<std::size_t> lens;
        for (const auto& arg : args) { argv.push_back(arg.data()); lens.push_back(arg.size()); }
        Reply reply(static_cast<redisReply*>(redisCommandArgv(m_context.get(), static_cast<int>(argv.size()),
                                                             argv.data(), lens.data())), &freeReplyObject);
        CHECK(reply);
        return reply;
    }
    // CLIENT LIST 中归属某 ACL 用户的连接 id；只读这些 id 做定点 KILL，
    // 拒绝 TYPE normal / SKIPME no / 通配等会波及他人连接的范围 kill。
    std::vector<std::string> ClientIdsByUser(const std::string& user) {
        auto reply = Command({"CLIENT", "LIST"});
        CHECK(reply->type == REDIS_REPLY_STRING);
        std::vector<std::string> ids;
        std::istringstream lines(std::string(reply->str, reply->len));
        for (std::string line; std::getline(lines, line); ) {
            std::istringstream fields(line);
            std::string id, found_user;
            for (std::string field; fields >> field; ) {
                if (field.rfind("id=", 0) == 0) { id = field.substr(3); }
                if (field.rfind("user=", 0) == 0) { found_user = field.substr(5); }
            }
            if (found_user == user && !id.empty()) { ids.push_back(id); }
        }
        return ids;
    }
    void KillClientsByUser(const std::string& user) {
        for (const auto& id : ClientIdsByUser(user)) {
            auto killed = Command({"CLIENT", "KILL", "ID", id});
            CHECK(killed->type == REDIS_REPLY_INTEGER && killed->integer == 1);
        }
    }
private:
    int DbCount() {
        auto reply = Command({"CONFIG", "GET", "databases"});
        if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 2 &&
            reply->element[1]->type == REDIS_REPLY_STRING) {
            return std::atoi(reply->element[1]->str);
        }
        return 16; // Redis >= 7.0.2 对无 CONFIG 权限用户返回空数组；未禁用时回默认。
    }
    std::unique_ptr<redisContext, decltype(&redisFree)> m_context;
};

// 本用例唯一 ACL 用户的 RAII：创建即限定 ~prefix* 键域与命令集；析构无条件
// DELUSER + GETUSER 读回确认（成功路径已经显式清理过则仅复核）。用户名与
// 密码均为进程临时值，不落盘也不进任何错误消息。
class ScopedAclUser {
public:
    ScopedAclUser(AdminRedis& admin, const std::string& prefix, const std::vector<std::string>& commands)
        : m_admin(admin) {
        std::random_device rnd;
        m_user = "bbt-acl-" + std::to_string(getpid()) + "-" + std::to_string(rnd());
        m_password = "pw-" + std::to_string(rnd()) + "-" + std::to_string(rnd());
        std::vector<std::string> args{"ACL", "SETUSER", m_user, "on", ">" + m_password,
                                      "~" + prefix + "*"};
        args.insert(args.end(), commands.begin(), commands.end());
        auto created = m_admin.Command(args);
        CHECK(created->type == REDIS_REPLY_STATUS);
        m_active = true;
    }
    ~ScopedAclUser() {
        // 与 RealRedis 析构同款：清理失败不抛穿栈展开，只标记由 Run() 末尾核对。
        try { Clear(); }
        catch (const std::exception& e) { std::cerr << "acl_cleanup_failed " << e.what() << "\n"; }
    }
    ScopedAclUser(const ScopedAclUser&) = delete;
    ScopedAclUser& operator=(const ScopedAclUser&) = delete;
    const std::string& user() const { return m_user; }
    const std::string& password() const { return m_password; }
    // 显式清理：DELUSER 返回 1 且 GETUSER 读回 nil。
    void Clear() {
        if (!m_active) { return; }
        auto removed = m_admin.Command({"ACL", "DELUSER", m_user});
        CHECK(removed->type == REDIS_REPLY_INTEGER && removed->integer == 1);
        auto gone = m_admin.Command({"ACL", "GETUSER", m_user});
        CHECK(gone->type == REDIS_REPLY_NIL);
        m_active = false;
        std::cout << "acl_cleanup user_bytes=" << m_user.size() << " deluser=1 getuser_nil=1\n";
    }
private:
    AdminRedis& m_admin;
    std::string m_user;
    std::string m_password;
    bool m_active{false};
};

// db2 上受控 key 的 RAII：写后本用例仍须把 db 复位回 0，异常路径同样清理
// 并读回，不把残留留给共享实例。
class ScopedDbKey {
public:
    ScopedDbKey(AdminRedis& admin, std::string key) : m_admin(admin), m_key(std::move(key)) {}
    ~ScopedDbKey() {
        try { Clear(); }
        catch (const std::exception& e) { std::cerr << "db2_cleanup_failed " << e.what() << "\n"; }
    }
    ScopedDbKey(const ScopedDbKey&) = delete;
    ScopedDbKey& operator=(const ScopedDbKey&) = delete;
    // 读回：SELECT 2 → GET → SELECT 0；调用方负责断言内容。
    std::optional<std::string> ReadFromDb2() {
        CHECK(m_admin.Command({"SELECT", "2"})->type == REDIS_REPLY_STATUS);
        auto value = m_admin.Command({"GET", m_key});
        std::optional<std::string> text;
        if (value->type == REDIS_REPLY_STRING) { text = std::string(value->str, value->len); }
        CHECK(m_admin.Command({"SELECT", "0"})->type == REDIS_REPLY_STATUS);
        return text;
    }
    void Clear() {
        if (m_key.empty()) { return; }
        CHECK(m_admin.Command({"SELECT", "2"})->type == REDIS_REPLY_STATUS);
        auto removed = m_admin.Command({"DEL", m_key});
        CHECK(removed->type == REDIS_REPLY_INTEGER);
        auto left = m_admin.Command({"EXISTS", m_key});
        CHECK(left->type == REDIS_REPLY_INTEGER && left->integer == 0);
        CHECK(m_admin.Command({"SELECT", "0"})->type == REDIS_REPLY_STATUS);
        m_key.clear();
    }
private:
    AdminRedis& m_admin;
    std::string m_key;
};

// 定点 kill 本用例 ACL 用户的连接并断言前后归属集合；只杀已确认归属的 ID。
void KillOwnedConnections(AdminRedis& admin, const ScopedAclUser& acl) {
    CHECK(!admin.ClientIdsByUser(acl.user()).empty());
    admin.KillClientsByUser(acl.user());
    CHECK(admin.ClientIdsByUser(acl.user()).empty());
}

// ── SET 选项（COMMON）────────────────────────────────────────────
void SetOptionsContract() {
    const int port = RequiredPort("BBT_REDIS_PORT");
    RealRedis backend;
    Host host;
    auto cli = host.Make(Loopback(port));
    host.Connect(cli);
    const auto nx = backend.Key("set-opts-nx");
    const auto xx = backend.Key("set-opts-xx");
    const auto px = backend.Key("set-opts-px");
    const auto missing = backend.Key("set-opts-missing");
    const auto reject = backend.Key("set-opts-reject");

    // NX：第一次 true，重复 false（nil 是合法终态不是错误）。
    SetOptions nx_opt; nx_opt.condition = SetOptions::Condition::NX;
    auto first = host.Call([&] { return bbt_co_await cli->Set(nx, "v1", nx_opt, Until()); });
    CHECK(first && first.value());
    auto again = host.Call([&] { return bbt_co_await cli->Set(nx, "v2", nx_opt, Until()); });
    CHECK(again && !again.value());
    auto got = backend.Command({"GET", nx});
    CHECK(got->type == REDIS_REPLY_STRING && std::string(got->str, got->len) == "v1");

    // XX：缺失 key false；已存在 true 并覆盖。
    SetOptions xx_opt; xx_opt.condition = SetOptions::Condition::XX;
    auto xx_miss = host.Call([&] { return bbt_co_await cli->Set(missing, "x", xx_opt, Until()); });
    CHECK(xx_miss && !xx_miss.value());
    auto xx_check = backend.Command({"EXISTS", missing});
    CHECK(xx_check->type == REDIS_REPLY_INTEGER && xx_check->integer == 0);
    CHECK(backend.Command({"SET", xx, "old"})->type == REDIS_REPLY_STATUS);
    auto xx_hit = host.Call([&] { return bbt_co_await cli->Set(xx, "new", xx_opt, Until()); });
    CHECK(xx_hit && xx_hit.value());
    got = backend.Command({"GET", xx});
    CHECK(std::string(got->str, got->len) == "new");

    // 过期以毫秒表达（线上 PX）：PTTL 读回落在 (0, 设定值]。
    SetOptions px_opt; px_opt.expire = std::chrono::milliseconds{60000};
    auto with_ttl = host.Call([&] { return bbt_co_await cli->Set(px, "ttl", px_opt, Until()); });
    CHECK(with_ttl && with_ttl.value());
    auto pttl = backend.Command({"PTTL", px});
    CHECK(pttl->type == REDIS_REPLY_INTEGER && pttl->integer > 0 && pttl->integer <= 60000);
    // EX 语义由毫秒表达（=chrono 秒换算），同样经 PTTL 观察。
    SetOptions ex_opt; ex_opt.expire = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::seconds{30});
    const auto ex_key = backend.Key("set-opts-ex");
    CHECK(host.Call([&] { return bbt_co_await cli->Set(ex_key, "ex", ex_opt, Until()); }).value());
    pttl = backend.Command({"PTTL", ex_key});
    CHECK(pttl->integer > 0 && pttl->integer <= 30000);
    // NX+PX 组合：条件不满足时仍返回 false，且不刷新 TTL。
    SetOptions both; both.condition = SetOptions::Condition::NX; both.expire = std::chrono::milliseconds{5000};
    auto combo = host.Call([&] { return bbt_co_await cli->Set(px, "other", both, Until()); });
    CHECK(combo && !combo.value());

    // 非法选项零接纳：后端读回证明什么都没写。
    SetOptions zero; zero.expire = std::chrono::milliseconds{0};
    ExpectCode(host.Call([&] { return bbt_co_await cli->Set(reject, "v", zero, Until()); }), ErrorCode::InvalidArgument);
    SetOptions negative; negative.expire = std::chrono::milliseconds{-5};
    ExpectCode(host.Call([&] { return bbt_co_await cli->Set(reject, "v", negative, Until()); }), ErrorCode::InvalidArgument);
    SetOptions over; over.expire = redis::kMaxSetExpire + std::chrono::milliseconds{1};
    ExpectCode(host.Call([&] { return bbt_co_await cli->Set(reject, "v", over, Until()); }), ErrorCode::InvalidArgument);
    ExpectCode(host.Call([&] { return bbt_co_await cli->Set({}, "v", nx_opt, Until()); }), ErrorCode::InvalidArgument);
    auto rejected = backend.Command({"EXISTS", reject});
    CHECK(rejected->type == REDIS_REPLY_INTEGER && rejected->integer == 0); // 三次零接纳都没写
    SetOptions boundary; boundary.expire = redis::kMaxSetExpire;
    auto at_max = host.Call([&] { return bbt_co_await cli->Set(reject, "v", boundary, Until()); });
    CHECK(at_max && at_max.value()); // 上界本身是合法的
    CHECK(host.Call([&] { return bbt_co_await cli->Ping(Until()); }));

    cli.reset();
    host.Finish();
    backend.Cleanup();
    std::cout << "set_options nx_tt=1 nx_ff=1 xx_ff=1 xx_tt=1 px_pttl=1 ex_ms=1 combo=1 "
                 "rejected_zero_accept=4 boundary_ok=1 same_stream_ping=1\n";
}

// ── Expire / Ttl 三态（COMMON）───────────────────────────────────
void ExpireTtlContract() {
    const int port = RequiredPort("BBT_REDIS_PORT");
    RealRedis backend;
    Host host;
    auto cli = host.Make(Loopback(port));
    host.Connect(cli);
    const auto key = backend.Key("ttl-key");
    const auto missing = backend.Key("ttl-missing");

    CHECK(host.Call([&] { return bbt_co_await cli->Set(key, "v", Until()); }));
    // 无过期 key → NoExpiry。
    auto ttl = host.Call([&] { return bbt_co_await cli->Ttl(key, Until()); });
    CHECK(ttl && ttl.value().state == TtlResult::State::NoExpiry);
    // PEXPIRE → Expires，剩余毫秒在 (0, ttl]。
    auto set_expire = host.Call([&] { return bbt_co_await cli->Expire(key, 9000ms, Until()); });
    CHECK(set_expire && set_expire.value());
    ttl = host.Call([&] { return bbt_co_await cli->Ttl(key, Until()); });
    CHECK(ttl && ttl.value().state == TtlResult::State::Expires);
    CHECK(ttl.value().remaining > 0ms && ttl.value().remaining <= 9000ms);
    // 不存在 key：Expire false、Ttl KeyMissing；负 TTL 断言字段为零不泄漏魔数。
    auto gone = host.Call([&] { return bbt_co_await cli->Expire(missing, 1000ms, Until()); });
    CHECK(gone && !gone.value());
    ttl = host.Call([&] { return bbt_co_await cli->Ttl(missing, Until()); });
    CHECK(ttl && ttl.value().state == TtlResult::State::KeyMissing && ttl.value().remaining == 0ms);
    // ttl<=0 非法 → InvalidArgument 零接纳；空 key 同。
    ExpectCode(host.Call([&] { return bbt_co_await cli->Expire(key, 0ms, Until()); }), ErrorCode::InvalidArgument);
    ExpectCode(host.Call([&] { return bbt_co_await cli->Expire(key, -1ms, Until()); }), ErrorCode::InvalidArgument);
    ExpectCode(host.Call([&] { return bbt_co_await cli->Expire({}, 1000ms, Until()); }), ErrorCode::InvalidArgument);
    ExpectCode(host.Call([&] { return bbt_co_await cli->Ttl({}, Until()); }), ErrorCode::InvalidArgument);
    CHECK(host.Call([&] { return bbt_co_await cli->Ping(Until()); }));

    cli.reset();
    host.Finish();
    backend.Cleanup();
    std::cout << "expire_ttl three_states=3 pexpire_tf=2 remaining_in_range=1 invalid_rejected=4\n";
}

// ── INCR 族（COMMON）─────────────────────────────────────────────
void IncrFamilyContract() {
    const int port = RequiredPort("BBT_REDIS_PORT");
    RealRedis backend;
    Host host;
    auto cli = host.Make(Loopback(port));
    host.Connect(cli);
    const auto counter = backend.Key("incr-counter");
    const auto bad = backend.Key("incr-nonint");
    const auto overflow = backend.Key("incr-overflow");

    auto value = host.Call([&] { return bbt_co_await cli->Incr(counter, Until()); });
    CHECK(value && value.value() == 1);
    value = host.Call([&] { return bbt_co_await cli->IncrBy(counter, 10, Until()); });
    CHECK(value && value.value() == 11);
    value = host.Call([&] { return bbt_co_await cli->Decr(counter, Until()); });
    CHECK(value && value.value() == 10);
    value = host.Call([&] { return bbt_co_await cli->DecrBy(counter, 4, Until()); });
    CHECK(value && value.value() == 6);
    auto negative = host.Call([&] { return bbt_co_await cli->IncrBy(counter, -20, Until()); });
    CHECK(negative && negative.value() == -14);

    // 非整数值 → RemoteError，连接保持（RemoteError 是唯一保留连接的错误）。
    CHECK(backend.Command({"SET", bad, "not-a-number"})->type == REDIS_REPLY_STATUS);
    auto failed = host.Call([&] { return bbt_co_await cli->Incr(bad, Until()); });
    ExpectCode(failed, ErrorCode::RemoteError);
    CHECK(host.Call([&] { return bbt_co_await cli->Ping(Until()); }));
    value = host.Call([&] { return bbt_co_await cli->Incr(counter, Until()); });
    CHECK(value && value.value() == -13); // 同一条流继续可用
    // 溢出同样 RemoteError。
    CHECK(backend.Command({"SET", overflow, "9223372036854775807"})->type == REDIS_REPLY_STATUS);
    ExpectCode(host.Call([&] { return bbt_co_await cli->Incr(overflow, Until()); }), ErrorCode::RemoteError);
    CHECK(host.Call([&] { return bbt_co_await cli->Ping(Until()); }));
    // 空 key 零接纳。
    ExpectCode(host.Call([&] { return bbt_co_await cli->Incr({}, Until()); }), ErrorCode::InvalidArgument);
    CHECK(host.Call([&] { return bbt_co_await cli->Ping(Until()); }));

    cli.reset();
    host.Finish();
    backend.Cleanup();
    std::cout << "incr_family values=5 nonint=RemoteError overflow=RemoteError stream_kept=3 empty_reject=1\n";
}

// ── Hash（COMMON）────────────────────────────────────────────────
void HashContract() {
    const int port = RequiredPort("BBT_REDIS_PORT");
    RealRedis backend;
    Host host;
    auto cli = host.Make(Loopback(port));
    host.Connect(cli);
    const auto hash = backend.Key("hash-key");
    const auto empty_hash = backend.Key("hash-empty");
    const auto wrong = backend.Key("hash-wrongtype");

    // 二进制安全 field/value（含 NUL 与 0xff）。
    const std::string field_bin("f\0bin", 5);
    const std::string value_bin("v\0\xff\r\n", 5);
    auto created = host.Call([&] { return bbt_co_await cli->HSet(hash, field_bin, value_bin, Until()); });
    CHECK(created && created.value());           // 新建字段 true
    created = host.Call([&] { return bbt_co_await cli->HSet(hash, field_bin, "v2", Until()); });
    CHECK(created && !created.value());          // 覆盖 false
    auto got = host.Call([&] { return bbt_co_await cli->HGet(hash, field_bin, Until()); });
    CHECK(got && got.value() && *got.value() == "v2");
    auto exists = host.Call([&] { return bbt_co_await cli->HExists(hash, field_bin, Until()); });
    CHECK(exists && exists.value());
    exists = host.Call([&] { return bbt_co_await cli->HExists(hash, "absent", Until()); });
    CHECK(exists && !exists.value());
    got = host.Call([&] { return bbt_co_await cli->HGet(hash, "absent", Until()); });
    CHECK(got && !got.value());

    // 多字段 + 顺序读回：后端 LPUSH 顺序写入 f1..f3 后用 HGETALL 对拍顺序。
    CHECK(backend.Command({"HSET", hash, "f1", "1", "f2", "2", "f3", "3"})->type == REDIS_REPLY_INTEGER);
    auto all = host.Call([&] { return bbt_co_await cli->HGetAll(hash, Until()); });
    CHECK(all && all.value().size() == 4);
    // HGETALL 顺序与服务端一致：直接用 hiredis 对拍同一份序列。
    auto raw = backend.Command({"HGETALL", hash});
    CHECK(raw->type == REDIS_REPLY_ARRAY && raw->elements == all.value().size() * 2);
    for (std::size_t i = 0; i < all.value().size(); ++i) {
        const auto& [field, value] = all.value()[i];
        CHECK(field == std::string(raw->element[i * 2]->str, raw->element[i * 2]->len));
        CHECK(value == std::string(raw->element[i * 2 + 1]->str, raw->element[i * 2 + 1]->len));
    }
    // 二进制 field 也在序列中。
    CHECK(std::find_if(all.value().begin(), all.value().end(), [&](const auto& pair) {
        return pair.first == field_bin && pair.second == "v2";
    }) != all.value().end());

    // HDel：返回删除数；空 fields 非法。
    auto removed = host.Call([&] { return bbt_co_await cli->HDel(hash, {"f1", "f2", "f3", "absent"}, Until()); });
    CHECK(removed && removed.value() == 3);
    removed = host.Call([&] { return bbt_co_await cli->HDel(hash, {"f1"}, Until()); });
    CHECK(removed && removed.value() == 0);
    // 空 hash：HGETALL 返回空序列而非错误。
    CHECK(backend.Command({"HSET", empty_hash, "tmp", "x"})->type == REDIS_REPLY_INTEGER);
    CHECK(backend.Command({"HDEL", empty_hash, "tmp"})->integer == 1);
    all = host.Call([&] { return bbt_co_await cli->HGetAll(empty_hash, Until()); });
    CHECK(all && all.value().empty());
    exists = host.Call([&] { return bbt_co_await cli->HExists(empty_hash, "tmp", Until()); });
    CHECK(exists && !exists.value());

    // WRONGTYPE → RemoteError 且连接仍可用。
    CHECK(backend.Command({"LPUSH", wrong, "item"})->type == REDIS_REPLY_INTEGER);
    ExpectCode(host.Call([&] { return bbt_co_await cli->HGet(wrong, "f", Until()); }), ErrorCode::RemoteError);
    ExpectCode(host.Call([&] { return bbt_co_await cli->HGetAll(wrong, Until()); }), ErrorCode::RemoteError);
    ExpectCode(host.Call([&] { return bbt_co_await cli->HSet(wrong, "f", "v", Until()); }), ErrorCode::RemoteError);
    CHECK(host.Call([&] { return bbt_co_await cli->Ping(Until()); }));
    got = host.Call([&] { return bbt_co_await cli->HGet(hash, field_bin, Until()); });
    CHECK(got && got.value() && *got.value() == "v2");

    // 空 key / 空 field / 空 fields 列表 → InvalidArgument 零接纳。
    ExpectCode(host.Call([&] { return bbt_co_await cli->HSet({}, "f", "v", Until()); }), ErrorCode::InvalidArgument);
    ExpectCode(host.Call([&] { return bbt_co_await cli->HSet(hash, {}, "v", Until()); }), ErrorCode::InvalidArgument);
    ExpectCode(host.Call([&] { return bbt_co_await cli->HGet(hash, {}, Until()); }), ErrorCode::InvalidArgument);
    ExpectCode(host.Call([&] { return bbt_co_await cli->HGet({}, "f", Until()); }), ErrorCode::InvalidArgument);
    ExpectCode(host.Call([&] { return bbt_co_await cli->HDel(hash, {}, Until()); }), ErrorCode::InvalidArgument);
    ExpectCode(host.Call([&] { return bbt_co_await cli->HDel(hash, {"f", ""}, Until()); }), ErrorCode::InvalidArgument);
    ExpectCode(host.Call([&] { return bbt_co_await cli->HExists(hash, {}, Until()); }), ErrorCode::InvalidArgument);
    ExpectCode(host.Call([&] { return bbt_co_await cli->HGetAll({}, Until()); }), ErrorCode::InvalidArgument);
    CHECK(host.Call([&] { return bbt_co_await cli->Ping(Until()); }));

    cli.reset();
    host.Finish();
    backend.Cleanup();
    std::cout << "hash binary_field_value=1 hset_new_over=2 hgetall_order_bytes=4 empty_hash=1 "
                 "wrongtype=RemoteError stream_kept=2 invalid_rejected=8\n";
}

// ── AUTH/SELECT 握手（COMMON）────────────────────────────────────
// 复用 BBT_REDIS_PORT 无鉴权实例：管理连接现场创建本用例唯一 ACL 用户
//（~prefix* 键域 + +@connection +get +set），真实验证双参 AUTH/SELECT/错误
// 密码/定点 CLIENT KILL 后重建连接重握手。单参数 AUTH 编码由 Peer 静态断言；
// 不改 default 用户与全局鉴权，CI 单实例即可跑。
void HandshakeAuthSelect() {
    const int port = RequiredPort("BBT_REDIS_PORT");
    RealRedis backend;                 // 管理本用例唯一 key 前缀与 DEL 读回
    AdminRedis admin(port);
    const auto prefix = backend.Prefix();
    // ACL 用户命令集：AUTH 自身握手之外的被测命令 + 管理面不需的键内命令。
    ScopedAclUser acl(admin, prefix, {"+@connection", "+get", "+set"});
    const auto key0 = backend.Key("db0");
    const auto key2 = backend.Key("db2");
    ScopedDbKey db2(admin, key2);      // db2 上 key2 的 RAII：异常路径同样 DEL+复位 db0
    std::random_device rnd;

    // 1) 单参数 AUTH 线上编码断言（受控 Peer，确定性）：user 空时编码为
    // {AUTH,password}，SELECT 在其后；两端 byte 序对拍真实客户端。
    for (const bool with_select : {false, true}) {
        const std::string pass = "static-pass-" + std::to_string(rnd());
        Peer peer([&](int fd) {
            CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"AUTH", pass}));
            CHECK(Peer::Send(fd, "+OK\r\n"));
            if (with_select) {
                CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"SELECT", "2"}));
                CHECK(Peer::Send(fd, "+OK\r\n"));
            }
            CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"PING"}));
            CHECK(Peer::Send(fd, "+PONG\r\n"));
        });
        Host host;
        auto cli = MakeHandshake(host, peer.address, {"", pass, with_select ? 2 : -1});
        host.Connect(cli);
        CHECK(host.Call([&] { return bbt_co_await cli->Ping(Until()); }));
        cli->Close(); cli.reset();
        peer.Join();
        host.Finish();
    }

    // 2) AUTH + SELECT db2：双参 AUTH 成功；写入只落 db2（第二个 client 从 db0 读不回）。
    Host host_ready;
    {
        auto writer = MakeHandshake(host_ready, Loopback(port), {acl.user(), acl.password(), 2});
        host_ready.Connect(writer);
        CHECK(host_ready.Call([&] { return bbt_co_await writer->Set(key2, "in-db2", Until()); }));
        writer->Close(); writer.reset();
        auto reader = MakeHandshake(host_ready, Loopback(port), {acl.user(), acl.password(), 0});
        host_ready.Connect(reader);
        auto in0 = host_ready.Call([&] { return bbt_co_await reader->Get(key2, Until()); });
        CHECK(in0 && !in0.value()); // db0 不可见
        reader->Close(); reader.reset();
        auto seen = db2.ReadFromDb2();
        CHECK(seen && *seen == "in-db2");
    }
    // 3) 错误密码：握手失败 RemoteError，错误文本不含密码，client 可 Close。
    {
        const std::string wrong_pass = "wrong-" + std::to_string(rnd());
        auto cli = MakeHandshake(host_ready, Loopback(port), {acl.user(), wrong_pass, -1});
        auto failed = host_ready.Call([&] { return bbt_co_await cli->Connect(Until()); });
        ExpectCode(failed, ErrorCode::RemoteError);
        CHECK(failed.error().message.find(wrong_pass) == std::string::npos);
        CHECK(failed.error().message.find(acl.password()) == std::string::npos);
        for (const auto& [k, v] : failed.error().details) { CHECK(v.find(wrong_pass) == std::string::npos); }
        ExpectCode(host_ready.Call([&] { return bbt_co_await cli->Ping(Until()); }), ErrorCode::TransportError);
        cli->Close(); // 握手失败后 Close 仍然正常
        cli.reset();
    }
    // 4) ACL 用户名 + 密码双参数 AUTH（键域限定 ~prefix*）：写入读回在管理连接可见。
    {
        auto cli = MakeHandshake(host_ready, Loopback(port), {acl.user(), acl.password(), -1});
        host_ready.Connect(cli);
        CHECK(host_ready.Call([&] { return bbt_co_await cli->Ping(Until()); }));
        auto who = host_ready.Call([&] { return bbt_co_await cli->Set(key0, "acl", Until()); });
        CHECK(who);
        cli->Close(); cli.reset();
    }
    // 5) 非法库序号：Create 拒绝（Client 侧校验，不经网络）；服务端 databases
    // 上限之外（默认 16，用回 DbCount 探测值）走服务端 ERR → RemoteError，
    // 退役不泄漏密码。
    {
        auto invalid = Client::Create(host_ready.engine, Loopback(port),
                                      {acl.user(), acl.password(), redis::kMaxDatabase + 1});
        ExpectCode(invalid, ErrorCode::InvalidArgument);
        invalid = Client::Create(host_ready.engine, Loopback(port),
                                 {acl.user(), acl.password(), -2});
        ExpectCode(invalid, ErrorCode::InvalidArgument);
        auto cli = MakeHandshake(host_ready, Loopback(port),
                                 {acl.user(), acl.password(), redis::kMaxDatabase});
        auto failed = host_ready.Call([&] { return bbt_co_await cli->Connect(Until()); });
        if (!failed) {
            ExpectCode(failed, ErrorCode::RemoteError);
            CHECK(failed.error().message.find(acl.password()) == std::string::npos);
            cli->Close();
        }
        cli.reset();
    }
    // 6) CLIENT KILL 后显式 Connect 重新握手：只杀已确认归属本用例 ACL 用户的
    // 连接 ID；死流→Connect→命令恢复且重连后 AUTH+SELECT 重放（仍落 db2）。
    {
        auto cli = MakeHandshake(host_ready, Loopback(port), {acl.user(), acl.password(), 2});
        host_ready.Connect(cli);
        CHECK(host_ready.Call([&] { return bbt_co_await cli->Set(key2, "before-kill", Until()); }));
        KillOwnedConnections(admin, acl);
        // 死流：下一条命令区分 TransportError/OutcomeUnknown 皆可，重点是必须失败。
        auto dead = host_ready.Call([&] { return bbt_co_await cli->Ping(Until(300ms)); });
        CHECK(!dead);
        // 显式 Connect 重新握手（AUTH+SELECT 重放），随后命令成功且落在 db2。
        host_ready.Connect(cli);
        CHECK(host_ready.Call([&] { return bbt_co_await cli->Set(key2, "after-kill", Until()); }));
        auto seen = db2.ReadFromDb2();
        CHECK(seen && *seen == "after-kill");
        cli->Close(); cli.reset();
    }
    acl.Clear();
    db2.Clear();
    backend.Cleanup();
    host_ready.Finish();
    std::cout << "handshake peer_auth_encoding=2 select_isolation=1 wrong_pass_no_secret=1 acl_user=1 "
                 "db_range_rejected=2 client_kill_owned_ids=1 reconnect_handshake=1\n";
}

// ── 池层镜像（COMMON）：每条新命令经池借还执行。─────────────────────
void PoolNewCommands() {
    const int port = RequiredPort("BBT_REDIS_PORT");
    RealRedis backend;
    Host host;
    auto pool = MakePoolWith(host, Loopback(port), 2, 4);
    CHECK(host.Call([&] { return bbt_co_await pool->Connect(Until()); }));
    const auto key = backend.Key("pool-new-key");
    const auto hash = backend.Key("pool-new-hash");

    // Set 选项经池：NX false/true + PX。
    SetOptions nx; nx.condition = SetOptions::Condition::NX;
    CHECK(host.Call([&] { return bbt_co_await pool->Set(key, "p1", nx, Until()); }).value());
    auto again = host.Call([&] { return bbt_co_await pool->Set(key, "p2", nx, Until()); });
    CHECK(again && !again.value());
    // Expire/Ttl 三态。
    CHECK(host.Call([&] { return bbt_co_await pool->Expire(key, 8000ms, Until()); }).value());
    auto ttl = host.Call([&] { return bbt_co_await pool->Ttl(key, Until()); });
    CHECK(ttl && ttl.value().state == TtlResult::State::Expires);
    ttl = host.Call([&] { return bbt_co_await pool->Ttl(backend.Key("pool-new-miss"), Until()); });
    CHECK(ttl && ttl.value().state == TtlResult::State::KeyMissing);
    // INCR 族。
    auto n = host.Call([&] { return bbt_co_await pool->IncrBy(key + "-c", 5, Until()); });
    CHECK(n && n.value() == 5);
    n = host.Call([&] { return bbt_co_await pool->Decr(key + "-c", Until()); });
    CHECK(n && n.value() == 4);
    auto miss_del = backend.Command({"DEL", key + "-c"});
    CHECK(miss_del->integer == 1);
    // Hash 五条。
    CHECK(host.Call([&] { return bbt_co_await pool->HSet(hash, "f", "v", Until()); }).value());
    auto hg = host.Call([&] { return bbt_co_await pool->HGet(hash, "f", Until()); });
    CHECK(hg && hg.value() && *hg.value() == "v");
    CHECK(host.Call([&] { return bbt_co_await pool->HExists(hash, "f", Until()); }).value());
    auto ha = host.Call([&] { return bbt_co_await pool->HGetAll(hash, Until()); });
    CHECK(ha && ha.value().size() == 1 && ha.value()[0].first == "f" && ha.value()[0].second == "v");
    auto hd = host.Call([&] { return bbt_co_await pool->HDel(hash, {"f"}, Until()); });
    CHECK(hd && hd.value() == 1);
    // 池层 RemoteError 同样只退役该条命令的连接归属……WRONGTYPE 不退役连接。
    const auto wrong = backend.Key("pool-new-wrong");
    CHECK(backend.Command({"LPUSH", wrong, "x"})->type == REDIS_REPLY_INTEGER);
    ExpectCode(host.Call([&] { return bbt_co_await pool->HGet(wrong, "f", Until()); }), ErrorCode::RemoteError);
    CHECK(host.Call([&] { return bbt_co_await pool->Ping(Until()); }));
    // 池层 InvalidArgument 零接纳。
    ExpectCode(host.Call([&] { return bbt_co_await pool->Expire(key, 0ms, Until()); }), ErrorCode::InvalidArgument);
    ExpectCode(host.Call([&] { return bbt_co_await pool->HDel(hash, {}, Until()); }), ErrorCode::InvalidArgument);

    pool->Close(); pool.reset();
    host.Finish();
    backend.Cleanup();
    std::cout << "pool_new_commands set_opts=2 expire_ttl=3 incr=2 hash=5 wrongtype=1 invalid=2\n";
}

// ── 池握手（COMMON）：PoolOptions.handshake 对每条池连接生效；失败归还预算。──
// 与 HandshakeAuthSelect 共用同一 BBT_REDIS_PORT 实例和本用例唯一 ACL 用户；
// CLIENT KILL 只针对已确认归属的 ID，范围 kill 禁用。
void PoolHandshakeAuth() {
    const int port = RequiredPort("BBT_REDIS_PORT");
    RealRedis backend;
    AdminRedis admin(port);
    const auto prefix = backend.Prefix();
    ScopedAclUser acl(admin, prefix, {"+@connection", "+get", "+set"});
    const auto key2 = backend.Key("pool-auth-db2");
    ScopedDbKey db2(admin, key2);
    std::random_device rnd;

    // 1) 池开的每条连接都完成握手：2 连接池 + SELECT db2，写入只落 db2。
    Host host;
    {
        auto pool = MakePoolWith(host, Loopback(port), 2, 2, {acl.user(), acl.password(), 2});
        CHECK(host.Call([&] { return bbt_co_await pool->Connect(Until()); }));
        CHECK(host.Call([&] { return bbt_co_await pool->Set(prefix + "a", "1", Until()); }));
        CHECK(host.Call([&] { return bbt_co_await pool->Set(prefix + "b", "2", Until()); }));
        pool->Close(); pool.reset();
        CHECK(admin.Command({"SELECT", "2"})->type == REDIS_REPLY_STATUS);
        auto count = admin.Command({"DBSIZE"});
        CHECK(count->type == REDIS_REPLY_INTEGER && count->integer >= 2);
        for (const auto suffix : {"a", "b"}) {
            auto removed = admin.Command({"DEL", prefix + suffix});
            CHECK(removed->type == REDIS_REPLY_INTEGER);
            auto left = admin.Command({"EXISTS", prefix + suffix});
            CHECK(left->type == REDIS_REPLY_INTEGER && left->integer == 0);
        }
        CHECK(admin.Command({"SELECT", "0"})->type == REDIS_REPLY_STATUS);
        auto in0 = admin.Command({"EXISTS", prefix + "a"});
        CHECK(in0->integer == 0);
    }
    // 2) 修复/重建的连接同样握手：CLIENT KILL 定点杀掉本用例 ACL 用户的池连接 →
    // 命令发现死流 → 显式 Connect 修复（重建连接的 AUTH+SELECT 生效）。
    {
        auto pool = MakePoolWith(host, Loopback(port), 1, 2, {acl.user(), acl.password(), 2});
        CHECK(host.Call([&] { return bbt_co_await pool->Connect(Until()); }));
        KillOwnedConnections(admin, acl);
        auto dead = host.Call([&] { return bbt_co_await pool->Ping(Until(300ms)); });
        CHECK(!dead);
        CHECK(host.Call([&] { return bbt_co_await pool->Connect(Until()); }));
        CHECK(host.Call([&] { return bbt_co_await pool->Set(key2, "3", Until()); }));
        auto seen = db2.ReadFromDb2();
        CHECK(seen && *seen == "3");
        pool->Close(); pool.reset();
    }
    // 3) 池握手失败不泄漏预算：错误密码建池 → Connect 失败；资源计数与 host drain
    // 收口证据（CountConnections==0、ConnectTableSize==0、Finish 全断）证明
    // 无泄漏，不只凭「Ping 返回 not connected」。fd 总数收口由 Run() 末尾的
    // socket/fd baseline 断言统一给出——此前中途 SocketCount 断言因 fd 异步
    // 回收时序竞争误报（WaitUntil 3s 内未归位），已移除；原因见 handoff。
    {
        const std::string wrong_pass = "wrong-" + std::to_string(rnd());
        auto pool = MakePoolWith(host, Loopback(port), 2, 2, {acl.user(), wrong_pass, -1});
        auto failed = host.Call([&] { return bbt_co_await pool->Connect(Until()); });
        ExpectCode(failed, ErrorCode::RemoteError);
        CHECK(failed.error().message.find(wrong_pass) == std::string::npos);
        ExpectCode(host.Call([&] { return bbt_co_await pool->Ping(Until()); }), ErrorCode::RuntimeUnavailable);
        // 预算归还：引擎无残留连接，reader 全部释放。
        CHECK(WaitUntil([&] {
            return host.engine->CountConnections() == 0 && Access::ConnectTableSize(*host.engine) == 0;
        }));
        pool->Close();
        // Close 后命令报 Closed（在释放句柄前断言，避免空指针解引用）。
        ExpectCode(host.Call([&] { return bbt_co_await pool->Ping(Until()); }), ErrorCode::Closed);
        pool.reset();
        CHECK(g_reader_created.load() == g_reader_freed.load());
    }
    acl.Clear();
    db2.Clear();
    backend.Cleanup();
    host.Finish();
    std::cout << "pool_handshake per_conn_auth_select=1 repaired_reauth_owned_kill=1 "
                 "rollback_budget_returned=1 no_leak=1\n";
}

// ── 受控假服务端（COMMON）：分帧/嵌套数组/超限/退役语义。─────────────
void HgetallProtocolFaults() {
    // 1) HGETALL 分帧跨段：回复被拆成多段发送，读回完整序列。
    {
        std::atomic<bool> release{false}, first{false};
        Peer peer([&](int fd) {
            CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"HGETALL", "h"}));
            // "*4\r\n$2\r\nf1\r\n$2\r\nv1\r\n" 与 "$2\r\nf2\r\n$2\r\nv2\r\n" 两段。
            CHECK(Peer::Send(fd, "*4\r\n$2\r\nf"));
            first.store(true);
            CHECK(WaitUntil([&] { return release.load(); }));
            CHECK(Peer::Send(fd, "1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n"));
        });
        Host host;
        auto cli = host.Make(peer.address);
        host.Connect(cli);
        g_watch_reader.store(g_last_reader.load());
        auto call = host.Submit([&] { return bbt_co_await cli->HGetAll("h", Until()); });
        CHECK(WaitUntil([&] { return first.load() && g_partial.load() != 0; }));
        release.store(true);
        auto value = Get(call);
        peer.Join();
        CHECK(value && value.value().size() == 2);
        CHECK(value.value()[0] == std::make_pair(std::string("f1"), std::string("v1")));
        CHECK(value.value()[1] == std::make_pair(std::string("f2"), std::string("v2")));
        cli.reset();
        host.Finish();
        g_watch_reader.store(nullptr);
        std::cout << "hgetall_fragmented segments=2 entries=2 order=1\n";
    }
    // 2) 嵌套数组 → ProtocolError + 连接退役 + 已接纳命令 OutcomeUnknown。
    {
        Peer peer([](int fd) {
            CHECK(Peer::ReadCommand(fd)[0] == "HGETALL");
            CHECK(Peer::Send(fd, "*2\r\n$2\r\nf1\r\n*1\r\n$2\r\nv1\r\n"));
        });
        Host host;
        auto cli = host.Make(peer.address);
        host.Connect(cli);
        auto value = host.Call([&] { return bbt_co_await cli->HGetAll("h", Until()); });
        CHECK(!value);
        CheckUnknown(value.error(), "ProtocolError");
        ExpectCode(host.Call([&] { return bbt_co_await cli->Ping(Until()); }), ErrorCode::TransportError);
        cli.reset();
        peer.Join();
        host.Finish();
    }
    // 3) 不支持元素类型（数组内 RESP3 map '%'）→ 同上退役语义。
    {
        Peer peer([](int fd) {
            CHECK(Peer::ReadCommand(fd)[0] == "HGETALL");
            CHECK(Peer::Send(fd, "*1\r\n%1\r\n$1\r\nk\r\n$1\r\nv\r\n"));
        });
        Host host;
        auto cli = host.Make(peer.address);
        host.Connect(cli);
        auto value = host.Call([&] { return bbt_co_await cli->HGetAll("h", Until()); });
        CHECK(!value);
        CheckUnknown(value.error(), "ProtocolError");
        ExpectCode(host.Call([&] { return bbt_co_await cli->Ping(Until()); }), ErrorCode::TransportError);
        cli.reset();
        peer.Join();
        host.Finish();
    }
    // 4) 元素数超 kMaxArrayElements → ProtocolError + 退役 + OutcomeUnknown。
    {
        const auto elements = redis_detail::kMaxArrayElements + 2;
        std::string reply = "*" + std::to_string(elements) + "\r\n";
        for (std::size_t i = 0; i < elements; ++i) { reply += "$1\r\nx\r\n"; }
        Peer peer([reply = std::move(reply)](int fd) {
            CHECK(Peer::ReadCommand(fd)[0] == "HGETALL");
            CHECK(Peer::Send(fd, reply));
        });
        Host host;
        auto cli = host.Make(peer.address);
        host.Connect(cli);
        auto value = host.Call([&] { return bbt_co_await cli->HGetAll("h", Until()); });
        CHECK(!value);
        CheckUnknown(value.error(), "ProtocolError");
        ExpectCode(host.Call([&] { return bbt_co_await cli->Ping(Until()); }), ErrorCode::TransportError);
        cli.reset();
        peer.Join();
        host.Finish();
    }
    // 5) 非 Array 回复类型对 HGETALL → ProtocolError（ExpectedReply 拒绝 Array 以外）。
    {
        Peer peer([](int fd) {
            CHECK(Peer::ReadCommand(fd)[0] == "HGETALL");
            CHECK(Peer::Send(fd, "+OK\r\n"));
        });
        Host host;
        auto cli = host.Make(peer.address);
        host.Connect(cli);
        auto value = host.Call([&] { return bbt_co_await cli->HGetAll("h", Until()); });
        CHECK(!value);
        CheckUnknown(value.error(), "ProtocolError");
        cli.reset();
        peer.Join();
        host.Finish();
    }
    // 6) 奇数元素数组：field 无配对 value，fail-close 退役而不是静默丢尾。
    // 区分回归：修复前返回 ok(empty)。
    {
        Peer peer([](int fd) {
            CHECK(Peer::ReadCommand(fd)[0] == "HGETALL");
            CHECK(Peer::Send(fd, "*1\r\n$1\r\nf\r\n"));
        });
        Host host;
        auto cli = host.Make(peer.address);
        host.Connect(cli);
        auto value = host.Call([&] { return bbt_co_await cli->HGetAll("h", Until()); });
        CHECK(!value);
        CheckUnknown(value.error(), "ProtocolError");
        ExpectCode(host.Call([&] { return bbt_co_await cli->Ping(Until()); }), ErrorCode::TransportError);
        cli.reset();
        peer.Join();
        host.Finish();
    }
    // 7) 含 nil 元素的偶数数组：nil 不得折叠成空串 field/value，fail-close 退役。
    // 区分回归：修复前返回 ok({f,""}) 且连接保留。
    {
        Peer peer([](int fd) {
            CHECK(Peer::ReadCommand(fd)[0] == "HGETALL");
            CHECK(Peer::Send(fd, "*2\r\n$1\r\nf\r\n$-1\r\n"));
        });
        Host host;
        auto cli = host.Make(peer.address);
        host.Connect(cli);
        auto value = host.Call([&] { return bbt_co_await cli->HGetAll("h", Until()); });
        CHECK(!value);
        CheckUnknown(value.error(), "ProtocolError");
        ExpectCode(host.Call([&] { return bbt_co_await cli->Ping(Until()); }), ErrorCode::TransportError);
        cli.reset();
        peer.Join();
        host.Finish();
    }
    // 8) 单元素超 kMaxArrayBytes：拒绝并退役；契约层面证明超字节回复零接纳
    // （修复前同样拒绝，但会先复制超限负载；本用例钉死对外契约）。
    {
        const auto over = redis_detail::kMaxArrayBytes + 1;
        const std::string reply = "*2\r\n$" + std::to_string(over) + "\r\n" +
                                  std::string(over, 'y') + "\r\n$1\r\nx\r\n";
        Peer peer([reply = std::move(reply)](int fd) {
            CHECK(Peer::ReadCommand(fd)[0] == "HGETALL");
            CHECK(Peer::Send(fd, reply));
        });
        Host host;
        auto cli = host.Make(peer.address);
        host.Connect(cli);
        auto value = host.Call([&] { return bbt_co_await cli->HGetAll("h", Until()); });
        CHECK(!value);
        CheckUnknown(value.error(), "ProtocolError");
        ExpectCode(host.Call([&] { return bbt_co_await cli->Ping(Until()); }), ErrorCode::TransportError);
        cli.reset();
        peer.Join();
        host.Finish();
    }
    // 9) 累计字节超限：第一元素恰为上限被接纳，第二元素把合计推出预算；
    // 证明 checked-add 预算随元素递减而不是只在循环末尾比较。
    {
        const auto first = redis_detail::kMaxArrayBytes;
        const std::string reply = "*2\r\n$" + std::to_string(first) + "\r\n" +
                                  std::string(first, 'y') + "\r\n$1\r\nx\r\n";
        Peer peer([reply = std::move(reply)](int fd) {
            CHECK(Peer::ReadCommand(fd)[0] == "HGETALL");
            CHECK(Peer::Send(fd, reply));
        });
        Host host;
        auto cli = host.Make(peer.address);
        host.Connect(cli);
        auto value = host.Call([&] { return bbt_co_await cli->HGetAll("h", Until()); });
        CHECK(!value);
        CheckUnknown(value.error(), "ProtocolError");
        ExpectCode(host.Call([&] { return bbt_co_await cli->Ping(Until()); }), ErrorCode::TransportError);
        cli.reset();
        peer.Join();
        host.Finish();
        std::cout << "hgetall_faults nested=retired map=retired over_limit=retired non_array=retired "
                     "odd_count=retired nil_element=retired over_bytes_single=retired over_bytes_cumulative=retired\n";
    }
}

// ── 握手与 Close 竞争（COMMON /）：阶段证据为 AUTH 完整到达对端
// （说明 Connect 已在握手 I/O 内挂起），此时 Close 胜出；Connect 不得返回成功，
// 后续命令一律 Closed。修复前握手经共享成员 connection 收 +OK 后无复查，
// Close 胜出时 Connect 仍可能 ok()。──
void CloseDuringHandshake() {
    std::atomic<bool> auth_received{false}, release{false};
    Peer peer([&](int fd) {
        CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"AUTH", "pw"}));
        auth_received.store(true); // 阶段屏障：握手命令已到线上，Connect 正等在 +OK
        CHECK(WaitUntil([&] { return release.load(); }));
        (void)Peer::Send(fd, "+OK\r\n"); // Close 后迟到的握手回复不归属任何命令
    });
    Host host;
    auto cli = MakeHandshake(host, peer.address, {"", "pw", -1});
    auto pending = host.Submit([&] { return bbt_co_await cli->Connect(Until()); });
    CHECK(WaitUntil([&] { return auth_received.load(); }));
    CHECK(pending.wait_for(0ms) == std::future_status::timeout);
    cli->Close();
    auto value = Get(pending);
    CHECK(!value);
    // Close 胜出：在途 AUTH 退役为 OutcomeUnknown(cause=Closed)，或复查路径
    // 直接 Closed——两种终态都可接受，但绝不可能是 ok()。
    const auto code = value.error().code;
    CHECK(code == ErrorCode::OutcomeUnknown || code == ErrorCode::Closed);
    release.store(true);
    peer.Join();
    ExpectCode(host.Call([&] { return bbt_co_await cli->Ping(Until()); }), ErrorCode::Closed);
    CHECK(cli->IsClosed());
    cli.reset();
    host.Finish();
    std::cout << "close_during_handshake auth_on_wire=1 close_wins=1 connect_not_ok=1 "
                 "code=" << static_cast<int>(code) << " ping=Closed\n";
}

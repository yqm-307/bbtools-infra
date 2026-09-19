#include "redis/RedisConnection.hpp"

#include <boost/asio/posix/descriptor.hpp>

#include "redis/CoRedisCliImpl.hpp"
#include "redis/RedisDetail.hpp"

namespace bbt::infra::redis_detail {

// hiredis ev 钩子 thunk（私有静态成员）：全部在所属 strand 上被同步
// 调用；privdata 即 ac->ev.data = RedisEvBridge*。

void RedisConnection::EvAddRead(void* priv) {
    auto& b = *static_cast<RedisEvBridge*>(priv);
    b.want_read = true;
    if (b.ac != nullptr && !b.read_armed)
        ArmRead(b);
}

void RedisConnection::EvDelRead(void* priv) {
    auto& b = *static_cast<RedisEvBridge*>(priv);
    b.want_read = false;
    // 已 arm 的等待不撤消：触发时见 want_read=false 即返回，下次 addRead 重新 arm。
}

void RedisConnection::EvAddWrite(void* priv) {
    auto& b = *static_cast<RedisEvBridge*>(priv);
    b.want_write = true;
    if (b.ac != nullptr && !b.write_armed)
        ArmWrite(b);
}

void RedisConnection::EvDelWrite(void* priv) {
    auto& b = *static_cast<RedisEvBridge*>(priv);
    b.want_write = false;
}

void RedisConnection::EvCleanup(void* priv) {
    auto& b = *static_cast<RedisEvBridge*>(priv);
    b.ac         = nullptr;
    b.want_read  = false;
    b.want_write = false;
    // fd 所有权归还 hiredis：release() 摘除 reactor 监视（在途等待以
    // operation_aborted 落定），绝不 close——redisFree 随后关 fd，
    // 提前 close 会让 hiredis 二次 close 撞复用 fd。
    try {
        static_cast<void>(b.desc.release());
    } catch (...) {
    }
}

void RedisConnection::EvScheduleTimer(void*, struct timeval) {
    // 本切片不设 hiredis connect/command timeout（deadline 由调用侧
    // CompletionSignal 承担），refreshTimeout 因此不会调到本 thunk。
}

void RedisConnection::OnConnectThunk(const redisAsyncContext* ac,
                                     int status) {
    auto* b = static_cast<RedisEvBridge*>(ac->ev.data);
    if (b == nullptr)
        return;
    if (auto conn = b->owner.lock())
        conn->OnConnectDone(status);
}

void RedisConnection::OnDisconnectThunk(const redisAsyncContext* ac,
                                        int status) {
    auto* b = static_cast<RedisEvBridge*>(ac->ev.data);
    if (b == nullptr)
        return;
    if (auto conn = b->owner.lock())
        conn->OnDisconnected(status, ac->err, ac->errstr);
}

void RedisConnection::ArmRead(RedisEvBridge& bridge) {
    bridge.read_armed = true;
    auto sb = bridge.shared_from_this();
    bridge.desc.async_wait(
        boost::asio::posix::stream_descriptor::wait_read,
        [sb](const boost::system::error_code& ec) {
            OnFdReadable(*sb, ec);
        });
}

void RedisConnection::ArmWrite(RedisEvBridge& bridge) {
    bridge.write_armed = true;
    auto sb = bridge.shared_from_this();
    bridge.desc.async_wait(
        boost::asio::posix::stream_descriptor::wait_write,
        [sb](const boost::system::error_code& ec) {
            OnFdWritable(*sb, ec);
        });
}

void RedisConnection::OnFdReadable(RedisEvBridge& bridge,
                                   const boost::system::error_code& ec) {
    bridge.read_armed = false;
    if (ec || !bridge.want_read || bridge.ac == nullptr)
        return;
    redisAsyncHandleRead(bridge.ac);
    // Handle* 内部可能已断开并 free（cleanup 把 ac 置空）；未死且仍要
    // 读才续 arm——hiredis 在 redisAsyncRead 里也会重复 _EL_ADD_READ。
    if (bridge.ac != nullptr && bridge.want_read && !bridge.read_armed)
        ArmRead(bridge);
}

void RedisConnection::OnFdWritable(RedisEvBridge& bridge,
                                   const boost::system::error_code& ec) {
    bridge.write_armed = false;
    if (ec || !bridge.want_write || bridge.ac == nullptr)
        return;
    redisAsyncHandleWrite(bridge.ac);
    // connect 未完成时 HandleWrite 提前返回、不经 redisAsyncWrite 的
    // _EL_ADD_WRITE 续 arm，必须在本层兜底续 arm，否则连接完成事件丢失。
    if (bridge.ac != nullptr && bridge.want_write && !bridge.write_armed)
        ArmWrite(bridge);
}

void RedisConnection::OpenOnIoDomain() {
    if (m_dead || m_state != State::kIdle)
        return;
    auto owner = m_owner.lock();
    if (!owner)
        return;
    m_state = State::kConnecting;

    // hiredis 在内部做 getaddrinfo+socket+nonblock connect：数值 IP 立刻
    // 返回，主机名则在 io 域内同步解析（本切片限制，见决策文档）。
    redisAsyncContext* ac = redisAsyncConnect(m_host.c_str(), m_port);
    if (ac == nullptr) {
        m_state = State::kIdle;
        NotifyDownOnIoDomain(MakeError(ErrorCode::InternalError,
            "redis: failed to allocate async context"));
        return;
    }
    if (ac->err != 0) {
        Error e = ClassifyHiredisError(ac->err, ac->errstr,
            "redis: connect initiate failed");
        redisAsyncFree(ac);   // ev 钩子未安装，cleanup 空转安全
        m_state = State::kIdle;
        NotifyDownOnIoDomain(std::move(e));
        return;
    }

    auto bridge = std::make_shared<RedisEvBridge>(m_io);
    bridge->ac    = ac;
    bridge->owner = weak_from_this();
    ac->ev.data          = bridge.get();
    ac->ev.addRead       = &RedisConnection::EvAddRead;
    ac->ev.delRead       = &RedisConnection::EvDelRead;
    ac->ev.addWrite      = &RedisConnection::EvAddWrite;
    ac->ev.delWrite      = &RedisConnection::EvDelWrite;
    ac->ev.cleanup       = &RedisConnection::EvCleanup;
    ac->ev.scheduleTimer = &RedisConnection::EvScheduleTimer;

    boost::system::error_code ec;
    bridge->desc.assign(ac->c.fd, ec);
    if (ec) {
        redisAsyncFree(ac);
        m_state = State::kIdle;
        NotifyDownOnIoDomain(MakeError(ErrorCode::InternalError,
            "redis: fd assign to io reactor failed"));
        return;
    }
    m_bridge = bridge;
    m_ac     = ac;

    // SetConnectCallback 内部立即 _EL_ADD_WRITE——钩子和 desc 必须先装好。
    if (redisAsyncSetConnectCallback(ac, &RedisConnection::OnConnectThunk) !=
            REDIS_OK ||
        redisAsyncSetDisconnectCallback(
            ac, &RedisConnection::OnDisconnectThunk) != REDIS_OK) {
        redisAsyncFree(ac);
        m_bridge.reset();
        m_ac    = nullptr;
        m_state = State::kIdle;
        NotifyDownOnIoDomain(MakeError(ErrorCode::InternalError,
            "redis: connect/disconnect callback install failed"));
        return;
    }
    // 未 CONNECTED 阶段 hiredis 只监听可写事件（connect 完成判定在
    // HandleWrite 里）；读兴趣由建立连接后的 hiredis 自行 _EL_ADD_READ。
    EvAddRead(bridge.get());
}

bool RedisConnection::SendOnIoDomain(redisCallbackFn* fn, void* privdata,
                                     int argc, const char** argv,
                                     const size_t* argvlen) {
    if (!CanSend())
        return false;
    return redisAsyncCommandArgv(m_ac, fn, privdata, argc, argv, argvlen) ==
           REDIS_OK;
}

void RedisConnection::OnConnectDone(int status) {
    if (status == REDIS_OK) {
        m_state = State::kConnected;
        NotifyUpOnIoDomain();
        return;
    }
    // connect 失败：hiredis 在回调返回后立即 free 本 context 且不触发
    // onDisconnect。err/errstr 只在回调内有效，先取再走。
    Error e;
    if (m_ac != nullptr && m_ac->err != 0)
        e = ClassifyHiredisError(m_ac->err, m_ac->errstr,
            "redis: connect failed");
    else
        e = MakeError(ErrorCode::TransportError, "redis: connect failed");
    m_ac    = nullptr;
    m_state = State::kIdle;
    NotifyDownOnIoDomain(std::move(e));
}

void RedisConnection::OnDisconnected(int status, int err,
                                     const char* errstr) {
    // context 正在/已经 free：只留状态，不再触碰 ac。
    m_ac = nullptr;
    if (!m_dead)
        m_state = State::kIdle;
    Error e = status == REDIS_OK
        ? MakeError(ErrorCode::TransportError, "redis: connection closed")
        : ClassifyHiredisError(err, errstr, "redis: connection lost");
    NotifyDownOnIoDomain(std::move(e));
}

void RedisConnection::NotifyUpOnIoDomain() {
    if (auto owner = m_owner.lock())
        owner->OnConnReadyOnIoDomain();
}

void RedisConnection::NotifyDownOnIoDomain(Error err) {
    if (auto owner = m_owner.lock())
        owner->OnConnDownOnIoDomain(std::move(err));
}

void RedisConnection::TeardownOnIoDomain() noexcept {
    m_dead = true;
    if (m_ac == nullptr)
        return;
    redisAsyncContext* ac = m_ac;
    m_ac    = nullptr;
    m_state = State::kClosed;
    // free 同步催出：pending 命令 NULL 回调 → cleanup（desc.release）
    // →（已连接时）onDisconnect→NotifyDown。bridge 此后只剩占位。
    redisAsyncFree(ac);
}

} // namespace bbt::infra::redis_detail

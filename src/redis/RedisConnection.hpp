#pragma once
// RedisConnection：单条 hiredis async 连接的拥有者与自定义 event adapter。
// 仅 src/redis/ 内部使用；同一时刻只在所属 strand（io 域）上发起、
// 取消、断开和释放，hiredis 不跨线程。
//
// adapter 形态：ac->ev.* 钩子由本类静态 thunk 填充；fd 用
// boost::asio::posix::stream_descriptor 挂进 strand 的 reactor
// （不拥有 io_context，不新建线程）。hiredis 约定：
//   - 可读 → redisAsyncHandleRead，可写 → redisAsyncHandleWrite；
//   - addRead/delRead/addWrite/delWrite 必须幂等（async.c 会重复调）；
//   - cleanup 在断开/释放路径上必然调用且可重复，fd 所有权归还
//     hiredis（desc.release()，绝不 close——redisFree 负责关）；
//   - scheduleTimer 仅在设置 connect/command timeout 时被调，本切片
//     不设 hiredis 超时（deadline 由调用侧 CompletionSignal 承担），
//     thunk 为空转。
// 关键时序：
//   - redisAsyncSetConnectCallback 内部立即 _EL_ADD_WRITE，因此 ev
//     钩子与 desc.assign 必须先完成；
//   - connect 失败路径只调 onConnect(REDIS_ERR) 随后 free（不调
//     onDisconnect）；已连接后断开才走 onDisconnect；
//   - free/断开时所有 pending 命令回调先收到 NULL reply，再
//     cleanup→onDisconnect；Handle* 返回后不得再触碰 ac。

#include <atomic>
#include <memory>
#include <string>

#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/system/error_code.hpp>

#include <hiredis/async.h>
#include <hiredis/hiredis.h>

#include <bbt/infra/Result.hpp>

#include "detail/IoSupport.hpp"

namespace bbt::infra::redis_detail {

class RedisConnection;
class CoRedisCliImpl;

// ev.data 承载的桥：hiredis 同步调用钩子（均在 strand 上），异步等待
// handler 持 shared_ptr 保活；cleanup 后置 ac=null，迟到 handler 安全返回。
struct RedisEvBridge : std::enable_shared_from_this<RedisEvBridge> {
    explicit RedisEvBridge(const detail::IoStrand& io) : desc(io) {}

    redisAsyncContext*                       ac{nullptr};
    std::weak_ptr<RedisConnection>           owner;
    boost::asio::posix::stream_descriptor    desc;
    bool want_read{false};
    bool want_write{false};
    bool read_armed{false};
    bool write_armed{false};
};

class RedisConnection : public std::enable_shared_from_this<RedisConnection> {
public:
    enum class State { kIdle, kConnecting, kConnected, kClosed };

    RedisConnection(const detail::IoStrand& io, std::string host,
                    std::uint16_t port, std::weak_ptr<CoRedisCliImpl> owner)
        : m_io(io), m_host(std::move(host)), m_port(port),
          m_owner(std::move(owner)) {}

    // 以下全部只在 io 域（strand）调用。
    // kIdle 时发起非阻塞连接；其余状态幂等返回。teardown 后拒绝重开。
    void OpenOnIoDomain();
    // 仅 kConnected 可发：经 redisAsyncCommandArgv 追加到输出缓冲并
    // 注册完成回调；返回 false 表示 context 已在断开/释放中。
    bool SendOnIoDomain(redisCallbackFn* fn, void* privdata,
                      int argc, const char** argv, const size_t* argvlen);
    bool CanSend() const { return m_state == State::kConnected && m_ac != nullptr; }

    // 一次性物理回收：redisAsyncFree 同步催出 pending 命令的 NULL
    // 回调、cleanup 与（已连接时）onDisconnect；返回后后端不再访问。
    void TeardownOnIoDomain() noexcept;
    bool Dead() const { return m_dead; }

    // 连接回调（thunk → 本类）：hiredis 在 Handle*/Free 内部同步调用。
    void OnConnectDone(int status);
    void OnDisconnected(int status, int err, const char* errstr);

    // fd 事件入口（wait handler → 本类桥）：调 hiredis Handle*。
    static void OnFdReadable(RedisEvBridge& bridge,
                             const boost::system::error_code& ec);
    static void OnFdWritable(RedisEvBridge& bridge,
                             const boost::system::error_code& ec);

private:
    static void ArmRead(RedisEvBridge& bridge);
    static void ArmWrite(RedisEvBridge& bridge);

    // hiredis ev/连接钩子 thunk：以私有静态成员实现，仅在成员函数内
    // 取地址注册到 ac->ev.*；hiredis 全部在 strand 上同步调用。
    static void EvAddRead(void* priv);
    static void EvDelRead(void* priv);
    static void EvAddWrite(void* priv);
    static void EvDelWrite(void* priv);
    static void EvCleanup(void* priv);
    static void EvScheduleTimer(void* priv, struct timeval tv);
    static void OnConnectThunk(const redisAsyncContext* ac, int status);
    static void OnDisconnectThunk(const redisAsyncContext* ac, int status);

    // connect/disconnect 阶段按 ac->err 组装契约 Error 并通知 owner。
    void NotifyDownOnIoDomain(Error err);
    void NotifyUpOnIoDomain();

    detail::IoStrand                  m_io;
    std::string                       m_host;
    std::uint16_t                     m_port;
    std::weak_ptr<CoRedisCliImpl>     m_owner;

    State                             m_state{State::kIdle};
    bool                              m_dead{false};
    redisAsyncContext*                m_ac{nullptr};
    std::shared_ptr<RedisEvBridge>    m_bridge;
};

} // namespace bbt::infra::redis_detail

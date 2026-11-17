#pragma once

#include <atomic>
#include <memory>
#include <bbt/infra/net/RawHandler.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/EngineSupport.hpp>

namespace bbt::infra::net {

class Reactor;

/**
 * owner 侧连接核心：唯一 fd、读/写泵与原始回调派发。
 */
class ConnCore : public std::enable_shared_from_this<ConnCore> {
public:
    ConnCore(
        std::shared_ptr<ConnectionState> state,
        TcpWait descriptor,
        Reactor* owner,
        std::shared_ptr<RawHandler> handler);

    ~ConnCore();

    const std::shared_ptr<ConnectionState>& State() const;

    std::size_t RecvHold() const;

    bool Closed() const;

    int Fd();

    std::atomic<bool> m_dirty_queued{false};
    ConnCore* m_dirty_next{nullptr};
    bool m_dirty_linked{false};

    void StartOwner();

    void PumpOwner();

    /**
     * 关闭前把 m_recv_hold 以不取 ring 数据锁的方式转入 state 寿命的 detached；
     * 失败则保留 hold 并返回 false（调用者本次不关闭，等下一次推进重试）。
     */
    bool StageHoldToRing();

    /**
     * owner：正常收口。分阶段幂等：stage hold → 关 backend → 发布读终态/结算写
     * → 逐项 OnWrite → 唯一 OnClose。中途异常可由下次进入从失败阶段继续，不因
     * 先标 closed/先擦表丢责任。
     */
    void ShutdownOwner(std::error_code reason);

    /**
     * 控制域（解绑后）：关闭 fd、冻结状态、唤醒内部等待者，不调用户回调。
     */
    void CancelControl();

private:
    void ArmReadOwner();

    void ArmWriteOwner();

    /**
     * 提交已取内核字节：TryAppend，Busy 走 AppendDeferred；均失败则保留在
     * m_recv_buf（m_recv_hold），下次 owner 推进重试，绝不再 recv 覆盖。
     */
    bool SubmitReceivedOwner(const char* data, std::size_t size);

    void DoReadsOwner();

    void NotifyReadOwner();

    void PumpWritesOwner();

    void DeliverTerminalOwner();

    std::shared_ptr<ConnectionState> m_state;
    TcpWait m_desc;
    Reactor* m_owner;
    std::shared_ptr<RawHandler> m_handler;
    bool m_read_waiting{false};
    bool m_write_waiting{false};
    bool m_read_paused{false};
    bool m_read_stopped{false};

    /**
     * terminal done：OnWrite/OnClose 已兑现
     */
    bool m_closed{false};
    bool m_backend_closed{false};
    bool m_ring_closed{false};
    bool m_onclose_done{false};
    std::unique_ptr<char[]> m_recv_buf;
    std::atomic<std::size_t> m_recv_hold{0};
};

// clang-format off
}
// clang-format on

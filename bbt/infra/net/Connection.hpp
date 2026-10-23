#pragma once

// 公共连接句柄（R2）：稳定值语义，持共享连接状态（ring/操作结果/关闭状态）。
// 句柄寿命独立于 socket/context：显式关闭或 Engine 收口后，已收数据仍可读；
// 残留句柄不访问已销毁 context，也不与 Engine 成强环。
//
// 线程/域约束：AsyncWrite/TryRead/状态查询/RequestClose 可在任意合法调用域；
// 物理 I/O 与原始回调只由 owner 推进。net 不做分帧、请求匹配或重发。
//
// 身份：Id() 只在创建它的 Engine 内唯一（连接 id 与建连操作 id 各自递增），
// 不是跨 Engine/跨进程身份，也不是取消、等待或查找句柄。无效句柄 Id() 为 0。
//
// 状态不公开：内部状态类型与 ring 表示不属冻结面。实现放在 Connection.cc，
// 本头只前向声明内部类型，因此消费者无法依赖 state/ring 布局，也不能凭任意
// 状态构造句柄——构造仅供 Engine owner 与内部 TestAccess 使用。

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <system_error>
#include <utility>

#include <bbt/infra/net/ReadAccess.hpp>
#include <bbt/infra/net/Types.hpp>

namespace bbt::infra::net {

class ConnectionState;
struct WriteOpRecord;

// 单次已接纳发送的观察句柄：持操作记录，终态在句柄寿命内永久可查；
// 释放句柄不取消已接纳发送。不是通用 Future/Task 平台。
// 无效句柄（未接纳或已被复用覆盖）的 Query 为 Unknown/0/空。
class WriteOperation {
public:
    WriteOperation() = default;

    bool Valid() const noexcept;
    std::uint64_t Id() const noexcept;
    WriteOutcome Query() const;

private:
    friend class Connection;
    friend class ConnectionTestAccess;
    explicit WriteOperation(std::shared_ptr<WriteOpRecord> record);

    std::shared_ptr<WriteOpRecord> m_record;
};

class Connection {
public:
    Connection() = default;

    bool Valid() const noexcept;
    std::uint64_t Id() const noexcept;

    // 全量接纳或零接纳；成功后库取得数据所有权，观察句柄用于终态查询/等待。
    // 每次调用先置 out_operation 无效：拒绝时保持无效且不泄露上次结果，
    // 成功时写入本次新身份。
    std::error_code AsyncWrite(std::string data, WriteOperation& out_operation);

    // 竞争 Busy（零消费）；空且无终态 WouldBlock；Ok 已复制并消费。
    ReadResult TryRead(std::string& out);

    // 组合 Peek+Consume 守卫（见 ReadAccess.hpp 的寿命与有效期约束）。
    // 守卫持本连接状态强引用：即使随后释放本句柄，守卫内访问仍安全。
    ReadAccess TryAccess();

    std::size_t ReadableBytes();
    std::error_code ReadError() const;
    bool ReadEof() const;

    std::error_code LastWriteError(std::uint64_t* operation = nullptr);
    AcceptanceState WriteAcceptance() const;

    bool Closed() const;
    std::error_code CloseReason() const;

    // 请求封口：立即拒绝新写并唤 owner；物理 fd 关闭、已 recv 尾部入真源、
    // 最终读 EOF 与 OnClose 由 owner 兑现。请求后读侧可能短暂 WouldBlock，
    // 不得在尾部尚未入 ring 前返回 EOF。幂等；首次请求的 reason（包括默认 {}）
    // 固定，后续请求不覆盖，最终关闭状态不回退。
    void RequestClose(std::error_code reason = {});

private:
    // 内部构造责任：只有 Engine owner（Engine/Reactor/ConnCore）与内部测试接缝
    // 可以凭状态构造句柄，消费者不能。
    friend class Engine;
    friend class Reactor;
    friend class ConnCore;
    friend struct CoAccess;
    friend class EngineTestAccess;
    friend class ConnectionTestAccess;
    friend class ReadAccess;
    explicit Connection(std::shared_ptr<ConnectionState> state);

    std::shared_ptr<ConnectionState> m_state;
};

} // namespace bbt::infra::net

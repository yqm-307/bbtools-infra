#pragma once

/**
 * @file ICoConnection.hpp
 * @brief 受管协程连接的公共接口。协程消费面只依赖本接口与值类型。
 *
 * 定位：协议无关的单连接协程视图。它只表达读、写、关闭与状态观察，不承诺分帧、请求匹配、
 * 重发或对端回包。raw 消费面（Connection 与 RawHandler 回调）独立保留，不经本接口。
 *
 * 域与门禁：
 *   - Read、Write、WaitClosed 之外的状态投影（Id、Valid、Status、IsClosed、IsReleased）
 *     可在任意合法调用域使用，不挂起。
 *   - Read、Write、WaitClosed 必须在受管协程内调用。非受管协程（普通线程、runtime idle、
 *     raw owner 回调）返回 operation_not_permitted，包括有现成数据、零长写或连接已就绪的快路径。
 *     门禁先于任何状态判定与副作用。
 *   - options 的 deadline 只影响本次等待。超时或取消不撤销已接纳的 I/O，不影响之后的快路径，
 *     也不释放供其它等待者取得的资源。
 *
 * 输出语义：Read 失败不交付 Buffer，也不消费数据。Write 返回的 WriteReport 每次调用覆盖本次
 * 结果，不保留上次结果。WaitClosed 成功只表示观察到 closed 终态，不代表物理资源已回收；
 * 物理回收由 IsReleased 体现。
 *
 * 生命周期：ICoConnection 是接口，其实现（如 CoTcpConnection）是值语义的视图。销毁视图不关闭
 * 连接；关闭由 RequestClose 发起，由 owner 完成物理收口。不自动为连接建业务协程，不自动关闭、
 * 重连或重发。
 */

#include <cstdint>
#include <string>
#include <system_error>
#include <tuple>
#include <utility>

#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/net/Buffer.hpp>
#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/ReadView.hpp>
#include <bbt/infra/net/Types.hpp>
#include <bbt/infra/net/WriteOperation.hpp>

namespace bbt::infra::net {

/**
 * @brief 连接状态投影：诊断与资源观察值。
 *
 * 每个字段来自既有真源，读、写两个方向分别采样，不承诺原子的总快照。需要一致结论的调用方应等待
 * WaitClosed 或 IsReleased 进入终态；不能靠一次 Status 证明下一次操作必然成功。
 */
struct ConnectionStatus {
    bool valid{false};                              ///< 句柄是否持有效共享状态
    std::size_t readable_bytes{0};                  ///< 库内已发布、尚未交付字节采样
    bool read_eof{true};                            ///< 读方向已见 EOF（可与尾部字节共存）
    std::error_code read_error;                     ///< 持久读错误（可与尾部字节共存）
    AcceptanceState write{AcceptanceState::Closed}; ///< 写接纳状态真源采样
    bool closed{true};                              ///< 逻辑关闭终态（acceptance Closed 或等价）
    std::error_code close_reason;                   ///< 第一次请求原因固定（含默认空原因）
    bool released{true};                            ///< fd 已关且 owner 本连接责任清理
};

/**
 * @brief 协议无关的单连接协程接口。
 *
 * 只表达读、写、关闭与状态观察；实现方（如 CoTcpConnection）是值语义视图，销毁视图不关闭连接。
 */
class ICoConnection {
public:
    virtual ~ICoConnection() = default;

    // ---- 读：等待后复制消费，或短时只读查看 ----
    /**
     * @brief 等待并复制全部已发布的可读字节到调用方 Buffer，然后消费连接数据。
     *
     * 仅业务协程可调用，可挂起。先执行门禁，再访问数据或快路径。有数据时，原子取走线性化点上
     * 所有已发布的可读字节，复制到 out 后消费连接数据。Busy 或 WouldBlock 由内部等待处理。先交付尾部数据，
     * 耗尽后才返回 Eof 或 Error。options 只约束等待；超时、取消或内存不足都不消费数据，
     * 也不会自动 RequestClose。
     */
    virtual std::error_code Read(Buffer& out, const bbt::coroutine::WaitOptions& options = {}) = 0;
    /**
     * @brief 等待读取并以 tuple 返回缓冲区和结果。
     *
     * 失败时不写入 out，也不消费连接数据。
     *
     * @param options 本次等待的选项；deadline 只约束本次等待。
     * @return 包含读取缓冲区和错误码的 tuple；失败时缓冲区为空。
     */
    std::tuple<Buffer, std::error_code> Read(const bbt::coroutine::WaitOptions& options = {});

    /// @brief 任意合法调用域；不等待、不校验协程上下文。
    virtual std::error_code TryRead(Buffer& out) = 0;
    /**
     * @brief 非等待读取并以 tuple 返回缓冲区和结果。
     *
     * @return 包含读取缓冲区和错误码的 tuple；失败时缓冲区为空。
     */
    std::tuple<Buffer, std::error_code> TryRead();

    /// @brief 非等待只读查看；不消费，不拥有底层存储，视图不可跨下一次 I/O 使用。
    virtual ReadView View() = 0;

    // ---- 写：非挂起提交 ----
    /**
     * @brief 全量接纳或零接纳，不等待容量或发送完成。
     *
     * 任意合法调用域可用，不挂起。空错误只表示库已接纳，后续由 owner 发送；
     * 队列满、连接关闭等接纳错误直接返回，不返回可等待票据、不自动重试。
     * 需要等待容量与本地发送完成时使用 Write。后续发送错误由 raw OnWrite/LastWriteError 观察。
     * 按值取得字符串，move 后即使被拒绝，也不保证调用方原字符串内容保留。
     */
    virtual std::error_code AsyncWrite(std::string data) = 0;

    // ---- 写：协程组合等待 ----
    /**
     * @brief 同一绝对 deadline 依次等待容量、接纳与本地完成，不等待业务回包。
     *
     * 仅业务协程可调用，可挂起。等容量失败时结果为 Rejected。接纳后即使等待结束，也返回 op 与
     * 真实采样。
     */
    virtual WriteReport Write(std::string data, const bbt::coroutine::WaitOptions& options = {}) = 0;

    // ---- 关闭 ----
    /**
     * @brief 请求封口：拒绝新写并唤醒 owner，首次请求的 reason 被固定。
     *
     * 任意合法调用域可用，不挂起，幂等。不保证冲刷未发完的写；owner 结算这些写，保留已发出的
     * 前缀，不把未发完的写报告为成功。fd 与已收到的尾部数据由 owner 收口。析构与普通 I/O 错误
     * 不会隐式触发这一策略。
     */
    virtual void RequestClose(std::error_code reason = {}) = 0;

    /**
     * @brief 等待 Status().closed 对应的既有谓词成立。
     *
     * 仅业务协程可调用，可挂起。返回的 error_code 只描述本次等待。成功后，关闭原因从
     * Status().close_reason 读取。它不是 WaitReleased，不消费 ring，也不等待业务协程或
     * RawHandler 回调全部退出。
     */
    virtual std::error_code WaitClosed(const bbt::coroutine::WaitOptions& options = {}) = 0;

    // ---- 状态投影 ----
    /**
     * @brief 返回连接状态独立副本，无网络探活副作用。
     *
     * 任意合法调用域可用，不挂起。返回值为独立副本，无需持锁使用。每个字段来自既有真源，读、写
     * 方向分别采样，不承诺原子的总快照。
     */
    virtual ConnectionStatus Status() const = 0;

    // 非虚便捷投影，不维护第二套标志；这些投影也不提供跨查询原子性。
    /// @brief 是否持有效共享状态；等价于 Status().valid。
    bool Valid() const
    {
        return Status().valid;
    }
    /// @brief 逻辑关闭终态；等价于 Status().closed。
    bool IsClosed() const
    {
        return Status().closed;
    }
    /// @brief 物理资源是否已回收；等价于 Status().released。
    bool IsReleased() const
    {
        return Status().released;
    }
};

} // namespace bbt::infra::net

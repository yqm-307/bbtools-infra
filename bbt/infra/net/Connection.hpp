#pragma once
#include <bbt/infra/CloseOptions.hpp>

/**
 * @file Connection.hpp
 * @brief 公共连接句柄：值语义，持有共享的连接状态（ring、操作结果、关闭状态）。
 *
 * 句柄寿命独立于 socket 与 context。显式关闭或 Engine 收口后，已收数据仍可读。
 * 残留句柄不访问已销毁的 context，也不与 Engine 形成强引用环。
 *
 * 线程与域约束：AsyncWrite、TryRead、状态查询与 Close(Immediate) 可在任意合法调用域使用。
 * 物理 I/O 与原始回调只由 owner 推进。net 不做分帧、请求匹配或重发。
 *
 * 身份：Id() 只在创建它的 Engine 内唯一。连接 id 与建连操作 id 各自递增。它不是跨 Engine
 * 或跨进程的身份，也不是取消、等待或查找句柄。无效句柄的 Id() 为 0。
 *
 * 状态不公开：内部状态类型与 ring 表示不属于公共契约。实现位于 Connection.cc，本头只前向
 * 声明内部类型。消费方不能依赖 state 或 ring 的布局，也不能凭任意状态构造句柄，构造只供
 * Engine owner 与内部测试接缝使用。
 */
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <system_error>
#include <utility>
#include <tuple>

#include <bbt/infra/net/Buffer.hpp>

#include <bbt/infra/net/ReadView.hpp>
#include <bbt/infra/net/Types.hpp>
#include <bbt/infra/net/WriteOperation.hpp>

namespace bbt::infra::net {

class ConnectionState;

/**
 * detail：物理回收共享释放记录，布局不出公共面
 */
struct ReclaimRecord;

/**
 * detail：Co 内部步骤门面（唯一内部构造入口）
 */
struct CoAccess;

/**
 * @brief 协议无关的连接句柄，读侧提供 ring 访问，写侧提供非挂起提交。
 *
 * 副本共享同一连接状态，销毁副本不关闭连接。不做分帧、请求匹配与重发。
 */
class Connection {
public:
    Connection() = default;

    /**
     * @brief 是否持有有效共享状态。
     */
    bool Valid() const noexcept;

    /**
     * @brief 连接身份，仅在创建它的 Engine 内唯一；无效句柄为 0。
     */
    std::uint64_t Id() const noexcept;

    /**
     * @brief 非挂起提交一次写，全量接纳或零接纳。
     *
     * 返回空错误仅表示已接纳，由 owner 后续发送，不表示本地发送完成。
     * 队列满或连接关闭等接纳错误立即返回；不等待、不返回操作票，不自动重试。
     * 后续终态经 raw OnWrite/LastWriteError 观察；需要等待时使用 CoTcpConnection::Write。
     * 按值接管输入，move 后即使被拒绝也不保证调用方原字符串内容保留。
     */
    std::error_code AsyncWrite(std::string data);

    /**
     * @brief 复制并消费当前已发布数据到调用方自己的 Buffer；成功不移交内部存储。
     */
    std::error_code TryRead(Buffer& out);

    /**
     * @brief 非挂起读取并以 tuple 返回缓冲区和结果。
     *
     * 委托给 TryRead(Buffer&)；失败时不消费数据，返回的 Buffer 为空。
     *
     * @return 包含读取缓冲区和错误码的 tuple；成功时错误码为空。
     */
    std::tuple<Buffer, std::error_code> TryRead();

    /**
     * @brief 兼容复制入口：竞争 Busy（零消费）；空且无终态 WouldBlock。
     */
    ReadResult TryRead(std::string& out);

    /**
     * @brief 非等待只读查看；视图只在本次同步使用期间有效，不消费数据。
     */
    ReadView View();

    /**
     * @brief 已发布、尚未交付的字节数采样；无效句柄为 0。
     */
    std::size_t ReadableBytes();

    /**
     * @brief 持久读错误，可与尾部字节共存；无效句柄视为无错误。
     */
    std::error_code ReadError() const;

    /**
     * @brief 读方向是否已见 EOF，可与尾部字节共存；无效句柄视为已到 EOF。
     */
    bool ReadEof() const;

    /**
     * @brief 最近一次写失败的原因。
     *
     * operation 非空时写入对应操作身份，无对应操作时为 0。无效句柄返回 NetErrc::Closed。
     */
    std::error_code LastWriteError(std::uint64_t* operation = nullptr);

    /**
     * @brief 写方向接纳状态的真源采样；无效句柄为 Closed。
     */
    AcceptanceState WriteAcceptance() const;

    /**
     * @brief 逻辑关闭终态；无效句柄视为已关闭。
     */
    bool Closed() const;

    /**
     * @brief 第一次请求封口的关闭原因；无效句柄返回 NetErrc::Closed。
     */
    std::error_code CloseReason() const;

    /**
     * @brief 物理回收观察：无效句柄返回 true。
     *
     * 有效句柄仅在底层 fd 已物理关闭，且 owner 清理完对本连接的资源责任后才返回 true。
     * Closed() 是逻辑接纳的终态，可能早于物理收口，例如请求封口后 fd 仍由 owner 持有。
     * Released() 不替代 Closed()。
     */
    bool Released() const noexcept;

    /**
     * @brief 请求封口：立即拒绝新写并唤醒 owner。
     *
     * 物理 fd 关闭、已接收的尾部数据入库、最终读 EOF 与 OnClose，均由 owner 完成。请求之后读侧
     * 可能短暂返回 WouldBlock，在尾部数据入 ring 之前不会返回 EOF。该操作幂等。首次请求的 reason
     * （包括默认的 {}）被固定，之后的请求不覆盖，已达到的关闭状态不会回退。
     */
    std::error_code Close(const CloseOptions& options = {}, std::error_code reason = {});

private:
    /**
     * 内部构造责任：只有 Engine owner（Engine/Reactor/ConnCore）与内部测试接缝
     * 可以凭状态构造句柄，消费者不能。
     */
    friend class Engine;
    friend class Reactor;
    friend class ConnCore;
    friend struct CoAccess;
    friend class EngineTestAccess;
    friend class ConnectionTestAccess;

    explicit Connection(std::shared_ptr<ConnectionState> state);

    std::shared_ptr<ConnectionState> m_state;
};

// clang-format off
}
// clang-format on

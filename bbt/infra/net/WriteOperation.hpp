#pragma once

/**
 * @file WriteOperation.hpp
 * @brief Write 的观察句柄与结算报告；AsyncWrite 不返回操作句柄。
 *
 * 与 Connection 同源，值语义稳定。拆成独立头，使 CoTcpConnection 与 ICoConnection 只依赖本头，
 * 而不是整个 Connection.hpp。
 *
 * 关键语义：
 *   - WriteOperation 是单次已接纳发送的观察句柄，持有操作记录。终态在句柄寿命内永久可查。
 *     释放句柄不会取消发送。无效句柄的 Query 返回 Unknown、0 与空错误。
 *   - Wait 等待同一条记录的终态。超时或取消只结束本次等待，不撤销已接纳的发送。owner 继续推进，
 *     之后到达的终态仍保存在记录上。
 *   - WriteReport 是成员 Write 的三元结果：wait_error 描述本次等待的结果；outcome 描述发送在
 *     采样时刻的状态；operation 供事后查询。任何字段都不推断其它字段的终态。
 */

#include <cstdint>
#include <memory>
#include <system_error>

#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/net/Types.hpp>

namespace bbt::infra::net {

class ConnectionState;
struct WriteOpRecord; // detail：写操作共享记录，布局不出公共面

/**
 * @brief 单次已接纳发送的观察与等待句柄。
 *
 * 值语义，默认无效。持有记录的强引用，连接句柄释放后仍可 Query 与 Wait。它不是通用的 Future
 * 或 Task。
 *
 * 只由 Write 在成功接纳后交付。无效句柄的 Query 返回 Unknown。
 * 释放句柄不取消发送，也不延长 Engine 或 fd 的寿命。
 */
class WriteOperation {
public:
    WriteOperation() = default;

    /// @brief 是否持有 Write 成功接纳的操作记录。
    bool Valid() const noexcept;
    /// @brief 操作身份；与 Write 返回的票对应。无效票返回 0。
    std::uint64_t Id() const noexcept;

    /**
     * @brief 非阻塞查询，已接纳的票查询同一条记录的终态。
     *
     * 句柄寿命内永久可查。无身份票返回 Unknown、0 与空错误。
     */
    WriteOutcome Query() const;

    /**
     * @brief 在协程内等待本操作的终态。
     *
     * 门禁先于快路径：非法调用域返回 operation_not_permitted，即使终态已经存在也如此。
     * 超时或取消只结束等待，不撤销已接纳的发送，终态仍保存在记录上，之后可再次 Query 或 Wait 取得。
     * 无身份票返回 invalid_argument。
     * outcome 非空时，写入返回时的采样。
     */
    std::error_code Wait(const bbt::coroutine::WaitOptions& options, WriteOutcome* outcome = nullptr) const;

private:
    // 构造责任：CoTcpConnection::Write 经 ConnectionState 分配记录并包装，
    // 消费方不能凭任意状态构造。state 以 weak 方式持有：Wait 在局部取得强引用来订阅 write hub，
    // 不与连接状态形成引用环。state 回收后，record 仍可被 Query。
    friend class CoTcpConnection;
    friend class ConnectionTestAccess;
    WriteOperation(std::shared_ptr<WriteOpRecord> record, std::weak_ptr<ConnectionState> state = {});

    std::shared_ptr<WriteOpRecord> m_record;
    std::weak_ptr<ConnectionState> m_state;
};

/**
 * @brief 成员 Write 的结算结果：等待错误、发送终态采样与观察句柄三者组合。
 *
 *   - wait_error：等待正常完成时为空。调用域非法、超时、取消或容量等待失败时，写入具体错误。
 *     它描述本次等待，不描述本次发送。
 *   - outcome：Rejected、0 与 error 表示未提交。InFlight 表示已接纳但仍在途。Done 且 error 为空，
 *     才表示本地完成，这不等于对端已回包。
 *   - operation：未提交或调用域非法时为无效句柄。已接纳时为有效句柄，终态可查。
 */
struct WriteReport {
    std::error_code wait_error;                        ///< 本次等待的结果，不描述发送本身
    WriteOutcome outcome{WritePhase::Rejected, 0, {}}; ///< 返回时对发送状态的采样
    WriteOperation operation;                          ///< 已接纳时的观察句柄；未提交时为无效
};

} // namespace bbt::infra::net

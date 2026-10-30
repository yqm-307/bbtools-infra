#pragma once

// 公共写句柄与写报告（docs/decisions/0012-icoconn-member-interface.md）：ICoConnection::Write/AsyncWrite 的
// 观察与等待面；与 Connection 同源的稳定值语义，拆出独立头以便
// CoTcpConnection/ICoConnection 只依赖本头而不是整个 Connection.hpp。
//
// 关键语义（与旧 CoWrite 自由函数同契约，收口为成员）：
//   - WriteOperation 是单次已接纳发送的观察句柄：持操作记录，终态在
//     句柄寿命内永久可查；释放句柄不取消发送，无效句柄 Query 为
//     Unknown/0/空。
//   - Wait 等同一条 record 的终态：超时/取消只结束本次等待，不撤销
//     已接纳发送，owner 继续推进，晚到终态仍在 record 上。
//   - WriteReport 是成员 Write 的三元结算：wait_error 描述等待结果，
//     outcome 描述该次发送当时采样，operation 供事后复查；任何字段
//     都不暗示其它字段终态。

#include <cstdint>
#include <memory>
#include <system_error>

#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/net/Types.hpp>

namespace bbt::infra::net {

class ConnectionState;
struct WriteOpRecord;  // detail：写操作共享记录，布局不出公共面

// 单次已接纳发送的观察/等待句柄：值语义，默认无效。持记录强引用，
// 连接句柄释放后仍可 Query/Wait；不是通用 Future/Task 平台。
//
// 拒绝票（docs/decisions/0012-icoconn-member-interface.md）：ICoConnection::AsyncWrite 的拒绝路径不分配操作
// 记录，把拒绝原因内联进票（m_rejection 非空、record 为空）——
// !Valid() 不再足以推断 Query()==Unknown；无身份且无拒绝原因才是
// Unknown。释放票不取消发送，也不延长 Engine/fd 寿命。
class WriteOperation {
public:
    WriteOperation() = default;

    bool Valid() const noexcept;
    std::uint64_t Id() const noexcept;

    // 非阻塞查询：已接纳票查同一 record 终态（句柄寿命内永久可查）；
    // 拒绝票返回 Rejected/0/具体原因；无身份票返回 Unknown/0/空。
    WriteOutcome Query() const;

    // 协程内等待本操作终态。gate 先于快路径：非法域返回
    // operation_not_permitted（含已有终态的快路径）。超时/取消只结束
    // 等待：已接纳发送不被撤销，终态仍在 record 上，可再次 Query/Wait
    // 取得。Rejected 票已落定：返回空错误、outcome 保留拒绝采样；
    // 无身份票返回 invalid_argument。outcome 非空时写入返回时采样。
    std::error_code Wait(const bbt::coroutine::WaitOptions& options,
                         WriteOutcome* outcome = nullptr) const;

private:
    // 内部构造责任：Connection::AsyncWrite 与 CoTcpConnection::Write 经
    // ConnectionState 分配 record 并包装；消费者不能凭任意状态构造。
    // state 以 weak 持有：Wait 取局部强引用订阅 write hub，不与连接
    // 状态成环（record 可在 state 回收后继续被 Query）。
    friend class Connection;
    friend class CoTcpConnection;
    friend class ConnectionTestAccess;
    WriteOperation(std::shared_ptr<WriteOpRecord> record,
                   std::weak_ptr<ConnectionState> state = {},
                   std::error_code rejection = {});

    std::shared_ptr<WriteOpRecord> m_record;
    std::weak_ptr<ConnectionState> m_state;
    std::error_code m_rejection;  // 非空 = 拒绝票（record 为空，Valid()=false）
};

// 成员 Write 的结算：等待错误 + 发送终态采样 + 观察句柄，三元一体。
// 字段语义与旧 CoWrite(outcome, operation) 完全对应：
//   - wait_error：合法等待完成为空；域非法/超时/取消/容量等待失败写具体
//     错误（描述「这次等待」，不描述「这次发送」）。
//   - outcome：Rejected/0/error 表示未提交；InFlight 表示已接纳仍在途；
//     Done+error 为空才表示本地完成（仍非对端回包）。
//   - operation：未提交/域非法时为无效句柄；已接纳则有效且终态可查。
struct WriteReport {
    std::error_code wait_error;
    WriteOutcome outcome{WritePhase::Rejected, 0, {}};
    WriteOperation operation;
};

} // namespace bbt::infra::net

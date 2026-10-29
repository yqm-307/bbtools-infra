#pragma once

// 受管协程连接的公共接口（终评 §2，成员化收口）：协程消费面只依赖
// 本接口与值类型，不再经 CoRead/CoWrite/CoAccept/CoDial 自由函数。
//
// 定位：这是「协议无关的单连接协程视图」——只表达 Read/Write/关闭/状态
// 观察，不承诺分帧、请求匹配、重发或对端回包。raw 消费面（Connection +
// RawHandler 回调）独立保留，不经本接口。
//
// 域与门禁（与旧 Co* 自由函数同契约）：
//   - Read/Write/WaitClosed 之外的状态投影（Id/Valid/Status/
//     IsClosed/IsReleased）可在任意合法调用域使用，不挂起。
//   - Read/Write/WaitClosed 必须在受管协程内调用；非受管协程（普通
//     线程、runtime idle、raw owner 回调）返回 operation_not_permitted，
//     包括有现成数据/零长写/连接已就绪的快路径——gate 先于任何状态
//     判定与副作用。
//   - options 的 deadline 只影响本次等待：超时/取消不撤销已接纳 I/O、
//     不表示「取消后永远不走快路径」、不弹出供其它等待者取得的资源。
//
// 输出语义：Read 失败不交付 Buffer 也不消费；Write 的 WriteReport 每次
// 调用覆盖本次语义（不泄露上次结果）；WaitClosed 成功只表示观察到
// closed 终态，不代表物理资源已回收（那是 IsReleased 的职责）。
//
// 生命周期：ICoConnection 是值语义的接口视图（见 CoTcpConnection 的薄
// 包装）：销毁视图不关闭连接；关闭由 RequestClose 发起、owner 兑现
// 物理收口。不自动为连接建业务协程、不自动关闭/重连/重发。

#include <cstdint>
#include <string>
#include <system_error>

#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/net/Buffer.hpp>
#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/Types.hpp>
#include <bbt/infra/net/WriteOperation.hpp>

namespace bbt::infra::net {

// 连接状态投影（终评 §2）：诊断/资源观察值，每字段来自既有真源，
// 跨方向分别采样，不承诺原子总快照。需要一致结论的调用方应等
// WaitClosed/IsReleased 终态；不可用一次 Status 来证明下一次操作必成功。
struct ConnectionStatus {
    bool valid{false};                    // 句柄是否持有效共享状态
    std::size_t readable_bytes{0};        // 库内已发布、尚未交付字节采样
    bool read_eof{true};                  // 读方向已见 EOF（可与尾部字节共存）
    std::error_code read_error;           // 持久读错误（可与尾部字节共存）
    AcceptanceState write{AcceptanceState::Closed};  // 写接纳状态真源采样
    bool closed{true};                    // 逻辑关闭终态（acceptance Closed 或等价）
    std::error_code close_reason;         // 第一次请求原因固定（含默认空原因）
    bool released{true};                  // fd 已关且 owner 本连接责任清理
};

enum class ReadState { Data, Eof, Error };

struct ReadOutcome {
    ReadState state{ReadState::Error};
    Buffer data;
    std::error_code error;
    // Data: data.Size() > 0 && !error；Eof: 空 data && !error；
    // Error: 空 data && error。失败零消费；没有「空 Data」或裸空 Buffer 表 EOF。
};

class ICoConnection {
public:
    virtual ~ICoConnection() = default;

    // ---- 读：真零拷贝移交 ----
    // 仅业务协程，可挂起；先 gate，再访问任何数据/快路径。
    // 等到有数据时，原子取走线性化点上所有已发布可读字节并移交 backing。
    // Busy/WouldBlock 内部等待；先交尾部，耗尽后才 Eof/Error。
    // options 只约束等待；超时/取消/OOM 不消费，不改为自动 RequestClose。
    virtual ReadOutcome Read(const bbt::coroutine::WaitOptions& options = {}) = 0;

    // ---- 写：非挂起提交 + 协程组合等待 ----
    // 任意合法调用域，非挂起；全量接纳或零接纳，不等容量/发送完成。
    // 按值取得字符串；接纳后库拥有发送数据。拒绝原因只经返回票 Query 表达。
    // 调用方 move 传入后，即使拒绝也不得依赖原字符串内容被保留。
    virtual WriteOperation AsyncWrite(std::string data) = 0;

    // 仅业务协程，可挂起；原 CoWrite 的成员化组合操作。
    // 同一绝对 deadline 等容量 -> 接纳 -> 等本地完成；不等业务回包。
    // 等容量失败为 Rejected；接纳后等待结束仍返回 op 与真实采样。
    virtual WriteReport Write(std::string data,
                              const bbt::coroutine::WaitOptions& options = {}) = 0;

    // ---- 关闭 ----
    // 任意合法域，非挂起、幂等；拒绝新写并唤 owner，第一次 reason 固定。
    // 不保证冲刷未发完写；owner 结算它们，保留已发前缀，不冒充发送成功。
    // fd/已 recv 尾部归 owner 收口；析构及普通 I/O 错误不隐式触发本策略。
    virtual void RequestClose(std::error_code reason = {}) = 0;

    // 仅业务协程，可挂起；等 Status().closed 对应的既有逻辑谓词。
    // 返回 error_code 只描述此次等待；成功后原因从 Status().close_reason 读。
    // 不是 WaitReleased，不消费 ring，不等业务协程/RawHandler 回调全部退出。
    virtual std::error_code WaitClosed(
        const bbt::coroutine::WaitOptions& options = {}) = 0;

    // ---- 状态投影 ----
    // 任意合法域，非挂起；返回值自有，无需持锁使用，无网络探活副作用。
    // 每字段来自既有真源，跨方向分别采样，不承诺原子总快照。
    virtual ConnectionStatus Status() const = 0;

    // 非虚便捷投影，不维护第二套标志；这些投影也不提供跨查询原子性。
    bool Valid() const { return Status().valid; }
    bool IsClosed() const { return Status().closed; }
    bool IsReleased() const { return Status().released; }
};

} // namespace bbt::infra::net

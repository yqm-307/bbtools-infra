#pragma once

// 公共网络值类型（D1）：读状态/读结果、写阶段/写结果、接纳状态。
//
// 这些值此前嵌在 detail 部件（ReceiveRing/ConnectionState）里，使公共头
// 间接冻结了 ring 布局与内部枚举位置。这里只冻结值语义与成员含义；
// 内部存储、通知与游标策略不随公共契约冻结。

#include <cstddef>
#include <system_error>

namespace bbt::infra::net {

// Ok 已取得数据保护；Busy 保护竞争（零消费，释放后需重试）；WouldBlock
// 暂无数据且无终态；Eof/Error 为持久终态。
enum class ReadStatus { Ok, Busy, WouldBlock, Eof, Error };

struct ReadResult {
    ReadStatus status{ReadStatus::WouldBlock};
    std::size_t bytes{0};
    std::error_code error;
};

// InFlight 已接纳未终态；Done 只表示该次发送已到终态，是否成功还须 error
// 为空（Done + 非空 error 是失败结算）；Rejected 表示未提交；Unknown 只来自
// 无效写句柄的 Query。
enum class WritePhase { InFlight, Done, Rejected, Unknown };

// 写操作观察结果：sent 是该次操作实际交给内核的字节前缀，error 是该操作的
// 真实错误。它是「回 X 时采样」，不表示调用返回时仍在途。
struct WriteOutcome {
    WritePhase phase{WritePhase::Unknown};
    std::size_t sent{0};
    std::error_code error;
};

// Open 可接纳新写；Settling 关闭/写失败结算中，新写被拒；Closed 终态。
enum class AcceptanceState { Open, Settling, Closed };

} // namespace bbt::infra::net

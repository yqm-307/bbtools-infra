#pragma once

// Engine::CoAccept / CoDial 的返回包（终评 §2）：一次 Co 建连/收连接的
// 完整结算——错误码 + 连接句柄 + 物理回收观察句柄。
//
// 语义与旧 CoAccept/CoDial 自由函数完全等价，只是形态收口为值包：
//   - error 非空：conn 无效（失败不覆盖调用者原连接）；reclaim 视提交阶段
//     可能有效也可能无效，由实现按「是否已进入已提交责任」给出。
//   - error 为空：conn 有效；reclaim 描述本次 socket 责任，Released()
//     与该连接 Released 同终态。
//
// 保留 reclaim 字段的理由：提交被接纳之后，即使后续等待/取消/竞态失败，
// 本次 socket 责任仍然真实存在过；reclaim 让调用者能确认 fd 已物理收口，
// 而不是凭「调用返回」推断资源已还。

#include <system_error>

#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>

namespace bbt::infra::net {

struct ConnectionResult {
    // 本调用的错误：空 = 成功交付 connection；非空 = 按 CoAccept/CoDial
    // 判定优先级返回的具体错误（执行域/终态/配置/容量/超时等）。
    std::error_code error;
    // 成功交付的连接句柄：失败时保持无效（不覆盖调用者原有变量，
    // 调用者应只用本返回值，不向 Engine/Connection 再要结果）。
    CoTcpConnection connection;
    // 本次 socket 物理回收观察句柄（仅 CoDial 路径有真实语义；CoAccept
    // 成功交付的 reclaim 与该连接 IsReleased 同终态，失败路径视实现）：
    //   - 提交前拒绝（上下文/配置/容量）：无效句柄，Released()=true，
    //     无已提交资源责任。
    //   - 提交接纳后：有效句柄，Released()=false 直至 fd 真实关闭且
    //     owner 资源责任清理，或责任随成功交付转入连接。
    ReclaimReceipt reclaim;
};

} // namespace bbt::infra::net

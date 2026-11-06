#pragma once

/**
 * @file ConnectionResult.hpp
 * @brief Engine::CoAccept 与 Engine::CoDial 的返回值。
 *
 * 一次协程建连或收连接的完整结算，包含错误码、连接句柄与物理回收观察句柄。
 *
 * - error 非空：connection 无效，失败不覆盖调用方原有的连接变量。reclaim 是否有效取决于
 *   失败发生的阶段，见下方 reclaim 字段。
 * - error 为空：connection 有效。reclaim 描述本次 socket 的责任，其 Released() 与该连接的
 *   Released() 处于同一终态。
 *
 * 保留 reclaim 字段的理由：提交被接纳之后，即使后续等待、取消或竞态导致失败，本次 socket 责任
 * 仍然真实存在过。reclaim 让调用方能确认 fd 已物理收口，而不必凭「调用已经返回」推断资源已释放。
 */

#include <system_error>

#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>

namespace bbt::infra::net {

/** @brief 一次协程建连或收连接的结算结果。 */
struct ConnectionResult {
    /// 本次调用的错误。空表示成功交付 connection；非空为按 CoAccept/CoDial 判定优先级给出的具体错误，
    /// 包括执行域、终态、配置、容量与超时等。
    std::error_code error;

    /// 成功交付的连接。失败时保持无效；调用方应只使用本返回值，不要再向 Engine 或 Connection 索取结果。
    CoTcpConnection connection;

    /// 本次 socket 物理回收的观察句柄。
    ///   - 执行域或配置校验失败、提交前被拒绝：无效句柄，没有已提交的资源责任。
    ///   - CoDial 提交被接纳后：有效句柄。fd 真实关闭且 owner 完成资源清理之前 Released() 为 false；
    ///     未交付的失败由 owner 收口，成功交付则责任转入连接，之后与连接的 Released() 同终态。
    ///   - CoAccept 成功交付：有效句柄，与该连接的 Released() 同源。CoAccept 失败时为无效句柄。
    ReclaimReceipt reclaim;
};

} // namespace bbt::infra::net

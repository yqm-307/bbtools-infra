#pragma once
// HttpIoEngine：HTTP 后端 I/O 的串行执行域封装，不拥有 io_context 或线程。
// 执行域本体在 src/detail/IoSupport.hpp（Issue #6 起与 Redis 模块共用），
// 本类只附加 HTTP 切片的 NetworkLimits；注释与清理约定见基类文件。
//
// 所有 Asio/Beast I/O 对象（socket/resolver/acceptor/timer）与发起型
// 投递统一落在 coroutine 共享 executor 派生的 strand（io 域）上；实际驱动
// 线程是 Scheduler 现有 PollOnce 事件循环线程。runtime 与其派生的
// client/server 经 shared_ptr 共享持有本对象；业务 handler 永不进入此域。

#include <bbt/infra/NetworkTypes.hpp>

#include "detail/IoSupport.hpp"

namespace bbt::infra::http_detail {

class HttpIoEngine : public bbt::infra::detail::IoEngine {
public:
    using IoStrand = bbt::infra::detail::IoStrand;

    explicit HttpIoEngine(const NetworkLimits& limits) : m_limits(limits) {}

    const NetworkLimits& Limits() const noexcept { return m_limits; }

private:
    NetworkLimits m_limits;
};

} // namespace bbt::infra::http_detail

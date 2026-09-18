#include "http/HttpIoEngine.hpp"

#include <bbt/coroutine/io/IoExecutor.hpp>

namespace bbt::infra::http_detail {

result<void> HttpIoEngine::Start() {
    boost::asio::any_io_executor ex;
    try {
        ex = bbt::coroutine::io::GetExecutor();
    } catch (...) {
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "coroutine shared io executor unavailable"));
    }
    if (!ex)
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "coroutine shared io executor is empty"));

    std::lock_guard<std::mutex> lk(m_post_mtx);
    if (m_io)
        return result<void>::ok();   // 幂等：并发 Start 只建立一次
    m_io.emplace(ex);
    return result<void>::ok();
}

void HttpIoEngine::SealOnIoDomain() noexcept {
    std::lock_guard<std::mutex> lk(m_post_mtx);
    m_stopped = true;
}

} // namespace bbt::infra::http_detail

#pragma once
// 父头末尾包含的实现；依赖父头已完成类型与声明。

namespace bbt::infra::mongo_detail {

template <class F> inline bool MongoIoEngine::TryPost(F&& f)
{
    std::lock_guard<std::mutex> lk(m_post_mtx);
    if (m_stopped || !m_io)
        return false;
    try {
        boost::asio::post(*m_io, std::forward<F>(f));
    } catch (...) {
        return false;
    }
    return true;
}

} // namespace bbt::infra::mongo_detail

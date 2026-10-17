#include <bbt/infra/net/CoTcpConnection.hpp>

#include <stdexcept>
#include <utility>

#include <bbt/infra/net/detail/ConnectionBridge.hpp>

namespace bbt::infra::net {

CoTcpConnection::CoTcpConnection() noexcept = default;

// 会话类型在本头文件不完整：析构外联。
CoTcpConnection::~CoTcpConnection() = default;

std::error_code CoTcpConnection::Wake()
{
    if (!m_session) {
        throw std::logic_error("bbt::infra::net: CoTcpConnection 未由内部工厂绑定");
    }
    if (const auto error = RequireCoroutine()) { // 副作用前现查受管协程环境
        return error;
    }
    return m_session->Wake();
}

std::error_code CoTcpConnection::Write(std::string data, CoDeadline deadline)
{
    if (!m_session) {
        throw std::logic_error("bbt::infra::net: CoTcpConnection 未由内部工厂绑定");
    }
    if (const auto error = RequireCoroutine()) {
        return error;
    }
    return m_session->Write(std::move(data), deadline);
}

std::error_code CoTcpConnection::Close(CloseMode mode, std::error_code reason, CoDeadline deadline)
{
    if (!m_session) {
        throw std::logic_error("bbt::infra::net: CoTcpConnection 未由内部工厂绑定");
    }
    if (const auto error = RequireCoroutine()) {
        return error;
    }
    return m_session->Close(mode, std::move(reason), deadline);
}

std::size_t CoTcpConnection::InboundBuffered() const
{
    if (!m_session) {
        throw std::logic_error("bbt::infra::net: CoTcpConnection 未由内部工厂绑定");
    }
    return m_session->InboundBuffered();
}

std::string_view CoTcpConnection::Peek() const
{
    if (!m_session) {
        throw std::logic_error("bbt::infra::net: CoTcpConnection 未由内部工厂绑定");
    }
    return m_session->Peek();
}

std::error_code CoTcpConnection::Discard(std::size_t bytes)
{
    if (!m_session) {
        throw std::logic_error("bbt::infra::net: CoTcpConnection 未由内部工厂绑定");
    }
    return m_session->Discard(bytes);
}

std::size_t CoTcpConnection::Id() const noexcept
{
    return m_session ? m_session->Id() : 0;
}

Address CoTcpConnection::PeerAddress() const
{
    if (!m_session) {
        throw std::logic_error("bbt::infra::net: CoTcpConnection 未由内部工厂绑定");
    }
    return m_session->PeerAddress();
}

bbt::coroutine::CoObjectInfo CoTcpConnection::GetObjectInfo() const
{
    if (!m_session) {
        throw std::logic_error("bbt::infra::net: CoTcpConnection 未由内部工厂绑定");
    }
    return m_session->ObjectInfo(); // 组合既有 CoObject 身份，不引入新 id 机制
}

} // namespace bbt::infra::net

#pragma once

// HTTP/1.1 连接交换层（内部部件，仅 http 模块实现 TU 包含）：
//   - 序列化/解析交给 Beast（request_serializer + response_parser 手工喂养）；
//   - 网络 I/O 只经 bbt::infra::net 协程成员接口（CoTcpConnection::Write/Read），
//     不触碰 Asio socket/native fd/net detail；
//   - 一条连接同一时刻只承载一个活动请求（Active 独占由池保证）；
//   - 超时语义：write 档从实际写开始，read 档从每次 Read 开始起算；
//     各自与不重置的调用总 deadline 取 min，池等待不消耗后续阶段档。
//
// 请求阶段事实（Result.hpp RequestPhase）：Write 终态成功前失败=确定失败
// （Writing）；请求字节已完整写出后（Done 且无错误）的读侧失败按 OutcomeUnknown
// 交付（请求可能已生效，回复丢失）。任何失败路径连接不可复用，由池退役。

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include <boost/asio/buffer.hpp>
#include <boost/beast/http.hpp>

#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/ICoConnection.hpp>

#include <bbt/infra/http/Client.hpp>

namespace bbt::infra::http {
namespace detail {

// 拍平序列化接收器：Beast SyncWriteStream 概念的最小实现，把消息写进
// std::string，避免引入真实 socket 类型。
class StringSink {
public:
    explicit StringSink(std::string& out) noexcept : m_out(out) {}
    template<class ConstBufferSequence>
    std::size_t write_some(const ConstBufferSequence& buffers) {
        std::size_t total = 0;
        for (auto it = boost::asio::buffer_sequence_begin(buffers);
             it != boost::asio::buffer_sequence_end(buffers); ++it) {
            const boost::asio::const_buffer& buffer = *it;  // 迭代器可能无 operator->
            m_out.append(static_cast<const char*>(buffer.data()), buffer.size());
            total += buffer.size();
        }
        return total;
    }
    template<class ConstBufferSequence>
    std::size_t write_some(const ConstBufferSequence& buffers,
                           boost::system::error_code& ec) {
        ec = {};
        return write_some(buffers);
    }

private:
    std::string& m_out;
};

// 只在阶段开始时计算期限；入池排队不得提前消耗 connect/write/read 的预算。
struct PhaseDeadlines {
    Timeout timeout;
    bbt::coroutine::Deadline total;
    bbt::coroutine::Deadline pool;
};

bbt::coroutine::Deadline StageDeadline(const PhaseTimeout& phase,
                                      bbt::coroutine::Deadline total);
std::error_code CheckCall(bbt::coroutine::Deadline deadline);
PhaseDeadlines ResolveDeadlines(const Timeout& effective,
                                bbt::coroutine::Deadline total_deadline,
                                std::chrono::steady_clock::time_point now);

// 序列化必须在借用连接前完成：输入异常不占用池名额。
result<std::string> SerializeRequest(const PreparedRequest& request);

class Http11Connection {
public:
    Http11Connection(net::Connection connection, std::uint64_t max_body_bytes) noexcept
        : m_connection(std::move(connection)), m_max_body_bytes(max_body_bytes) {}

    struct ExchangeResult {
        Response response;
        bool keep_alive{false};
    };
    bbt_co_async(result<ExchangeResult>) Exchange(const PreparedRequest& request,
                                                  const std::string& serialized,
                                                  const PhaseDeadlines& deadlines);

    net::Connection& Net() noexcept { return m_connection; }

private:
    result<void> WriteRequest(const std::string& bytes,
                              bbt::coroutine::Deadline write_deadline);
    result<ExchangeResult> ReadResponse(const PreparedRequest& request,
                                        const PhaseDeadlines& deadlines);

    net::Connection m_connection;
    std::uint64_t m_max_body_bytes;
};

} // namespace detail
} // namespace bbt::infra::http

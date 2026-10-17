#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>

#include <bbt/coroutine/object/interface/ICoObject.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/ICoConnection.hpp>

namespace bbt::infra::net {

class ConnectionBridge;
class ConnectionSession;

// 公共 TCP 连接（P3-c1）：ICoConnection 的真实实现。
//
// 创建：只能经产品内部私有 factory（ConnectionBridge，未来
// Engine 的创建接缝）在 owner 域创建；公共头不暴露 detail 工厂参数、
// 无测试专用构造器、无 Adopt(EventLoop&) 公共入口。本头文件
// 只含标准库与既有自有类型。
//
// 身份：组合既有 CoObject（经内部会话委托 GetObjectInfo），不引入菱形
// 或新 id 机制；连接 id 与对端地址为创建时取得的自有值快照，创建后
// 不变。连接对象不得活过其 ConnectionBridge（Engine 拥有连接的语义；
// 本包契约测试遵守，公共层不做运行时防护）。
//
// 域约定与错误/完成点契约见 ICoConnection 各方法注释；所有业务命令
// 在副作用前 RequireCoroutine（每次现查环境，不缓存 TLS）。
class CoTcpConnection final
    : public ICoConnection,
      public std::enable_shared_from_this<CoTcpConnection> {
public:
    CoTcpConnection(const CoTcpConnection&) = delete;
    CoTcpConnection& operator=(const CoTcpConnection&) = delete;
    ~CoTcpConnection();

    // ---- ICoConnection：跨域业务命令（受管协程）与事件内 Reader ----
    std::error_code Wake() override;
    std::error_code Write(std::string data, CoDeadline deadline) override;
    std::error_code Close(CloseMode mode, std::error_code reason, CoDeadline deadline) override;
    std::size_t     InboundBuffered() const override;
    std::string_view Peek() const override;
    std::error_code Discard(std::size_t bytes) override;

    // ---- 创建时不变快照 ----
    // 内部工厂分配的连接 id；未绑定（理论不可达）返回 0。
    std::size_t     Id() const noexcept;
    // 接纳时记录的对端地址自有值快照；未绑定抛 std::logic_error。
    Address         PeerAddress() const;

    bbt::coroutine::CoObjectInfo GetObjectInfo() const override;

private:
    friend class ConnectionBridge;

    // 只能由内部工厂构造并绑定 m_session；析构外联（会话类型在此不完整）。
    CoTcpConnection() noexcept;

    // 内部会话（detail/ConnectionBridge.hpp 定义）；析构外联。
    std::shared_ptr<ConnectionSession> m_session;
};

} // namespace bbt::infra::net

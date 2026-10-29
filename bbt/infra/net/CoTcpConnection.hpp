#pragma once

// TCP 连接的 ICoConnection 薄实现（终评 §2 / A1）：纯值包装，不复制状态。
//
// 本对象只持有一个 Connection 值句柄（内部是 shared_ptr<ConnectionState>），
// 不复制 ring、不缓存状态投影、不建第二队列；所有 ICoConnection 成员直接
// 转发到底层 Connection/ConnectionState 的同一真源。值语义：拷贝本对象
// 只是再持一份同一连接的视图，销毁视图不影响连接寿命；关闭经
// RequestClose 发起、owner 兑现。
//
// 构造责任：Engine::CoAccept/CoDial 与测试接缝以 Connection 构造本对象；
// 消费者不能凭裸状态构造（Connection 的私有构造约束原样继承）。
//
// 实现位置：成员函数体在 CoTcpConnection.cc——Read/Write/WaitClosed 的
// 等待逻辑需要 ConnectionState/detail 部件，按现有约定不进公共头；
// Status/Valid/Id/RequestClose 是纯转发，inline 留在头内零开销。

#include <cstdint>
#include <string>
#include <system_error>

#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/net/Buffer.hpp>
#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/ICoConnection.hpp>
#include <bbt/infra/net/Types.hpp>
#include <bbt/infra/net/WriteOperation.hpp>

namespace bbt::infra::net {

class CoTcpConnection final : public ICoConnection {
public:
    CoTcpConnection() = default;          // 无效值；无 socket 责任
    // 值包装：持 Connection 句柄，不复制状态。explicit 防隐式转换，
    // 保持「视图必须显式从 Connection/Engine 取得」的边界。
    explicit CoTcpConnection(Connection connection) noexcept
        : m_conn(std::move(connection)) {}
    ~CoTcpConnection() override = default; // 不隐式 RequestClose，不等物理回收

    // 以下每个 override 完整继承 ICoConnection 的域/挂起/所有权/错误契约。
    ReadOutcome Read(const bbt::coroutine::WaitOptions& options = {}) override;
    WriteOperation AsyncWrite(std::string data) override;
    WriteReport Write(std::string data,
                      const bbt::coroutine::WaitOptions& options = {}) override;
    std::error_code WaitClosed(
        const bbt::coroutine::WaitOptions& options = {}) override;
    void RequestClose(std::error_code reason = {}) override {
        m_conn.RequestClose(reason);
    }
    ConnectionStatus Status() const override;

    // 薄包装的底层句柄：raw 消费面与 Co 视图共存的桥。调用者可据此继续
    // 用 TryAccess/AsyncWrite 等 raw API；返回的是同一共享状态，不是副本。
    const Connection& Raw() const noexcept { return m_conn; }

private:
    Connection m_conn;                    // 只有共享值句柄，无第二数据队列/关闭槽
};

} // namespace bbt::infra::net

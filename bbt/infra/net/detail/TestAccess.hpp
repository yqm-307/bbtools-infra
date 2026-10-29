#pragma once

// 内部测试/故障注入接缝（不列入公共头自检，不属公共冻结面）。
//
// 这些入口只给仓内测试建立确定性交错与直接状态注入用：产品代码不使用，
// 也不承诺任何源码兼容。对应产品面分别是 Connection/WriteOperation 的私有
// 构造、公共 ReadAccess 的私有构造，以及 CoConnection.cc 内的 CoDial 屏障。

#include <cstddef>
#include <functional>
#include <memory>

#include <bbt/infra/net/Buffer.hpp>
#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/ReadAccess.hpp>

namespace bbt::infra::net {

class ConnectionState;
struct WriteOpRecord;

class ConnectionTestAccess {
public:
    // 从内部状态构造公共句柄（等价于 Engine owner 的构造路径）。
    static Connection Make(std::shared_ptr<ConnectionState> state);
    static std::shared_ptr<ConnectionState> StateOf(const Connection& connection);
    static WriteOperation MakeWrite(std::shared_ptr<WriteOpRecord> record,
                                    std::shared_ptr<ConnectionState> state = {});

    // 只取数据保护（不要求有数据），用于确定性制造 append-Busy。守卫持传入
    // 状态的强引用，寿命语义与产品 TryAccess 一致。
    static ReadAccess HoldForTest(std::shared_ptr<ConnectionState> state);

    // 显式提前释放守卫（等价于让其析构）。
    static void Reset(ReadAccess& access);
};

class CoTestAccess {
public:
    // 私有 test 屏障：CoDial 提交之后、内部等待 attach 之前（正常为空）。
    // 传空 function 即清除。
    static void SetDialBarrier(std::function<void()> barrier);
};

// pop-all 零拷贝证据接缝：核 Buffer 的 backing 身份与段布局，断言「交付的
// 段指针就是 ring 移交的同一存储」以及「Buffer 存续让移交存储保持有效」。
// 只在 Buffer 内部读私有成员，不构成公共消费面。
struct BufferTestAccess {
    static const void* Backing(const Buffer& buffer) { return buffer.m_backing.get(); }
    static std::shared_ptr<void> BackingOwner(const Buffer& buffer) {
        return buffer.m_backing;
    }
    static std::size_t BackingUseCount(const Buffer& buffer) {
        return static_cast<std::size_t>(buffer.m_backing.use_count());
    }
    // 凭移交原语构造 Buffer 的唯一测试接缝：等价于 CoTcpConnection::Read
    // 内部的移交构造；公共面不可达。
    static Buffer Make(std::shared_ptr<void> backing,
                       std::vector<BufferSegment> segments) {
        return Buffer(std::move(backing), std::move(segments));
    }
};

} // namespace bbt::infra::net

#pragma once

/**
 * 内部测试/故障注入接缝（不列入公共头自检，不属公共冻结面）。
 *
 * 这些入口只给仓内测试建立确定性交错与直接状态注入用：产品代码不使用，
 * 也不承诺任何源码兼容。对应产品面分别是 Connection/WriteOperation 的私有
 * 构造，以及 CoInternal.cc 内的 CoDial 屏障。
 */
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>

#include <bbt/infra/net/Buffer.hpp>
#include <bbt/infra/net/Connection.hpp>

namespace bbt::infra::net {

class ConnectionState;
struct WriteOpRecord;

class ConnectionTestAccess {
public:
    /**
     * 从内部状态构造公共句柄（等价于 Engine owner 的构造路径）。
     */
    static Connection Make(std::shared_ptr<ConnectionState> state);
    static std::shared_ptr<ConnectionState> StateOf(const Connection& connection);
    static WriteOperation MakeWrite(std::shared_ptr<WriteOpRecord> record, std::shared_ptr<ConnectionState> state = {});
    static std::unique_lock<std::mutex> HoldForTest(std::shared_ptr<ConnectionState> state);
    static void Reset(const std::shared_ptr<ConnectionState>& state, std::unique_lock<std::mutex>& lock);
};

class CoTestAccess {
public:
    /**
     * 私有 test 屏障：CoDial 提交之后、内部等待 attach 之前（正常为空）。
     * 传空 function 即清除。
     */
    static void SetDialBarrier(std::function<void()> barrier);
};

/**
 * 构造任意分段输入，覆盖 Buffer 流操作跨段消费及持有外部 backing 的寿命。
 * 只供内部测试，不代表 Connection::Read 的复制读取路径。
 */
struct BufferTestAccess {
    static const void* Backing(const Buffer& buffer) { return buffer.m_backing.get(); }

    /**
     * 从自有 backing 构造分段输入；公共面不可达。
     */
    static Buffer Make(std::shared_ptr<void> backing, std::vector<BufferSegment> segments);
};

// clang-format off
}
// clang-format on

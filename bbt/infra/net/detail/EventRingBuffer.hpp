#pragma once

/**
 * 有界分段接收缓冲与独立事件快照；保留内部类型名以隔离调用方。
 * Boost.Beast multi_buffer 管理存储与消费，包装负责超限拒绝和独立快照。
 * 单 owner 使用，非线程安全；同步仍由 ReceiveRing 负责。
 */
#include <algorithm>
#include <cstddef>
#include <functional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include <boost/asio/buffer.hpp>
#include <boost/beast/core/multi_buffer.hpp>

namespace bbt::infra::net {

/**
 * 独立拥有的连续事件快照：构造时从源复制全部可读字节，owner 后续继续
 * append/consume/reset 不改本对象 backing storage；可复制/移动延寿。
 * Peek 返回借用视图：仅在下一次本快照非 const 调用前有效，不得声称裸
 * view 可独立延寿（延寿靠复制/移动快照本身）。
 */
class EventSnapshot {
public:
    EventSnapshot() = default;

    /**
     * 拷贝/移动延寿：拷贝得到独立 backing storage；移动把 data_ 与
     * cursor 一并转移（目标保留原已消费位置），来源保持合法空快照
     * （Remaining()==0，Discard/Consume/再赋值仍可用），自移动安全；
     * data_ 与 cursor 必须一并转移、归位，否则移动后来源的 Remaining() 会发生无符号下溢。
     */
    EventSnapshot(const EventSnapshot&) = default;
    EventSnapshot& operator=(const EventSnapshot&) = default;
    EventSnapshot(EventSnapshot&& other) noexcept;
    EventSnapshot& operator=(EventSnapshot&& other) noexcept;

    std::size_t Remaining() const noexcept { return data_.size() - cursor_; }

    bool Empty() const noexcept { return Remaining() == 0; }

    std::string_view Peek() const noexcept { return std::string_view(data_.data() + cursor_, Remaining()); }

    /**
     * n 超过剩余量时拒绝且不变。
     */
    std::error_code Discard(std::size_t n);

    /**
     * 拷出 n 字节并前移游标；n 超过剩余量时拒绝且不变。
     */
    std::error_code Consume(std::size_t n, std::string& out);

private:
    friend class EventRingBuffer;
    explicit EventSnapshot(std::string data);

    /**
     * 独立连续 backing storage
     */
    std::string data_;
    std::size_t cursor_{0};
};

/**
 * 单 owner 有界分段缓冲。消费可回收已读块，Capacity 表示当前免分配容量。
 */
class EventRingBuffer {
public:
    /**
     * max_bytes == 0 时仅空操作合法；构造不预分配。
     */
    explicit EventRingBuffer(std::size_t max_bytes) noexcept;

    EventRingBuffer(const EventRingBuffer&) = delete;
    EventRingBuffer& operator=(const EventRingBuffer&) = delete;

    /**
     * Boost 移动后来源 Size/Capacity 为 0，max_bytes 保留，可继续追加。
     */
    EventRingBuffer(EventRingBuffer&& other) noexcept;
    EventRingBuffer& operator=(EventRingBuffer&& other) noexcept;

    std::size_t Size() const noexcept { return storage_.size(); }

    std::size_t Capacity() const noexcept { return storage_.capacity(); }

    std::size_t MaxBytes() const noexcept { return max_bytes_; }

    bool Empty() const noexcept { return storage_.size() == 0; }

    /**
     * 支持自身 Peek view/子串作为输入：prepare 保持可读段有效。
     * 超限拒绝或分配失败时可读内容不变，不为别名额外复制载荷。
     */
    std::error_code Append(std::string_view data);

    /**
     * 读前预留总容量（读侧消除不可回滚消费的最小原语）：total_capacity
     * 超过 MaxBytes 拒绝且不变；容量已足时无操作。扩容失败时可读内容不变。
     * 成功预留后，等量 Append 只做 char 提交，不再首次分配。
     */
    std::error_code Reserve(std::size_t total_capacity);

    /**
     * 首段连续可读 view，下一次非 const 调用后失效；保存 snapshot 才延寿。
     */
    std::string_view Peek() const noexcept;

    std::error_code Discard(std::size_t n);

    /**
     * 拷出 n 字节并前移；非法 n 拒绝且不变。
     */
    std::error_code Consume(std::size_t n, std::string& out);

    EventSnapshot TakeSnapshot() const;

    boost::beast::multi_buffer::const_buffers_type Segments() const noexcept { return storage_.data(); }

    void Clear() noexcept { storage_.clear(); }

private:
    boost::beast::multi_buffer storage_;
    std::size_t max_bytes_;
};

// clang-format off
}
// clang-format on

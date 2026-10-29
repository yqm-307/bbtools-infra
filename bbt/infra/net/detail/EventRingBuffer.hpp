#pragma once

// 有界弹性环形缓冲与事件快照：后续 Reader/Writer 要消费的内部部件
// （detail 路径，namespace 不增第四层；不进公共头自检）。
//
// 语义依据：gnet v2.10.0 pkg/buffer/ring/ring_buffer.go（MIT，见
// docs/dependencies/gnet-2.10.0.provenance.md）与 0010 决策「业务独占事件
// 快照」。仅移植必要语义，不复制 gnet 的 pool/同步库。
//
// 与参考 ring 的差异：参考 ring 满时由上层覆盖/腾挪，本部件追加超限在
// 任何修改之前拒绝（no_buffer_space），不覆盖最旧数据丢字节。
//
// 单 owner 使用，非线程安全；不内置任何锁（0010：无共享大锁）。

#include <cstddef>
#include <functional>
#include <memory>
#include <boost/circular_buffer.hpp>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace bbt::infra::net {

// 独立拥有的连续事件快照：构造时从源复制全部可读字节，owner 后续继续
// append/consume/reset 不改本对象 backing storage；可复制/移动延寿。
// Peek 返回借用视图：仅在下一次本快照非 const 调用前有效，不得声称裸
// view 可独立延寿（延寿靠复制/移动快照本身）。
class EventSnapshot {
public:
    EventSnapshot() = default;

    // 拷贝/移动延寿：拷贝得到独立 backing storage；移动把 data_ 与
    // cursor 一并转移（目标保留原已消费位置），来源保持合法空快照
    // （Remaining()==0，Discard/Consume/再赋值仍可用），自移动安全；
    // 不留「data_ 已转移而 cursor 未随移/未归位」的标量分叉（父级 probe
    // 观察到的来源 Remaining() 下溢 18446744073709551614）。
    EventSnapshot(const EventSnapshot&) = default;
    EventSnapshot& operator=(const EventSnapshot&) = default;
    EventSnapshot(EventSnapshot&& other) noexcept
        : data_(std::move(other.data_)),
          cursor_(std::exchange(other.cursor_, 0)) {}
    EventSnapshot& operator=(EventSnapshot&& other) noexcept
    {
        if (this != &other) {
            data_ = std::move(other.data_);
            cursor_ = std::exchange(other.cursor_, 0);
        }
        return *this;
    }

    std::size_t Remaining() const noexcept { return data_.size() - cursor_; }
    bool Empty() const noexcept { return Remaining() == 0; }

    std::string_view Peek() const noexcept
    {
        return std::string_view(data_.data() + cursor_, Remaining());
    }

    // n 超过剩余量时拒绝且不变。
    std::error_code Discard(std::size_t n)
    {
        if (n > Remaining()) {
            return std::make_error_code(std::errc::result_out_of_range);
        }
        cursor_ += n;
        return {};
    }

    // 拷出 n 字节并前移游标；n 超过剩余量时拒绝且不变。
    std::error_code Consume(std::size_t n, std::string& out)
    {
        if (n > Remaining()) {
            return std::make_error_code(std::errc::result_out_of_range);
        }
        out.append(data_, cursor_, n);
        cursor_ += n;
        return {};
    }

private:
    friend class EventRingBuffer;
    explicit EventSnapshot(std::string data)
        : data_(std::move(data)) {}

    std::string data_;          // 独立连续 backing storage
    std::size_t cursor_{0};
};

// 单 owner 有界弹性环形缓冲。Boost 管理 head/size/绕环不变量；
// 包装只预检总量并按需增长，永不收缩或覆盖。char 的 erase_begin 为 O(1)。
class EventRingBuffer {
public:
    static constexpr std::size_t kInitialCapacity = 1024;

    // max_bytes == 0 时仅空操作合法；构造不预分配。
    explicit EventRingBuffer(std::size_t max_bytes) noexcept
        : max_bytes_(max_bytes) {}

    EventRingBuffer(const EventRingBuffer&) = delete;
    EventRingBuffer& operator=(const EventRingBuffer&) = delete;
    // Boost 移动后来源 Size/Capacity 为 0，max_bytes 保留，可继续追加。
    EventRingBuffer(EventRingBuffer&& other) noexcept
        : storage_(std::move(other.storage_)), max_bytes_(other.max_bytes_) {}
    EventRingBuffer& operator=(EventRingBuffer&& other) noexcept
    {
        if (this != &other) {
            storage_ = std::move(other.storage_);
            max_bytes_ = other.max_bytes_;
        }
        return *this;
    }

    std::size_t Size() const noexcept { return storage_.size(); }
    std::size_t Capacity() const noexcept { return storage_.capacity(); }
    std::size_t MaxBytes() const noexcept { return max_bytes_; }
    bool Empty() const noexcept { return storage_.empty(); }

    // 支持自身 Peek view/子串作为输入：修改前保存别名副本。超限拒绝或
    // 分配失败时原缓冲不变；外部非别名输入不额外复制。
    std::error_code Append(std::string_view data)
    {
        if (data.empty()) {
            return {};
        }
        if (data.size() > max_bytes_ - Size()) {
            return std::make_error_code(std::errc::no_buffer_space);
        }
        const auto overlaps = [data](const auto& segment) {
            // std::less 提供不相关指针的全序；只在各有效区间内求尾指针。
            const std::less<const char*> less;
            return segment.second != 0
                && less(data.data(), segment.first + segment.second)
                && less(segment.first, data.data() + data.size());
        };
        std::string owned_input;
        if (overlaps(storage_.array_one()) || overlaps(storage_.array_two())) {
            owned_input.assign(data.data(), data.size());
            data = owned_input;
        }
        const std::size_t need = Size() + data.size();
        if (need > Capacity()) {
            storage_.set_capacity(GrowTarget(need)); // 只增长，强异常保证
        }
        // 足够空位且 char 复制不抛；不会进入 Boost 满缓冲的覆盖语义。
        storage_.insert(storage_.end(), data.begin(), data.end());
        return {};
    }

    // 读前预留总容量（读侧消除不可回滚消费的最小原语）：total_capacity
    // 超过 MaxBytes 拒绝且不变；只增长不收缩；容量已足时无操作。扩容
    // 失败（bad_alloc）时内容与容量均保持不变（set_capacity 强异常保证）。
    // 成功预留后，等量 Append 只做 char 提交，不再首次分配。
    std::error_code Reserve(std::size_t total_capacity)
    {
        if (total_capacity <= Capacity()) {
            return {};
        }
        if (total_capacity > max_bytes_) {
            return std::make_error_code(std::errc::no_buffer_space);
        }
        storage_.set_capacity(GrowTarget(total_capacity)); // 只增长，强异常保证
        return {};
    }

    // 首段连续可读 view，下一次非 const 调用后失效；保存 snapshot 才延寿。
    std::string_view Peek() const noexcept
    {
        if (Empty()) {
            return {};
        }
        const auto segment = storage_.array_one();
        return std::string_view(segment.first, segment.second);
    }

    std::error_code Discard(std::size_t n)
    {
        if (n > Size()) {
            return std::make_error_code(std::errc::result_out_of_range);
        }
        storage_.erase_begin(n);
        return {};
    }

    // 拷出 n 字节并前移；非法 n 拒绝且不变。
    std::error_code Consume(std::size_t n, std::string& out)
    {
        if (n > Size()) {
            return std::make_error_code(std::errc::result_out_of_range);
        }
        if (n == 0) {
            return {};
        }
        const auto one = storage_.array_one();
        const std::size_t first = n < one.second ? n : one.second;
        out.append(one.first, first);
        if (n > first) {
            out.append(storage_.array_two().first, n - first);
        }
        storage_.erase_begin(n);
        return {};
    }

    EventSnapshot TakeSnapshot() const
    {
        std::string data;
        const auto one = storage_.array_one();
        const auto two = storage_.array_two();
        if (one.second != 0) {
            data.append(one.first, one.second);
        }
        if (two.second != 0) {
            data.append(two.first, two.second);
        }
        return EventSnapshot(std::move(data));
    }

    // pop-all 移交：把当前 storage（含绕环两段）整体移入共享 backing，
    // 不复制载荷；返回的 const 对象不可再修改，array_one()/array_two()
    // 指针在其寿命内保持有效。成功后本对象回到空、容量 0，下一次
    // Append/Reserve 按需重新增长——额度（Size 语义）在移交成功瞬间释放，
    // 不等 backing 析构。
    // 失败（make_shared 分配失败）时本对象内容完全不变；move 构造不抛。
    std::shared_ptr<const boost::circular_buffer<char>> TakeStorage()
    {
        // boost::circular_buffer 的移动构造转移底层分配指针；移后 storage_
        // Size/Capacity 为 0（与类头移动语义注释一致），可直接复用追加。
        return std::make_shared<const boost::circular_buffer<char>>(std::move(storage_));
    }

    void Clear() noexcept { storage_.clear(); }

private:
    // need <= max_bytes_；先判 max/2 再翻倍，无溢出。
    std::size_t GrowTarget(std::size_t need) const noexcept
    {
        std::size_t target = Capacity() == 0
            ? (max_bytes_ < kInitialCapacity ? max_bytes_ : kInitialCapacity)
            : Capacity();
        while (target < need) {
            if (target > max_bytes_ / 2) {
                return max_bytes_;
            }
            target *= 2;
        }
        return target;
    }

    boost::circular_buffer<char> storage_;
    std::size_t max_bytes_;
};

} // namespace bbt::infra::net

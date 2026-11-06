#pragma once

/**
 * @file Buffer.hpp
 * @brief 公共读缓冲：Read 交付的拥有式字节视图，实现真零拷贝移交。
 *
 * Buffer 持有 owning 的 backing。它从接收 ring 取走的是真实存储，包括绕环的两段，不复制载荷。
 * backing 与 ring 共享同一块底层存储：ring 端按引用计数延后复用，Buffer 析构时才归还可复用的额度。
 * ring 的语义额度（已读字节上限）在移交成功的瞬间立即释放，不等 Buffer 析构。
 *
 * 分段：ring 回绕时，缓冲可能拆成多个段。本对象如实暴露段列表，不隐式拼接。
 * 需要连续视图时，调用 CopyTo 复制，或由消费方逐段处理。
 *
 * 值语义：只可移动，移动后源对象的 Segments() 为空。不提供拷贝，因为拷贝赋值在 backing 与段表分步
 * 更新、其中某步失败时会留下悬空段。Buffer 不是环形游标视图，不提供「读一部分后丢弃再 Peek」的游标。
 * 消费进度由调用方自行维护，按 Segment::size 推进。
 */

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace bbt::infra::net {

/**
 * @brief 单段连续可读视图。
 *
 * data 与 size 借自 backing，在所属 Buffer 存续期间有效。不得外借，不得跨 Buffer 析构使用。
 * 需要延长寿命时，用 CopyTo 复制成 std::string。
 */
struct BufferSegment {
    const char* data{nullptr}; ///< 借用的段首地址，指向 backing 内部存储
    std::size_t size{0};       ///< 段字节数

    /// @brief 以 string_view 返回本段，不做复制。
    std::string_view View() const noexcept
    {
        return {data, size};
    }
};

/**
 * @brief 拥有式读缓冲，可拆成绕环的两段。
 *
 * 只能经移交路径构造非空对象（CoTcpConnection::Read），或由内部测试接缝构造。
 * 拷贝在 backing 已替换、段表分配失败时可能留下指向已释放存储的段，因此删除拷贝、
 * 只保留移动；owning 不变量（段来自 backing）只能经移交路径建立。
 */
class Buffer {
public:
    Buffer() = default;

    /// @brief 拷贝构造已删除：拷贝无法原子地同时更新 backing 与段表，可能留下悬空段。
    Buffer(const Buffer&) = delete;
    /// @brief 拷贝赋值已删除，原因同拷贝构造。
    Buffer& operator=(const Buffer&) = delete;
    /// @brief 移动构造：接管 backing 与段表，源对象变为空。
    Buffer(Buffer&&) noexcept = default;
    /// @brief 移动赋值：先释放自身 backing，再接管源对象，源对象变为空。
    Buffer& operator=(Buffer&&) noexcept = default;

    /// @brief 是否没有任何段，即没有可读字节。
    bool Empty() const noexcept
    {
        return m_segments.empty();
    }

    /**
     * @brief 可读总字节，即各段之和。
     *
     * 它与 ring 的语义 Size 来源不同：ring 的已读额度在移交时已释放，本值只反映本 Buffer
     * 持有的字节数。
     */
    std::size_t Size() const noexcept;

    /// @brief 段数。1 表示连续存储；2 表示绕环的两段，即首段尾部与回绕段。
    std::size_t SegmentCount() const noexcept
    {
        return m_segments.size();
    }

    /// @brief 段视图。仅在 Buffer 存续且未被移动时有效。
    const std::vector<BufferSegment>& Segments() const noexcept
    {
        return m_segments;
    }

    /**
     * @brief 复制全部数据到一个连续字符串并返回。
     *
     * Buffer 本身不消费。需要连续视图且不想逐段处理时使用；零拷贝路径不需要调用本函数。
     */
    std::string CopyTo() const;

    /// @brief 将数据追加复制到已有的 out 末尾，不清空 out。
    void AppendTo(std::string& out) const;

private:
    // owning backing 句柄：实现由 Connection 的非等待移交原语从
    // ReceiveRing::TryPopAll 的 PopAllTakeout 构造；布局不出公共契约。
    // shared_ptr<void> 保证「分段共享同一移交存储」的引用计数成立。
    // private 构造责任：只有移交路径（Connection::TryRead）与测试接缝
    // 可凭 backing+段构造非空 Buffer；消费者不能伪造「段来自 backing」的
    // owning 不变量。
    explicit Buffer(std::shared_ptr<void> backing, std::vector<BufferSegment> segments);

    friend class Connection;
    friend struct BufferTestAccess;

    std::shared_ptr<void> m_backing;
    std::vector<BufferSegment> m_segments;
};

} // namespace bbt::infra::net

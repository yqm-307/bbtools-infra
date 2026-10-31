#pragma once

// 公共读缓冲（docs/decisions/0012-icoconn-member-interface.md）：Read 交付的拥有式字节视图，真零拷贝移交语义。
//
// 与旧 CoRead(std::string& out) 的本质差异：Buffer 持 owning backing，
// 从 ReceiveRing/EventRingBuffer 取走的是真实存储（含绕环两段），不复制
// 载荷。backing 与 ring 内部共享同一块底层存储：ring 端按引用计数延后
// 复用，Buffer 析构才归还可复用额度；ring 的语义额度（已读字节上限）
// 在移交成功的瞬间立即释放，不等 Buffer 析构。
//
// 分段（绕环）：底层 ring 回绕时缓冲可能拆为多个 Segment，本对象如实暴露
// 段列表；不隐式拼接。需要连续视图时 CopyTo 复制，或消费方逐段处理。
//
// 值语义：只可移动（移动后来源 Segments() 为空）；拷贝已删除——拷贝赋值
// 在 backing/段表分步失败时会留下悬空段。Buffer 不是环形游标视图——
// 没有「读一半丢弃再 Peek」的游标，消费进度由调用者自己经 Segment::size
// 推进。

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace bbt::infra::net {

// 单段连续可读视图：data/size 借自 backing，本对象存续期间有效。
// 生命周期规则与 EventSnapshot::Peek 相同——不得外借、不得跨 Buffer
// 析构使用；要延寿需 CopyTo 成 std::string。
struct BufferSegment {
    const char* data{nullptr};
    std::size_t size{0};

    std::string_view View() const noexcept { return {data, size}; }
};

class Buffer {
public:
    Buffer() = default;

    // 值语义只保留移动：拷贝会在 backing 已换、段表分配失败时留下指向已释放
    // 存储的旧段，且无法以「不可复制」之外的更弱方式伪造强保证——
    // owning 不变量（段来自 backing）只允许经移交路径建立。
    Buffer(const Buffer&) = delete;
    Buffer& operator=(const Buffer&) = delete;
    Buffer(Buffer&&) noexcept = default;
    Buffer& operator=(Buffer&&) noexcept = default;

    bool Empty() const noexcept { return m_segments.empty(); }

    // 可读总字节（各段之和）。与 Ring 的语义 Size 不同源——ring 的已读
    // 额度在移交时已释放，本值只反映本 Buffer 持有的字节数。
    std::size_t Size() const noexcept {
        std::size_t total = 0;
        for (const auto& seg : m_segments) {
            total += seg.size;
        }
        return total;
    }

    // 分段数：1 = 连续存储；2 = 绕环两段（首段尾部 + 回绕段）。
    std::size_t SegmentCount() const noexcept { return m_segments.size(); }

    // 段视图：仅在 Buffer 存续且未移动走时有效。
    const std::vector<BufferSegment>& Segments() const noexcept { return m_segments; }

    // 复制全部到连续 out（追加语义）；Buffer 本身不消费。
    // 需要连续视图且不想逐段消费时使用；零拷贝路径不需要调用本函数。
    std::string CopyTo() const {
        std::string out;
        out.reserve(Size());
        for (const auto& seg : m_segments) {
            out.append(seg.data, seg.size);
        }
        return out;
    }

    // 追加复制到既有 out（等价 CoRead 的 out 追加语义）。
    void AppendTo(std::string& out) const {
        for (const auto& seg : m_segments) {
            out.append(seg.data, seg.size);
        }
    }

private:
    // owning backing 句柄：实现由 CoTcpConnection 的 Read 路径从
    // ReceiveRing::TryPopAll 的 PopAllTakeout 构造；布局不出公共契约。
    // shared_ptr<void> 保证「分段共享同一移交存储」的引用计数成立。
    // private 构造责任：只有移交路径（CoTcpConnection::Read）与测试接缝
    // 可凭 backing+段构造非空 Buffer；消费者不能伪造「段来自 backing」的
    // owning 不变量。
    explicit Buffer(std::shared_ptr<void> backing,
                    std::vector<BufferSegment> segments)
        : m_backing(std::move(backing)), m_segments(std::move(segments)) {}

    friend class CoTcpConnection;
    friend struct BufferTestAccess;

    std::shared_ptr<void> m_backing;
    std::vector<BufferSegment> m_segments;
};

} // namespace bbt::infra::net

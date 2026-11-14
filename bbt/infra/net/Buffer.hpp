#pragma once

/**
 * @file Buffer.hpp
 * @brief 用户拥有的分段字节缓冲与二进制流；Read 会把接收数据复制到这里。
 *
 * Buffer 不持连接、锁或 socket；它不代表连接内部接收存储的所有权。
 * 整数按类型宽度使用大端字节序，bool 为 0/1 单字节，float/double 为 IEEE 754 位表示。
 * 字符串编码为 uint32 大端字节长度加原始内容（含内嵌零）；裸字节使用 WriteBytes/ReadBytes。
 * 线上字段应使用固定宽度类型，不直接序列化指针、size_t 或结构体内存布局。
 *
 * 只可移动；移动不复制载荷，来源变为空且可复用。不提供内部同步，包括 const 段视图缓存。
 * 所有段视图都只借用存储，下一次非 const 操作或析构后不可继续使用。
 * 单次提取失败不修改输出或消费位置；链式提取不是整条消息事务，先前成功的字段仍已消费。
 * Error 保留首个校验错误，后续流操作无动作，ClearError 后可补数据重试；分配异常仍向外抛出。
 */
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <vector>

namespace bbt::infra::net {

/**
 * @brief 单段借用视图，不延长所属 Buffer 的寿命。
 */
struct BufferSegment {

    /**
     * 连续区域首地址
     */
    const char* data{nullptr};

    /**
     * 区域字节数
     */
    std::size_t size{0};

    /**
     * @brief 无复制查看本段。
     */
    std::string_view View() const noexcept { return {data, size}; }
};

/**
 * @brief 可构造、追加、消费的拥有式二进制缓冲，不持连接、锁或 socket。
 */
class Buffer {
public:
    /**
     * @brief 创建空缓冲，不预分配载荷存储。
     */
    Buffer() noexcept;
    ~Buffer();

    Buffer(const Buffer&) = delete;
    Buffer& operator=(const Buffer&) = delete;

    /**
     * @brief 移交存储与错误状态，不复制载荷，来源恢复为空且无错误。
     */
    Buffer(Buffer&& other) noexcept;

    /**
     * @brief 释放自身旧数据并移交存储；自移动保持不变。
     */
    Buffer& operator=(Buffer&& other) noexcept;

    /**
     * @brief 是否没有未消费字节；与流错误状态无关。
     */
    bool Empty() const noexcept { return Size() == 0; }

    /**
     * @brief 未消费字节总量，常量时间查询。
     */
    std::size_t Size() const noexcept;

    /**
     * @brief 当前连续可读区域数，不能假定固定段数。
     */
    std::size_t SegmentCount() const noexcept;

    /**
     * @brief 未消费段视图；混合接收与写入数据时可能分配段元数据，不拼接载荷。
     */
    const std::vector<BufferSegment>& Segments() const;

    /**
     * @brief 无消费复制全部未读数据到新字符串。
     */
    std::string CopyTo() const;

    /**
     * @brief 无消费追加全部未读数据到 out。
     */
    void AppendTo(std::string& out) const;

    /**
     * @brief 为后续追加预留 additionalBytes 字节；已有可读内容保持不变。
     */
    bool Reserve(std::size_t additionalBytes);

    /**
     * @brief 追加原始字节，不附加长度；支持输入借自本 Buffer 的可读段。
     */
    bool WriteBytes(std::string_view bytes);

    /**
     * @brief 读取并消费 count 字节；失败时输出与游标不变。out 必须指向独立可写区域。
     */
    bool ReadBytes(void* out, std::size_t count);

    /**
     * @brief 丢弃 count 个未读字节，越界不消费；已有错误时不执行。
     */
    bool Consume(std::size_t count) noexcept;

    /**
     * @brief 清空数据和错误，保留可写存储容量供复用。
     */
    void Clear() noexcept;

    /**
     * @brief 首个校验错误：不足/越界为 result_out_of_range，无效 bool 为 illegal_byte_sequence。
     */
    std::error_code Error() const noexcept { return m_error; }

    /**
     * @brief 清除错误但不消费数据，允许补齐后重试。
     */
    void ClearError() noexcept { m_error.clear(); }

    /**
     * @brief 是否无校验错误，可用于 if (buffer >> value)，不是 Empty 的反义。
     */
    explicit operator bool() const noexcept { return !m_error; }

    /**
     * @name 类型编码与解码
     * @brief 按 wire 类型执行大端编码、跨段读取和失败时的消费约束。
     * @{
     */

    /**
     * @brief 按类型宽度追加大端整数，不支持 bool 以外的隐式结构体序列化。
     */
    template <class T, std::enable_if_t<std::is_integral_v<T> && !std::is_same_v<T, bool>, int> = 0>
    Buffer& operator<<(T value);

    /**
     * @brief 跨段提取大端整数，成功才更新 value 并消费。
     */
    template <class T, std::enable_if_t<std::is_integral_v<T> && !std::is_same_v<T, bool>, int> = 0>
    Buffer& operator>>(T& value);

    /**
     * @brief 以 uint32 字节长度前缀编码字符串，长度与正文一次提交。
     */
    Buffer& operator<<(std::string_view value);

    /**
     * @brief 编码零结尾字符串；空指针记 invalid_argument，不写入。
     */
    Buffer& operator<<(const char* value);

    /**
     * @brief 提取带长度的字符串；长度或正文不足时不消费，不分配正文。
     */
    Buffer& operator>>(std::string& value);

    /**
     * @brief 编码单字节 bool。
     */
    Buffer& operator<<(bool value);

    /**
     * @brief 只接受 0/1，非法值不消费。
     */
    Buffer& operator>>(bool& value);

    /**
     * @brief 编码 IEEE 754 binary32 的大端位表示。
     */
    Buffer& operator<<(float value);

    /**
     * @brief 提取 IEEE 754 binary32，不进行未对齐访问或类型双关。
     */
    Buffer& operator>>(float& value);

    /**
     * @brief 编码 IEEE 754 binary64 的大端位表示。
     */
    Buffer& operator<<(double value);

    /**
     * @brief 提取 IEEE 754 binary64。
     */
    Buffer& operator>>(double& value);

    // clang-format off

    /**
     * @}
     */
    // clang-format on

    /**
     * @brief 拒绝将普通指针隐式转成 bool 后序列化；字符指针仅按字符串处理。
     */
    template <class T, std::enable_if_t<!std::is_same_v<std::remove_cv_t<T>, char>, int> = 0>
    Buffer& operator<<(T*) = delete;
    Buffer& operator<<(std::nullptr_t) = delete;

private:
    struct WriteStorage;
    explicit Buffer(std::shared_ptr<void> backing, std::vector<BufferSegment> segments);
    bool _Append(std::string_view prefix, std::string_view bytes);
    bool _RequireReadable(std::size_t count) noexcept;
    bool _Fail(std::errc error) noexcept;
    void _CopyBytes(std::size_t offset, void* out, std::size_t count) const noexcept;
    void _Consume(std::size_t count) noexcept;

    friend class Connection;
    friend struct BufferTestAccess;

    /**
     * 原接收存储保持只读；写入区独立，避免破坏 owning 移交的不变量。
     */
    std::shared_ptr<void> m_backing;
    std::vector<BufferSegment> m_segments;
    std::size_t m_sourceBytes{0};
    std::unique_ptr<WriteStorage> m_writeStorage;
    mutable std::vector<BufferSegment> m_view;
    std::error_code m_error;
};

// clang-format off
}
// clang-format on

#include <bbt/infra/net/__TBuffer.hpp>

/**
 * RPC 会话的增量解帧实现：按握手和数据阶段限制待解析字节，解码完整帧。
 */
#include <bbt/infra/rpc/SessionCodec.hpp>

#include <algorithm>
#include <utility>

#include <bbt/infra/rpc/detail/SessionCodecInternal.hpp>

namespace bbt::infra::rpc {

class SessionFrameDecoder::Impl {
    friend class SessionFrameDecoder;

public:
    explicit Impl(std::size_t max_frame_bytes)
        : max_frame_bytes(max_frame_bytes)
    {
    }

private:
    /**
     * 当前阶段的单帧声明上限：握手态 min(max,64KiB)，数据态 max。
     */
    std::size_t DeclaredLimit() const noexcept
    {
        return hello_phase ? std::min(max_frame_bytes, kRpcV2HelloMaxFrameBytes) : max_frame_bytes;
    }

    /**
     * 未决字节硬上限：前缀 + 两帧当前阶段上限（容纳合法流水线：同一 recv 内
     * 至少两个完整帧 + 一帧前缀）。握手态整体缓冲天然 ≤4+2*64KiB，大声明
     * 前缀在复制 body 前已被 DeclaredLimit 拒绝。
     */
    std::size_t PendingLimit() const noexcept { return 4 + 2 * std::min(DeclaredLimit(), kRpcV2MaxFrameBytes); }

private:
    std::size_t max_frame_bytes;
    std::vector<std::uint8_t> buffer;
    std::optional<Error> terminal_error;

    /*
     * hello_phase=true（默认）：握手态，前缀限额收窄到 min(max,64KiB)。
     */
    bool hello_phase{true};
};

SessionFrameDecoder::SessionFrameDecoder(std::size_t max_frame_bytes)
    : m_impl(new Impl(max_frame_bytes))
{
}

SessionFrameDecoder::~SessionFrameDecoder() = default;
SessionFrameDecoder::SessionFrameDecoder(SessionFrameDecoder&&) noexcept = default;
SessionFrameDecoder& SessionFrameDecoder::operator=(SessionFrameDecoder&&) noexcept = default;

void SessionFrameDecoder::SetHelloPhase(bool hello_phase) noexcept
{
    m_impl->hello_phase = hello_phase;
}

std::size_t SessionFrameDecoder::PendingBytes() const noexcept
{
    return m_impl->buffer.size();
}

std::vector<std::uint8_t> SessionFrameDecoder::TakePending()
{
    return std::move(m_impl->buffer);
}

result<void> SessionFrameDecoder::Feed(const std::uint8_t* data, std::size_t size)
{
    if (m_impl->terminal_error)
        return result<void>::err(*m_impl->terminal_error);
    if (data == nullptr && size != 0)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "null feed data with non-zero size"));

    /**
     * 分配/复制前检查未决字节硬上限（随阶段收窄：握手态 4+2*min(max,64KiB)）。
     * 缓冲可合法超过首帧声明长度——流水线帧是正常形态，由 Next() 逐帧取走。
     */
    if (size > m_impl->PendingLimit() || m_impl->buffer.size() + size > m_impl->PendingLimit()) {
        m_impl->terminal_error = MakeError(ErrorCode::ProtocolError, "pending decode bytes exceed hard limit");
        return result<void>::err(*m_impl->terminal_error);
    }

    /**
     * 逐帧复制：每一帧先拼齐 4 字节前缀并按当前阶段 DeclaredLimit 校验，
     * 校验通过才复制其 body（ADR §5.2：检查长度后才允许分配 body）。覆盖
     * 三种落点：首帧前缀、完整帧后的流水线帧前缀（同批或跨 Feed 分批）、
     * 以及前缀已齐未验的防御路径——超限声明在任一位置都只保留前缀字节、
     * body 永不入缓冲。
     * 缓冲不变量（Session 层每解出一帧即 Next/TakePending）：完整帧只能
     * 出现在缓冲头部，其后至多是同一个未完成帧。
     *
     * 未消费的输入下标
     */
    std::size_t offset = 0;

    /**
     * 当前帧在 buffer 的起点
     */
    std::size_t frame_start = 0;
    const std::size_t limit = m_impl->DeclaredLimit();
    while (true) {
        const std::size_t have = m_impl->buffer.size() - frame_start;
        if (have < 4) {

            /**
             * 当前帧前缀未齐：尽量补到 4；输入耗尽即返回。
             */
            const std::size_t take = std::min(4 - have, size - offset);
            m_impl->buffer.insert(m_impl->buffer.end(), data + offset, data + offset + take);
            offset += take;
            if (m_impl->buffer.size() - frame_start < 4)
                return result<void>::ok();
        }
        if (frame_start != 0 && m_impl->hello_phase) {

            /**
             * 握手阶段完整帧之后的余字节是未验证 stash 候选（Session 在每帧
             * 解出后 TakePending 移交，握手验证完成后才按数据阶段限额补喂）：
             * 其前缀属未来阶段，不能按握手 DeclaredLimit 判定——原样收进
             * 缓冲（总量仍受 PendingLimit 约束），校验延后到阶段切换。
             */
            m_impl->buffer.insert(m_impl->buffer.end(), data + offset, data + size);
            return result<void>::ok();
        }
        const auto declared = DecodeFramePrefix(m_impl->buffer.data() + frame_start);
        if (declared > limit) {
            m_impl->terminal_error = MakeError(ErrorCode::ProtocolError, "declared frame length exceeds phase limit");
            return result<void>::err(*m_impl->terminal_error);
        }
        const std::size_t frame_end = frame_start + 4 + declared;
        if (m_impl->buffer.size() >= frame_end) {

            /**
             * 该帧已完整在缓冲（等 Next 取走）：跳到下一帧前缀继续校验。
             */
            frame_start = frame_end;
            continue;
        }
        if (offset >= size)

            /**
             * 前缀已验，无输入可吃
             */
            return result<void>::ok();

        /**
         * declared ≤ limit 已成立：帧全长 4+declared ≤ PendingLimit 成立
         * （PendingLimit ≥ 4+2*limit），按帧界复制不会越过已验前缀。
         */
        const std::size_t need = frame_end - m_impl->buffer.size();
        const std::size_t take = std::min(need, size - offset);
        m_impl->buffer.insert(m_impl->buffer.end(), data + offset, data + offset + take);
        offset += take;
        if (m_impl->buffer.size() < frame_end)

            /**
             * body 未齐，待下次 Feed
             */
            return result<void>::ok();
        if (offset >= size)

            /**
             * 完整帧留待 Next 取走
             */
            return result<void>::ok();

        /**
         * 流水线：校验下一帧前缀
         */
        frame_start = frame_end;
    }
}

result<void> SessionFrameDecoder::Feed(const std::vector<std::uint8_t>& bytes)
{
    return Feed(bytes.data(), bytes.size());
}

result<void> SessionFrameDecoder::Feed(const std::string& bytes)
{
    return Feed(reinterpret_cast<const std::uint8_t*>(bytes.data()), bytes.size());
}

result<std::optional<SessionFrame>> SessionFrameDecoder::Next()
{
    using R = result<std::optional<SessionFrame>>;
    if (m_impl->terminal_error)
        return R::err(*m_impl->terminal_error);
    if (m_impl->buffer.size() < 4)
        return R::ok(std::nullopt);
    const auto declared = DecodeFramePrefix(m_impl->buffer.data());
    if (declared == 0) {
        m_impl->terminal_error = MakeError(ErrorCode::ProtocolError, "frame payload_length zero is rejected");
        return R::err(*m_impl->terminal_error);
    }
    if (declared > m_impl->DeclaredLimit()) {
        m_impl->terminal_error = MakeError(ErrorCode::ProtocolError, "frame payload_length exceeds phase limit");
        return R::err(*m_impl->terminal_error);
    }
    if (m_impl->buffer.size() < 4 + declared)
        return R::ok(std::nullopt);

    /**
     * 完整帧就绪：先按 declared 上限做数据帧粗检（kind 细分上限在 body 解码
     * 后按 kind 复核——前缀阶段不分配 body 副本之外的大内存）。
     */
    auto frame = DecodeSessionFrame(m_impl->buffer.data() + 4, declared);
    if (!frame) {
        m_impl->terminal_error = frame.error();
        return R::err(*m_impl->terminal_error);
    }
    const std::size_t kind_limit = FrameByteLimit(frame.value().kind, m_impl->max_frame_bytes);
    if (declared > kind_limit) {
        m_impl->terminal_error = MakeError(ErrorCode::ProtocolError, "frame body exceeds kind byte limit");
        return R::err(*m_impl->terminal_error);
    }
    m_impl->buffer.erase(m_impl->buffer.begin(), m_impl->buffer.begin() + 4 + declared);
    return R::ok(std::move(frame).value());
}

bool SessionFrameDecoder::IsTerminal() const noexcept
{
    return m_impl->terminal_error.has_value();
}

} // namespace bbt::infra::rpc

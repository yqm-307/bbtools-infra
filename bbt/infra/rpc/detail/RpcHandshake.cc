// 在未安装的字节通道上交换 Hello / HelloAck。
#include <bbt/infra/rpc/detail/RpcSessionState.hpp>
#include <bbt/infra/rpc/detail/Session.hpp>

#include <iterator>
#include <algorithm>
#include <string>
#include <utility>
#include <vector>

namespace bbt::infra::rpc {

namespace {

/*
 * ReadHandshakeFrame 的增量补喂块大小：两次 Next() 之间 decoder 缓冲只
 * 容纳不完整单帧（声明≤64KiB+4），32KiB 块使 buffer+take 恒小于握手
 * pending 界（4+2*64KiB），合法帧头不触硬界、非法大声明仍在解码点被拒。
 */
constexpr std::size_t kHandshakeFeedChunk = 32 * 1024;

/**
 * Handshake helpers（trusted-network plaintext，ADR §3.2/§3.4）
 * 受限握手阶段 I/O：直接在 conn 上 Write/Read（未建 lane worker）。
 * Hello/HelloAck 帧上限 min(max_frame,64KiB) 由 codec 在 hello_phase
 * 前缀阶段强制。先耗 decoder 缓存再 Read；同批 Read 只消费到「下一个
 * 完整握手帧」为止，排在其后的字节经 HandshakeStash 移交调用方——调用
 * 方在验证握手边界的时刻切数据态后按数据限额补喂（合法最终
 * HelloAck+大首数据帧合包不被握手 pending 界误拒；未验证前这些字节
 * 尚未进 decoder，大声明仍按握手限额拒绝）。
 *
 * HandshakeStash：同批 Read 解出握手帧后、decoder 里尚未耗尽的余字节
 * （排在握手帧之后的合包数据）。由调用方持有到握手边界验证完成，再按
 * 数据态补喂——验证前这些字节不属任何阶段限额，未验证不进入 decoder。
 */
struct HandshakeStash {

    /*
     * owning 余量（可为空）
     */
    std::vector<std::uint8_t> bytes;

    bool Empty() const noexcept { return bytes.empty(); }
};

/**
 * 读一帧握手帧。同批 Read 只消费到「下一个完整握手帧」为止——解码成功
 * 的瞬间把 decoder 里尚未耗尽的同批余字节 TakePending 进 stash 返回，
 * 由调用方在握手边界验证完成后按数据态补喂（合法最终 HelloAck+
 * 大首数据帧合包不被握手 pending 界误拒；未验证前这些字节未进
 * decoder，大声明仍按握手限额拒绝）。stash 无跨批语义，每帧调用清零。
 */
result<SessionFrame> ReadHandshakeFrame(
    detail::ICoChannel& channel,
    SessionFrameDecoder& decoder,
    bbt::coroutine::Deadline deadline,
    HandshakeStash& stash)
{

    /**
     * 帧接受门禁：kind 校验 + 已喂未解余字节移交 stash。stash 内字节序 =
     * 字节流序（decoder 余量在前，其后是调用方补喂的未喂输入）。
     */
    const auto accept = [&](SessionFrame&& frame) -> result<SessionFrame> {
        if (frame.kind != FrameKind::Hello && frame.kind != FrameKind::HelloAck
            && frame.kind != FrameKind::SessionError) {
            return result<SessionFrame>::err(Err(ErrorCode::ProtocolError, "non-handshake frame during handshake"));
        }
        if (decoder.PendingBytes() != 0) {

            /**
             * decoder 余量在字节流序中先于 stash 中尚未喂入的尾部——前置
             * 而非覆盖，否则 stash 分支（本调用已从 stash 喂过一块）丢字节。
             */
            auto pending = decoder.TakePending();
            if (!stash.bytes.empty()) {
                pending.insert(
                    pending.end(),
                    std::make_move_iterator(stash.bytes.begin()),
                    std::make_move_iterator(stash.bytes.end()));
            }
            stash.bytes = std::move(pending);
        }
        return result<SessionFrame>::ok(std::move(frame));
    };
    for (;;) {
        if (const auto gate = CheckCall(deadline)) {
            return result<SessionFrame>::err(detail::TcpCallError(gate, "handshake timed out", false));
        }

        /**
         * 先耗缓存：上次已移交的字节先解。一帧到手即返回。
         */
        {
            auto cached = decoder.Next();
            if (!cached) {
                return result<SessionFrame>::err(cached.error());
            }
            if (cached.value().has_value()) {
                return accept(std::move(cached.value().value()));
            }
        }

        /**
         * 再耗 stash：同批未验证余字节仍按握手限额增量补喂（与下方 Read
         * 批同规则）；解出首个帧即停，未喂余量留在 stash 交调用方裁决。
         */
        while (!stash.Empty()) {
            const std::size_t take = std::min(stash.bytes.size(), kHandshakeFeedChunk);
            const auto fed = decoder.Feed(stash.bytes.data(), take);
            if (!fed) {
                return result<SessionFrame>::err(Err(ErrorCode::ProtocolError, "handshake stash decode rejected"));
            }
            stash.bytes.erase(stash.bytes.begin(), stash.bytes.begin() + take);
            auto nxt = decoder.Next();
            if (!nxt) {
                return result<SessionFrame>::err(nxt.error());
            }
            if (nxt.value().has_value()) {
                return accept(std::move(nxt.value().value()));
            }
        }
        auto [read_data, read_error] = channel.ReadBytes({deadline});
        if (!read_error) {

            /**
             * 同一 Read 批不整批先按 hello_phase Feed——分段内按
             * kHandshakeFeedChunk 块补喂，每块后尝试解帧；解出首个握手帧即
             * 停喂，本批未喂字节排在 decoder 余量之后移交 stash（由调用方
             * 在边界验证后按数据态补喂）。decoder 缓冲在两次 Next 之间只
             * 容纳不完整的单帧（≤64KiB+4），单块补喂不会触达握手 pending
             * 界；合法最终 HelloAck+大首数据帧合包因此不被误拒。
             */
            const auto& segments = read_data.Segments();
            for (std::size_t si = 0; si < segments.size(); ++si) {
                const auto* base = reinterpret_cast<const std::uint8_t*>(segments[si].data);
                std::size_t offset = 0;
                while (offset < segments[si].size) {
                    const std::size_t take = std::min(segments[si].size - offset, kHandshakeFeedChunk);
                    const auto fed = decoder.Feed(base + offset, take);
                    if (!fed) {
                        return result<SessionFrame>::err(
                            Err(ErrorCode::ProtocolError, "handshake frame decode rejected"));
                    }
                    offset += take;
                    auto nxt = decoder.Next();
                    if (!nxt) {
                        return result<SessionFrame>::err(nxt.error());
                    }
                    if (nxt.value().has_value()) {
                        auto accepted = accept(std::move(nxt.value().value()));
                        if (!accepted) {
                            return accepted;
                        }

                        /**
                         * 未喂输入排在 decoder 余量之后（字节流序）。
                         */
                        stash.bytes.insert(stash.bytes.end(), base + offset, base + segments[si].size);
                        for (std::size_t sj = si + 1; sj < segments.size(); ++sj) {
                            const auto* tail = reinterpret_cast<const std::uint8_t*>(segments[sj].data);
                            stash.bytes.insert(stash.bytes.end(), tail, tail + segments[sj].size);
                        }
                        return accepted;
                    }
                }
            }
            continue;
        }
        if (read_error == bbt::infra::net::make_error_code(bbt::infra::net::NetErrc::Eof)) {
            return result<SessionFrame>::err(Err(ErrorCode::Unavailable, "peer closed during handshake"));
        }
        return result<SessionFrame>::err(detail::TcpCallError(read_error, "handshake read failed", false));
    }
}

/**
 * 验证最终握手帧完成后的边界：切数据态并把 stash 余字节按数据限额
 * 一次性补喂回 decoder。调用前提：ack/echo 已逐项验证——此后这些字节
 * 按数据帧上限判定（数据 pending 界 4+2*max_frame 可容纳整批）。握手
 * 中途的 stash 补喂不经过本函数：ReadHandshakeFrame 内按
 * kHandshakeFeedChunk 增量消耗，超界 stash 不会撞握手 pending 硬界。
 */
result<void> CommitStashAfterFinalAck(SessionFrameDecoder& decoder, HandshakeStash& stash)
{
    decoder.SetHelloPhase(false);
    if (stash.Empty()) {
        return result<void>::ok();
    }
    const auto fed = decoder.Feed(stash.bytes.data(), stash.bytes.size());
    stash.bytes.clear();
    if (!fed) {
        return result<void>::err(Err(ErrorCode::ProtocolError, "handshake stash decode rejected"));
    }
    return result<void>::ok();
}

result<void> WriteHandshakeFrame(
    detail::ICoChannel& channel,
    const SessionFrame& frame,
    bbt::coroutine::Deadline deadline,
    std::size_t max_frame)
{
    auto bytes = EncodeFramedSessionFrame(frame, max_frame);
    if (!bytes) {
        return result<void>::err(bytes.error());
    }
    std::string out(reinterpret_cast<const char*>(bytes.value().data()), bytes.value().size());
    const auto total = out.size();
    const auto report = channel.WriteBytes(std::move(out), {deadline});
    return detail::TcpSettleWrite(report, total, "handshake write failed");
}

SessionFrame MakeHello(const ClientOptions& options, std::uint64_t epoch, std::uint64_t nonce)
{
    SessionFrame frame;
    frame.kind = FrameKind::Hello;
    frame.traffic = TrafficClass::System;
    frame.peer_identity = options.LocalIdentity();
    frame.session_epoch = epoch;
    frame.connection_nonce = nonce;
    return frame;
}

SessionFrame MakeHelloAck(const ClientOptions& options, std::uint64_t epoch, std::uint64_t peer_nonce)
{
    SessionFrame frame;
    frame.kind = FrameKind::HelloAck;
    frame.traffic = TrafficClass::System;
    frame.peer_identity = options.LocalIdentity();
    frame.session_epoch = epoch;
    frame.connection_nonce = peer_nonce;
    return frame;
}

} // namespace

/**
 * 出站方向（拨号侧）：写 Hello → 读 Hello → epoch owner 决定 epoch →
 * 写 HelloAck → 读 HelloAck echo。identity 较小一侧是 epoch owner。
 * 成功时 out_decoder 收编握手 decoder（含未耗尽的合包余帧），供 session
 * live reader 继续消费（余帧移交）。
 */
result<std::uint64_t> detail::OutboundHandshakeOnChannel(
    detail::ICoChannel& channel,
    const ClientOptions& options,
    std::uint64_t proposal,
    std::uint64_t nonce,
    std::string& out_peer_identity,
    SessionFrameDecoder& out_decoder,
    bbt::coroutine::Deadline deadline)
{
    using R = result<std::uint64_t>;
    HandshakeStash stash;
    auto hello = MakeHello(options, proposal, nonce);
    if (auto wrote = WriteHandshakeFrame(channel, hello, deadline, options.MaxFrameBytes()); !wrote) {
        return R::err(wrote.error());
    }
    SessionFrameDecoder decoder(options.MaxFrameBytes());
    auto peer_hello = ReadHandshakeFrame(channel, decoder, deadline, stash);
    if (!peer_hello) {
        return R::err(peer_hello.error());
    }
    if (peer_hello.value().kind != FrameKind::Hello) {
        return R::err(Err(ErrorCode::ProtocolError, "expected Hello from peer"));
    }
    const auto& remote = peer_hello.value();
    if (remote.peer_identity == options.LocalIdentity()) {
        return R::err(Err(ErrorCode::ProtocolError, "self-connection rejected"));
    }
    out_peer_identity = remote.peer_identity;
    const bool i_own_epoch = options.LocalIdentity() < remote.peer_identity;
    std::uint64_t epoch = 0;
    if (i_own_epoch) {

        /**
         * epoch owner：本端 proposal 即 selected。
         */
        epoch = proposal;
        auto ack = MakeHelloAck(options, epoch, remote.connection_nonce);
        if (auto wrote = WriteHandshakeFrame(channel, ack, deadline, options.MaxFrameBytes()); !wrote) {
            return R::err(wrote.error());
        }
    }

    /**
     * 第一帧取出时若有同批余字节（合包 HelloAck 前缀/数据帧等），留在
     * stash——ReadHandshakeFrame 内按握手限额增量补喂，不整批硬喂。
     */
    auto peer_ack = ReadHandshakeFrame(channel, decoder, deadline, stash);
    if (!peer_ack) {
        return R::err(peer_ack.error());
    }
    if (peer_ack.value().kind != FrameKind::HelloAck) {
        return R::err(Err(ErrorCode::ProtocolError, "expected HelloAck from peer"));
    }
    const auto& ack = peer_ack.value();
    if (ack.peer_identity != remote.peer_identity) {
        return R::err(Err(ErrorCode::ProtocolError, "HelloAck identity mismatch"));
    }
    if (i_own_epoch) {
        if (ack.session_epoch != epoch || ack.connection_nonce != nonce) {
            return R::err(Err(ErrorCode::ProtocolError, "HelloAck echo mismatch"));
        }

        /**
         * 验证完成的边界：切数据态后 stash 余字节按数据限额补喂。
         */
        if (auto committed = CommitStashAfterFinalAck(decoder, stash); !committed) {
            return R::err(committed.error());
        }
        out_decoder = std::move(decoder);
        return R::ok(epoch);
    }

    /**
     * 对端是 epoch owner：原样 echo selected epoch。
     */
    epoch = ack.session_epoch;
    auto echo = MakeHelloAck(options, epoch, remote.connection_nonce);
    if (auto wrote = WriteHandshakeFrame(channel, echo, deadline, options.MaxFrameBytes()); !wrote) {
        return R::err(wrote.error());
    }
    if (auto committed = CommitStashAfterFinalAck(decoder, stash); !committed) {
        return R::err(committed.error());
    }
    out_decoder = std::move(decoder);
    return R::ok(epoch);
}

/**
 * 入站方向（接纳侧）：读 Hello → 写 Hello → 同样规则。
 * out_decoder 同 OutboundHandshake：成功时收编含余帧的握手 decoder。
 */
result<std::uint64_t> detail::InboundHandshakeOnChannel(
    detail::ICoChannel& channel,
    const ClientOptions& options,
    std::uint64_t proposal,
    std::uint64_t nonce,
    std::string& out_peer_identity,
    SessionFrameDecoder& out_decoder,
    bbt::coroutine::Deadline deadline)
{
    using R = result<std::uint64_t>;
    HandshakeStash stash;
    SessionFrameDecoder decoder(options.MaxFrameBytes());
    auto peer_hello = ReadHandshakeFrame(channel, decoder, deadline, stash);
    if (!peer_hello) {
        return R::err(peer_hello.error());
    }
    if (peer_hello.value().kind != FrameKind::Hello) {
        return R::err(Err(ErrorCode::ProtocolError, "expected Hello from inbound peer"));
    }
    const auto& remote = peer_hello.value();
    if (remote.peer_identity == options.LocalIdentity()) {
        return R::err(Err(ErrorCode::ProtocolError, "self-connection rejected"));
    }
    out_peer_identity = remote.peer_identity;
    auto hello = MakeHello(options, proposal, nonce);
    if (auto wrote = WriteHandshakeFrame(channel, hello, deadline, options.MaxFrameBytes()); !wrote) {
        return R::err(wrote.error());
    }
    const bool i_own_epoch = options.LocalIdentity() < remote.peer_identity;
    std::uint64_t epoch = 0;
    if (i_own_epoch) {
        epoch = proposal;
        auto ack = MakeHelloAck(options, epoch, remote.connection_nonce);
        if (auto wrote = WriteHandshakeFrame(channel, ack, deadline, options.MaxFrameBytes()); !wrote) {
            return R::err(wrote.error());
        }
        auto peer_ack = ReadHandshakeFrame(channel, decoder, deadline, stash);
        if (!peer_ack) {
            return R::err(peer_ack.error());
        }
        if (peer_ack.value().kind != FrameKind::HelloAck || peer_ack.value().session_epoch != epoch
            || peer_ack.value().connection_nonce != nonce || peer_ack.value().peer_identity != remote.peer_identity) {
            return R::err(Err(ErrorCode::ProtocolError, "HelloAck echo mismatch"));
        }

        /**
         * 验证完成边界：切数据态，stash 余字节按数据限额补喂。
         */
        if (auto committed = CommitStashAfterFinalAck(decoder, stash); !committed) {
            return R::err(committed.error());
        }
        out_decoder = std::move(decoder);
        return R::ok(epoch);
    }

    /**
     * 对端是 epoch owner：先读 selected epoch，再 echo。
     */
    auto peer_ack = ReadHandshakeFrame(channel, decoder, deadline, stash);
    if (!peer_ack) {
        return R::err(peer_ack.error());
    }
    if (peer_ack.value().kind != FrameKind::HelloAck || peer_ack.value().connection_nonce != nonce
        || peer_ack.value().peer_identity != remote.peer_identity) {
        return R::err(Err(ErrorCode::ProtocolError, "HelloAck echo mismatch"));
    }
    epoch = peer_ack.value().session_epoch;
    auto echo = MakeHelloAck(options, epoch, remote.connection_nonce);
    if (auto wrote = WriteHandshakeFrame(channel, echo, deadline, options.MaxFrameBytes()); !wrote) {
        return R::err(wrote.error());
    }
    if (auto committed = CommitStashAfterFinalAck(decoder, stash); !committed) {
        return R::err(committed.error());
    }
    out_decoder = std::move(decoder);
    return R::ok(epoch);
}

} // namespace bbt::infra::rpc

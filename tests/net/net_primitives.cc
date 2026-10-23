// net 纯逻辑包定向测试：buffer（有界 ring/事件快照/待发分段队列）、
// options（值归一化）、load_balancing（三算法选择）。与 net_foundation.cc
// 分开，避免把所有逻辑堆进一个文件。CHECK 为显式检查，Release/NDEBUG 仍生效。

#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/LoadBalancing.hpp>
#include <bbt/infra/net/Options.hpp>
// detail 内部件仅测试同仓引用，不构成公开 API。
#include <bbt/infra/net/detail/EventRingBuffer.hpp>
#include <bbt/infra/net/detail/OptionsNormalize.hpp>
#include <bbt/infra/net/detail/OutboundQueue.hpp>
#include <bbt/infra/net/detail/SourceAddrHash.hpp>

#include <chrono>
#include <cstdint>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <vector>

using namespace bbt::infra::net;

static void Check(bool value, const char* expression, int line) {
    if (!value) {
        throw std::runtime_error("line " + std::to_string(line) + ": " + expression);
    }
}
#define CHECK(expression) Check(static_cast<bool>(expression), #expression, __LINE__)

static bool IsNoBufferSpace(const std::error_code& ec) {
    return ec == std::errc::no_buffer_space;
}
static bool IsOutOfRange(const std::error_code& ec) {
    return ec == std::errc::result_out_of_range;
}
static bool IsInvalidArgument(const std::error_code& ec) {
    return ec == std::errc::invalid_argument;
}

// 父级用 python zlib 固定的独立 IEEE CRC32 校验向量。
constexpr std::uint32_t kCrcEmpty = 0u;
constexpr std::uint32_t kCrcDigits = 3421780262u;       // "123456789"
constexpr std::uint32_t kCrcIpv4 = 2116851123u;         // "127.0.0.1:12345"
constexpr std::uint32_t kCrcIpv6 = 2030060662u;         // "[::1]:80"
constexpr std::uint32_t kCrcUnixAbstract = 3208599909u; // "@example"

// ---------------------------------------------------------------------------
// buffer：EventRingBuffer / EventSnapshot / OutboundQueue
// ---------------------------------------------------------------------------

static void CheckRingMatchesModel(const EventRingBuffer& ring, const std::string& model) {
    CHECK(ring.Size() == model.size());
    if (model.empty()) {
        CHECK(ring.Peek().empty());
        return;
    }
    // 快照是当前全部可读字节的独立拷贝，用于与参考模型整体比对。
    EventSnapshot snapshot = ring.TakeSnapshot();
    CHECK(snapshot.Remaining() == model.size());
    std::string drained;
    CHECK(!snapshot.Consume(snapshot.Remaining(), drained));
    CHECK(drained == model);
}

static void TestRingBasics() {
    EventRingBuffer ring(256);
    CHECK(ring.MaxBytes() == 256);
    CHECK(ring.Empty());

    // 空操作合法。
    CHECK(!ring.Append(""));
    CHECK(!ring.Discard(0));
    std::string out;
    CHECK(!ring.Consume(0, out));
    CHECK(ring.Peek().empty());

    CHECK(!ring.Append("hello"));
    CHECK(ring.Size() == 5);
    CHECK(ring.Peek() == "hello");
    CHECK(!ring.Discard(2));
    CHECK(ring.Peek() == "llo");
    CHECK(!ring.Consume(3, out));
    CHECK(out == "llo");
    CHECK(ring.Empty());

    // 非法 consume 拒绝且不变。
    CHECK(IsOutOfRange(ring.Discard(1)));
    CHECK(ring.Size() == 0);
    out.clear();
    CHECK(IsOutOfRange(ring.Consume(1, out)));
    CHECK(out.empty());

    // 总界：超限在修改前拒绝。
    std::string payload(100, 'a');
    CHECK(!ring.Append(payload));
    CHECK(IsNoBufferSpace(ring.Append(std::string(157, 'b'))));
    CHECK(ring.Size() == 100);
    CHECK(ring.Peek() == payload);

    // 清空后 ring 可复用。
    ring.Clear();
    CHECK(ring.Empty());
    CHECK(!ring.Append("xyz"));
    CHECK(ring.Peek() == "xyz");
}

static void TestRingWrapAndGrowth() {
    // 绕环：小容量反复追加/消费，覆盖 Peek 截断与跨绕环 Consume。
    EventRingBuffer ring(32);
    CHECK(!ring.Append("abcdefghij"));                  // 10
    CHECK(!ring.Discard(6));                            // head=6，剩 "ghij"
    CHECK(!ring.Append("012345678901234567890123"));    // 尾段 22 + 绕头 2（"23"）
    CHECK(ring.Size() == 28);
    CHECK(!ring.Append("AB"));                          // 写 pos2-3
    CHECK(ring.Size() == 30);
    std::string model = "ghij012345678901234567890123AB";
    CheckRingMatchesModel(ring, model);

    // Peek 只给首段连续区（pos6..31，26 字节），不消费、不跨绕环。
    const std::string_view first = ring.Peek();
    CHECK(first == "ghij0123456789012345678901");
    CHECK(ring.Size() == 30);

    // Consume 跨绕环：26 连续 + 绕环段 2 字节。
    std::string out;
    CHECK(!ring.Consume(28, out));
    CHECK(out == "ghij012345678901234567890123");
    CHECK(ring.Size() == 2);
    CHECK(ring.Peek() == "AB");
    out.clear();
    CHECK(!ring.Consume(2, out));
    CHECK(out == "AB");
    CHECK(ring.Empty());

    // 增长保序：max 1024，100 字节分片追加。
    EventRingBuffer growing(1024);
    std::string grown_model;
    std::mt19937 rng(20261015u);
    for (int i = 0; i < 10; ++i) {
        std::string chunk(100, 0);
        for (char& c : chunk) { c = static_cast<char>('A' + (rng() % 26)); }
        CHECK(!growing.Append(chunk));
        grown_model += chunk;
        CheckRingMatchesModel(growing, grown_model);
    }
    CHECK(growing.Capacity() >= 1000);
    CHECK(growing.Capacity() <= 1024);
    CHECK(growing.Size() == 1000);

    // 增长封顶后仍受总界约束。
    CHECK(IsNoBufferSpace(growing.Append(std::string(25, 'z'))));
    CHECK(growing.Size() == 1000);
    CheckRingMatchesModel(growing, grown_model);

    // fix1 回归：真实跨越 1024→2048→4096→8192 的增长，且每次增长前先
    // 绕环/partial，增长后逐字节对参考模型。
    {
        EventRingBuffer cross(8192);
        std::string model;
        CHECK(cross.Capacity() == 0);               // 构造不预分配
        CHECK(!cross.Append(std::string(900, 'a')));
        model += std::string(900, 'a');
        CHECK(cross.Capacity() == 1024);            // 初始 1024
        CHECK(!cross.Discard(800));                 // head=800，绕环前置
        model.erase(0, 800);
        CHECK(!cross.Append(std::string(700, 'b'))); // 跨绕环写，仍 1024
        model += std::string(700, 'b');
        CHECK(cross.Capacity() == 1024);
        CheckRingMatchesModel(cross, model);
        CHECK(!cross.Append(std::string(400, 'c'))); // 1200 → 增长 2048
        model += std::string(400, 'c');
        CHECK(cross.Capacity() == 2048);
        CheckRingMatchesModel(cross, model);
        CHECK(!cross.Append(std::string(1000, 'd'))); // 2200 → 增长 4096
        model += std::string(1000, 'd');
        CHECK(cross.Capacity() == 4096);
        CheckRingMatchesModel(cross, model);
        CHECK(!cross.Append(std::string(2000, 'e'))); // 4200 → 增长 8192
        model += std::string(2000, 'e');
        CHECK(cross.Capacity() == 8192);
        CheckRingMatchesModel(cross, model);
        CHECK(!cross.Append(std::string(3992, 'f'))); // 到达 max
        model += std::string(3992, 'f');
        CHECK(cross.Size() == 8192);
        CHECK(IsNoBufferSpace(cross.Append("g")));
        CheckRingMatchesModel(cross, model);
    }
    // 非 2 次幂上限：2048 装不下时封顶到 3000（不落到 4096）。
    {
        EventRingBuffer capped(3000);
        CHECK(!capped.Append(std::string(2500, 'q')));
        CHECK(capped.Capacity() == 3000);
        CHECK(capped.Size() == 2500);
        CHECK(IsNoBufferSpace(capped.Append(std::string(501, 'r'))));
        CHECK(!capped.Append(std::string(500, 'r')));
        CheckRingMatchesModel(capped, std::string(2500, 'q') + std::string(500, 'r'));
    }
}

static void TestRingAliasAppend() {
    // 无增长：完整 view 自追加。
    EventRingBuffer small(64);
    CHECK(!small.Append("abcdef"));
    CHECK(!small.Append(small.Peek()));
    CheckRingMatchesModel(small, "abcdefabcdef");

    // 真实增长：原1024存储被替换，输入必须先拥有独立副本。
    EventRingBuffer growing(4096);
    const std::string payload(700, 'x');
    CHECK(!growing.Append(payload));
    CHECK(growing.Capacity() == 1024);
    CHECK(!growing.Append(growing.Peek()));
    CHECK(growing.Capacity() == 2048);
    CheckRingMatchesModel(growing, payload + payload);

    // 绕环后首段及子串，两次追加均保持逻辑顺序。
    EventRingBuffer wrapped(32);
    CHECK(!wrapped.Append("abcdefghijklmnopqrstuvwx"));
    CHECK(!wrapped.Discard(20));
    CHECK(!wrapped.Append("0123456789"));
    CHECK(wrapped.Peek() == "uvwx01234567");
    CHECK(!wrapped.Append(wrapped.Peek().substr(2, 4)));
    CheckRingMatchesModel(wrapped, "uvwx0123456789wx01");
    CHECK(!wrapped.Append(wrapped.Peek()));
    const std::string expected = "uvwx0123456789wx01uvwx01234567";
    CheckRingMatchesModel(wrapped, expected);
    const auto capacity = wrapped.Capacity();
    CHECK(IsNoBufferSpace(wrapped.Append(wrapped.Peek())));
    CHECK(wrapped.Capacity() == capacity);
    CheckRingMatchesModel(wrapped, expected);
}

static void TestRingModelFuzz() {
    // 固定 seed 的 std::string 参考模型测试：最多几千步、小容量，
    // 覆盖绕环/边界/增长/拒绝不变/partial consume，不做长随机压测。
    EventRingBuffer ring(64);
    std::string model;
    std::mt19937 rng(20261015u);
    const int kSteps = 2000;
    for (int step = 0; step < kSteps; ++step) {
        const int op = static_cast<int>(rng() % 100);
        if (op < 50) {
            const std::size_t len = rng() % 40;
            std::string data(len, 0);
            for (char& c : data) { c = static_cast<char>('a' + (rng() % 26)); }
            const bool overflow = model.size() + len > ring.MaxBytes();
            const std::error_code ec = ring.Append(data);
            if (overflow) {
                CHECK(IsNoBufferSpace(ec));
            } else {
                CHECK(!ec);
                model += data;
            }
        } else if (op < 75) {
            const std::size_t n = rng() % 16;
            const bool illegal = n > model.size();
            const std::error_code ec = ring.Discard(n);
            if (illegal) {
                CHECK(IsOutOfRange(ec));
            } else {
                CHECK(!ec);
                model.erase(0, n);
            }
        } else {
            const std::size_t n = rng() % 16;
            const bool illegal = n > model.size();
            std::string out;
            const std::error_code ec = ring.Consume(n, out);
            if (illegal) {
                CHECK(IsOutOfRange(ec));
                CHECK(out.empty());
            } else {
                CHECK(!ec);
                CHECK(out == model.substr(0, n));
                model.erase(0, n);
            }
        }
        CHECK(ring.Size() == model.size());
        CHECK(ring.Size() <= ring.MaxBytes());
        // Peek 首段必须是模型前缀。
        const std::string_view peek = ring.Peek();
        CHECK(model.compare(0, peek.size(), peek) == 0);
        if (step % 100 == 0) {
            CheckRingMatchesModel(ring, model);
        }
    }
    CheckRingMatchesModel(ring, model);
    // 收尾整体消费一次。
    std::string drained;
    CHECK(!ring.Consume(model.size(), drained));
    CHECK(drained == model);
    CHECK(ring.Empty());
}

static void TestSnapshotLifetime() {
    EventSnapshot a;
    {
        EventRingBuffer ring(64);
        CHECK(!ring.Append("abcdef"));
        a = ring.TakeSnapshot();
        CHECK(a.Peek() == "abcdef");

        // owner 继续多次更改：append、discard、clear、重新追加。
        CHECK(!ring.Append("ghij"));
        CHECK(!ring.Discard(3));
        ring.Clear();
        CHECK(!ring.Append(std::string(64, 'x')));
    }   // 原缓冲销毁。

    // A 内容不变，独立延寿。
    CHECK(a.Remaining() == 6);
    CHECK(a.Peek() == "abcdef");

    // 快照自身 cursor 语义 + 复制延寿。
    CHECK(!a.Discard(2));
    CHECK(a.Peek() == "cdef");
    EventSnapshot b = a;
    CHECK(!b.Discard(1));
    CHECK(b.Peek() == "def");
    CHECK(a.Peek() == "cdef");          // 拷贝互不影响

    EventSnapshot c = std::move(b);
    CHECK(c.Peek() == "def");
    std::string out;
    CHECK(!c.Consume(2, out));
    CHECK(out == "de");
    CHECK(c.Peek() == "f");
    CHECK(IsOutOfRange(c.Discard(2)));
    CHECK(c.Peek() == "f");

    // 空快照合法。
    EventRingBuffer empty_ring(8);
    EventSnapshot empty = empty_ring.TakeSnapshot();
    CHECK(empty.Empty());
    CHECK(empty.Peek().empty());
    CHECK(!empty.Discard(0));

    // fix1 回归：移动构造后来源保持合法空快照（cursor 归零不下溢），
    // 可继续空操作与再赋值；移动赋值替换非空目标；自移动安全。
    CHECK(b.Empty());                               // 上文 b 已被移动
    CHECK(b.Peek().empty());
    CHECK(IsOutOfRange(b.Discard(1)));
    CHECK(!b.Discard(0));
    {
        EventRingBuffer ring2(32);
        CHECK(!ring2.Append("12345"));
        EventSnapshot src = ring2.TakeSnapshot();
        CHECK(!src.Discard(2));                     // 剩 "345"
        EventSnapshot dst = a;                      // 拷贝 "cdef"（cursor=2）
        dst = std::move(src);
        CHECK(dst.Peek() == "345");
        CHECK(src.Empty());                         // 来源合法空
        CHECK(src.Peek().empty());
        src = a;                                    // 拷贝赋值复活
        CHECK(src.Peek() == "cdef");
        EventSnapshot& dst_ref = dst;               // 经引用自移动
        dst = std::move(dst_ref);
        CHECK(dst.Peek() == "345");
    }
}

static void TestOutboundQueueBasics() {
    OutboundQueue q(16, 64, 4);
    CHECK(q.RingCapacity() == 16);
    CHECK(q.MaxTotalBytes() == 64);
    CHECK(q.MaxSegments() == 4);
    CHECK(q.Empty());

    // 空写合法且不产生段。
    CHECK(!q.Append(""));
    CHECK(q.SegmentCount() == 0);

    // 静态 ring 填满 → 溢出到 list 段。
    CHECK(!q.Append("0123456789abcdef"));
    CHECK(q.TotalBytes() == 16);
    CHECK(q.SegmentCount() == 1);
    CHECK(!q.Append("ABCDEFGHIJKLMNOP"));
    CHECK(q.TotalBytes() == 32);
    CHECK(q.SegmentCount() == 2);

    std::string_view views[8] = {};
    const std::size_t count = q.GatherViews(views, 8);
    CHECK(count == 2);
    CHECK(views[0] == "0123456789abcdef");
    CHECK(views[1] == "ABCDEFGHIJKLMNOP");

    // 跨 ring/list 部分消费已发送前缀。
    CHECK(!q.ConsumeSent(20));
    CHECK(q.TotalBytes() == 12);
    CHECK(q.SegmentCount() == 1);
    const std::size_t count2 = q.GatherViews(views, 8);
    CHECK(count2 == 1);
    CHECK(views[0] == "EFGHIJKLMNOP");

    // 0 字节进展合法；超量拒绝且不变。
    CHECK(!q.ConsumeSent(0));
    CHECK(IsOutOfRange(q.ConsumeSent(13)));
    CHECK(q.TotalBytes() == 12);

    // FIFO：spill 非空时新数据排在队尾（并入尾段），不回 ring 插队。
    CHECK(!q.Append("xy"));
    CHECK(q.TotalBytes() == 14);
    CHECK(q.SegmentCount() == 1);                   // 12+2 ≤ 4096 → 并入尾段
    const std::size_t count3 = q.GatherViews(views, 8);
    CHECK(count3 == 1);
    CHECK(views[0] == "EFGHIJKLMNOPxy");
    CHECK(!q.ConsumeSent(14));
    CHECK(q.Empty());
    CHECK(q.SegmentCount() == 0);

    // 清空后 ring 可复用（head 回卷正确）。
    CHECK(!q.Append("reusable"));
    CHECK(q.GatherViews(views, 8) == 1);
    CHECK(views[0] == "reusable");
}

// ---------------------------------------------------------------------------
// fix1 回归：FIFO 保序 / 几何契约 / move 不变量 / 参考模型
// ---------------------------------------------------------------------------

static std::string QueueBytes(const OutboundQueue& q) {
    std::string_view views[16] = {};
    const std::size_t n = q.GatherViews(views, 16);
    std::string out;
    for (std::size_t i = 0; i < n; ++i) {
        out.append(views[i]);
    }
    return out;
}

static void TestOutboundQueueFifoOrdering() {
    // P1 回归：spill 非空时新数据必须排在逻辑队尾，不得复用 ring 已释放
    // 空位插队（缺陷形态：cdijEFGH）。
    {
        OutboundQueue q(4, 32, 4);
        CHECK(!q.Append("abcdEFGH"));
        CHECK(!q.ConsumeSent(2));
        CHECK(!q.Append("ij"));
        CHECK(QueueBytes(q) == "cdEFGHij");
        CHECK(q.TotalBytes() == 8);
        CHECK(!q.ConsumeSent(8));
        CHECK(q.Empty());
    }
    // ring 已全部消费、spill 仍有数据：新数据也必须排在 spill 尾。
    {
        OutboundQueue q(4, 32, 4);
        CHECK(!q.Append("abcdEFGH"));
        CHECK(!q.ConsumeSent(4));               // ring 空，spill "EFGH"
        CHECK(!q.Append("ij"));
        CHECK(QueueBytes(q) == "EFGHij");
        CHECK(q.SegmentCount() == 1);           // 合并进尾段，不回 ring
        CHECK(!q.ConsumeSent(6));
        CHECK(q.Empty());
    }
    // 混合顺序：ring 满→spill→部分消费→多次追加，全程保序。
    {
        OutboundQueue q(4, 64, 4);
        CHECK(!q.Append("abcd"));
        CHECK(!q.Append("EFGH"));               // 溢出段
        CHECK(!q.ConsumeSent(2));               // "cdEFGH"
        CHECK(!q.Append("ij"));
        CHECK(!q.Append("kl"));                 // 均并入溢出尾段
        CHECK(QueueBytes(q) == "cdEFGHijkl");
        CHECK(q.SegmentCount() == 2);           // ring 段 + 1 溢出段
        CHECK(!q.ConsumeSent(4));               // 吃完 ring 段 + 溢出段前 2 字节
        CHECK(q.SegmentCount() == 1);
        CHECK(!q.Append("mn"));                 // spill 非空：仍入队尾
        CHECK(QueueBytes(q) == "GHijklmn");
        CHECK(!q.ConsumeSent(0));               // 0 进展不影响顺序
        CHECK(QueueBytes(q) == "GHijklmn");
    }
    // 总量满拒绝发生在修改前，顺序与内容不变。
    {
        OutboundQueue q(4, 10, 4);
        CHECK(!q.Append("abcdEFGH"));
        CHECK(!q.Append("ij"));
        CHECK(QueueBytes(q) == "abcdEFGHij");
        CHECK(IsNoBufferSpace(q.Append("k")));
        CHECK(QueueBytes(q) == "abcdEFGHij");
    }
    // 段数界拒绝发生在修改前（总量未满），拒绝后不变。
    {
        OutboundQueue q(4, 3 * 4096, 2);
        CHECK(!q.Append("abcd"));
        CHECK(!q.Append(std::string(4096, 'E')));   // 新溢出段 1（超合并上限）
        CHECK(!q.Append(std::string(4096, 'X')));   // 新溢出段 2
        CHECK(q.SegmentCount() == 3);
        const std::string before = QueueBytes(q);
        CHECK(IsNoBufferSpace(q.Append("Z")));      // 溢出段满：拒绝
        CHECK(QueueBytes(q) == before);
        CHECK(q.SegmentCount() == 3);
        CHECK(q.TotalBytes() == 4 + 4096 + 4096);
    }
}

static void TestOutboundQueueGeometryContract() {
    // 几何契约：任一为 0 或 ring>total 时构造不失败，但非空追加被保守
    // 拒绝，且不为无效几何分配不受 total 约束的 ring。
    {
        OutboundQueue zero_ring(0, 8, 2);
        CHECK(zero_ring.RingCapacity() == 0);
        CHECK(IsNoBufferSpace(zero_ring.Append("a")));
        CHECK(zero_ring.Empty());
        CHECK(zero_ring.SegmentCount() == 0);
    }
    {
        OutboundQueue zero_total(4, 0, 2);
        CHECK(IsNoBufferSpace(zero_total.Append("a")));
        CHECK(zero_total.Empty());
    }
    {
        OutboundQueue zero_segments(4, 8, 0);
        CHECK(IsNoBufferSpace(zero_segments.Append("a")));
        CHECK(zero_segments.Empty());
    }
    {
        OutboundQueue over(16, 8, 2);               // ring > total
        CHECK(over.RingCapacity() == 0);            // 未分配不受约束的 ring
        CHECK(IsNoBufferSpace(over.Append("a")));
    }
    {
        OutboundQueue ok(4, 8, 2);                  // 合法几何不回退
        CHECK(ok.RingCapacity() == 4);
        CHECK(!ok.Append("abcd"));
        CHECK(!ok.Append("ef"));
        CHECK(QueueBytes(ok) == "abcdef");
        CHECK(ok.SegmentCount() == 2);
    }
}

static void TestOutboundQueueMove() {
    static_assert(!std::is_copy_constructible_v<OutboundQueue>);
    static_assert(!std::is_copy_assignable_v<OutboundQueue>);

    OutboundQueue source(8, 64, 4);
    CHECK(!source.Append("12345678"));
    CHECK(!source.Append("abcd"));                  // 溢出段
    const std::string expect = "12345678abcd";

    OutboundQueue moved = std::move(source);        // 移动构造
    CHECK(QueueBytes(moved) == expect);
    CHECK(moved.TotalBytes() == 12);
    CHECK(moved.RingCapacity() == 8);
    CHECK(source.Empty());                          // 来源合法空
    CHECK(source.TotalBytes() == 0);
    CHECK(source.SegmentCount() == 0);
    CHECK(!source.ConsumeSent(0));
    CHECK(!source.Append("zz"));                    // 来源可继续复用
    CHECK(QueueBytes(source) == "zz");

    OutboundQueue target(4, 64, 4);                 // 移动赋值到非空目标
    CHECK(!target.Append("old-payload"));
    target = std::move(moved);
    CHECK(QueueBytes(target) == expect);
    CHECK(moved.Empty());
    CHECK(!moved.Append("yy"));
    CHECK(QueueBytes(moved) == "yy");

    OutboundQueue& target_qref = target;            // 经引用自移动
    target = std::move(target_qref);
    CHECK(QueueBytes(target) == expect);
    CHECK(!target.Append("tail"));
    CHECK(QueueBytes(target) == expect + "tail");
}

static void TestRingMove() {
    static_assert(!std::is_copy_constructible_v<EventRingBuffer>);
    static_assert(!std::is_copy_assignable_v<EventRingBuffer>);

    EventRingBuffer source(64);
    CHECK(!source.Append("abcdefgh"));
    CHECK(!source.Discard(3));                      // head 前移后的状态
    const std::string expect = "defgh";

    EventRingBuffer moved = std::move(source);
    CHECK(moved.Size() == 5);
    CHECK(moved.Peek() == "defgh");
    CHECK(moved.MaxBytes() == 64);
    CHECK(source.Size() == 0);                      // 来源合法空缓冲
    CHECK(source.Capacity() == 0);
    CHECK(source.MaxBytes() == 64);                 // 几何保留，可复用
    CHECK(!source.Append("xy"));
    CHECK(source.Peek() == "xy");

    EventRingBuffer target(32);                     // 移动赋值到非空目标
    CHECK(!target.Append("stale-data"));
    target = std::move(moved);
    CHECK(target.Size() == 5);
    CheckRingMatchesModel(target, expect);
    CHECK(moved.Size() == 0);
    CHECK(moved.Capacity() == 0);
    CHECK(moved.MaxBytes() == 64);      // 来源保留自身 max_bytes（合法可复用）

    EventRingBuffer& target_ref = target;           // 经引用自移动（绕开
    target = std::move(target_ref);                 // -Wself-move 的直接形式）
    CheckRingMatchesModel(target, expect);
    CHECK(!target.Append("ij"));
    CheckRingMatchesModel(target, expect + "ij");
}

static void TestOutboundQueueModelFuzz() {
    // 固定 seed 的 std::string 参考模型：Append/ConsumeSent 交替、旧
    // spill 保留时再追加、单字节合并，每步全量比对 GatherViews 串接
    // （不只字节数）。段限额边界由 TestOutboundQueueFifoOrdering 覆盖。
    OutboundQueue q(8, 64, 4);
    std::string model;
    std::mt19937 rng(20261016u);
    for (int step = 0; step < 3000; ++step) {
        const int op = static_cast<int>(rng() % 100);
        if (op < 55) {
            const std::size_t len = rng() % 18;
            std::string data(len, 0);
            for (char& c : data) { c = static_cast<char>('A' + (rng() % 26)); }
            const bool overflow = model.size() + len > q.MaxTotalBytes();
            const std::error_code ec = q.Append(data);
            if (overflow) {
                CHECK(IsNoBufferSpace(ec));
            } else {
                CHECK(!ec);
                model += data;
            }
        } else {
            const std::size_t n = rng() % 24;
            const bool illegal = n > model.size();
            const std::error_code ec = q.ConsumeSent(n);
            if (illegal) {
                CHECK(IsOutOfRange(ec));
            } else {
                CHECK(!ec);
                model.erase(0, n);
            }
        }
        CHECK(q.TotalBytes() == model.size());
        CHECK(q.SegmentCount() <= 1 + q.MaxSegments());
        CHECK(QueueBytes(q) == model);
    }
    CHECK(QueueBytes(q) == model);
    CHECK(!q.ConsumeSent(model.size()));            // 收尾清空后可复用
    CHECK(q.Empty());
    CHECK(q.SegmentCount() == 0);
    CHECK(!q.Append("final"));
    CHECK(QueueBytes(q) == "final");
}

static void TestOutboundQueueWrapAndBounds() {
    // ring 绕环产生两个连续段。
    OutboundQueue q(16, 64, 4);
    CHECK(!q.Append("abcdefghij"));     // 10
    CHECK(!q.ConsumeSent(6));           // head=6
    CHECK(!q.Append("klmnopqrst"));     // 写 pos=10：尾 6 + 绕头 4
    CHECK(q.TotalBytes() == 14);
    std::string_view views[8] = {};
    const std::size_t count = q.GatherViews(views, 8);
    CHECK(count == 2);
    CHECK(views[0] == "ghijklmnop");    // 首段连续区
    CHECK(views[1] == "qrst");          // 绕环段
    // 跨两段部分消费。
    CHECK(!q.ConsumeSent(12));
    CHECK(q.TotalBytes() == 2);
    CHECK(q.GatherViews(views, 8) == 1);
    CHECK(views[0] == "st");

    // 总量界：拒绝在修改前发生。
    OutboundQueue bounded(8, 16, 4);
    CHECK(!bounded.Append("12345678"));
    CHECK(!bounded.Append("AAAAAAAA"));                     // ring 满 → 进 list 段
    CHECK(bounded.TotalBytes() == 16);
    CHECK(IsNoBufferSpace(bounded.Append("C")));            // 总量满
    CHECK(bounded.TotalBytes() == 16);
    CHECK(bounded.SegmentCount() == 2);

    // 段数界：溢出 > 合并上限（4096）时才成新段；达到上限在修改前拒绝。
    OutboundQueue segs(4, 32, 2);
    CHECK(!segs.Append("aaaa"));
    const std::string big(5000, 'B');       // 需要跨 2 段
    CHECK(IsNoBufferSpace(segs.Append(big)));               // 超 32B 总量先拒
    (void)big;
    OutboundQueue segs2(4, 32768, 2);
    CHECK(!segs2.Append("aaaa"));
    CHECK(!segs2.Append(std::string(5000, 'B')));           // 新段 1
    CHECK(segs2.SegmentCount() == 2);
    CHECK(!segs2.Append(std::string(5000, 'C')));           // 新段 2
    CHECK(segs2.SegmentCount() == 3);
    CHECK(IsNoBufferSpace(segs2.Append(std::string(5000, 'D')))); // 段满
    CHECK(segs2.SegmentCount() == 3);
    CHECK(segs2.TotalBytes() == 4 + 5000 + 5000);
    // 拒绝后已有数据不变。
    std::string_view sv[8] = {};
    CHECK(segs2.GatherViews(sv, 8) == 3);
    CHECK(sv[0] == "aaaa");
    CHECK(sv[1].size() == 5000);
    CHECK(sv[2].size() == 5000);

    // 小溢出合并尾段：段数不增长。
    OutboundQueue merged(4, 64, 4);
    CHECK(!merged.Append("aaaa"));
    CHECK(!merged.Append("bb"));
    CHECK(merged.SegmentCount() == 2);
    CHECK(!merged.Append("cc"));
    CHECK(merged.SegmentCount() == 2);      // 并入尾段
    std::string_view mv[8] = {};
    CHECK(merged.GatherViews(mv, 8) == 2);
    CHECK(mv[1] == "bbcc");
    CHECK(!merged.ConsumeSent(6));          // 段内部分消费；旧 GatherViews 视图自此失效（契约边界，不读取旧内存）
    CHECK(merged.GatherViews(mv, 8) == 1);
    CHECK(mv[0] == "cc");
}

static void TestBuffer() {
    TestRingBasics();
    TestRingWrapAndGrowth();
    TestRingAliasAppend();
    TestRingModelFuzz();
    TestRingMove();
    TestSnapshotLifetime();
    TestOutboundQueueBasics();
    TestOutboundQueueFifoOrdering();
    TestOutboundQueueGeometryContract();
    TestOutboundQueueWrapAndBounds();
    TestOutboundQueueMove();
    TestOutboundQueueModelFuzz();
}

// ---------------------------------------------------------------------------
// options：内部归一化 NormalizeOptions（detail 头；不是公共配置 API）
// ---------------------------------------------------------------------------

static NormalizedOptions NormalizeOrThrow(const Options& in) {
    NormalizedOptions out;
    const std::error_code ec = NormalizeOptions(in, out);
    if (ec) {
        throw std::runtime_error("NormalizeOptions failed: " + ec.message());
    }
    return out;
}

static void TestOptionsDefaultsAndLoops() {
    // 默认：loop=1（无 CPU 数推导）、socket 选项默认值、每连接总量界默认 1MiB。
    const NormalizedOptions out = NormalizeOrThrow(Options{});
    CHECK(out.lb == LoadBalancing::RoundRobin);
    CHECK(out.num_event_loop == 1);
    CHECK(!out.reuse_addr);
    CHECK(!out.reuse_port);
    CHECK(out.tcp_no_delay);
    CHECK(out.socket_recv_buffer == 0);         // 0 = 不设置
    CHECK(out.socket_send_buffer == 0);
    CHECK(out.max_inbound_bytes == 1024 * 1024);
    CHECK(out.max_outbound_bytes == 1024 * 1024);
    CHECK(out.max_pending_events == 1024);
    CHECK(out.tcp_keep_alive == std::chrono::seconds{0});

    // 显式 sub 数原样保留（server 另占 main 槽由 Engine 决定，不在此推导）。
    const Options explicit_loops = [] {
        Options o;
        o.num_event_loop = 2;
        return o;
    }();
    CHECK(NormalizeOrThrow(explicit_loops).num_event_loop == 2);

    // 负值拒绝，且失败不改 out（先全量验证再一次性提交）。
    const Options negative = [] {
        Options o;
        o.num_event_loop = -1;
        return o;
    }();
    NormalizedOptions sentinel;
    sentinel.num_event_loop = 777;
    sentinel.max_pending_events = 4321;
    CHECK(IsInvalidArgument(NormalizeOptions(negative, sentinel)));
    CHECK(sentinel.num_event_loop == 777);
    CHECK(sentinel.max_pending_events == 4321);
}

static void TestOptionsResourceBounds() {
    // 每连接总量界必须正值；不再有段容量字段与之交叉校验。
    const auto bounds = [](std::int64_t inbound, std::int64_t outbound, std::int64_t events) {
        Options o;
        o.max_inbound_bytes = inbound;
        o.max_outbound_bytes = outbound;
        o.max_pending_events = events;
        NormalizedOptions out;
        return NormalizeOptions(o, out);
    };
    CHECK(IsInvalidArgument(bounds(0, 1024, 1024)));
    CHECK(IsInvalidArgument(bounds(1024, -5, 1024)));
    CHECK(IsInvalidArgument(bounds(1024, 1024, 0)));
    CHECK(IsInvalidArgument(bounds(-1, 1024, 1024)));

    // 小总量界必须可用（旧版会被默认 64KiB 段容量 cap 交叉校验拒绝）。
    const auto small = [](std::int64_t value) {
        Options o;
        o.max_inbound_bytes = value;
        o.max_outbound_bytes = value;
        o.max_pending_events = 1;
        return NormalizeOrThrow(o);
    };
    CHECK(small(1).max_inbound_bytes == 1);
    CHECK(small(1).max_outbound_bytes == 1);
    CHECK(small(1).max_pending_events == 1);
    CHECK(small(4096).max_inbound_bytes == 4096);
    CHECK(small(4096).max_outbound_bytes == 4096);

    // 合法枚举原样保留；非法枚举拒绝。
    const auto lb = [](LoadBalancing mode) {
        Options o;
        o.lb = mode;
        NormalizedOptions out;
        return NormalizeOptions(o, out);
    };
    CHECK(!lb(LoadBalancing::RoundRobin));
    CHECK(!lb(LoadBalancing::LeastConnections));
    CHECK(!lb(LoadBalancing::SourceAddrHash));
    CHECK(IsInvalidArgument(lb(static_cast<LoadBalancing>(42))));
    CHECK(IsInvalidArgument(lb(static_cast<LoadBalancing>(-1))));

    // socket 选项直接映射；0 不设置，负值/超原生 int 拒绝。
    const auto sock = [](std::int64_t recv, std::int64_t send) {
        Options o;
        o.socket_recv_buffer = recv;
        o.socket_send_buffer = send;
        return NormalizeOrThrow(o);
    };
    CHECK(sock(0, 0).socket_recv_buffer == 0);
    CHECK(sock(65536, 131072).socket_recv_buffer == 65536);
    CHECK(sock(65536, 131072).socket_send_buffer == 131072);
    CHECK(sock(std::numeric_limits<std::int32_t>::max(), 0).socket_recv_buffer
        == std::numeric_limits<std::int32_t>::max());
    const auto sock_err = [](std::int64_t recv, std::int64_t send) {
        Options o;
        o.socket_recv_buffer = recv;
        o.socket_send_buffer = send;
        NormalizedOptions out;
        return NormalizeOptions(o, out);
    };
    CHECK(IsOutOfRange(sock_err(-1, 0)));
    CHECK(IsOutOfRange(sock_err(0, -1)));
    CHECK(IsOutOfRange(sock_err(std::int64_t{std::numeric_limits<std::int32_t>::max()} + 1, 0)));
    CHECK(IsOutOfRange(sock_err(0, std::int64_t{std::numeric_limits<std::int32_t>::max()} + 1)));

    // reuse flags 与 nodelay 原样映射（接线点同名字段）。
    const Options flags = [] {
        Options o;
        o.reuse_addr = true;
        o.reuse_port = true;
        o.tcp_no_delay = false;
        return o;
    }();
    const auto mapped = NormalizeOrThrow(flags);
    CHECK(mapped.reuse_addr && mapped.reuse_port && !mapped.tcp_no_delay);
}

static void TestOptionsKeepalive() {
    // 默认 interval=idle/5、count=5；除得 0 时最小 1 秒（C++ 差异）。
    const auto keep = [](int idle_s, int interval_s, std::int64_t count) {
        Options o;
        o.tcp_keep_alive = std::chrono::seconds{idle_s};
        o.tcp_keep_interval = std::chrono::seconds{interval_s};
        o.tcp_keep_count = count;
        return NormalizeOrThrow(o);
    };
    const auto k11 = keep(11, 0, 0);
    CHECK(k11.tcp_keep_alive == std::chrono::seconds{11});
    CHECK(k11.tcp_keep_interval == std::chrono::seconds{2});    // 11/5
    CHECK(k11.tcp_keep_count == 5);
    const auto k1 = keep(1, 0, 0);
    CHECK(k1.tcp_keep_interval == std::chrono::seconds{1});     // 1/5=0 → 1
    const auto k4 = keep(4, 0, 0);
    CHECK(k4.tcp_keep_interval == std::chrono::seconds{1});     // 4/5=0 → 1
    const auto k_explicit = keep(11, 7, 3);
    CHECK(k_explicit.tcp_keep_interval == std::chrono::seconds{7});
    CHECK(k_explicit.tcp_keep_count == 3);

    // 禁用时不静默修正：interval/count 保留原值。
    const auto disabled = keep(0, 9, 4);
    CHECK(disabled.tcp_keep_alive == std::chrono::seconds{0});
    CHECK(disabled.tcp_keep_interval == std::chrono::seconds{9});
    CHECK(disabled.tcp_keep_count == 4);
    const auto disabled_default = keep(0, 0, 0);
    CHECK(disabled_default.tcp_keep_interval == std::chrono::seconds{0});
    CHECK(disabled_default.tcp_keep_count == 0);

    const auto keep_err = [](int idle_s, int interval_s, std::int64_t count) {
        Options o;
        o.tcp_keep_alive = std::chrono::seconds{idle_s};
        o.tcp_keep_interval = std::chrono::seconds{interval_s};
        o.tcp_keep_count = count;
        NormalizedOptions out;
        return NormalizeOptions(o, out);
    };
    CHECK(IsInvalidArgument(keep_err(-1, 0, 0)));
    CHECK(IsInvalidArgument(keep_err(0, -1, 0)));
    CHECK(IsInvalidArgument(keep_err(0, 0, -1)));

    // 原生 int 秒上限本身合法（内核 int 秒），超界才拒绝。
    const auto k_intmax = keep(0x7fffffff, 0, 0);
    CHECK(k_intmax.tcp_keep_alive == std::chrono::seconds{0x7fffffff});
    CHECK(k_intmax.tcp_keep_interval == std::chrono::seconds{0x7fffffff / 5});
    {
        Options o;
        o.tcp_keep_alive = std::chrono::seconds{std::int64_t{1} << 40};
        NormalizedOptions out;
        CHECK(IsOutOfRange(NormalizeOptions(o, out)));
    }

    // keep count 正值不得超原生 int（TCP_KEEPCNT 截断防护）；启用与禁用都验证。
    {
        const std::int64_t kIntMax = std::numeric_limits<std::int32_t>::max();
        CHECK(keep(11, 0, kIntMax).tcp_keep_count == static_cast<std::size_t>(kIntMax));
        CHECK(keep(0, 9, kIntMax).tcp_keep_count == static_cast<std::size_t>(kIntMax));
        Options over;
        over.tcp_keep_count = kIntMax + 1;
        NormalizedOptions out;
        out.tcp_keep_count = 4321;
        CHECK(IsOutOfRange(NormalizeOptions(over, out)));
        CHECK(out.tcp_keep_count == 4321);
        Options over_disabled;
        over_disabled.tcp_keep_count = kIntMax + 1;
        NormalizedOptions out2;
        CHECK(IsOutOfRange(NormalizeOptions(over_disabled, out2)));
    }
}

static void TestOptions() {
    TestOptionsDefaultsAndLoops();
    TestOptionsResourceBounds();
    TestOptionsKeepalive();
}


// ---------------------------------------------------------------------------
// load_balancing：三算法纯选择
// ---------------------------------------------------------------------------

static void TestRoundRobin() {
    std::error_code ec;
    std::vector<std::uint64_t> counts{9, 9, 9};
    LoadBalancer rr(LoadBalancing::RoundRobin, 3, ec);
    CHECK(!ec);
    CHECK(rr.Size() == 3);
    // RR 忽略传入计数（不静默修正），序列 0,1,2 循环。
    for (int i = 0; i < 7; ++i) {
        const std::size_t picked = rr.Next(Address{}, counts, ec);
        CHECK(!ec);
        CHECK(picked == static_cast<std::size_t>(i % 3));
    }

    // 空集合拒绝，Next 报错不崩溃。
    std::error_code ec2;
    LoadBalancer empty(LoadBalancing::RoundRobin, 0, ec2);
    CHECK(IsInvalidArgument(ec2));
    CHECK(empty.Size() == 0);
    std::error_code ec3;
    (void)empty.Next(Address{}, {}, ec3);
    CHECK(IsInvalidArgument(ec3));

    // 非法枚举值拒绝。
    std::error_code ec4;
    LoadBalancer bad(static_cast<LoadBalancing>(42), 3, ec4);
    CHECK(IsInvalidArgument(ec4));
    CHECK(bad.Size() == 0);
}

static void TestLeastConnections() {
    std::error_code ec;
    LoadBalancer lc(LoadBalancing::LeastConnections, 3, ec);
    CHECK(!ec);

    std::vector<std::uint64_t> counts{5, 2, 9};
    CHECK(lc.Next(Address{}, counts, ec) == 1);
    counts = {2, 2, 9};
    CHECK(lc.Next(Address{}, counts, ec) == 0);     // 同值优先第一个
    counts = {9, 9, 1};
    CHECK(lc.Next(Address{}, counts, ec) == 2);
    counts = {1, 0, 0};
    CHECK(lc.Next(Address{}, counts, ec) == 1);

    // 计数集合与 loop 集合不一致必须校验；错误不改变状态。
    counts = {0, 1};
    const std::error_code bad = []() { return std::error_code{}; }();
    (void)bad;
    std::error_code ec2;
    (void)lc.Next(Address{}, counts, ec2);
    CHECK(IsInvalidArgument(ec2));
    counts = {0, 1, 0};
    CHECK(lc.Next(Address{}, counts, ec2) == 0);    // 同值 0/0 优先第一个
}

static void TestLoadBalancerErrorReuse() {
    // fix1 回归：同一 error 变量跨构造/多次 Next 复用时，所有成功路径
    // 必须清空错误码，失败路径保留明确错误（父级 probe stale error）。
    std::error_code ec = std::make_error_code(std::errc::io_error);
    {
        LoadBalancer rr(LoadBalancing::RoundRobin, 2, ec);
        CHECK(!ec);                                 // 构造成功清掉预置 io_error
        const std::vector<std::uint64_t> counts{1, 2};
        CHECK(rr.Next(Address{}, counts, ec) == 0);
        CHECK(!ec);                                 // 成功 Next 同样清空
        CHECK(rr.Next(Address{}, counts, ec) == 1);
        CHECK(!ec);
    }
    ec = std::make_error_code(std::errc::io_error);
    {
        LoadBalancer lc(LoadBalancing::LeastConnections, 2, ec);
        CHECK(!ec);
        CHECK(lc.Next(Address{}, {1}, ec) == 2);    // 长度不符：明确错误
        CHECK(IsInvalidArgument(ec));
        CHECK(lc.Next(Address{}, {2, 1}, ec) == 1); // 合法 Next 清空
        CHECK(!ec);
        CHECK(lc.Next(Address{}, {5, 5}, ec) == 0);
        CHECK(!ec);
    }
    // 失败构造后对象不可用，Next 一律报错。
    std::error_code ec2 = std::make_error_code(std::errc::io_error);
    LoadBalancer bad(LoadBalancing::RoundRobin, 0, ec2);
    CHECK(IsInvalidArgument(ec2));
    CHECK(bad.Size() == 0);
    CHECK(bad.Next(Address{}, {}, ec2) == 0);
    CHECK(IsInvalidArgument(ec2));
}

static void TestSourceAddrHash() {
    // 内部 hash 原语直接对照父级 zlib 固定向量。
    CHECK(Crc32Checksum("") == kCrcEmpty);
    CHECK(Crc32Checksum("123456789") == kCrcDigits);
    CHECK(Crc32Checksum("127.0.0.1:12345") == kCrcIpv4);
    CHECK(Crc32Checksum("[::1]:80") == kCrcIpv6);
    CHECK(Crc32Checksum("@example") == kCrcUnixAbstract);

    // 地址构造 + ToString 形态与向量一致（含端口/IPv6/Unix 抽象形态）。
    std::error_code ec;
    const Address v4 = Address::Ip("127.0.0.1", 12345, ec);
    CHECK(!ec);
    CHECK(v4.ToString() == "127.0.0.1:12345");
    const Address v6 = Address::Ip("::1", 80, ec);
    CHECK(!ec);
    CHECK(v6.ToString() == "[::1]:80");
    const Address unix_abstract = Address::Unix("example", ec, true);
    CHECK(!ec);
    CHECK(unix_abstract.ToString() == "@example");

    // 只对外暴露选择结果：多种 loop 数下对照固定向量取模。
    const std::size_t loop_counts[] = {1, 2, 3, 5, 8};
    for (const std::size_t n : loop_counts) {
        std::error_code lb_ec;
        LoadBalancer lb(LoadBalancing::SourceAddrHash, n, lb_ec);
        CHECK(!lb_ec);
        CHECK(lb.Next(v4, {}, lb_ec) == static_cast<std::size_t>(kCrcIpv4) % n);
        CHECK(!lb_ec);
        CHECK(lb.Next(v6, {}, lb_ec) == static_cast<std::size_t>(kCrcIpv6) % n);
        CHECK(lb.Next(unix_abstract, {}, lb_ec) == static_cast<std::size_t>(kCrcUnixAbstract) % n);
        // 无效地址 ToString 为空串：CRC32("")=0 → 索引 0。
        CHECK(lb.Next(Address{}, {}, lb_ec) == 0);
    }
}

static void TestLoadBalancing() {
    TestRoundRobin();
    TestLeastConnections();
    TestSourceAddrHash();
    TestLoadBalancerErrorReuse();
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: Test_net_primitives <buffer|options|load_balancing>\n";
        return 2;
    }
    const std::string name = argv[1];
    try {
        if (name == "buffer") {
            TestBuffer();
        } else if (name == "options") {
            TestOptions();
        } else if (name == "load_balancing") {
            TestLoadBalancing();
        } else {
            std::cerr << "unknown case: " << name << "\n";
            return 2;
        }
    } catch (const std::exception& error) {
        std::cerr << name << " failed: " << error.what() << "\n";
        return 1;
    }
    std::cout << name << " ok\n";
    return 0;
}

// 独立纯逻辑分配失败回归：不链接 coroutine/runtime。
// 第 k 次失败只作用于被测窗口；显式 CHECK 在 NDEBUG 下仍生效。
#include <bbt/infra/net/detail/EventRingBuffer.hpp>
#include <bbt/infra/net/detail/OutboundQueue.hpp>
#include <cstdio>
#include <cstdlib>
#include <new>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

using namespace bbt::infra::net;

static bool allocation_window = false;
static std::size_t allocation_count = 0;
static std::size_t fail_at = 0;

void* operator new(std::size_t size) {
    if (allocation_window && ++allocation_count == fail_at) {
        throw std::bad_alloc();
    }
    if (void* p = std::malloc(size == 0 ? 1 : size)) {
        return p;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

class AllocationWindow {
public:
    explicit AllocationWindow(std::size_t k) {
        allocation_count = 0;
        fail_at = k; // 0 只计数，不注入
        allocation_window = true;
    }
    ~AllocationWindow() { allocation_window = false; }
    AllocationWindow(const AllocationWindow&) = delete;
    AllocationWindow& operator=(const AllocationWindow&) = delete;
};

static void Check(bool value, const char* expression, int line) {
    if (!value) {
        throw std::runtime_error("line " + std::to_string(line) + ": " + expression);
    }
}
#define CHECK(expression) Check(static_cast<bool>(expression), #expression, __LINE__)

static std::string Bytes(const OutboundQueue& q) {
    std::string_view views[16];
    const auto count = q.GatherViews(views, 16);
    std::string result;
    for (std::size_t i = 0; i < count; ++i) {
        result.append(views[i]);
    }
    return result;
}

static std::size_t MoveAttempt(std::size_t k, bool must_fail) {
    OutboundQueue source(8, 4096, 8);
    const std::string payload = "abcdefgh" + std::string(80, 'x');
    CHECK(!source.Append(payload));
    CHECK(!source.ConsumeSent(3));
    const std::string expected = payload.substr(3);
    const auto segments = source.SegmentCount();
    alignas(OutboundQueue) unsigned char memory[sizeof(OutboundQueue)];
    OutboundQueue* target = nullptr;
    bool failed = false;
    std::size_t attempts;
    {
        AllocationWindow window(k);
        try {
            target = new (memory) OutboundQueue(std::move(source));
        } catch (const std::bad_alloc&) {
            failed = true;
        }
        attempts = allocation_count;
    } // 检查和恢复操作不在注入窗口
    if (target != nullptr) {
        // placement 对象也由 RAII 清理，断言抛出时不泄漏。
        struct Destroy {
            OutboundQueue* q;
            ~Destroy() { q->~OutboundQueue(); }
        } destroy{target};
        CHECK(!must_fail && !failed);
        CHECK(Bytes(*target) == expected);
        CHECK(target->TotalBytes() == expected.size());
        CHECK(target->RingCapacity() == 8);
        CHECK(target->SegmentCount() == segments);
        CHECK(source.Empty());
        CHECK(source.RingCapacity() == 0);
        CHECK(source.SegmentCount() == 0);
        CHECK(!target->ConsumeSent(5));
        CHECK(!target->Append("tail"));
        CHECK(Bytes(*target) == expected.substr(5) + "tail");
    } else {
        CHECK(must_fail && failed);
        CHECK(Bytes(source) == expected);
        CHECK(source.TotalBytes() == expected.size());
        CHECK(source.RingCapacity() == 8);
        CHECK(source.SegmentCount() == segments);
    }
    CHECK(source.MaxTotalBytes() == 4096);
    CHECK(source.MaxSegments() == 8);
    const auto remaining = Bytes(source);
    CHECK(!source.ConsumeSent(remaining.size()));
    CHECK(source.Empty());
    CHECK(!source.Append("reused"));
    CHECK(Bytes(source) == "reused");
    return attempts;
}

static void TestMove() {
    static_assert(!std::is_nothrow_move_constructible_v<OutboundQueue>);
    static_assert(std::is_nothrow_move_assignable_v<OutboundQueue>);
    const auto allocations = MoveAttempt(0, false);
    CHECK(allocations > 0 && allocations <= 8);
    for (std::size_t k = 1; k <= allocations; ++k) {
        CHECK(MoveAttempt(k, true) == k);
    }
    CHECK(MoveAttempt(allocations + 1, false) == allocations);
    std::printf("move_ctor allocations=%zu; every failure source_preserved\n", allocations);

    OutboundQueue source(8, 4096, 8), target(4, 4096, 8);
    const std::string expected = "abcdefgh" + std::string(80, 'x');
    CHECK(!source.Append(expected));
    CHECK(!target.Append("old-target"));
    {
        AllocationWindow window(1);
        target = std::move(source);
    }
    CHECK(allocation_count == 0);
    CHECK(Bytes(target) == expected);
    CHECK(source.Empty());
    CHECK(!source.Append("reused"));
    CHECK(Bytes(source) == "reused");
    {
        AllocationWindow window(1);
        OutboundQueue& self = target;
        target = std::move(self);
    }
    CHECK(allocation_count == 0);
    CHECK(Bytes(target) == expected);
}

static std::size_t AppendAttempt(std::size_t k, bool merge, bool must_fail) {
    OutboundQueue queue(8, 4096, 8);
    const std::string before = merge ? "abcdefgh" + std::string(80, 'x') : "abcd";
    const std::string input(128, 'y'); // 超 SSO，未超总量
    CHECK(!queue.Append(before));
    const auto segments = queue.SegmentCount();
    std::error_code ec;
    bool failed = false;
    std::size_t attempts;
    {
        AllocationWindow window(k);
        try {
            ec = queue.Append(input);
        } catch (const std::bad_alloc&) {
            failed = true;
        }
        attempts = allocation_count;
    }
    CHECK(failed == must_fail);
    CHECK(!ec);
    const auto expected = must_fail ? before : before + input;
    CHECK(Bytes(queue) == expected);
    CHECK(queue.TotalBytes() == expected.size());
    if (must_fail) {
        CHECK(queue.SegmentCount() == segments);
    }
    // 超限拒绝不分配/不改数据。
    const std::string overflow(4096, 'z');
    {
        AllocationWindow window(1);
        ec = queue.Append(overflow);
    }
    CHECK(ec == std::errc::no_buffer_space);
    CHECK(allocation_count == 0);
    CHECK(Bytes(queue) == expected);
    CHECK(!queue.ConsumeSent(expected.size()));
    CHECK(!queue.Append("reused"));
    CHECK(Bytes(queue) == "reused");
    return attempts;
}

static void TestAppend() {
    for (const bool merge : {false, true}) {
        const auto allocations = AppendAttempt(0, merge, false);
        CHECK(allocations > 0 && allocations <= 8);
        for (std::size_t k = 1; k <= allocations; ++k) {
            CHECK(AppendAttempt(k, merge, true) == k);
        }
        CHECK(AppendAttempt(allocations + 1, merge, false) == allocations);
        std::printf("append merge=%d allocations=%zu; every failure unchanged\n", merge, allocations);
    }
}

static std::size_t AliasAttempt(std::size_t k, bool must_fail) {
    EventRingBuffer ring(4096);
    const std::string input(700, 'x');
    CHECK(!ring.Append(input));
    const auto view = ring.Peek();
    bool failed = false;
    std::error_code ec;
    std::size_t attempts;
    {
        AllocationWindow window(k);
        try {
            ec = ring.Append(view);
        } catch (const std::bad_alloc&) {
            failed = true;
        }
        attempts = allocation_count;
    }
    CHECK(failed == must_fail && !ec);
    CHECK(ring.Capacity() == (must_fail ? 1024 : 2048));
    CHECK(ring.TakeSnapshot().Peek() == (must_fail ? input : input + input));
    CHECK(ring.Size() == (must_fail ? input.size() : input.size() * 2));
    CHECK(!ring.Discard(ring.Size()));
    CHECK(!ring.Append("reused"));
    CHECK(ring.Peek() == "reused");
    return attempts;
}

static void TestAliasAllocation() {
    const auto allocations = AliasAttempt(0, false);
    CHECK(allocations > 0 && allocations <= 8);
    for (std::size_t k = 1; k <= allocations; ++k) {
        CHECK(AliasAttempt(k, true) == k);
    }
    CHECK(AliasAttempt(allocations + 1, false) == allocations);
    std::printf("alias allocations=%zu; every failure unchanged\n", allocations);
}

// Reserve 原语直接回归：有效预留/越界拒绝/扩容失败内容容量保持；已备
// 容量内追加不再首次分配。旧 move/append 回归保持不变。
static std::size_t ReserveAttempt(std::size_t k, bool must_fail) {
    EventRingBuffer ring(4096);
    const std::string seed = "seed-data";
    CHECK(!ring.Append(seed));
    const auto size_before = ring.Size();
    const auto cap_before = ring.Capacity();
    const auto snapshot_before = ring.TakeSnapshot();
    const auto content_before = snapshot_before.Peek();
    bool failed = false;
    std::size_t attempts;
    {
        AllocationWindow window(k);
        try {
            CHECK(!ring.Reserve(2048));
        } catch (const std::bad_alloc&) {
            failed = true;
        }
        attempts = allocation_count;
    }
    CHECK(failed == must_fail);
    CHECK(ring.Size() == size_before);
    CHECK(ring.Capacity() == (must_fail ? cap_before : 2048));
    CHECK(ring.TakeSnapshot().Peek() == content_before);
    if (!must_fail) {
        // 已备容量内追加：只做 char 提交，不再首次分配。
        const std::string filler(2048 - size_before, 'x');
        {
            AllocationWindow window(1);
            CHECK(!ring.Append(filler));
            CHECK(allocation_count == 0);
        }
        CHECK(ring.Size() == 2048);
        CHECK(ring.Capacity() == 2048);
    }
    // 边界：超过 MaxBytes 拒绝且不分配、内容不变。
    {
        AllocationWindow window(1);
        CHECK(ring.Reserve(4097) == std::make_error_code(std::errc::no_buffer_space));
        CHECK(allocation_count == 0);
    }
    CHECK(ring.Size() == (must_fail ? size_before : 2048));
    return attempts;
}

static void TestReserve() {
    const auto allocations = ReserveAttempt(0, false);
    CHECK(allocations > 0 && allocations <= 8);
    for (std::size_t k = 1; k <= allocations; ++k) {
        CHECK(ReserveAttempt(k, true) == k);
    }
    CHECK(ReserveAttempt(allocations + 1, false) == allocations);
    std::printf("reserve allocations=%zu; every failure unchanged\n", allocations);
}

int main() {
    try {
        TestMove();
        TestAppend();
        TestAliasAllocation();
        TestReserve();
    } catch (const std::exception& error) {
        std::fprintf(stderr, "allocation regression failed: %s\n", error.what());
        return 1;
    }
    std::puts("allocation regression ok");
    return 0;
}

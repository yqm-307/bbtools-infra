#define BOOST_TEST_MODULE net_buffer_stream
#include <boost/config.hpp>
#include <boost/test/included/unit_test.hpp>

#include <bbt/infra/net/Buffer.hpp>
#include <bbt/infra/net/detail/TestAccess.hpp>

#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace {
thread_local bool probeActive = false;
thread_local std::size_t allocationCalls = 0;
thread_local std::size_t failAt = 0;

// 不在探针窗口内调用 Boost.Test；断言本身可能分配。
struct AllocationProbe {
    explicit AllocationProbe(std::size_t failure = 0)
    {
        allocationCalls = 0;
        failAt = failure;
        probeActive = true;
    }
    ~AllocationProbe()
    {
        probeActive = false;
    }
};
} // namespace

void* operator new(std::size_t size)
{
    if (probeActive && ++allocationCalls == failAt) {
        throw std::bad_alloc();
    }
    if (void* memory = std::malloc(size == 0 ? 1 : size)) {
        return memory;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t size)
{
    return ::operator new(size);
}
BOOST_NOINLINE void operator delete(void* memory) noexcept
{
    std::free(memory);
}
BOOST_NOINLINE void operator delete[](void* memory) noexcept
{
    std::free(memory);
}
BOOST_NOINLINE void operator delete(void* memory, std::size_t) noexcept
{
    std::free(memory);
}
BOOST_NOINLINE void operator delete[](void* memory, std::size_t) noexcept
{
    std::free(memory);
}

namespace {
using namespace bbt::infra::net;

static_assert(!std::is_copy_constructible_v<Buffer>);
static_assert(!std::is_copy_assignable_v<Buffer>);
static_assert(std::is_nothrow_move_constructible_v<Buffer>);
static_assert(std::is_nothrow_move_assignable_v<Buffer>);

template <class T, class = void> struct CanInsert : std::false_type {};
template <class T>
struct CanInsert<T, std::void_t<decltype(std::declval<Buffer&>() << std::declval<T>())>> : std::true_type {};
static_assert(!CanInsert<int*>::value);
static_assert(!CanInsert<std::nullptr_t>::value);
static_assert(CanInsert<const char*>::value);

template <class T> void CheckIntegerLimits()
{
    Buffer buffer;
    buffer << (std::numeric_limits<T>::min)() << (std::numeric_limits<T>::max)();
    T low{}, high{};
    buffer >> low >> high;
    BOOST_REQUIRE(static_cast<bool>(buffer));
    BOOST_CHECK(low == (std::numeric_limits<T>::min)());
    BOOST_CHECK(high == (std::numeric_limits<T>::max)());
    BOOST_CHECK(buffer.Empty());
}
} // namespace

BOOST_AUTO_TEST_CASE(golden_wire_and_integer_limits)
{
    Buffer buffer;
    buffer << std::uint8_t{0x12} << std::uint16_t{0x3456} << std::uint32_t{0x789abcde} << std::int16_t{-2} << true
           << false << 1.0f << -0.0 << "A";
    const std::string golden("\x12\x34\x56\x78\x9a\xbc\xde\xff\xfe\x01\x00"
                             "\x3f\x80\x00\x00\x80\x00\x00\x00\x00\x00\x00\x00"
                             "\x00\x00\x00\x01"
                             "A",
                             28);
    BOOST_CHECK_EQUAL(buffer.CopyTo(), golden);
    CheckIntegerLimits<std::uint8_t>();
    CheckIntegerLimits<std::uint16_t>();
    CheckIntegerLimits<std::uint32_t>();
    CheckIntegerLimits<std::uint64_t>();
    CheckIntegerLimits<std::int8_t>();
    CheckIntegerLimits<std::int16_t>();
    CheckIntegerLimits<std::int32_t>();
    CheckIntegerLimits<std::int64_t>();
}

BOOST_AUTO_TEST_CASE(strings_and_float_bits_roundtrip)
{
    Buffer buffer;
    const std::string embedded("a\0b", 3);
    buffer << embedded << std::string_view{} << "literal" << true << false;
    const float input = (std::numeric_limits<float>::infinity)();
    const double negativeZero = -0.0;
    buffer << input << negativeZero;
    std::string a, b, c;
    bool yes = false, no = true;
    float output{};
    double zero{};
    buffer >> a >> b >> c >> yes >> no >> output >> zero;
    BOOST_REQUIRE(static_cast<bool>(buffer));
    BOOST_CHECK_EQUAL(a, embedded);
    BOOST_CHECK(b.empty());
    BOOST_CHECK_EQUAL(c, "literal");
    BOOST_CHECK(yes && !no);
    BOOST_CHECK(std::memcmp(&input, &output, sizeof(input)) == 0);
    BOOST_CHECK(std::memcmp(&negativeZero, &zero, sizeof(zero)) == 0);
    BOOST_CHECK(buffer.Empty());
}

BOOST_AUTO_TEST_CASE(truncated_field_is_atomic_and_retryable)
{
    Buffer buffer;
    buffer << std::uint8_t{7} << std::uint32_t{3};
    BOOST_REQUIRE(buffer.WriteBytes("ab"));
    std::uint8_t first{};
    std::string output = "unchanged";
    buffer >> first >> output;
    BOOST_CHECK_EQUAL(first, 7);
    BOOST_CHECK_EQUAL(output, "unchanged");
    BOOST_CHECK_EQUAL(buffer.Size(), 6);
    BOOST_CHECK(buffer.Error() == std::errc::result_out_of_range);
    BOOST_CHECK(!buffer.WriteBytes("ignored"));
    buffer.ClearError();
    BOOST_REQUIRE(buffer.WriteBytes("c"));
    buffer >> output;
    BOOST_REQUIRE(static_cast<bool>(buffer));
    BOOST_CHECK_EQUAL(output, "abc");
    BOOST_CHECK(buffer.Empty());

    BOOST_REQUIRE(buffer.WriteBytes(std::string_view("\x01", 1)));
    std::uint32_t integer = 123;
    buffer >> integer;
    BOOST_CHECK_EQUAL(integer, 123);
    BOOST_CHECK_EQUAL(buffer.Size(), 1);
    buffer.ClearError();
    BOOST_REQUIRE(buffer.WriteBytes(std::string_view("\x02\x03\x04", 3)));
    buffer >> integer;
    BOOST_CHECK_EQUAL(integer, 0x01020304U);
}

BOOST_AUTO_TEST_CASE(received_segments_and_writable_tail_share_no_mutation)
{
    Buffer encoded;
    encoded << std::uint32_t{0x01020304} << "segmented";
    auto owner = std::make_shared<std::string>(encoded.CopyTo());
    std::weak_ptr<std::string> lifetime = owner;
    const auto original = *owner;
    std::vector<BufferSegment> segments;
    for (std::size_t i = 0; i < owner->size(); ++i) {
        segments.push_back({owner->data() + i, 1});
    }
    auto buffer = BufferTestAccess::Make(owner, std::move(segments));
    const auto* sourcePointer = buffer.Segments().front().data;
    buffer << std::uint16_t{99};
    BOOST_CHECK(buffer.Segments().front().data == sourcePointer);
    BOOST_CHECK_EQUAL(*owner, original);
    owner.reset();
    BOOST_CHECK(!lifetime.expired());
    std::uint32_t number{};
    std::string text;
    buffer >> number >> text;
    BOOST_REQUIRE(static_cast<bool>(buffer));
    BOOST_CHECK_EQUAL(number, 0x01020304U);
    BOOST_CHECK_EQUAL(text, "segmented");
    BOOST_CHECK(lifetime.expired());
    BOOST_CHECK_EQUAL(buffer.Size(), 2);
    std::uint16_t tail{};
    buffer >> tail;
    BOOST_CHECK_EQUAL(tail, 99);
    BOOST_CHECK(buffer.Empty());
}

BOOST_AUTO_TEST_CASE(multiple_blocks_and_self_append)
{
    Buffer buffer;
    for (std::uint32_t i = 0; i < 1000; ++i) {
        buffer << i;
    }
    BOOST_REQUIRE(buffer.SegmentCount() > 1);
    std::size_t total = 0;
    for (const auto& segment : buffer.Segments()) {
        total += segment.size;
    }
    BOOST_CHECK_EQUAL(total, buffer.Size());
    for (std::uint32_t i = 0; i < 1000; ++i) {
        std::uint32_t value{};
        buffer >> value;
        BOOST_REQUIRE_EQUAL(value, i);
    }
    BOOST_CHECK(buffer.Empty());
    const std::string payload(400, 'x');
    buffer.Clear();
    BOOST_REQUIRE(buffer.WriteBytes(payload));
    const auto ownView = buffer.Segments().front().View();
    BOOST_REQUIRE(buffer.WriteBytes(ownView));
    BOOST_CHECK_EQUAL(buffer.CopyTo(), payload + payload);
}

BOOST_AUTO_TEST_CASE(reserved_scalar_io_and_moves_do_not_allocate)
{
    Buffer buffer;
    BOOST_REQUIRE(buffer.Reserve(4096));
    bool correct = true;
    std::size_t calls;
    {
        AllocationProbe probe;
        for (std::uint32_t i = 0; i < 500; ++i) {
            buffer << i;
        }
        for (std::uint32_t i = 0; i < 500; ++i) {
            std::uint32_t value{};
            buffer >> value;
            correct = correct && value == i;
        }
        calls = allocationCalls;
    }
    BOOST_CHECK(correct);
    BOOST_CHECK_EQUAL(calls, 0);
    buffer << "move-me";
    const auto* pointer = buffer.Segments().front().data;
    Buffer destination;
    destination << "old";
    {
        AllocationProbe probe;
        Buffer moved(std::move(buffer));
        destination = std::move(moved);
        calls = allocationCalls;
    }
    BOOST_CHECK_EQUAL(calls, 0);
    BOOST_CHECK(destination.Segments().front().data == pointer);
    BOOST_CHECK(buffer.Empty());
    BOOST_CHECK(static_cast<bool>(buffer));
    buffer << "reused";
    std::string value;
    buffer >> value;
    BOOST_CHECK_EQUAL(value, "reused");
    Buffer& self = destination;
    destination = std::move(self);
    destination >> value;
    BOOST_CHECK_EQUAL(value, "move-me");
}

BOOST_AUTO_TEST_CASE(invalid_input_and_clear)
{
    Buffer buffer;
    BOOST_REQUIRE(buffer.WriteBytes(std::string_view("\x02", 1)));
    bool value = true;
    buffer >> value;
    BOOST_CHECK(value);
    BOOST_CHECK_EQUAL(buffer.Size(), 1);
    BOOST_CHECK(buffer.Error() == std::errc::illegal_byte_sequence);
    BOOST_CHECK(!buffer.Consume(1));
    buffer.Clear();
    BOOST_CHECK(buffer.Empty());
    BOOST_CHECK(static_cast<bool>(buffer));
    buffer << static_cast<const char*>(nullptr);
    BOOST_CHECK(buffer.Error() == std::errc::invalid_argument);
    buffer.Clear();
    BOOST_REQUIRE(buffer.WriteBytes("x"));
    BOOST_CHECK(!buffer.ReadBytes(nullptr, 1));
    BOOST_CHECK_EQUAL(buffer.Size(), 1);
    buffer.ClearError();
    BOOST_CHECK(!buffer.Reserve((std::numeric_limits<std::size_t>::max)()));
    BOOST_CHECK(buffer.Error() == std::errc::value_too_large);
    buffer.Clear();
    BOOST_CHECK(buffer.ReadBytes(nullptr, 0));
    buffer << (std::numeric_limits<std::uint32_t>::max)();
    std::string output = "keep";
    std::size_t calls;
    {
        AllocationProbe probe;
        buffer >> output;
        calls = allocationCalls;
    }
    BOOST_CHECK_EQUAL(calls, 0);
    BOOST_CHECK_EQUAL(buffer.Size(), 4);
    BOOST_CHECK_EQUAL(output, "keep");
}

BOOST_AUTO_TEST_CASE(allocation_failure_does_not_partially_commit)
{
    const std::string payload(8192, 'z');
    Buffer buffer;
    buffer << std::uint16_t{7};
    const auto before = buffer.CopyTo();
    bool threw = false;
    {
        AllocationProbe probe(1);
        try {
            buffer << payload;
        } catch (const std::bad_alloc&) {
            threw = true;
        }
    }
    BOOST_REQUIRE(threw);
    BOOST_CHECK_EQUAL(buffer.CopyTo(), before);
    BOOST_CHECK(static_cast<bool>(buffer));
    buffer << payload;
    std::uint16_t number{};
    buffer >> number;
    const auto wireSize = buffer.Size();
    std::string output = "keep";
    threw = false;
    {
        AllocationProbe probe(1);
        try {
            buffer >> output;
        } catch (const std::bad_alloc&) {
            threw = true;
        }
    }
    BOOST_REQUIRE(threw);
    BOOST_CHECK_EQUAL(output, "keep");
    BOOST_CHECK_EQUAL(buffer.Size(), wireSize);
    buffer >> output;
    BOOST_CHECK_EQUAL(output, payload);
    BOOST_CHECK(buffer.Empty());
}

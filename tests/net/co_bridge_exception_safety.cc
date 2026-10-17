// P1 bridge-only fixture：真实 TCP；不验证 Engine 的失败/析构策略。
#define main CoBridgeFixtureMain
#include "co_bridge_contract.cc"
#undef main
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <new>

namespace {
thread_local int fail_after = -1;
thread_local bool reject_new = false;
thread_local std::size_t new_attempts = 0;
thread_local bool inject_create = false;
thread_local int construct_index = 0;
thread_local bool inject_dispatch = false;
thread_local int close_fault = 0;
thread_local int close_hits = 0;
thread_local int register_hits = 0;
thread_local int watched_fd = -1;
thread_local int fd_closes = 0;
thread_local bool count_native_new = false;
thread_local std::size_t native_allocations = 0;
}
void* operator new(std::size_t size) {
    if (count_native_new) { ++native_allocations; }
    if (reject_new || fail_after >= 0) {
        ++new_attempts;
        if (reject_new || fail_after-- == 0) { fail_after = -1; throw std::bad_alloc(); }
    }
    if (void* value = std::malloc(size ? size : 1)) { return value; }
    throw std::bad_alloc();
}
void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void* value) noexcept { std::free(value); }
void operator delete(void* value, std::size_t) noexcept { std::free(value); }
void operator delete[](void* value) noexcept { std::free(value); }
void operator delete[](void* value, std::size_t) noexcept { std::free(value); }

extern "C" int __real_close(int);
extern "C" int __wrap_close(int fd) {
    if (fd == watched_fd) { ++fd_closes; }
    // 注入边界仅 bridge/StreamConnection；上游 Hook_Close 的 fd slot 建立
    // 自身会分配。保留真实 close/取消回调，不声称依赖也无分配。
    const bool rejecting = std::exchange(reject_new, false);
    count_native_new = true;
    try {
        const int result = __real_close(fd);
        count_native_new = false;
        reject_new = rejecting;
        return result;
    } catch (...) {
        count_native_new = false;
        reject_new = rejecting;
        throw;
    }
}
extern "C" std::shared_ptr<StreamConnection> RealCreate(EventLoop&, NativeSocket&&,
    const NormalizedOptions&, StreamConnection::Callbacks, std::error_code&)
    asm("__real__ZN3bbt5infra3net16StreamConnection6CreateERNS1_9EventLoopEONS1_12NativeSocketERKNS1_17NormalizedOptionsENS2_9CallbacksERSt10error_code");
extern "C" std::shared_ptr<StreamConnection> WrapCreate(EventLoop&, NativeSocket&&,
    const NormalizedOptions&, StreamConnection::Callbacks, std::error_code&)
    asm("__wrap__ZN3bbt5infra3net16StreamConnection6CreateERNS1_9EventLoopEONS1_12NativeSocketERKNS1_17NormalizedOptionsENS2_9CallbacksERSt10error_code");
extern "C" std::shared_ptr<StreamConnection> WrapCreate(EventLoop& loop, NativeSocket&& socket,
    const NormalizedOptions& options, StreamConnection::Callbacks callbacks, std::error_code& error) {
    // 不是直接 throw：实际进入 Create/new StreamConnection 的分配路径。
    if (inject_create) { fail_after = construct_index; }
    try {
        auto result = RealCreate(loop, std::move(socket), options, std::move(callbacks), error);
        fail_after = -1;
        return result;
    } catch (...) { fail_after = -1; throw; }
}
extern "C" void RealRegister(bbt::coroutine::detail::Scheduler*, std::size_t,
    const std::function<void()>&, const char*)
    asm("__real__ZN3bbt9coroutine6detail9Scheduler19RegistCoroutineTaskEmRKSt8functionIFvvEEPKc");
extern "C" void WrapRegister(bbt::coroutine::detail::Scheduler*, std::size_t,
    const std::function<void()>&, const char*)
    asm("__wrap__ZN3bbt9coroutine6detail9Scheduler19RegistCoroutineTaskEmRKSt8functionIFvvEEPKc");
extern "C" void WrapRegister(bbt::coroutine::detail::Scheduler* scheduler, std::size_t stack,
    const std::function<void()>& callback, const char* name) {
    if (inject_dispatch && name && std::strcmp(name, "co.bridge.dispatch") == 0) {
        ++register_hits;
        throw std::bad_alloc(); // 依赖边界失败，不是 allocator 证据。
    }
    RealRegister(scheduler, stack, callback, name);
}
extern "C" std::error_code RealClose(StreamConnection*, std::error_code)
    asm("__real__ZN3bbt5infra3net16StreamConnection5CloseESt10error_code");
extern "C" std::error_code WrapClose(StreamConnection*, std::error_code)
    asm("__wrap__ZN3bbt5infra3net16StreamConnection5CloseESt10error_code");
extern "C" std::error_code WrapClose(StreamConnection* connection, std::error_code reason) {
    ++close_hits;
    const int fault = std::exchange(close_fault, 0);
    if (fault == 1) { throw std::bad_alloc(); }
    if (fault == 2) { throw std::runtime_error("close fixture failure"); }
    return RealClose(connection, reason);
}

static bool Eof(NativeSocket& client) {
    pollfd eof{client.Fd(), POLLIN, 0};
    if (::poll(&eof, 1, 2000) != 1) { return false; }
    char byte{};
    return ::recv(eof.fd, &byte, 1, MSG_DONTWAIT) == 0;
}

static void AdoptFailure(int index, bool bound) {
    WarmRuntime();
    const auto fd_before = OpenFdCount();
    {
        EventLoop loop(64);
        CHECK(loop.Start());
        std::error_code error;
        auto bridge = ConnectionBridge::Create(loop, BridgeOptions(), error);
        auto listener = OpenStream();
        auto client = ConnectTo(ListenOn(listener));
        auto handler = std::make_shared<ProbeHandler>();
        bool threw = false, retained_input = false, rolled_back = false, bound_closed = false;
        long owner_tid = 0;
        int closes = 0, allocations = 0, registered = 0, close_calls = 0;
        OnOwner(loop, [&] {
            owner_tid = ::syscall(SYS_gettid);
            Address peer;
            auto socket = listener.Accept(peer, error);
            CHECK(!error && socket.Fd() >= 0);
            const int fd = socket.Fd();
            watched_fd = fd;
            fd_closes = 0;
            new_attempts = 0;
            inject_create = !bound;
            construct_index = index;
            inject_dispatch = bound;
            close_fault = bound ? 1 : 0;
            try { (void)bridge->Adopt(std::move(socket), peer, handler, error); }
            catch (const std::bad_alloc&) { threw = true; }
            inject_create = inject_dispatch = false;
            allocations = static_cast<int>(new_attempts);
            registered = register_hits;
            close_calls = close_hits;
            retained_input = socket.Fd() == fd && ::fcntl(fd, F_GETFD) >= 0 && fd_closes == 0;
            rolled_back = bridge->SessionCount() == 0 && bridge->GetStats().sessions_registered.load() == 0;
            auto session = bridge->SessionOf(0);
            bound_closed = session && session->SnapshotState().physical_closed;
            if (!bound) { CHECK(!socket.Close()); }
            closes = fd_closes;
            watched_fd = -1;
            // 红路径清理由同一桥管理入口负责，非 Engine 安全证据。
            CHECK(!bridge->StopServiceOwner());
        });
        CHECK(Eof(client));
        loop.RequestStop();
        CHECK(loop.Join());
        const auto detach_error = bridge->DetachOwner();
        Log("adopt_exception", 0, "bound=" + std::to_string(bound)
            + " owner_tid=" + std::to_string(owner_tid)
            + " allocations=" + std::to_string(allocations) + " registry_rollback=" + std::to_string(rolled_back)
            + " retained_input=" + std::to_string(retained_input) + " fd_closes=" + std::to_string(closes)
            + " bound_closed_before_fixture_stop=" + std::to_string(bound_closed)
            + " register_hits=" + std::to_string(registered) + " close_calls=" + std::to_string(close_calls));
        CHECK(threw && closes == 1);
        if (bound) { CHECK(bound_closed && registered == 1 && close_calls == 2); }
        else { CHECK(allocations == index + 1 && retained_input && rolled_back); }
        CHECK(!detach_error && loop.FailureCount() == 0);
    }
    CHECK(OpenFdCount() == fd_before);
}

static void StopWithoutSnapshot(int fault) {
    WarmRuntime();
    const auto fd_before = OpenFdCount();
    {
        ContractFixture fixture;
        fixture.Start();
        auto handler = std::make_shared<ProbeHandler>();
        for (int i = 0; i != 2; ++i) { fixture.Connect(handler); }
        Get(Task([] {}));
        std::size_t attempts = 0;
        std::size_t native_attempts = 0;
        int calls = 0;
        std::error_code first;
        bool threw = false;
        bool second_closed = false;
        long owner_tid = 0;
        OnOwner(fixture.loop, [&] {
            owner_tid = ::syscall(SYS_gettid);
            new_attempts = 0;
            native_allocations = 0;
            close_hits = 0;
            close_fault = fault;
            // 拒绝实际 new：仅 owner Stop 窗口，native close 依赖边界见 wrap。
            reject_new = fault == 0;
            try { first = fixture.bridge->StopServiceOwner(); }
            catch (const std::bad_alloc&) { threw = true; }
            reject_new = false;
            attempts = new_attempts;
            native_attempts = native_allocations;
            calls = close_hits;
            // 先记录首次结果再用 Stop 重试；不额外 CloseNative 自救。
            second_closed = fixture.bridge->SessionOf(1)->SnapshotState().physical_closed;
            CHECK(!fixture.bridge->StopServiceOwner());
        });
        for (int i = 0; i != 2; ++i) {
            CHECK(Eof(fixture.clients[i]));
            auto session = fixture.bridge->SessionOf(i);
            auto stats = OwnerStats(fixture.loop, session);
            const auto deadline = Until(5s);
            while (stats.wait_callbacks < stats.read_arms + stats.write_arms) {
                CHECK(Clock::now() < deadline);
                stats = OwnerStats(fixture.loop, session);
            }
            CHECK(stats.read_arms > 0 && stats.physical_closes == 1);
            CHECK(stats.wait_callbacks == stats.read_arms + stats.write_arms);
            Log("stop_wait_settled", i, "arms=" + std::to_string(stats.read_arms + stats.write_arms)
                + " callbacks=" + std::to_string(stats.wait_callbacks) + " eof=1");
        }
        fixture.loop.RequestStop();
        CHECK(fixture.loop.Join());
        CHECK(!fixture.bridge->DetachOwner());
        Log("stop_exception", 0, "fault=" + std::to_string(fault) + " attempts=" + std::to_string(attempts)
            + " owner_tid=" + std::to_string(owner_tid)
            + " native_allocations_excluded=" + std::to_string(native_attempts)
            + " close_calls=" + std::to_string(calls) + " error=" + std::to_string(first.value()));
        CHECK(!threw && calls == 2 && second_closed);
        if (fault == 0) { CHECK(attempts == 0 && !first); }
        else { CHECK(first == (fault == 1 ? std::errc::not_enough_memory : std::errc::io_error)); }
    }
    CHECK(OpenFdCount() == fd_before);
}

#include "ProcessRuntimeTestMain.hpp"
int main(int argc, char** argv) {
    return bbt_net_test::RunTestProcessExit([&] {
        try {
            CHECK(argc == 2);
            const std::string name = argv[1];
            if (name == "adopt_construct_oom_rollback") { AdoptFailure(0, false); }
            else if (name == "adopt_member_oom_rollback") { AdoptFailure(1, false); }
            else if (name == "adopt_bound_exception") { AdoptFailure(0, true); }
            else if (name == "stop_no_snapshot_allocation") { StopWithoutSnapshot(0); }
            else if (name == "stop_bad_alloc_classification") { StopWithoutSnapshot(1); }
            else if (name == "stop_other_exception_classification") { StopWithoutSnapshot(2); }
            else { return 2; }
            std::cout << "CASE OK exception_safety " << name << '\n';
            return 0;
        } catch (const std::exception& error) {
            std::cerr << "RUN FAILED: " << error.what() << '\n';
            return 1;
        }
    });
}

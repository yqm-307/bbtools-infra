// 一次 Full 故障阶段：精确命中 EOF 收口 Submit，恢复后由公共 Wake 推进。
// linkwrap + 调用栈筛选，不增加生产 hook，不用 sleep 推断命中来源。
#define main PeerEofFullFixtureMain
#include "co_bridge_contract.cc"
#undef main
#include <bbt/infra/net/Engine.hpp>
#include <cstring>
#include <dlfcn.h>
#include <execinfo.h>
#include <sys/socket.h>

namespace {
std::atomic<int> full_remaining{0};
std::atomic<int> rejected{0};
std::atomic<int> accepted{0};
std::atomic<int> physical_closes{0};
std::promise<void> rejected_stage;

bool IsPeerCloseSubmit() {
    void* frames[24];
    const int count = ::backtrace(frames, 24);
    for (int i = 0; i < count; ++i) {
        Dl_info info{};
        if (::dladdr(frames[i], &info) && info.dli_sname
            && std::strstr(info.dli_sname, "ConnectionSession19CloseOnPeerTerminalEv")) {
            return true;
        }
    }
    return false;
}

struct FullHandler final : EventHandler {
    ICoConnection* connection{nullptr}; // engine registry 保活，Stop 前使用
    std::promise<void> opened;
    std::promise<void> closed;
    std::string received;
    std::error_code reason;
    std::atomic<int> closes{0};
    std::atomic<int> wakes{0};
    Action OnOpen(ICoConnection& value) override {
        connection = &value;
        opened.set_value();
        return Action::None;
    }
    Action OnTraffic(ICoConnection& value, bool wake) override {
        if (wake) { ++wakes; return Action::None; }
        const auto view = value.Peek();
        if (!view.empty()) { received.append(view.data(), view.size()); }
        CHECK(!value.Discard(view.size()));
        return Action::None;
    }
    void OnClose(ICoConnection&, const std::error_code& value) override {
        reason = value;
        CHECK(++closes == 1);
        CHECK(received == "tail"); // 尾帧必须先于 Close 消费
        closed.set_value();
    }
};
}

extern "C" SubmitStatus RealPeerSubmit(EventLoop*, std::function<void()>)
    asm("__real__ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEE");
extern "C" SubmitStatus WrapPeerSubmit(EventLoop*, std::function<void()>)
    asm("__wrap__ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEE");
extern "C" SubmitStatus WrapPeerSubmit(EventLoop* loop, std::function<void()> command) {
    const bool peer = IsPeerCloseSubmit();
    if (peer && full_remaining.load() > 0) {
        const int left = full_remaining.fetch_sub(1) - 1;
        ++rejected;
        if (left == 0) { rejected_stage.set_value(); }
        return SubmitStatus::Full; // 未接纳，不执行闭包
    }
    const auto status = RealPeerSubmit(loop, std::move(command));
    if (peer && status == SubmitStatus::Accepted) { ++accepted; }
    return status;
}
extern "C" std::error_code RealPeerClose(StreamConnection*, std::error_code)
    asm("__real__ZN3bbt5infra3net16StreamConnection5CloseESt10error_code");
extern "C" std::error_code WrapPeerClose(StreamConnection*, std::error_code)
    asm("__wrap__ZN3bbt5infra3net16StreamConnection5CloseESt10error_code");
extern "C" std::error_code WrapPeerClose(StreamConnection* connection, std::error_code reason) {
    ++physical_closes;
    return RealPeerClose(connection, reason);
}

static int RunFullCase(int argc, char** argv) {
    CHECK(argc == 2);
    const int budget = std::string(argv[1]) == "once" ? 1 : 2;
    CHECK(std::string(argv[1]) == "once" || std::string(argv[1]) == "recover");
    WarmRuntime();
    const auto before = OpenFdCount();
    {
        auto handler = std::make_shared<FullHandler>();
        Engine engine(handler);
        std::error_code error;
        const auto address = Address::Ip("127.0.0.1", 0, error);
        CHECK(!error);
        Options options;
        options.num_event_loop = 1;
        CHECK(!engine.Run(address, options));
        std::vector<Address> addresses;
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1);
        auto client = ConnectTo(addresses.front());
        Get(handler->opened.get_future());
        CHECK(engine.CountConnections() == 1);
        full_remaining = budget;
        CHECK(ClientSend(client, "tail"));
        CHECK(!client.Shutdown(SHUT_WR));
        Get(rejected_stage.get_future()); // 确认真实 EOF close Submit 已耗尽注入阶段
        CHECK(rejected == budget);
        if (budget == 2) {
            // 正常可达 owner 事件，不调用私有关闭函数、不显式 Close/Stop。
            auto wake = Task([handler] { CHECK(!handler->connection->Wake()); });
            Get(wake);
        }
        Get(handler->closed.get_future()); // 旧 latch 实现 recover 在此超时（行为红）
        CHECK(!handler->reason);
        CHECK(handler->closes == 1);
        const auto deadline = Until(5s);
        while (engine.CountConnections() != 0 || accepted.load() != 1) {
            CHECK(Clock::now() < deadline);
            std::this_thread::yield(); // 只等可观测谓词，不以时间推断时序
        }
        CHECK(physical_closes == 1);
        pollfd pfd{client.Fd(), POLLIN | POLLHUP, 0};
        CHECK(::poll(&pfd, 1, 5000) > 0);
        char byte;
        CHECK(client.Receive(&byte, 1).eof); // 内核侧真实收口，不只回调计数
        Log("peer_eof_full", 0, "rejected=" + std::to_string(rejected.load())
            + " accepted=1 onclose=1 CountConnections=0 physical_closes=1 tail=tail reason=clean");
        // 只有全部自然收口断言之后才 Stop，不能以 Stop 掩盖自动关闭失败。
        CHECK(!engine.Stop());
    }
    CHECK(OpenFdCount() == before);
    Log("peer_eof_full_fd", 0, "baseline=" + std::to_string(before) + " restored=1");
    return 0;
}
int main(int argc, char** argv) {
    return bbt_net_test::RunTestProcessExit([&] { return RunFullCase(argc, argv); });
}

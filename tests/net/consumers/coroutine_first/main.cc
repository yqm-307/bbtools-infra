// L2 回归 consumer：coroutine-first parent 只 link bbt::infra_net；
// 编译期消费 CoObject/NativeSocket 公共头（复用分支 include 修复的可编译证明），
// 运行期在受管协程内做真实 UDP 往返并核对身份环境。
#include <bbt/infra/net/CoObject.hpp>
#include <bbt/infra/net/NativeSocket.hpp>

#include <poll.h>

#include <bbt/coroutine/coroutine.hpp>

#include <chrono>
#include <cstring>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <system_error>

using namespace bbt::infra::net;
using namespace std::chrono_literals;

static void Check(bool value, const char* expression, int line) {
    if (!value) { throw std::runtime_error("line " + std::to_string(line) + ": " + expression); }
}
#define CHECK(expression) Check(static_cast<bool>(expression), #expression, __LINE__)

#include "../../ProcessRuntimeTestMain.hpp"

static int RunConsumerCases() {
    try {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 1;
        g_bbt_coroutine_config->m_cfg_stack_size = 65536;
        g_scheduler->Start();
        auto object = std::make_shared<CoObject>("consumer", "coroutine-first");
        auto promise = std::make_shared<std::promise<int>>();
        auto future = promise->get_future();
        g_scheduler->RegistCoroutineTask(65536, [object, promise] {
            try {
                CHECK(object->GetEnvironment().CanSuspend());
                CHECK(!object->RequireCoroutine());
                std::error_code error;
                const auto any = Address::Ip("127.0.0.1", 0, error);
                CHECK(!error && any.Valid());
                auto receiver = NativeSocket::Open(AddressFamily::IPv4, SocketType::Datagram, error);
                CHECK(!error);
                CHECK(!receiver.Bind(any));
                const auto bound = receiver.LocalAddress(error);
                CHECK(!error && bound.Valid());
                auto sender = NativeSocket::Open(AddressFamily::IPv4, SocketType::Datagram, error);
                CHECK(!error);
                const char payload[] = "coroutine-first";
                const auto sent = sender.SendTo(payload, sizeof(payload), bound);
                CHECK(!sent.error && sent.bytes == sizeof(payload));
                pollfd ready{receiver.Fd(), POLLIN, 0};
                CHECK(::poll(&ready, 1, 2000) == 1);
                char bytes[64]{};
                Address peer;
                const auto received = receiver.ReceiveFrom(bytes, sizeof(bytes), peer);
                CHECK(!received.error && received.bytes == sizeof(payload) && !received.truncated);
                CHECK(std::memcmp(payload, bytes, sizeof(payload)) == 0);
                CHECK(!receiver.Close() && !sender.Close());
                promise->set_value(0);
            } catch (...) { promise->set_exception(std::current_exception()); }
        }, "consume.coroutine-first");
        CHECK(future.wait_for(10s) == std::future_status::ready);
        CHECK(future.get() == 0);
        CHECK(!object->GetEnvironment().CanSuspend());
        std::cout << "coroutine-first consumer: public headers compile via bbt::infra_net; managed UDP roundtrip\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL " << error.what() << '\n';
        return 1;
    }
}

int main() {
    // 测试进程退出边界：局部 RAII 收口 → 刷新输出 → _Exit，见
    // ../../ProcessRuntimeTestMain.hpp 头注（静态析构竞态为已知限制，未修复）。
    return bbt_net_test::RunTestProcessExit([] { return RunConsumerCases(); });
}

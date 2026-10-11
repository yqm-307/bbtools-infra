// 公开 RpcClient/RpcServer 的独立双进程 consumer。
//
// 同一份无关 framework 的 peer 可执行文件扮演两个自有进程：
//   - server / client：成功调用、结构化错误（NotFound）、关联字段不匹配、
//     坏消息（非 protobuf Content-Type / 空 body / 非法 protobuf 字节）经
//     原始 HTTP 请求打到正式 RPC 入口，服务端必须回 wire 错误 envelope；
//   - server + stdin "stop" → StopAccepting()，随后的 client_stop_check 必须
//     是有界失败（不是成功回复），即「只停新接纳」；
//   - server_hold + client_hold：请求已完整写出后对端失联（测试侧 SIGKILL）
//     ⇒ OutcomeUnknown + RequestPhase::RequestCommitted。
//
// 端口用内核动态分配（服务端写 port file），临时文件在 TMPDIR scratch；
// 每个子进程都有 RAII 兜底（用例失败/中断时 kill+reap），不残留子进程。

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <spawn.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

namespace {

std::string ScratchTemplate(const char* tag) {
    const char* tmp = std::getenv("TMPDIR");
    std::string dir = (tmp != nullptr && *tmp != '\0') ? tmp : "/tmp";
    return dir + "/rpc_runtime_" + tag + "_XXXXXX";
}

// 预留一个唯一路径后立即删除：文件内容（端口/marker）只由 peer 进程写入，
// 避免「预先存在的空文件」让有界轮询立刻误判为事件已发生。
std::string MakeTempPath() {
    std::string path = ScratchTemplate("tmp");
    std::vector<char> buf(path.begin(), path.end());
    buf.push_back('\0');
    const int fd = ::mkstemp(buf.data());
    BOOST_REQUIRE(fd >= 0);
    ::close(fd);
    std::string reserved(buf.data());
    ::unlink(reserved.c_str());
    return reserved;
}

struct TmpFileGuard {
    std::string path;
    explicit TmpFileGuard(std::string p) : path(std::move(p)) {}
    ~TmpFileGuard() {
        if (!path.empty())
            ::unlink(path.c_str());
    }
    TmpFileGuard(const TmpFileGuard&)            = delete;
    TmpFileGuard& operator=(const TmpFileGuard&) = delete;
};

// 子进程兜底：正常路径显式 Reap；用例失败/中断时析构里 kill + reap，
// 不让任何子进程残留。
struct ChildGuard {
    pid_t pid    = -1;
    bool  reaped = false;

    int Reap() {
        int status = 0;
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds{30};
        for (;;) {
            const auto result = ::waitpid(pid, &status, WNOHANG);
            if (result == pid) {
                reaped = true;
                return status;
            }
            BOOST_REQUIRE(result == 0);
            if (std::chrono::steady_clock::now() >= deadline)
                BOOST_FAIL("peer exit deadline exceeded");
            std::this_thread::sleep_for(std::chrono::milliseconds{10});
        }
    }
    ~ChildGuard() {
        if (pid > 0 && !reaped) {
            ::kill(pid, SIGKILL);
            int status = 0;
            ::waitpid(pid, &status, 0);
        }
    }
    ChildGuard() = default;
    ChildGuard(const ChildGuard&)            = delete;
    ChildGuard& operator=(const ChildGuard&) = delete;
};

std::string PeerPath() {
    char        self_path[4096];
    const auto  length = ::readlink("/proc/self/exe", self_path,
                                    sizeof(self_path) - 1);
    BOOST_REQUIRE(length > 0);
    self_path[length] = '\0';
    std::string directory = self_path;
    directory.resize(directory.find_last_of('/'));
    return directory + "/bbt_rpc_runtime_peer";
}

pid_t SpawnPeer(const std::string& exe, const std::vector<std::string>& args,
                int stdin_fd = -1, int close_fd = -1) {
    std::vector<char*> argv;
    argv.push_back(const_cast<char*>(exe.c_str()));
    for (const auto& arg : args)
        argv.push_back(const_cast<char*>(arg.c_str()));
    argv.push_back(nullptr);

    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    if (stdin_fd >= 0)
        posix_spawn_file_actions_adddup2(&actions, stdin_fd, STDIN_FILENO);
    if (close_fd >= 0)
        posix_spawn_file_actions_addclose(&actions, close_fd);

    pid_t     pid = -1;
    const int rc  = ::posix_spawn(&pid, exe.c_str(), &actions, nullptr,
                                  argv.data(), environ);
    posix_spawn_file_actions_destroy(&actions);
    BOOST_REQUIRE_EQUAL(rc, 0);
    return pid;
}

// 动态端口：服务端把内核分配的端口写入 port file；有界轮询（失败上限）。
std::uint16_t WaitForPort(const std::string& path, int budget_ms = 10000) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(budget_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        std::ifstream file(path);
        std::uint16_t port = 0;
        if (file >> port && port > 0)
            return port;
        std::this_thread::sleep_for(std::chrono::milliseconds{50});
    }
    BOOST_FAIL("peer did not publish port");
    return 0;
}

bool WaitForFile(const std::string& path, int budget_ms = 10000) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(budget_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        struct stat info {};
        if (::stat(path.c_str(), &info) == 0)
            return true;
        std::this_thread::sleep_for(std::chrono::milliseconds{20});
    }
    return false;
}

// 读回 peer 写入的 ack 内容（只取首个 token）。
std::string ReadAck(const std::string& path) {
    std::ifstream file(path);
    std::string   token;
    file >> token;
    return token;
}

} // namespace

BOOST_AUTO_TEST_SUITE(rpc_runtime_roundtrip)

// 成功 / 结构化错误 / 关联不匹配 / 坏消息 / StopAccepting 后拒绝新接纳。
BOOST_AUTO_TEST_CASE(t_dual_process_public_client_server_matrix) {
    const std::string peer = PeerPath();
    TmpFileGuard      port_file(MakeTempPath());
    TmpFileGuard      stopped_file(MakeTempPath());

    int pipefd[2];
    BOOST_REQUIRE(::pipe(pipefd) == 0);

    ChildGuard server;
    server.pid = SpawnPeer(peer, {"server", port_file.path, stopped_file.path},
                           /*stdin_fd=*/pipefd[0], /*close_fd=*/pipefd[1]);
    ::close(pipefd[0]);

    const std::uint16_t port = WaitForPort(port_file.path);
    const std::string   port_value = std::to_string(port);

    // 进程 A 侧消费者矩阵（成功、错误、坏消息……）必须以退出码 0 结束。
    {
        ChildGuard client;
        client.pid = SpawnPeer(peer, {"client", port_value},
                               /*stdin_fd=*/-1, /*close_fd=*/pipefd[1]);
        const int status = client.Reap();
        BOOST_REQUIRE(WIFEXITED(status));
        BOOST_CHECK_EQUAL(WEXITSTATUS(status), 0);
    }

    // StopAccepting：只停新接纳（已接纳 handler 不取消）。peer 的 ack 只在
    // 「acceptor 已物理关闭、新连接被拒绝」之后写出（服务端内有界 raw connect
    // 探针），因此随后的调用是「连接未建立」的确定失败——不靠 sleep 猜
    // StopAccepting 的异步收口时序（M1）。
    const char stop[] = "stop\n";
    BOOST_REQUIRE(::write(pipefd[1], stop, sizeof(stop) - 1) > 0);
    BOOST_REQUIRE(WaitForFile(stopped_file.path));
    BOOST_REQUIRE_EQUAL(ReadAck(stopped_file.path), "refused");

    // StopAccepting 之后的新调用必须是有界失败，不是成功回复。
    {
        ChildGuard after_stop;
        after_stop.pid = SpawnPeer(peer, {"client_stop_check", port_value},
                                   /*stdin_fd=*/-1, /*close_fd=*/pipefd[1]);
        const int status = after_stop.Reap();
        BOOST_REQUIRE(WIFEXITED(status));
        BOOST_CHECK_EQUAL(WEXITSTATUS(status), 0);
    }

    // 关闭 stdin：服务端收口退出（同步 Close 返回即物理释放）。
    ::close(pipefd[1]);
    const int server_status = server.Reap();
    BOOST_REQUIRE(WIFEXITED(server_status));
    BOOST_CHECK_EQUAL(WEXITSTATUS(server_status), 0);
}

// 回复丢失：请求已完整写出后被对端失联 ⇒ OutcomeUnknown + RequestCommitted。
BOOST_AUTO_TEST_CASE(t_dual_process_reply_loss_is_outcome_unknown) {
    const std::string peer = PeerPath();
    TmpFileGuard      port_file(MakeTempPath());
    TmpFileGuard      marker_file(MakeTempPath());

    // server_hold 在收到完整请求后写 marker（因果事件）并有界阻塞等待被 kill。
    ChildGuard server;
    server.pid = SpawnPeer(peer, {"server_hold", port_file.path, marker_file.path});
    const std::uint16_t port = WaitForPort(port_file.path);

    ChildGuard client;
    client.pid = SpawnPeer(peer, {"client_hold", std::to_string(port)});

    // 服务端已进入 handler ⇒ 请求已完整写出（对端读到请求结束符）。
    BOOST_REQUIRE(WaitForFile(marker_file.path));

    BOOST_REQUIRE_EQUAL(::kill(server.pid, SIGKILL), 0);
    const int server_status = server.Reap();
    BOOST_CHECK(WIFSIGNALED(server_status));

    const int client_status = client.Reap();
    BOOST_REQUIRE(WIFEXITED(client_status));
    BOOST_CHECK_EQUAL(WEXITSTATUS(client_status), 0);
}

BOOST_AUTO_TEST_SUITE_END()

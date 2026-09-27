// Issue #8：RpcEnvelope wire profile 双进程 round-trip。
//
// 测试进程 posix_spawn 独立的 bbt_rpc_wire_peer 二进制（同 build 目录）：
//   <peer> server <port_file>
//   <peer> client <port>
// 双方都是真实进程，经 infra HTTP loopback 完成一次 wire profile 调用。

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <thread>

#include <spawn.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

BOOST_AUTO_TEST_SUITE(rpc_wire_roundtrip)

BOOST_AUTO_TEST_CASE(t_dual_process_roundtrip) {
    std::string self = "/proc/self/exe";
    char self_path[4096];
    ssize_t n = ::readlink(self.c_str(), self_path, sizeof(self_path) - 1);
    BOOST_REQUIRE(n > 0);
    self_path[n] = '\0';
    std::string dir = self_path;
    auto slash = dir.find_last_of('/');
    BOOST_REQUIRE(slash != std::string::npos);
    dir = dir.substr(0, slash);
    std::string peer = dir + "/bbt_rpc_wire_peer";

    char port_file[] = "/tmp/rpc_wire_port_XXXXXX";
    int fd = ::mkstemp(port_file);
    BOOST_REQUIRE(fd >= 0);
    ::close(fd);

    int pipefd[2];
    BOOST_REQUIRE(::pipe(pipefd) == 0);

    // posix_spawn 避免 fork 与 Boost.Test signal/栈状态交互。
    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_adddup2(&fa, pipefd[0], STDIN_FILENO);
    posix_spawn_file_actions_addclose(&fa, pipefd[0]);
    posix_spawn_file_actions_addclose(&fa, pipefd[1]);

    pid_t pid = -1;
    {
        const std::string srv = "server";
        const std::string pf  = port_file;
        char* const args[] = {
            const_cast<char*>(peer.c_str()),
            const_cast<char*>(srv.c_str()),
            const_cast<char*>(pf.c_str()),
            nullptr};
        int rc = ::posix_spawn(&pid, peer.c_str(), &fa, nullptr,
                               args, environ);
        BOOST_REQUIRE_EQUAL(rc, 0);
        posix_spawn_file_actions_destroy(&fa);
    }
    ::close(pipefd[0]);

    std::uint16_t server_port = 0;
    for (int i = 0; i < 200 && server_port == 0; ++i) {
        std::ifstream f(port_file);
        if (f >> server_port && server_port > 0)
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    BOOST_REQUIRE_NE(server_port, 0);

    int status = 0;
    pid_t cpid = -1;
    {
        const std::string cli  = "client";
        const std::string port = std::to_string(server_port);
        char* const args[] = {
            const_cast<char*>(peer.c_str()),
            const_cast<char*>(cli.c_str()),
            const_cast<char*>(port.c_str()),
            nullptr};
        int rc = ::posix_spawn(&cpid, peer.c_str(), nullptr, nullptr,
                               args, environ);
        BOOST_REQUIRE_EQUAL(rc, 0);
    }
    BOOST_REQUIRE(::waitpid(cpid, &status, 0) == cpid);
    BOOST_REQUIRE(WIFEXITED(status));
    BOOST_CHECK_EQUAL(WEXITSTATUS(status), 0);

    ::close(pipefd[1]);
    BOOST_REQUIRE(::waitpid(pid, &status, 0) == pid);
    BOOST_REQUIRE(WIFEXITED(status));
    BOOST_CHECK_EQUAL(WEXITSTATUS(status), 0);
    ::unlink(port_file);
}

BOOST_AUTO_TEST_SUITE_END()

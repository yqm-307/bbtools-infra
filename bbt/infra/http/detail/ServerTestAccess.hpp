#pragma once
// HTTP 测试专用注入，不是公共契约。
namespace bbt::infra::http {
class Server;
class ServerTestAccess {
public:
    static void FailNextTask(Server& server);
};
}

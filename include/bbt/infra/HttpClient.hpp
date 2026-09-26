#pragma once
// co-network/v1 N1b-1：HTTP/1.1 客户端公共面。
// Asio/Beast、线程、io_context 全部隐藏于 src/http/ 实现，不进公开头。

#include <memory>

#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/ICoNetwork.hpp>
#include <bbt/infra/NetworkTypes.hpp>

namespace bbt::infra {

// HttpClient：出站 HTTP/1.1 组件，面向多 endpoint；连接按请求建立，
// 首版不承诺连接复用。仅在协程上下文调用 Request，等待期间挂起当前协程。
//
// Issue #37：出站配额由所属 NetworkRuntime（资源 owner）统一持有——
// 同一 runtime 下所有 HttpClient 共享同一份 max_connections /
// max_inflight 预算，不是每个 client 独立计量。Request 在发起任何
// 底层 I/O 之前原子预留名额；名额不足立即返回 Error(Overloaded)，
// 此时不创建 socket/resolver、不投递到 io 域、不排队。名额在请求
// 物理收口后准确归还一次（覆盖正常完成、发起失败、deadline、cancel、
// RequestClose 与强制 Stop），不依赖挂起协程的栈析构。本配额只覆盖
// HTTP 出站路径，与 transport #32 的 CoTCP/CoUDP 在途门禁是不同入口。
class HttpClient : public ICoNetwork, public ICoCloseable {
public:
    // 语义边界（契约 §N1）：
    //  - 正常 4xx/5xx 是有效 HttpResponse，不是网络异常；
    //  - 非法 framing/超限/断连返回 Error；不足一条消息不当成功响应；
    //  - 非协程调用返回 Error(InvalidContext)；Closed 后返回 Error(Closed)；
    //  - https 首切片显式拒绝（InvalidArgument），不降级明文。
    virtual result<HttpResponse> Request(HttpRequest request,
                                         const CallOptions& options) = 0;
};

} // namespace bbt::infra

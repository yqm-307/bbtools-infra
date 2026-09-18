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

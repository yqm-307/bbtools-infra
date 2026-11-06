#pragma once

namespace bbt::infra::rpc {

inline Response Response::FromRequest(const Request& request)
{
    Response response;
    response.SetRequestId(request.RequestId())
        .SetService(request.Service())
        .SetMethod(request.Method())
        .SetRequestSchema(request.RequestSchema())
        .SetResponseSchema(request.ResponseSchema());
    return response;
}

} // namespace bbt::infra::rpc

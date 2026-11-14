#include <bbt/infra/rpc/Response.hpp>

namespace bbt::infra::rpc {

Response Response::FromRequest(const Request& request)
{
    Response response;
    response.SetRequestId(request.RequestId())
        .SetService(request.Service())
        .SetMethod(request.Method())
        .SetRequestSchema(request.RequestSchema())
        .SetResponseSchema(request.ResponseSchema());
    return response;
}

// clang-format off
}
// clang-format on

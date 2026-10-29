# BBT_INFRA_NETWORK_ONLY=ON：严格的新 net + 显式声明的新协议闭包。
# 与普通模式共用公共段（cmake/Common.cmake：common/internal_debug/coroutine
# 源码接入/pin/AsyncMisuse/Threads 全部只定义一次）；本文件只负责 net 本体与
# 逐协议开关的接入，不探测旧 src/ 栈、bbtools-core、hiredis/mongo/protobuf
# （除非对应 *_NET 开关显式打开）。
#
# 目标图（按需闭包）：
#   bbt::infra_net            始终
#   bbt::infra_redis_net      BBT_INFRA_REDIS_NET（需 BBT_HIREDIS_PREFIX）
#   bbt::infra_http_net       BBT_INFRA_HTTP_NET
#   bbt::infra_rpc_net        BBT_INFRA_RPC_NET（需 BBT_INFRA_HTTP_NET +
#                             BBT_PROTOBUF_PREFIX）

include("${CMAKE_CURRENT_LIST_DIR}/NetBase.cmake")
# 先确定测试开关：include(CTest) 定义 BUILD_TESTING。示例自身的 CTest 注册依赖它，
# 只有这样才能做到「测试开关关闭时不注册、也不强启测试」。
include(CTest)
# P5 推荐协程网络示例：network-only 下唯一直达示例入口（examples/ 目录其余示例依赖旧模块）。
add_subdirectory(examples/co_net_service)
# ICoConnection 成员化收口消费示例（终评 §2 组合验收）：只依赖 bbt::infra_net。
add_subdirectory(examples/ico_connection)
if(BUILD_TESTING)
    add_subdirectory(tests/net)
endif()
if(BBT_INFRA_REDIS_NET)
    include("${CMAKE_CURRENT_LIST_DIR}/RedisNet.cmake")
endif()
if(BBT_INFRA_HTTP_NET)
    include("${CMAKE_CURRENT_LIST_DIR}/HttpNet.cmake")
endif()
if(BBT_INFRA_RPC_NET)
    include("${CMAKE_CURRENT_LIST_DIR}/RpcNet.cmake")
endif()

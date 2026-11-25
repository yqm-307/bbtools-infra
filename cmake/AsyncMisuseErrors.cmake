# bbt_co_async/bbt_co_await 的误用默认只有编译器警告兜底，对本仓代码必须当作错误：
#   - return-type：顺序 async 函数用 bbt_co_return / bbt_co_return_void；
#     惰性操作体若声明了非 void 返回类型，也必须返回值；
#   - unused-result：丢弃 AsyncOperation 时由 [[nodiscard]] 提供诊断；
#   - type-boundary：普通值没有 AwaitHelper/FireHelper 重载，不能进入 async 入口。
# 必须在 bbtools-coroutine 接入之后 include：directory 级 COMPILE_OPTIONS 只作用于
# 之后创建的目标，不改变被 pin 的上游源码。依据见 bbtools-coroutine 的
# bbt/coroutine/syntax/Async.hpp 宏处注释。
add_compile_options(-Werror=return-type -Werror=unused-result)

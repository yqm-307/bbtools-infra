# bbt_co_async/bbt_co_await 的误用默认只有编译器警告兜底，对本仓代码必须当作错误：
#   - return-type：bbt_co_async(void) 函数走到末尾不写 `return {};` 是未定义行为；
#   - unused-result：漏写 bbt_co_await 时函数体仍会执行，只有 AsyncReturn 的
#     [[nodiscard]] 报 -Wunused-result。
# 必须在 bbtools-coroutine 接入之后 include：directory 级 COMPILE_OPTIONS 只作用于
# 之后创建的目标，不改变被 pin 的上游源码。依据见 bbtools-coroutine 的
# bbt/coroutine/syntax/Async.hpp 宏处注释。
add_compile_options(-Werror=return-type -Werror=unused-result)

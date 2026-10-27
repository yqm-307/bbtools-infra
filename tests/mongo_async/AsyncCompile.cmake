# Mongo async 正负编译门禁（与 tests/redis_net/AsyncCompile.cmake 同规约）：
# 正例必须编译通过；负例（直接当 result 访问）必须被编译器拒绝，且报错文本
# 命中 "value"。不链接、不运行，只做 -fsyntax-only。
file(STRINGS "${INCLUDES}" _includes)
set(_flags -std=c++17 -fsyntax-only)
foreach(_include IN LISTS _includes)
    if(NOT _include STREQUAL "")
        list(APPEND _flags "-I${_include}")
    endif()
endforeach()
execute_process(COMMAND "${CXX}" ${_flags} "${SOURCE}" RESULT_VARIABLE _positive
    OUTPUT_VARIABLE _out ERROR_VARIABLE _error)
if(NOT _positive EQUAL 0)
    message(FATAL_ERROR "mongo async 正例编译失败: ${_out}${_error}")
endif()
execute_process(COMMAND "${CXX}" ${_flags} -DBBT_NEGATIVE_ASYNC "${SOURCE}"
    RESULT_VARIABLE _negative OUTPUT_VARIABLE _out ERROR_VARIABLE _error)
if(_negative EQUAL 0 OR NOT _error MATCHES "value")
    message(FATAL_ERROR
        "mongo async 负例没有按直接 result 访问被拒绝: ${_out}${_error}")
endif()
message(STATUS "mongo async 正例可编译，直接 value() 负例被拒绝")

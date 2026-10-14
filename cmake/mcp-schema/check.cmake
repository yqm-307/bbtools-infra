# This file is included only by the optional private schema adapter.
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" OR
   NOT CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64)$" OR
   NOT CMAKE_SIZEOF_VOID_P EQUAL 8 OR CMAKE_CROSSCOMPILING)
    message(FATAL_ERROR "MCP Schema lock supports native Linux/amd64 only")
endif()
find_package(Python3 3.11.8 REQUIRED COMPONENTS Interpreter)
execute_process(
    COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_LIST_DIR}/deps.py" check
            --jsoncons-root "${MCP_JSONCONS_ROOT}" --icu-root "${MCP_ICU_ROOT}"
    RESULT_VARIABLE _mcp_deps_rc
    OUTPUT_VARIABLE _mcp_deps_out ERROR_VARIABLE _mcp_deps_err)
if(NOT _mcp_deps_rc EQUAL 0)
    message(FATAL_ERROR "MCP Schema dependency lock check failed: ${_mcp_deps_err}")
endif()
message(STATUS "${_mcp_deps_out}")
# find_library uses cached values even with NO_DEFAULT_PATH: discard both bindings
# on every configure so root changes can never silently retain the previous root.
foreach(_icu_lib icui18n icuuc icudata)
    unset(BBT_MCP_ICU_${_icu_lib}_LIB CACHE)
    unset(BBT_MCP_ICU_${_icu_lib}_LIB)
endforeach()

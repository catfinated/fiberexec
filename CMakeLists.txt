cmake_minimum_required(VERSION 3.25)

project(fiberexec
  VERSION 0.1.0
  DESCRIPTION "A fiber-based scheduler for stdexec (P2300)"
  LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(CMAKE_EXPORT_COMPILE_COMMANDS)
  execute_process(
    COMMAND ${CMAKE_COMMAND} -E create_symlink
      ${CMAKE_BINARY_DIR}/compile_commands.json
      ${CMAKE_SOURCE_DIR}/compile_commands.json
  )
endif()

if(NOT LINUX)
  message(FATAL_ERROR "fiberexec requires Linux (io_uring)")
endif()

option(FIBEREXEC_BUILD_TESTS    "Build test suite" ON)
option(FIBEREXEC_BUILD_EXAMPLES "Build examples"   ON)

include(cmake/Dependencies.cmake)

add_subdirectory(src)

if(FIBEREXEC_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

if(FIBEREXEC_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

include(FetchContent)

# stdexec — P2300 reference implementation (not in vcpkg, header-only)
FetchContent_Declare(
  stdexec
  GIT_REPOSITORY https://github.com/NVIDIA/stdexec.git
  GIT_TAG        main
  GIT_SHALLOW    TRUE
)
set(STDEXEC_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(STDEXEC_BUILD_TESTS    OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(stdexec)

# Boost.Fiber + Boost.Context — provided by vcpkg
find_package(Boost REQUIRED COMPONENTS fiber context)

# liburing — provided by vcpkg via pkg-config
find_package(PkgConfig REQUIRED)
pkg_check_modules(liburing REQUIRED IMPORTED_TARGET liburing)

# Catch2 — provided by vcpkg
if(FIBEREXEC_BUILD_TESTS)
  find_package(Catch2 3 CONFIG REQUIRED)
endif()

# Google Benchmark + Boost.Asio + asioexec interface — provided by vcpkg / stdexec FetchContent
if(FIBEREXEC_BUILD_BENCHMARKS)
  find_package(benchmark CONFIG REQUIRED)
  find_package(Boost REQUIRED COMPONENTS asio system)

  # Configure exec/asio/asio_config.hpp from stdexec's template so the
  # exec/asio headers know to use Boost.Asio as their backend.
  set(STDEXEC_ASIO_USES_BOOST 1)
  set(STDEXEC_ASIO_USES_STANDALONE 0)
  configure_file(
    ${stdexec_SOURCE_DIR}/include/exec/asio/asio_config.hpp.in
    ${CMAKE_BINARY_DIR}/include/exec/asio/asio_config.hpp
  )

  # Interface target that bundles the configured header path, stdexec, and
  # Boost.Asio.  bench_echo links against this to access exec::asio::.
  add_library(asioexec_iface INTERFACE)
  target_include_directories(asioexec_iface INTERFACE ${CMAKE_BINARY_DIR}/include)
  target_link_libraries(asioexec_iface INTERFACE STDEXEC::stdexec Boost::asio)
endif()

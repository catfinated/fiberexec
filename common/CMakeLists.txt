# Header-only interface target for POSIX TCP helpers shared by examples and
# benchmarks.  Adds the project root to the include path so consumers can
# write #include <common/tcp_helpers.hpp>.
add_library(fiberexec_common INTERFACE)
target_include_directories(fiberexec_common INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/..)

# Asio helpers are only meaningful when benchmarks are built, since Asio is
# only pulled in as a dependency in that configuration.
if(FIBEREXEC_BUILD_BENCHMARKS)
    add_library(fiberexec_asio_common INTERFACE)
    target_include_directories(fiberexec_asio_common INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/..)
    target_link_libraries(fiberexec_asio_common INTERFACE fiberexec_common asioexec_iface)
endif()

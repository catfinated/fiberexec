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

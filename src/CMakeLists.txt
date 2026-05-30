add_library(fiberexec
  fiber_context.cpp
  async_io.cpp
  fixed_buffer_pool.cpp
  multishot_acceptor.cpp
  multishot_recv.cpp
)

add_library(fiberexec::fiberexec ALIAS fiberexec)

target_include_directories(fiberexec
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(fiberexec
  PUBLIC
    STDEXEC::stdexec
    Boost::fiber
    Boost::context
  PRIVATE
    PkgConfig::liburing
)

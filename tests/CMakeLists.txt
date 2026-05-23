include(CTest)
include(Catch)

add_executable(fiberexec_tests
  test_scheduler.cpp
  test_networking.cpp
)

target_link_libraries(fiberexec_tests
  PRIVATE
    fiberexec::fiberexec
    Catch2::Catch2WithMain
)

catch_discover_tests(fiberexec_tests)

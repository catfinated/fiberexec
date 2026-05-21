#include <fiberexec/fiber_context.hpp>

#include <catch2/catch_test_macros.hpp>

TEST_CASE("fiber_scheduler satisfies stdexec::scheduler concept", "[scheduler]") {
    STATIC_REQUIRE(stdexec::scheduler<fiberexec::fiber_scheduler>);
}

// Runtime scheduling tests will be added once the fiber event loop is
// implemented.

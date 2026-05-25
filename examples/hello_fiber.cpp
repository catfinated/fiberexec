#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <iostream>

int main() {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    auto work = stdexec::schedule(sched) | stdexec::then([] { std::cout << "Hello from a fiber!\n"; });

    stdexec::sync_wait(work);
}

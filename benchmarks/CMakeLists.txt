function(add_bench name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name} PRIVATE
        fiberexec::fiberexec
        benchmark::benchmark
    )
    # Excluded from the default 'all' target so normal builds stay fast.
    set_target_properties(${name} PROPERTIES EXCLUDE_FROM_ALL TRUE)
endfunction()

add_bench(bench_scheduler bench_scheduler.cpp)
add_bench(bench_echo bench_echo.cpp)
target_link_libraries(bench_echo PRIVATE asioexec_iface)
add_bench(bench_fanout bench_fanout.cpp)
add_bench(bench_latency bench_latency.cpp)
target_link_libraries(bench_latency PRIVATE asioexec_iface)

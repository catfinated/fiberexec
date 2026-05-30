add_executable(hello_fiber hello_fiber.cpp)
target_link_libraries(hello_fiber PRIVATE fiberexec::fiberexec)

add_executable(async_pipeline async_pipeline.cpp)
target_link_libraries(async_pipeline PRIVATE fiberexec::fiberexec)

add_executable(echo_server echo_server.cpp)
target_link_libraries(echo_server PRIVATE fiberexec::fiberexec fiberexec_common)

add_executable(cancellation cancellation.cpp)
target_link_libraries(cancellation PRIVATE fiberexec::fiberexec)

add_executable(parallel_gather parallel_gather.cpp)
target_link_libraries(parallel_gather PRIVATE fiberexec::fiberexec)

add_executable(channel_backpressure channel_backpressure.cpp)
target_link_libraries(channel_backpressure PRIVATE fiberexec::fiberexec)

add_executable(echo_server_pool echo_server_pool.cpp)
target_link_libraries(echo_server_pool PRIVATE fiberexec::fiberexec fiberexec_common)

add_executable(sync_wait_fanout sync_wait_fanout.cpp)
target_link_libraries(sync_wait_fanout PRIVATE fiberexec::fiberexec)

add_executable(echo_to_file echo_to_file.cpp)
target_link_libraries(echo_to_file PRIVATE fiberexec::fiberexec fiberexec_common)

add_executable(echo_server_multishot echo_server_multishot.cpp)
target_link_libraries(echo_server_multishot PRIVATE fiberexec::fiberexec fiberexec_common)

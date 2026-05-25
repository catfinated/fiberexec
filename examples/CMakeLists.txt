add_executable(hello_fiber hello_fiber.cpp)
target_link_libraries(hello_fiber PRIVATE fiberexec::fiberexec)

add_executable(async_pipeline async_pipeline.cpp)
target_link_libraries(async_pipeline PRIVATE fiberexec::fiberexec)

add_executable(echo_server echo_server.cpp)
target_link_libraries(echo_server PRIVATE fiberexec::fiberexec)

add_executable(cancellation cancellation.cpp)
target_link_libraries(cancellation PRIVATE fiberexec::fiberexec)

add_executable(parallel_gather parallel_gather.cpp)
target_link_libraries(parallel_gather PRIVATE fiberexec::fiberexec)

add_executable(hello_fiber hello_fiber.cpp)
target_link_libraries(hello_fiber PRIVATE fiberexec::fiberexec)

add_executable(async_pipeline async_pipeline.cpp)
target_link_libraries(async_pipeline PRIVATE fiberexec::fiberexec)

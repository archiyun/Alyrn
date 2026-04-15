include(CTest)

add_executable(memory_pool_smoke_test
    unit/test_memory_pool_smoke.cpp
)

target_include_directories(memory_pool_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME memory_pool_smoke_test COMMAND memory_pool_smoke_test)

add_executable(object_pool_smoke_test
    unit/test_object_pool_smoke.cpp
)

target_include_directories(object_pool_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME object_pool_smoke_test COMMAND object_pool_smoke_test)

add_executable(logger_smoke_test
    unit/test_logger_smoke.cpp
)

target_include_directories(logger_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(logger_smoke_test
    PRIVATE
        runtime_foundation
)

add_test(NAME logger_smoke_test COMMAND logger_smoke_test)

add_executable(buffer_smoke_test
    unit/test_buffer_smoke.cpp
)

target_include_directories(buffer_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(buffer_smoke_test
    PRIVATE
        runtime_net
)

add_test(NAME buffer_smoke_test COMMAND buffer_smoke_test)

add_executable(http_smoke_test
    unit/test_http_smoke.cpp
)

target_include_directories(http_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(http_smoke_test
    PRIVATE
        runtime_http
)

add_test(NAME http_smoke_test COMMAND http_smoke_test)



add_executable(epoll_poller_smoke_test
    unit/test_epoll_poller_smoke.cpp
)

target_include_directories(epoll_poller_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(epoll_poller_smoke_test
    PRIVATE
        runtime_net
)

add_test(NAME epoll_poller_smoke_test COMMAND epoll_poller_smoke_test)

add_executable(event_loop_smoke_test
    integration/test_event_loop_smoke.cpp
)

target_include_directories(event_loop_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(event_loop_smoke_test
    PRIVATE
        runtime_net
)

add_test(NAME event_loop_smoke_test COMMAND event_loop_smoke_test)

add_executable(tcp_server_smoke_test
    integration/test_tcp_server_smoke.cpp
)

target_include_directories(tcp_server_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(tcp_server_smoke_test
    PRIVATE
        runtime_net
)

add_test(NAME tcp_server_smoke_test COMMAND tcp_server_smoke_test)

add_executable(memory_pool_bench
    unit/bench_memory_pool.cpp
)

target_include_directories(memory_pool_bench
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_compile_options(memory_pool_bench PRIVATE -O2)

find_package(GTest CONFIG QUIET)
if(GTest_FOUND)
    include(GoogleTest)

    add_executable(runtime_unit_tests
        unit/test_buffer.cpp
        unit/test_logger.cpp
        unit/test_memory_pool.cpp
        unit/test_object_pool.cpp
        unit/test_sanity.cpp
        unit/test_thread_pool.cpp
        unit/test_scheduler.cpp
    )

    target_link_libraries(runtime_unit_tests
        PRIVATE
            GTest::gtest_main
            runtime_net
            runtime_task
    )

    gtest_discover_tests(runtime_unit_tests)

    add_executable(runtime_integration_tests
        integration/test_event_loop.cpp
        integration/test_tcp_server.cpp
        integration/test_trigger_mode.cpp
        integration/test_http_server.cpp
    )

    target_link_libraries(runtime_integration_tests
        PRIVATE
            GTest::gtest_main
            runtime_net
            runtime_http
    )

    gtest_discover_tests(runtime_integration_tests)
else()
    message(STATUS "GTest not found; skipping gtest-based unit tests.")
endif()

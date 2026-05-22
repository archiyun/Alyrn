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

add_executable(pool_smoke_test
    unit/test_pool_smoke.cpp
)

target_include_directories(pool_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(pool_smoke_test
    PRIVATE
        runtime_foundation
)

add_test(NAME pool_smoke_test COMMAND pool_smoke_test)

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

add_executable(rst_storm_smoke_test
    integration/test_rst_storm_smoke.cpp
)

target_include_directories(rst_storm_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(rst_storm_smoke_test
    PRIVATE
        runtime_net
)

add_test(NAME rst_storm_smoke_test COMMAND rst_storm_smoke_test)
set_tests_properties(rst_storm_smoke_test PROPERTIES TIMEOUT 60)

add_executable(slowloris_smoke_test
    integration/test_slowloris_smoke.cpp
)

target_include_directories(slowloris_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(slowloris_smoke_test
    PRIVATE
        runtime_http
)

add_test(NAME slowloris_smoke_test COMMAND slowloris_smoke_test)
set_tests_properties(slowloris_smoke_test PROPERTIES TIMEOUT 30)

add_executable(rbtree_validator
    unit/test_rbtree_validator.cpp
)

target_include_directories(rbtree_validator
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME rbtree_validator COMMAND rbtree_validator)

add_executable(load_balancer_smoke_test
    unit/test_load_balancer_smoke.cpp
)

target_include_directories(load_balancer_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(load_balancer_smoke_test
    PRIVATE
        runtime_gateway
)

add_test(NAME load_balancer_smoke_test COMMAND load_balancer_smoke_test)

add_executable(health_checker_smoke_test
    unit/test_health_checker_smoke.cpp
)

target_include_directories(health_checker_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(health_checker_smoke_test
    PRIVATE
        runtime_gateway
)

add_test(NAME health_checker_smoke_test COMMAND health_checker_smoke_test)

add_executable(circuit_breaker_smoke_test
    unit/test_circuit_breaker_smoke.cpp
)

target_include_directories(circuit_breaker_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(circuit_breaker_smoke_test
    PRIVATE
        runtime_gateway
)

add_test(NAME circuit_breaker_smoke_test COMMAND circuit_breaker_smoke_test)

add_executable(fallback_config_smoke_test
    unit/test_fallback_config_smoke.cpp
)

target_include_directories(fallback_config_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(fallback_config_smoke_test
    PRIVATE
        runtime_gateway
)

add_test(NAME fallback_config_smoke_test COMMAND fallback_config_smoke_test)

add_executable(rate_limiter_smoke_test
    unit/test_rate_limiter_smoke.cpp
)

target_include_directories(rate_limiter_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(rate_limiter_smoke_test
    PRIVATE
        runtime_gateway
)

add_test(NAME rate_limiter_smoke_test COMMAND rate_limiter_smoke_test)

add_executable(gateway_metrics_smoke_test
    unit/test_gateway_metrics_smoke.cpp
)

target_include_directories(gateway_metrics_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(gateway_metrics_smoke_test
    PRIVATE
        Threads::Threads
)

add_test(NAME gateway_metrics_smoke_test COMMAND gateway_metrics_smoke_test)

add_executable(proxy_e2e_smoke_test
    unit/test_proxy_e2e_smoke.cpp
)

target_include_directories(proxy_e2e_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(proxy_e2e_smoke_test
    PRIVATE
        runtime_gateway
        runtime_http
        runtime_net
)

add_test(NAME proxy_e2e_smoke_test COMMAND proxy_e2e_smoke_test)

add_executable(resilience_integration_smoke_test
    unit/test_resilience_integration_smoke.cpp
)

target_include_directories(resilience_integration_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(resilience_integration_smoke_test
    PRIVATE
        runtime_gateway
)

add_test(NAME resilience_integration_smoke_test COMMAND resilience_integration_smoke_test)

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
        unit/test_scheduler.cpp
    )

    target_link_libraries(runtime_unit_tests
        PRIVATE
            GTest::gtest_main
            runtime_net
            runtime_task
    )

    gtest_discover_tests(runtime_unit_tests)

    set(INTEGRATION_TEST_SOURCES
        integration/test_event_loop.cpp
        integration/test_tcp_server.cpp
        integration/test_trigger_mode.cpp
        integration/test_http_server.cpp
    )

    if(RUNTIME_ENABLE_HTTP2)
        list(APPEND INTEGRATION_TEST_SOURCES integration/test_http2_session.cpp)
    endif()

    add_executable(runtime_integration_tests ${INTEGRATION_TEST_SOURCES})

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

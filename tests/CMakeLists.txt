include(CTest)

add_executable(panic_check_smoke_test
    unit/test_panic_check_smoke.cc
)

target_include_directories(panic_check_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(panic_check_smoke_test
    PRIVATE
        vexo_foundation
)

add_test(NAME panic_check_smoke_test COMMAND panic_check_smoke_test)

add_executable(memory_pool_smoke_test
    unit/test_memory_pool_smoke.cc
)

target_include_directories(memory_pool_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME memory_pool_smoke_test COMMAND memory_pool_smoke_test)

add_executable(object_pool_smoke_test
    unit/test_object_pool_smoke.cc
)

target_include_directories(object_pool_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME object_pool_smoke_test COMMAND object_pool_smoke_test)

# coro 叶子:Task<T> 惰性 + 对称转移 + Spawn/JoinHandle,不绑定网络后端或错误类型。
add_executable(coro_smoke_test
    unit/test_coro_smoke.cc
)

target_include_directories(coro_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(coro_smoke_test PRIVATE vexo_coro vexo_foundation)

add_test(NAME coro_smoke_test COMMAND coro_smoke_test)

add_executable(io_buffer_smoke_test
    unit/test_io_buffer_smoke.cc
)

target_include_directories(io_buffer_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(io_buffer_smoke_test
    PRIVATE
        vexo_io
)

add_test(NAME io_buffer_smoke_test COMMAND io_buffer_smoke_test)

# EventLoop 协程工作源:Schedule + ready_ + 快照 drain。链接 vexo_net(EventLoop 实现)。
add_executable(event_loop_coro_smoke_test
    unit/test_event_loop_coro_smoke.cc
)

target_include_directories(event_loop_coro_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(event_loop_coro_smoke_test
    PRIVATE
        vexo_net
)

add_test(NAME event_loop_coro_smoke_test COMMAND event_loop_coro_smoke_test)

# C3:detached 连接协程(Spawn)park→resume→自毁,无泄漏/无 UAF。链 vexo_net。
add_executable(spawn_c3_smoke_test
    unit/test_spawn_c3_smoke.cc
)

target_include_directories(spawn_c3_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(spawn_c3_smoke_test
    PRIVATE
        vexo_net
)

add_test(NAME spawn_c3_smoke_test COMMAND spawn_c3_smoke_test)

add_executable(pool_smoke_test
    unit/test_pool_smoke.cc
)

target_include_directories(pool_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(pool_smoke_test
    PRIVATE
        vexo_foundation
)

add_test(NAME pool_smoke_test COMMAND pool_smoke_test)

add_executable(pmr_pool_resource_smoke_test
    unit/test_pmr_pool_resource_smoke.cc
)

target_include_directories(pmr_pool_resource_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(pmr_pool_resource_smoke_test
    PRIVATE
        vexo_foundation
)

add_test(NAME pmr_pool_resource_smoke_test COMMAND pmr_pool_resource_smoke_test)

add_executable(logger_smoke_test
    unit/test_logger_smoke.cc
)

target_include_directories(logger_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(logger_smoke_test
    PRIVATE
        vexo_foundation
)

add_test(NAME logger_smoke_test COMMAND logger_smoke_test)

add_executable(buffer_smoke_test
    unit/test_buffer_smoke.cc
)

target_include_directories(buffer_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(buffer_smoke_test
    PRIVATE
        vexo_net
)

add_test(NAME buffer_smoke_test COMMAND buffer_smoke_test)

add_executable(http_smoke_test
    unit/test_http_smoke.cc
)

target_include_directories(http_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(http_smoke_test
    PRIVATE
        vexo_http
)

add_test(NAME http_smoke_test COMMAND http_smoke_test)

add_executable(facade_smoke_test
    unit/test_facade_smoke.cc
)

target_include_directories(facade_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME facade_smoke_test COMMAND facade_smoke_test)



add_executable(epoll_poller_smoke_test
    unit/test_epoll_poller_smoke.cc
)

target_include_directories(epoll_poller_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(epoll_poller_smoke_test
    PRIVATE
        vexo_net
)

add_test(NAME epoll_poller_smoke_test COMMAND epoll_poller_smoke_test)

add_executable(event_loop_smoke_test
    integration/test_event_loop_smoke.cc
)

target_include_directories(event_loop_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(event_loop_smoke_test
    PRIVATE
        vexo_net
)

add_test(NAME event_loop_smoke_test COMMAND event_loop_smoke_test)

add_executable(timer_tree_smoke_test
    unit/test_timer_tree_smoke.cc
)

target_include_directories(timer_tree_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(timer_tree_smoke_test
    PRIVATE
        vexo_foundation
)

add_test(NAME timer_tree_smoke_test COMMAND timer_tree_smoke_test)

add_executable(tcp_server_smoke_test
    integration/test_tcp_server_smoke.cc
)

target_include_directories(tcp_server_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(tcp_server_smoke_test
    PRIVATE
        vexo_net
)

add_test(NAME tcp_server_smoke_test COMMAND tcp_server_smoke_test)

add_executable(rst_storm_smoke_test
    integration/test_rst_storm_smoke.cc
)

target_include_directories(rst_storm_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(rst_storm_smoke_test
    PRIVATE
        vexo_net
)

add_test(NAME rst_storm_smoke_test COMMAND rst_storm_smoke_test)
set_tests_properties(rst_storm_smoke_test PROPERTIES TIMEOUT 60)

add_executable(intrusive_rbtree_clear_smoke_test
    unit/test_intrusive_rbtree_clear_smoke.cc
)

target_include_directories(intrusive_rbtree_clear_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME intrusive_rbtree_clear_smoke_test COMMAND intrusive_rbtree_clear_smoke_test)

add_executable(rbtree_validator
    unit/test_rbtree_validator.cc
)

target_include_directories(rbtree_validator
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME rbtree_validator COMMAND rbtree_validator)

add_executable(splaytree_validator
    unit/test_splaytree_validator.cc
)

target_include_directories(splaytree_validator
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME splaytree_validator COMMAND splaytree_validator)

add_executable(quad_heap_test
    unit/test_quad_heap.cc
)

target_include_directories(quad_heap_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME quad_heap_test COMMAND quad_heap_test)

add_executable(intrusive_list_smoke_test
    unit/test_intrusive_list_smoke.cc
)

target_include_directories(intrusive_list_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME intrusive_list_smoke_test COMMAND intrusive_list_smoke_test)

add_executable(intrusive_queue_smoke_test
    unit/test_intrusive_queue_smoke.cc
)

target_include_directories(intrusive_queue_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME intrusive_queue_smoke_test COMMAND intrusive_queue_smoke_test)

add_executable(intrusive_hash_table_smoke_test
    unit/test_intrusive_hash_table_smoke.cc
)

target_include_directories(intrusive_hash_table_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME intrusive_hash_table_smoke_test COMMAND intrusive_hash_table_smoke_test)

add_executable(load_balancer_smoke_test
    unit/test_load_balancer_smoke.cc
)

target_include_directories(load_balancer_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(load_balancer_smoke_test
    PRIVATE
        vexo_gateway
)

add_test(NAME load_balancer_smoke_test COMMAND load_balancer_smoke_test)

add_executable(health_checker_smoke_test
    unit/test_health_checker_smoke.cc
)

target_include_directories(health_checker_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(health_checker_smoke_test
    PRIVATE
        vexo_gateway
)

add_test(NAME health_checker_smoke_test COMMAND health_checker_smoke_test)

add_executable(circuit_breaker_smoke_test
    unit/test_circuit_breaker_smoke.cc
)

target_include_directories(circuit_breaker_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(circuit_breaker_smoke_test
    PRIVATE
        vexo_gateway
)

add_test(NAME circuit_breaker_smoke_test COMMAND circuit_breaker_smoke_test)

add_executable(fallback_config_smoke_test
    unit/test_fallback_config_smoke.cc
)

target_include_directories(fallback_config_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(fallback_config_smoke_test
    PRIVATE
        vexo_gateway
)

add_test(NAME fallback_config_smoke_test COMMAND fallback_config_smoke_test)

add_executable(rate_limiter_smoke_test
    unit/test_rate_limiter_smoke.cc
)

target_include_directories(rate_limiter_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(rate_limiter_smoke_test
    PRIVATE
        vexo_gateway
)

add_test(NAME rate_limiter_smoke_test COMMAND rate_limiter_smoke_test)

add_executable(gateway_metrics_smoke_test
    unit/test_gateway_metrics_smoke.cc
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
    unit/test_proxy_e2e_smoke.cc
)

target_include_directories(proxy_e2e_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(proxy_e2e_smoke_test
    PRIVATE
        vexo_gateway
        vexo_http
        vexo_net
)

add_test(NAME proxy_e2e_smoke_test COMMAND proxy_e2e_smoke_test)

add_executable(resilience_integration_smoke_test
    unit/test_resilience_integration_smoke.cc
)

target_include_directories(resilience_integration_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(resilience_integration_smoke_test
    PRIVATE
        vexo_gateway
)

add_test(NAME resilience_integration_smoke_test COMMAND resilience_integration_smoke_test)

add_executable(gateway_adversarial_test
    adversarial/test_gateway_resilience_adversarial.cc
)

target_include_directories(gateway_adversarial_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(gateway_adversarial_test
    PRIVATE
        vexo_gateway
        vexo_net
)

add_test(NAME gateway_adversarial_test COMMAND gateway_adversarial_test)
set_tests_properties(gateway_adversarial_test PROPERTIES LABELS "adversarial")

add_executable(health_checker_lifetime_reproducer
    EXCLUDE_FROM_ALL
    adversarial/test_health_checker_lifetime_reproducer.cc
)

target_include_directories(health_checker_lifetime_reproducer
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(health_checker_lifetime_reproducer
    PRIVATE
        vexo_gateway
        vexo_net
)

add_executable(memory_pool_bench
    unit/bench_memory_pool.cc
)

target_include_directories(memory_pool_bench
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_compile_options(memory_pool_bench PRIVATE -O2)

find_package(GTest CONFIG QUIET)
if(GTest_FOUND)
    include(GoogleTest)

    add_executable(vexo_unit_tests
        unit/test_buffer.cc
        unit/test_logger.cc
        unit/test_memory_pool.cc
        unit/test_net_utils.cc
        unit/test_object_pool.cc
        unit/test_sanity.cc
        unit/test_blocking_executor.cc
    )

    target_link_libraries(vexo_unit_tests
        PRIVATE
            GTest::gtest_main
            vexo_net
            vexo_task
    )

    gtest_discover_tests(vexo_unit_tests DISCOVERY_MODE PRE_TEST)

    set(INTEGRATION_TEST_SOURCES
        integration/test_event_loop.cc
        integration/test_tcp_server.cc
        integration/test_trigger_mode.cc
        integration/test_http_server.cc
    )

    add_executable(vexo_integration_tests ${INTEGRATION_TEST_SOURCES})

    target_link_libraries(vexo_integration_tests
        PRIVATE
            GTest::gtest_main
            vexo_net
            vexo_http
    )

    gtest_discover_tests(vexo_integration_tests DISCOVERY_MODE PRE_TEST)
else()
    message(STATUS "GTest not found; skipping gtest-based unit tests.")
endif()

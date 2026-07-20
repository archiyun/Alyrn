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

add_executable(mpsc_bounded_queue_smoke_test
    unit/test_mpsc_bounded_queue_smoke.cc
)

target_include_directories(mpsc_bounded_queue_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME mpsc_bounded_queue_smoke_test COMMAND mpsc_bounded_queue_smoke_test)

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

add_executable(coro_frame_allocator_smoke_test
    unit/test_coro_frame_allocator_smoke.cc
)

target_include_directories(coro_frame_allocator_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(coro_frame_allocator_smoke_test PRIVATE vexo_coro)

add_test(NAME coro_frame_allocator_smoke_test COMMAND coro_frame_allocator_smoke_test)

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

add_executable(reactor_stream_smoke_test
    unit/test_reactor_stream_smoke.cc
)

target_include_directories(reactor_stream_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(reactor_stream_smoke_test
    PRIVATE
        vexo_net
)

add_test(NAME reactor_stream_smoke_test COMMAND reactor_stream_smoke_test)

add_executable(reactor_listener_smoke_test
    unit/test_reactor_listener_smoke.cc
)

target_include_directories(reactor_listener_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(reactor_listener_smoke_test
    PRIVATE
        vexo_net
)

add_test(NAME reactor_listener_smoke_test COMMAND reactor_listener_smoke_test)

if(VEXO_ENABLE_URING)
    add_executable(luring_coro_smoke_test
        unit/test_luring_coro_smoke.cc
    )

    target_include_directories(luring_coro_smoke_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
            ${URING_INCLUDE_DIRS}
    )

    target_link_libraries(luring_coro_smoke_test
        PRIVATE
            vexo_luring
    )

    target_compile_definitions(luring_coro_smoke_test PRIVATE VEXO_ENABLE_URING)

    add_test(NAME luring_coro_smoke_test COMMAND luring_coro_smoke_test)

    add_executable(luring_timer_smoke_test
        unit/test_luring_timer_smoke.cc
    )

    target_include_directories(luring_timer_smoke_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
            ${URING_INCLUDE_DIRS}
    )

    target_link_libraries(luring_timer_smoke_test
        PRIVATE
            vexo_luring
    )

    target_compile_definitions(luring_timer_smoke_test PRIVATE VEXO_ENABLE_URING)

    add_test(NAME luring_timer_smoke_test COMMAND luring_timer_smoke_test)

    add_executable(luring_msg_ring_smoke_test
        unit/test_luring_msg_ring_smoke.cc
    )

    target_include_directories(luring_msg_ring_smoke_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
            ${URING_INCLUDE_DIRS}
    )

    target_link_libraries(luring_msg_ring_smoke_test
        PRIVATE
            vexo_luring
    )

    target_compile_definitions(luring_msg_ring_smoke_test PRIVATE VEXO_ENABLE_URING)

    add_test(NAME luring_msg_ring_smoke_test COMMAND luring_msg_ring_smoke_test)

    add_executable(luring_connector_smoke_test
        unit/test_luring_connector_smoke.cc
    )

    target_include_directories(luring_connector_smoke_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
            ${URING_INCLUDE_DIRS}
    )

    target_link_libraries(luring_connector_smoke_test
        PRIVATE
            vexo_luring
    )

    target_compile_definitions(luring_connector_smoke_test PRIVATE VEXO_ENABLE_URING)

    add_test(NAME luring_connector_smoke_test COMMAND luring_connector_smoke_test)

    add_executable(luring_stream_smoke_test
        unit/test_luring_stream_smoke.cc
    )

    target_include_directories(luring_stream_smoke_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
            ${URING_INCLUDE_DIRS}
    )

    target_link_libraries(luring_stream_smoke_test
        PRIVATE
            vexo_luring
    )

    target_compile_definitions(luring_stream_smoke_test PRIVATE VEXO_ENABLE_URING)

    add_test(NAME luring_stream_smoke_test COMMAND luring_stream_smoke_test)

    add_executable(luring_listener_smoke_test
        unit/test_luring_listener_smoke.cc
    )

    target_include_directories(luring_listener_smoke_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
            ${URING_INCLUDE_DIRS}
    )

    target_link_libraries(luring_listener_smoke_test
        PRIVATE
            vexo_luring
    )

    target_compile_definitions(luring_listener_smoke_test PRIVATE VEXO_ENABLE_URING)

    add_test(NAME luring_listener_smoke_test COMMAND luring_listener_smoke_test)

    add_executable(luring_worker_group_smoke_test
        unit/test_luring_worker_group_smoke.cc
    )

    target_include_directories(luring_worker_group_smoke_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
            ${URING_INCLUDE_DIRS}
    )

    target_link_libraries(luring_worker_group_smoke_test
        PRIVATE
            vexo_luring
    )

    target_compile_definitions(luring_worker_group_smoke_test PRIVATE VEXO_ENABLE_URING)

    add_test(NAME luring_worker_group_smoke_test COMMAND luring_worker_group_smoke_test)

    add_executable(luring_server_smoke_test
        unit/test_luring_server_smoke.cc
    )

    target_include_directories(luring_server_smoke_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
            ${URING_INCLUDE_DIRS}
    )

    target_link_libraries(luring_server_smoke_test
        PRIVATE
            vexo_luring
    )

    target_compile_definitions(luring_server_smoke_test PRIVATE VEXO_ENABLE_URING)

    add_test(NAME luring_server_smoke_test COMMAND luring_server_smoke_test)

    add_executable(luring_gateway_smoke_test
        unit/test_luring_gateway_smoke.cc
    )

    target_include_directories(luring_gateway_smoke_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
            ${URING_INCLUDE_DIRS}
    )

    target_link_libraries(luring_gateway_smoke_test
        PRIVATE
            vexo_gateway
            vexo_luring
    )

    target_compile_definitions(luring_gateway_smoke_test PRIVATE VEXO_ENABLE_URING)

    add_test(NAME luring_gateway_smoke_test COMMAND luring_gateway_smoke_test)
endif()

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

add_executable(http_smoke_test
    unit/test_http_smoke.cc
)

target_include_directories(http_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(http_smoke_test
    PRIVATE
        vexo_http_core
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

add_executable(net_move_smoke_test
    unit/test_net_move_smoke.cc
)

target_include_directories(net_move_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(net_move_smoke_test
    PRIVATE
        vexo_net
)

add_test(NAME net_move_smoke_test COMMAND net_move_smoke_test)

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

add_executable(lru_cache_test
    unit/test_lru_cache.cc
)

target_include_directories(lru_cache_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(lru_cache_test
    PRIVATE
        vexo_foundation
)

add_test(NAME lru_cache_test COMMAND lru_cache_test)

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

add_executable(gateway_core_smoke_test
    unit/test_gateway_core_smoke.cc
)

target_include_directories(gateway_core_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(gateway_core_smoke_test
    PRIVATE
        vexo_gateway_core
)

add_test(NAME gateway_core_smoke_test COMMAND gateway_core_smoke_test)

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
        unit/test_logger.cc
        unit/test_memory_pool.cc
        unit/test_net_utils.cc
        unit/test_object_pool.cc
        unit/test_sanity.cc
    )

    target_link_libraries(vexo_unit_tests
        PRIVATE
            GTest::gtest_main
            vexo_net
    )

    gtest_discover_tests(vexo_unit_tests DISCOVERY_MODE PRE_TEST)

    set(INTEGRATION_TEST_SOURCES
        integration/test_event_loop.cc
    )

    add_executable(vexo_integration_tests ${INTEGRATION_TEST_SOURCES})

    target_link_libraries(vexo_integration_tests
        PRIVATE
            GTest::gtest_main
            vexo_net
    )

    gtest_discover_tests(vexo_integration_tests DISCOVERY_MODE PRE_TEST)
else()
    message(STATUS "GTest not found; skipping gtest-based unit tests.")
endif()

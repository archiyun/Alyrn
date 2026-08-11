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
        coropact_foundation
)

add_test(NAME panic_check_smoke_test COMMAND panic_check_smoke_test)

# Central compile-time seam: every enabled network backend must satisfy the
# same CoreStream/CoreListener/CoreConnector concepts. Runtime lifecycle
# behavior remains covered by the backend-specific smoke tests below.
add_executable(io_contract_smoke_test
    unit/test_io_contracts.cc
)

target_include_directories(io_contract_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
        ${CMAKE_CURRENT_SOURCE_DIR}/contracts
)

target_link_libraries(io_contract_smoke_test
    PRIVATE
        coropact_reactor
)

if(COROPACT_ENABLE_URING)
    target_link_libraries(io_contract_smoke_test PRIVATE coropact_luring)
    target_compile_definitions(io_contract_smoke_test PRIVATE COROPACT_ENABLE_URING)
endif()

add_test(NAME io_contract_smoke_test COMMAND io_contract_smoke_test)

# Runs one application-observable Shutdown/Close contract suite against every
# enabled backend. Backend-specific smoke tests continue to cover physical
# Channel and SQE/CQE interleavings.
add_executable(stream_lifecycle_conformance_test
    integration/test_stream_lifecycle_conformance.cc
)

target_include_directories(stream_lifecycle_conformance_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(stream_lifecycle_conformance_test
    PRIVATE
        coropact_reactor
)

if(COROPACT_ENABLE_URING)
    target_link_libraries(stream_lifecycle_conformance_test PRIVATE coropact_luring)
    target_compile_definitions(stream_lifecycle_conformance_test PRIVATE COROPACT_ENABLE_URING)
endif()

add_test(NAME stream_lifecycle_conformance_test COMMAND stream_lifecycle_conformance_test)

# Runs one application-observable Accept/AcceptSource/Close contract suite
# against every enabled backend. Backend-specific tests retain physical
# readiness and SQE/CQE coverage.
add_executable(listener_lifecycle_conformance_test
    integration/test_listener_lifecycle_conformance.cc
)

target_include_directories(listener_lifecycle_conformance_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(listener_lifecycle_conformance_test
    PRIVATE
        coropact_reactor
)

if(COROPACT_ENABLE_URING)
    target_link_libraries(listener_lifecycle_conformance_test PRIVATE coropact_luring)
    target_compile_definitions(listener_lifecycle_conformance_test PRIVATE COROPACT_ENABLE_URING)
endif()

add_test(NAME listener_lifecycle_conformance_test COMMAND listener_lifecycle_conformance_test)

# Runs one application-observable Connect contract suite against every
# enabled backend. Backend-specific tests retain raw readiness and CQE details.
add_executable(connector_lifecycle_conformance_test
    integration/test_connector_lifecycle_conformance.cc
)

target_include_directories(connector_lifecycle_conformance_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(connector_lifecycle_conformance_test
    PRIVATE
        coropact_reactor
)

if(COROPACT_ENABLE_URING)
    target_link_libraries(connector_lifecycle_conformance_test PRIVATE coropact_luring)
    target_compile_definitions(connector_lifecycle_conformance_test PRIVATE COROPACT_ENABLE_URING)
endif()

add_test(NAME connector_lifecycle_conformance_test COMMAND connector_lifecycle_conformance_test)

add_executable(runtime_builder_smoke_test
    unit/test_runtime_builder_smoke.cc
)

target_link_libraries(runtime_builder_smoke_test
    PRIVATE
        coropact_reactor
)

if(COROPACT_ENABLE_URING)
    target_link_libraries(runtime_builder_smoke_test PRIVATE coropact_luring)
    target_compile_definitions(runtime_builder_smoke_test PRIVATE COROPACT_ENABLE_URING)
endif()

add_test(NAME runtime_builder_smoke_test COMMAND runtime_builder_smoke_test)

add_executable(completion_gate_smoke_test
    unit/test_completion_gate_smoke.cc
)

target_include_directories(completion_gate_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME completion_gate_smoke_test COMMAND completion_gate_smoke_test)

add_executable(reactor_result_state_smoke_test
    unit/test_reactor_result_state_smoke.cc
)

target_include_directories(reactor_result_state_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME reactor_result_state_smoke_test COMMAND reactor_result_state_smoke_test)

add_executable(split_release_lifecycle_smoke_test
    unit/test_split_release_lifecycle_smoke.cc
)

target_include_directories(split_release_lifecycle_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME split_release_lifecycle_smoke_test COMMAND split_release_lifecycle_smoke_test)

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

target_link_libraries(coro_smoke_test PRIVATE coropact_coro coropact_foundation)

add_test(NAME coro_smoke_test COMMAND coro_smoke_test)

add_executable(coro_frame_allocator_smoke_test
    unit/test_coro_frame_allocator_smoke.cc
)

target_include_directories(coro_frame_allocator_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(coro_frame_allocator_smoke_test PRIVATE coropact_coro)

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
        coropact_io_contract
)

add_test(NAME io_buffer_smoke_test COMMAND io_buffer_smoke_test)

add_executable(accept_source_state_smoke_test
    unit/test_accept_source_state_smoke.cc
)

target_include_directories(accept_source_state_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(accept_source_state_smoke_test
    PRIVATE
        coropact_net
)

add_test(NAME accept_source_state_smoke_test COMMAND accept_source_state_smoke_test)

add_executable(recv_source_state_smoke_test
    unit/test_recv_source_state_smoke.cc
)

target_include_directories(recv_source_state_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(recv_source_state_smoke_test
    PRIVATE
        coropact_coro
        coropact_net
)

add_test(NAME recv_source_state_smoke_test COMMAND recv_source_state_smoke_test)

add_executable(stream_lifecycle_smoke_test
    unit/test_stream_lifecycle_smoke.cc
)

target_include_directories(stream_lifecycle_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(stream_lifecycle_smoke_test
    PRIVATE
        coropact_net
)

add_test(NAME stream_lifecycle_smoke_test COMMAND stream_lifecycle_smoke_test)

add_executable(luring_buffer_storage_smoke_test
    unit/test_luring_buffer_storage_smoke.cc
)

target_include_directories(luring_buffer_storage_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(luring_buffer_storage_smoke_test
    PRIVATE
        coropact_foundation
)

add_test(NAME luring_buffer_storage_smoke_test COMMAND luring_buffer_storage_smoke_test)

# C3:detached 连接协程(Spawn)park→resume→自毁,无泄漏/无 UAF。链 coropact_reactor。
add_executable(spawn_c3_smoke_test
    unit/test_spawn_c3_smoke.cc
)

target_include_directories(spawn_c3_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(spawn_c3_smoke_test
    PRIVATE
        coropact_reactor
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
        coropact_reactor
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
        coropact_reactor
)

add_test(NAME reactor_listener_smoke_test COMMAND reactor_listener_smoke_test)

add_executable(reactor_recv_source_smoke_test
    unit/test_reactor_recv_source_smoke.cc
)

target_include_directories(reactor_recv_source_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(reactor_recv_source_smoke_test
    PRIVATE
        coropact_reactor
)

add_test(NAME reactor_recv_source_smoke_test COMMAND reactor_recv_source_smoke_test)

add_executable(reactor_worker_group_smoke_test
    unit/test_reactor_worker_group_smoke.cc
)

target_include_directories(reactor_worker_group_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(reactor_worker_group_smoke_test
    PRIVATE
        coropact_reactor
)

add_test(NAME reactor_worker_group_smoke_test COMMAND reactor_worker_group_smoke_test)

if(COROPACT_ENABLE_URING)
    add_executable(luring_op_smoke_test
        unit/test_luring_op_smoke.cc
    )

    target_include_directories(luring_op_smoke_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
            ${URING_INCLUDE_DIRS}
    )

    target_link_libraries(luring_op_smoke_test
        PRIVATE
            coropact_luring
    )

    target_compile_definitions(luring_op_smoke_test PRIVATE COROPACT_ENABLE_URING)

    add_test(NAME luring_op_smoke_test COMMAND luring_op_smoke_test)

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
            coropact_luring
    )

    target_compile_definitions(luring_coro_smoke_test PRIVATE COROPACT_ENABLE_URING)

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
            coropact_luring
    )

    target_compile_definitions(luring_timer_smoke_test PRIVATE COROPACT_ENABLE_URING)

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
            coropact_luring
    )

    target_compile_definitions(luring_msg_ring_smoke_test PRIVATE COROPACT_ENABLE_URING)

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
            coropact_luring
    )

    target_compile_definitions(luring_connector_smoke_test PRIVATE COROPACT_ENABLE_URING)

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
            coropact_luring
    )

    target_compile_definitions(luring_stream_smoke_test PRIVATE COROPACT_ENABLE_URING)

    add_test(NAME luring_stream_smoke_test COMMAND luring_stream_smoke_test)

    add_executable(luring_move_smoke_test
        unit/test_luring_move_smoke.cc
    )

    target_include_directories(luring_move_smoke_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
            ${URING_INCLUDE_DIRS}
    )

    target_link_libraries(luring_move_smoke_test
        PRIVATE
            coropact_luring
    )

    target_compile_definitions(luring_move_smoke_test PRIVATE COROPACT_ENABLE_URING)

    add_test(NAME luring_move_smoke_test COMMAND luring_move_smoke_test)

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
            coropact_luring
    )

    target_compile_definitions(luring_listener_smoke_test PRIVATE COROPACT_ENABLE_URING)

    add_test(NAME luring_listener_smoke_test COMMAND luring_listener_smoke_test)

    add_executable(luring_multishot_accept_smoke_test
        unit/test_luring_multishot_accept_smoke.cc
    )

    target_include_directories(luring_multishot_accept_smoke_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
            ${URING_INCLUDE_DIRS}
    )

    target_link_libraries(luring_multishot_accept_smoke_test
        PRIVATE
            coropact_luring
    )

    target_compile_definitions(luring_multishot_accept_smoke_test
        PRIVATE
            COROPACT_ENABLE_URING
    )

    add_test(NAME luring_multishot_accept_smoke_test
        COMMAND luring_multishot_accept_smoke_test
    )

    add_executable(luring_recv_source_smoke_test
        unit/test_luring_recv_source_smoke.cc
    )

    target_include_directories(luring_recv_source_smoke_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
            ${URING_INCLUDE_DIRS}
    )

    target_link_libraries(luring_recv_source_smoke_test
        PRIVATE
            coropact_luring
    )

    target_compile_definitions(luring_recv_source_smoke_test PRIVATE COROPACT_ENABLE_URING)

    add_test(NAME luring_recv_source_smoke_test
        COMMAND luring_recv_source_smoke_test
    )

    add_executable(luring_send_zc_smoke_test
        unit/test_luring_send_zc_smoke.cc
    )

    target_include_directories(luring_send_zc_smoke_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
            ${URING_INCLUDE_DIRS}
    )

    target_link_libraries(luring_send_zc_smoke_test
        PRIVATE
            coropact_luring
    )

    target_compile_definitions(luring_send_zc_smoke_test PRIVATE COROPACT_ENABLE_URING)

    add_test(NAME luring_send_zc_smoke_test
        COMMAND luring_send_zc_smoke_test
    )

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
            coropact_luring
    )

    target_compile_definitions(luring_worker_group_smoke_test PRIVATE COROPACT_ENABLE_URING)

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
            coropact_luring
    )

    target_compile_definitions(luring_server_smoke_test PRIVATE COROPACT_ENABLE_URING)

    add_test(NAME luring_server_smoke_test COMMAND luring_server_smoke_test)

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
        coropact_foundation
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
        coropact_foundation
)

add_test(NAME pmr_pool_resource_smoke_test COMMAND pmr_pool_resource_smoke_test)

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
        coropact_reactor
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
        coropact_reactor
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
        coropact_reactor
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
        coropact_foundation
)

add_test(NAME timer_tree_smoke_test COMMAND timer_tree_smoke_test)

add_executable(clock_smoke_test
    unit/test_clock_smoke.cc
)

target_include_directories(clock_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME clock_smoke_test COMMAND clock_smoke_test)

add_executable(timestamp_smoke_test
    unit/test_timestamp_smoke.cc
)

target_include_directories(timestamp_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME timestamp_smoke_test COMMAND timestamp_smoke_test)

add_executable(try_smoke_test
    unit/test_try_smoke.cc
)

target_include_directories(try_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(try_smoke_test
    PRIVATE
        coropact_coro
)

add_test(NAME try_smoke_test COMMAND try_smoke_test)

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

if(BUILD_EXPERIMENTAL_TESTS)
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
endif()

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

find_package(GTest CONFIG QUIET)
if(GTest_FOUND)
    include(GoogleTest)

    add_executable(coropact_unit_tests
        unit/test_net_utils.cc
    )

    target_link_libraries(coropact_unit_tests
        PRIVATE
            GTest::gtest_main
            coropact_net
    )

    gtest_discover_tests(coropact_unit_tests DISCOVERY_MODE PRE_TEST)

else()
    message(STATUS "GTest not found; skipping gtest-based unit tests.")
endif()

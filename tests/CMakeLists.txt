include(CTest)

add_test(NAME namespace_layout_check
    COMMAND python3 ${CMAKE_CURRENT_SOURCE_DIR}/../scripts/check-namespace-layout.py)
set_tests_properties(namespace_layout_check PROPERTIES
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/..)

add_executable(panic_check_smoke_test
    unit/test_panic_check_smoke.cc
)

target_include_directories(panic_check_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(panic_check_smoke_test
    PRIVATE
        alyrn_foundation
)

add_test(NAME panic_check_smoke_test COMMAND panic_check_smoke_test)

add_executable(public_api_surface_smoke_test
    unit/test_public_api_surface.cc
)

target_link_libraries(public_api_surface_smoke_test
    PRIVATE
        alyrn_io
)

target_compile_features(public_api_surface_smoke_test PUBLIC cxx_std_23)

add_test(NAME public_api_surface_smoke_test COMMAND public_api_surface_smoke_test)

# Remaining targets below assume the Linux epoll backend.
if(NOT ALYRN_HAS_EPOLL)
    return()
endif()

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
        alyrn_epoll
)

if(ALYRN_ENABLE_URING)
    target_link_libraries(io_contract_smoke_test PRIVATE alyrn_luring)
    target_compile_definitions(io_contract_smoke_test PRIVATE ALYRN_ENABLE_URING)
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
        alyrn_epoll
)

if(ALYRN_ENABLE_URING)
    target_link_libraries(stream_lifecycle_conformance_test PRIVATE alyrn_luring)
    target_compile_definitions(stream_lifecycle_conformance_test PRIVATE ALYRN_ENABLE_URING)
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
        alyrn_epoll
)

if(ALYRN_ENABLE_URING)
    target_link_libraries(listener_lifecycle_conformance_test PRIVATE alyrn_luring)
    target_compile_definitions(listener_lifecycle_conformance_test PRIVATE ALYRN_ENABLE_URING)
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
        alyrn_epoll
)

if(ALYRN_ENABLE_URING)
    target_link_libraries(connector_lifecycle_conformance_test PRIVATE alyrn_luring)
    target_compile_definitions(connector_lifecycle_conformance_test PRIVATE ALYRN_ENABLE_URING)
endif()

add_test(NAME connector_lifecycle_conformance_test COMMAND connector_lifecycle_conformance_test)

add_executable(runtime_builder_smoke_test
    unit/test_runtime_builder_smoke.cc
)

target_link_libraries(runtime_builder_smoke_test
    PRIVATE
        alyrn_epoll
)

if(ALYRN_ENABLE_URING)
    target_link_libraries(runtime_builder_smoke_test PRIVATE alyrn_luring)
    target_compile_definitions(runtime_builder_smoke_test PRIVATE ALYRN_ENABLE_URING)
endif()

add_test(NAME runtime_builder_smoke_test COMMAND runtime_builder_smoke_test)

add_executable(completion_gate_smoke_test
    unit/test_completion_gate_smoke.cc
)

target_include_directories(completion_gate_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(completion_gate_smoke_test
    PRIVATE
        alyrn_coro
)

add_test(NAME completion_gate_smoke_test COMMAND completion_gate_smoke_test)

add_executable(epoll_result_state_smoke_test
    unit/test_epoll_result_state_smoke.cc
)

target_include_directories(epoll_result_state_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME epoll_result_state_smoke_test COMMAND epoll_result_state_smoke_test)

add_executable(split_release_lifecycle_smoke_test
    unit/test_split_release_lifecycle_smoke.cc
)

target_include_directories(split_release_lifecycle_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME split_release_lifecycle_smoke_test COMMAND split_release_lifecycle_smoke_test)

add_executable(single_result_lifecycle_smoke_test
    unit/test_single_result_lifecycle_smoke.cc
)

target_include_directories(single_result_lifecycle_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME single_result_lifecycle_smoke_test COMMAND single_result_lifecycle_smoke_test)

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

target_link_libraries(coro_smoke_test PRIVATE alyrn_coro alyrn_foundation)

add_test(NAME coro_smoke_test COMMAND coro_smoke_test)

add_executable(coro_channel_smoke_test
    unit/test_coro_channel_smoke.cc
)

target_include_directories(coro_channel_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(coro_channel_smoke_test PRIVATE alyrn_coro alyrn_foundation)

add_test(NAME coro_channel_smoke_test COMMAND coro_channel_smoke_test)

add_executable(coro_frame_allocator_smoke_test
    unit/test_coro_frame_allocator_smoke.cc
)

target_include_directories(coro_frame_allocator_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(coro_frame_allocator_smoke_test PRIVATE alyrn_coro)

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
        alyrn_io_contract
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
        alyrn_net
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
        alyrn_coro
        alyrn_net
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
        alyrn_net
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
        alyrn_foundation
)

add_test(NAME luring_buffer_storage_smoke_test COMMAND luring_buffer_storage_smoke_test)

# C3:detached 连接协程(Spawn)park→resume→自毁,无泄漏/无 UAF。链 alyrn_epoll。
add_executable(spawn_c3_smoke_test
    unit/test_spawn_c3_smoke.cc
)

target_include_directories(spawn_c3_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(spawn_c3_smoke_test
    PRIVATE
        alyrn_epoll
)

add_test(NAME spawn_c3_smoke_test COMMAND spawn_c3_smoke_test)

add_executable(epoll_stream_smoke_test
    unit/test_epoll_stream_smoke.cc
)

target_include_directories(epoll_stream_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(epoll_stream_smoke_test
    PRIVATE
        alyrn_epoll
)

add_test(NAME epoll_stream_smoke_test COMMAND epoll_stream_smoke_test)

add_executable(epoll_listener_smoke_test
    unit/test_epoll_listener_smoke.cc
)

target_include_directories(epoll_listener_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(epoll_listener_smoke_test
    PRIVATE
        alyrn_epoll
)

add_test(NAME epoll_listener_smoke_test COMMAND epoll_listener_smoke_test)

add_executable(epoll_recv_source_smoke_test
    unit/test_epoll_recv_source_smoke.cc
)

target_include_directories(epoll_recv_source_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(epoll_recv_source_smoke_test
    PRIVATE
        alyrn_epoll
)

add_test(NAME epoll_recv_source_smoke_test COMMAND epoll_recv_source_smoke_test)

add_executable(epoll_worker_group_smoke_test
    unit/test_epoll_worker_group_smoke.cc
)

target_include_directories(epoll_worker_group_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(epoll_worker_group_smoke_test
    PRIVATE
        alyrn_epoll
)

add_test(NAME epoll_worker_group_smoke_test COMMAND epoll_worker_group_smoke_test)

if(ALYRN_ENABLE_URING)
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
            alyrn_luring
    )

    target_compile_definitions(luring_op_smoke_test PRIVATE ALYRN_ENABLE_URING)

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
            alyrn_luring
    )

    target_compile_definitions(luring_coro_smoke_test PRIVATE ALYRN_ENABLE_URING)

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
            alyrn_luring
    )

    target_compile_definitions(luring_timer_smoke_test PRIVATE ALYRN_ENABLE_URING)

    add_test(NAME luring_timer_smoke_test COMMAND luring_timer_smoke_test)

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
            alyrn_luring
    )

    target_compile_definitions(luring_connector_smoke_test PRIVATE ALYRN_ENABLE_URING)

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
            alyrn_luring
    )

    target_compile_definitions(luring_stream_smoke_test PRIVATE ALYRN_ENABLE_URING)

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
            alyrn_luring
    )

    target_compile_definitions(luring_move_smoke_test PRIVATE ALYRN_ENABLE_URING)

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
            alyrn_luring
    )

    target_compile_definitions(luring_listener_smoke_test PRIVATE ALYRN_ENABLE_URING)

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
            alyrn_luring
    )

    target_compile_definitions(luring_multishot_accept_smoke_test
        PRIVATE
            ALYRN_ENABLE_URING
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
            alyrn_luring
    )

    target_compile_definitions(luring_recv_source_smoke_test PRIVATE ALYRN_ENABLE_URING)

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
            alyrn_luring
    )

    target_compile_definitions(luring_send_zc_smoke_test PRIVATE ALYRN_ENABLE_URING)

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
            alyrn_luring
    )

    target_compile_definitions(luring_worker_group_smoke_test PRIVATE ALYRN_ENABLE_URING)

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
            alyrn_luring
    )

    target_compile_definitions(luring_server_smoke_test PRIVATE ALYRN_ENABLE_URING)

    add_test(NAME luring_server_smoke_test COMMAND luring_server_smoke_test)

    add_executable(luring_single_shot_echo_smoke_test
        unit/test_luring_single_shot_echo_smoke.cc
    )

    target_include_directories(luring_single_shot_echo_smoke_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
            ${URING_INCLUDE_DIRS}
    )

    target_link_libraries(luring_single_shot_echo_smoke_test
        PRIVATE
            alyrn_luring
    )

    target_compile_definitions(luring_single_shot_echo_smoke_test PRIVATE ALYRN_ENABLE_URING)

    add_test(NAME luring_single_shot_echo_smoke_test
        COMMAND luring_single_shot_echo_smoke_test
    )

endif()



add_executable(epoll_poller_smoke_test
    unit/test_epoll_poller_smoke.cc
)

target_include_directories(epoll_poller_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(epoll_poller_smoke_test
    PRIVATE
        alyrn_epoll
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
        alyrn_epoll
)

add_test(NAME net_move_smoke_test COMMAND net_move_smoke_test)

add_executable(epoll_loop_smoke_test
    integration/test_epoll_loop_smoke.cc
)

target_include_directories(epoll_loop_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(epoll_loop_smoke_test
    PRIVATE
        alyrn_epoll
)

add_test(NAME epoll_loop_smoke_test COMMAND epoll_loop_smoke_test)

add_executable(timer_tree_smoke_test
    unit/test_timer_tree_smoke.cc
)

target_include_directories(timer_tree_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(timer_tree_smoke_test
    PRIVATE
        alyrn_foundation
)

add_test(NAME timer_tree_smoke_test COMMAND timer_tree_smoke_test)

add_executable(timer_index_smoke_test
    unit/test_timer_index_smoke.cc
)

target_include_directories(timer_index_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(timer_index_smoke_test
    PRIVATE
        alyrn_foundation
)

add_test(NAME timer_index_smoke_test COMMAND timer_index_smoke_test)

add_executable(clock_smoke_test
    unit/test_clock_smoke.cc
)

target_include_directories(clock_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_test(NAME clock_smoke_test COMMAND clock_smoke_test)

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

find_package(GTest CONFIG QUIET)
if(GTest_FOUND)
    include(GoogleTest)

    add_executable(alyrn_unit_tests
        unit/test_net_utils.cc
    )

    target_link_libraries(alyrn_unit_tests
        PRIVATE
            GTest::gtest_main
            alyrn_net
    )

    gtest_discover_tests(alyrn_unit_tests DISCOVERY_MODE PRE_TEST)

else()
    message(STATUS "GTest not found; skipping gtest-based unit tests.")
endif()

add_executable(demo_logger_example
    demo_logger.cpp
)

target_link_libraries(demo_logger_example
    PRIVATE
        runtime_foundation
)

add_executable(demo_echo_server
    demo_echo_server.cpp
)

target_link_libraries(demo_echo_server
    PRIVATE
        runtime_net
)


add_executable(demo_http_server
    demo_http_server.cpp
)

target_link_libraries(demo_http_server
    PRIVATE
        runtime_http
        runtime_foundation
)

add_executable(demo_gateway
    demo_gateway.cpp
)

target_link_libraries(demo_gateway
    PRIVATE
        runtime_gateway
)

add_executable(demo_bench_gateway
    demo_bench_gateway.cpp
)

target_link_libraries(demo_bench_gateway
    PRIVATE
        runtime_gateway
)

add_executable(demo_rbtree
    demo_rbtree.cpp
)

target_link_libraries(demo_rbtree
    PRIVATE
        runtime_foundation
)

add_subdirectory(io_uring_echo)



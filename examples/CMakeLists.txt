add_executable(simple_logger_example
    simple_logger.cpp
)

target_link_libraries(simple_logger_example
    PRIVATE
        runtime_foundation
)

add_executable(simple_echo_server
    simple_echo_server.cpp
)

target_link_libraries(simple_echo_server
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

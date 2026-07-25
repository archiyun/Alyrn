# io_uring examples —— 基于 coropact_luring 的示例

if(COROPACT_ENABLE_URING)
    add_executable(demo_luring_raw_echo_server
        demo_luring_raw_echo_server.cc
    )
    target_link_libraries(demo_luring_raw_echo_server
        PRIVATE
            coropact_luring
    )
endif()

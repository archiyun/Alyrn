# io_uring examples —— 基于 coropact_luring 的示例

if(COROPACT_ENABLE_URING)
    add_executable(demo_luring_raw_echo_server
        demo_luring_raw_echo_server.cc
    )
    target_link_libraries(demo_luring_raw_echo_server
        PRIVATE
            coropact_luring
    )

    add_executable(demo_luring_multishot_accept
        demo_luring_multishot_accept.cc
    )
    target_link_libraries(demo_luring_multishot_accept
        PRIVATE
            coropact_luring
    )

    add_executable(demo_bench_http_luring
        demo_bench_http_luring.cc
    )
    target_link_libraries(demo_bench_http_luring
        PRIVATE
            coropact_luring
    )

    add_executable(demo_bench_http_liburing
        demo_bench_http_liburing.cc
    )
    target_include_directories(demo_bench_http_liburing
        PRIVATE
            ${URING_INCLUDE_DIRS}
    )
    target_link_libraries(demo_bench_http_liburing
        PRIVATE
            ${URING_LIBRARIES}
            Threads::Threads
    )
endif()

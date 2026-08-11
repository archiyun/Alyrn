# LUring examples are available only when the io_uring backend is enabled.

add_executable(demo_luring_read_into_echo
    03_read_into_echo.cc
)

target_link_libraries(demo_luring_read_into_echo
    PRIVATE
        coropact_luring
)

add_executable(demo_luring_half_close
    04_half_close.cc
)

target_link_libraries(demo_luring_half_close
    PRIVATE
        coropact_luring
)

add_executable(demo_luring_close_pending_read
    05_close_pending_read.cc
)

target_link_libraries(demo_luring_close_pending_read
    PRIVATE
        coropact_luring
)

add_executable(demo_bench_http_luring
    demo_bench_http_luring.cc
)

target_include_directories(demo_bench_http_luring
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../net
)

target_link_libraries(demo_bench_http_luring
    PRIVATE
        coropact_luring
)

add_executable(demo_bench_http_liburing
    demo_bench_http_liburing.cc
)

target_link_libraries(demo_bench_http_liburing
    PRIVATE
        PkgConfig::URING
        Threads::Threads
)

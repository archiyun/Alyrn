# LUring examples are available only when the io_uring backend is enabled.

add_executable(demo_luring_read_into_echo
    03_read_into_echo.cc
)

target_link_libraries(demo_luring_read_into_echo
    PRIVATE
        coropact_luring
)

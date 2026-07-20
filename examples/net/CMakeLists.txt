# net 层 examples —— 依赖当前 Reactor/io_uring backend
#   demo_bench_timer       : TimerQueue 定时器基准
#   demo_reactor_coro_echo : Reactor 协程回显服务器
#   demo_luring_raw_echo_server: io_uring 回显服务器

add_executable(demo_raw_tcp_bench
    demo_raw_tcp_bench.cc
)

add_executable(demo_bench_timer
    demo_bench_timer.cc
)
target_link_libraries(demo_bench_timer
    PRIVATE
        vexo_net
)

add_executable(demo_reactor_coro_echo
    demo_reactor_coro_echo.cc
)
target_link_libraries(demo_reactor_coro_echo
    PRIVATE
        vexo_net
)

if(VEXO_ENABLE_URING)
    add_executable(demo_luring_raw_echo_server
        demo_luring_raw_echo_server.cc
    )
    target_link_libraries(demo_luring_raw_echo_server
        PRIVATE
            vexo_luring
            vexo_net
    )
endif()

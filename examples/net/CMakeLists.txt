# net 层 examples —— 依赖 runtime_net
#   demo_echo_server : One-Loop-Per-Thread 回显服务器
#   demo_redis_lite  : RESP 协议的极简 Redis（SET/GET/DEL），展示增量解析
#   demo_bench_timer : TimerQueue 定时器基准
#   demo_uring_echo  : io_uring proactor 回显 demo（需 RUNTIME_ENABLE_URING）

add_executable(demo_echo_server
    demo_echo_server.cpp
)
target_link_libraries(demo_echo_server
    PRIVATE
        runtime_net
)

add_executable(demo_redis_lite
    demo_redis_lite.cpp
)
target_link_libraries(demo_redis_lite
    PRIVATE
        runtime_net
        runtime_foundation
)

add_executable(demo_bench_timer
    demo_bench_timer.cpp
)
target_link_libraries(demo_bench_timer
    PRIVATE
        runtime_net
)

if(RUNTIME_ENABLE_URING)
    add_executable(demo_uring_echo
        demo_uring_echo.cpp
    )
    target_link_libraries(demo_uring_echo
        PRIVATE
            runtime_uring
            runtime_foundation
    )
endif()

# net 层 examples —— 依赖 vexo_net
#   demo_echo_server : One-Loop-Per-Thread 回显服务器
#   demo_redis_lite  : RESP 协议的极简 Redis（SET/GET/DEL），展示增量解析
#   demo_bench_timer : TimerQueue 定时器基准

add_executable(demo_echo_server
    demo_echo_server.cc
)
target_link_libraries(demo_echo_server
    PRIVATE
        vexo_net
)

add_executable(demo_redis_lite
    demo_redis_lite.cc
)
target_link_libraries(demo_redis_lite
    PRIVATE
        vexo_net
        vexo_foundation
)

add_executable(demo_bench_timer
    demo_bench_timer.cc
)
target_link_libraries(demo_bench_timer
    PRIVATE
        vexo_net
)

# net 层正式示例。第三方库对比基准位于被忽略的 benchmarks/third_party/。

add_executable(demo_bench_http_epoll
    demo_bench_http_epoll.cc
)
target_link_libraries(demo_bench_http_epoll
    PRIVATE
        alyrn_epoll
)

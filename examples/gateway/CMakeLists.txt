# gateway 层 examples —— 依赖 runtime_gateway
#   demo_gateway             : 反向代理最小示例
#   demo_bench_gateway       : 单上游代理基准
#   demo_bench_gateway_multi : 多上游 / 负载均衡基准

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

add_executable(demo_bench_gateway_multi
    demo_bench_gateway_multi.cpp
)
target_link_libraries(demo_bench_gateway_multi
    PRIVATE
        runtime_gateway
)

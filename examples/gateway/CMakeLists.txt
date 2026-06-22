# gateway 层 examples —— 依赖 vexo_gateway
#   demo_gateway             : 反向代理最小示例
#   demo_bench_gateway       : 单上游代理基准
#   demo_bench_gateway_multi : 多上游 / 负载均衡基准

add_executable(demo_gateway
    demo_gateway.cc
)
target_link_libraries(demo_gateway
    PRIVATE
        vexo_gateway
)

add_executable(demo_bench_gateway
    demo_bench_gateway.cc
)
target_link_libraries(demo_bench_gateway
    PRIVATE
        vexo_gateway
)

add_executable(demo_bench_gateway_multi
    demo_bench_gateway_multi.cc
)
target_link_libraries(demo_bench_gateway_multi
    PRIVATE
        vexo_gateway
)

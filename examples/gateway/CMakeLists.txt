# gateway 层 examples —— 依赖 coropact_gateway
#   demo_gateway             : 反向代理最小示例
#   demo_bench_gateway       : 单上游代理基准
#   demo_bench_gateway_multi : 多上游 / 负载均衡基准

add_executable(demo_gateway
    demo_gateway.cc
)
target_link_libraries(demo_gateway
    PRIVATE
        coropact_gateway
)

if(TARGET coropact_gateway_config)
    add_executable(demo_gateway_config
        demo_gateway_config.cc
    )
    target_link_libraries(demo_gateway_config
        PRIVATE
            coropact_gateway_config
    )
endif()

add_executable(demo_bench_gateway
    demo_bench_gateway.cc
)
target_link_libraries(demo_bench_gateway
    PRIVATE
        coropact_gateway
)

add_executable(demo_bench_gateway_multi
    demo_bench_gateway_multi.cc
)
target_link_libraries(demo_bench_gateway_multi
    PRIVATE
        coropact_gateway
)

if(COROPACT_ENABLE_URING)
    add_executable(demo_bench_gateway_luring
        demo_bench_gateway_luring.cc
    )
    target_link_libraries(demo_bench_gateway_luring
        PRIVATE
            coropact_gateway
            coropact_luring
    )
endif()

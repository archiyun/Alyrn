# examples —— 按 runtime 各层拆分到子目录
#
#   foundation/  → coropact_foundation （log / base 等基础设施）
#   net/         → coropact_net        （地址 / socket 基础能力）
#   reactor/     → coropact_reactor    （epoll / EventLoop / Reactor stream）
#   gateway/     → coropact_gateway    （反向代理 / 基准）
#
# 各子目录自带 CMakeLists.txt，只声明本层的 demo 目标。

add_executable(simple_echo
    simple_echo/reactor_main.cpp
)
target_link_libraries(simple_echo
    PRIVATE
        coropact_reactor
)

if(COROPACT_ENABLE_URING)
    add_executable(simple_echo_luring
        simple_echo/luring_main.cpp
    )
    target_link_libraries(simple_echo_luring
        PRIVATE
            coropact_luring
    )
endif()

add_subdirectory(foundation)
add_subdirectory(net)
add_subdirectory(reactor)
add_subdirectory(gateway)

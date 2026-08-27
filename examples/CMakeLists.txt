# examples —— 按 runtime 各层拆分到子目录
#
#   foundation/  → alyrn_foundation （base 等基础设施）
#   net/         → alyrn_net        （地址 / socket 基础能力）
#   reactor/     → alyrn_reactor    （epoll / Loop / Reactor stream）
#
# 各子目录自带 CMakeLists.txt，只声明本层的 demo 目标。

add_executable(simple_echo
    simple_echo/reactor_main.cpp
)
target_link_libraries(simple_echo
    PRIVATE
        alyrn_reactor
)

# The container image starts this variant. It binds every IPv4 interface so
# Docker's published port is reachable from the host.
add_executable(simple_echo_container
    simple_echo/container_main.cpp
)
target_link_libraries(simple_echo_container
    PRIVATE
        alyrn_reactor
)

if(ALYRN_ENABLE_URING)
    add_executable(simple_echo_luring
        simple_echo/luring_main.cpp
    )
    target_link_libraries(simple_echo_luring
        PRIVATE
            alyrn_luring
    )
endif()

add_subdirectory(foundation)
add_subdirectory(net)
add_subdirectory(reactor)

if(ALYRN_ENABLE_URING)
    add_subdirectory(luring)
endif()

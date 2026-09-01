# Epoll examples —— 基于 epoll/Loop 的示例

add_executable(demo_epoll_coro_echo
    demo_epoll_coro_echo.cc
)
target_link_libraries(demo_epoll_coro_echo
    PRIVATE
        alyrn_epoll
)

add_executable(demo_epoll_recv_echo
    demo_epoll_recv_echo.cc
)

target_link_libraries(demo_epoll_recv_echo
    PRIVATE
        alyrn_epoll
)

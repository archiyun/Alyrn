# Reactor examples —— 基于 epoll/EventLoop 的示例

add_executable(demo_reactor_coro_echo
    demo_reactor_coro_echo.cc
)
target_link_libraries(demo_reactor_coro_echo
    PRIVATE
        coropact_reactor
)

add_executable(demo_reactor_read_into_echo
    demo_reactor_read_into_echo.cc
)

target_link_libraries(demo_reactor_read_into_echo
    PRIVATE
        coropact_reactor
)

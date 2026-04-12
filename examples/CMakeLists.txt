# 临时示例构建入口：
# 这里单独把 simple_logger 接进 CMake，方便作为日志模块的 smoke test。
# 如果后续你不想保留这个测试流程，只需要：
# 1. 删除本文件里的 simple_logger_example 目标
# 2. 或者在顶层 CMakeLists.txt 里去掉 add_subdirectory(examples)
# 3. 未来最小 echo server 也建议继续放在本目录单独加 target

add_executable(simple_logger_example
    simple_logger.cpp
)

target_link_libraries(simple_logger_example
    PRIVATE
        runtime_foundation
)

add_executable(simple_echo_server
    simple_echo_server.cpp
)

target_link_libraries(simple_echo_server
    PRIVATE
        runtime_net
)


add_executable(demo_http_server
    demo_http_server.cpp
)

target_link_libraries(demo_http_server
    PRIVATE
        runtime_http
        runtime_foundation
)

# 把 example 挂进 CTest，便于直接通过 ctest 跑通日志 smoke test。
if(BUILD_TESTS)
    add_test(NAME simple_logger_example_smoke COMMAND simple_logger_example)
endif()

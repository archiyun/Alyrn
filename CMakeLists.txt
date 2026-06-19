cmake_minimum_required(VERSION 3.20) 
project(high_concurrency_runtime 
        VERSION 0.1.0 
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(BUILD_TESTS "Build tests" ON)
option(BUILD_EXAMPLES "Build example programs" ON)
option(RUNTIME_ENABLE_URING "Enable io_uring proactor backend (requires liburing, Linux 5.19+)" OFF)

find_package(Threads REQUIRED)

if(RUNTIME_ENABLE_URING)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(URING REQUIRED liburing)
endif()

# Sanitizer toggle. Empty/OFF = no instrumentation (default). Accepts any
# value the compiler understands after -fsanitize=, e.g.:
#   cmake -B build-tsan -DRUNTIME_SANITIZER=thread
#   cmake -B build-asan -DRUNTIME_SANITIZER=address,undefined
# Applied globally to every target defined below (must precede add_library).
# address and thread are mutually exclusive — pick one build dir per sanitizer.
set(RUNTIME_SANITIZER "" CACHE STRING
    "Build with a sanitizer: address | thread | undefined | address,undefined (empty = off)")

if(RUNTIME_SANITIZER)
    message(STATUS "Building with sanitizer: -fsanitize=${RUNTIME_SANITIZER}")
    add_compile_options(-fsanitize=${RUNTIME_SANITIZER} -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=${RUNTIME_SANITIZER})
endif()

add_library(runtime_foundation STATIC
    src/log/async_logger.cc
    src/log/logger.cc
    src/memory/pool.cc
    src/time/timestamp.cc
    src/time/timer_tree.cc
)

target_include_directories(runtime_foundation
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(runtime_foundation
    PUBLIC
        Threads::Threads
)

target_compile_features(runtime_foundation PUBLIC cxx_std_20)

add_library(runtime_task STATIC
    src/task/thread_pool.cc
    src/task/task.cc
    src/task/task_handle.cc
    src/task/work_queue.cc
    src/task/blocking_executor.cc
)

target_include_directories(runtime_task
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(runtime_task
    PUBLIC
        runtime_foundation
        Threads::Threads
)

target_compile_features(runtime_task PUBLIC cxx_std_20)

set(RUNTIME_BUFFER_IMPL "muduo" CACHE STRING "Buffer implementation: muduo | ringbuf | nginx")

if(RUNTIME_BUFFER_IMPL STREQUAL "ringbuf")
    set(RUNTIME_BUFFER_SRC src/net/buffer_ringbuf.cc)
elseif(RUNTIME_BUFFER_IMPL STREQUAL "nginx")
    set(RUNTIME_BUFFER_SRC src/net/buffer_nginx.cc)
else()
    set(RUNTIME_BUFFER_SRC src/net/buffer_muduo.cc)
endif()

add_library(runtime_net STATIC
    ${RUNTIME_BUFFER_SRC}
    src/net/acceptor.cc
    src/net/channel.cc
    src/net/connector.cc
    src/net/tcp_client.cc
    src/net/epoll_poller.cc
    src/net/event_loop.cc
    src/net/event_loop_thread.cc
    src/net/event_loop_thread_pool.cc
    src/net/inet_address.cc
    src/net/net_utils.cc
    src/net/poller.cc
    src/net/reactor_backend.cc
    src/net/socket.cc
    src/net/tcp_connection.cc
    src/net/tcp_server.cc
    src/net/timer_queue.cc
    src/net/poll_poller.cc
    src/net/select.cc
)

target_include_directories(runtime_net
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(runtime_net
    PUBLIC
        runtime_foundation
        Threads::Threads
)

target_compile_features(runtime_net PUBLIC cxx_std_20)

# memmem 是 GNU 扩展, buffer.h 内联用到, 通过 PUBLIC 让上层库也能 include buffer.h
target_compile_definitions(runtime_net PUBLIC _GNU_SOURCE)

# runtime_uring —— io_uring proactor 后端（已清空，待重写）。
# 重写时在此重新 add_library(runtime_uring STATIC src/uring/*.cc)：
#   PUBLIC link runtime_net（复用 Buffer/InetAddress/Socket）+ ${URING_LIBRARIES}，
#   include ${URING_INCLUDE_DIRS}，
#   target_compile_definitions(runtime_uring PUBLIC RUNTIME_ENABLE_URING)，cxx_std_20。
# 开关 option(RUNTIME_ENABLE_URING)（上方）与 pkg_check_modules(URING) 已保留备用。

set(RUNTIME_HTTP_SOURCES
    src/http/http_types.cc
    src/http/http_request.cc
    src/http/http_context.cc
    src/http/http_response.cc
    src/http/router.cc
    src/http/http_server.cc
    src/http/metrics_handler.cc
)

add_library(runtime_http STATIC ${RUNTIME_HTTP_SOURCES})

target_include_directories(runtime_http
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(runtime_http
    PUBLIC
        runtime_net
        runtime_task
        runtime_foundation
        Threads::Threads
)

target_compile_features(runtime_http PUBLIC cxx_std_20)

add_library(runtime_gateway STATIC
    src/gateway/proxy_pass.cc
    src/gateway/health_checker.cc
    src/gateway/gateway_server.cc
)

target_include_directories(runtime_gateway PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(runtime_gateway PUBLIC
    runtime_http
    runtime_net
    runtime_foundation
    Threads::Threads
)

target_compile_features(runtime_gateway PUBLIC cxx_std_20)


if(BUILD_TESTS)
    enable_testing()
endif()

if(BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(BUILD_TESTS)
    add_subdirectory(tests)
endif()

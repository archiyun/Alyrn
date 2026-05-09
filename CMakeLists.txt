cmake_minimum_required(VERSION 3.20)
project(high_concurrency_runtime VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(BUILD_TESTS "Build tests" ON)
option(BUILD_EXAMPLES "Build example programs" ON)
option(RUNTIME_ENABLE_SSL "Enable TLS/SSL support (requires OpenSSL)" OFF)
option(RUNTIME_ENABLE_HTTP2 "Enable HTTP/2 support (requires nghttp2)" OFF)

find_package(Threads REQUIRED)

if(RUNTIME_ENABLE_SSL OR RUNTIME_ENABLE_HTTP2)
    find_package(OpenSSL REQUIRED)
endif()

if(RUNTIME_ENABLE_HTTP2)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(NGHTTP2 REQUIRED libnghttp2)
endif()

add_library(runtime_foundation STATIC
    src/base/current_thread.cpp
    src/log/async_logger.cpp
    src/log/logger.cpp
    src/time/timestamp.cpp
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
    src/task/thread_pool.cpp
    src/task/task.cpp
    src/task/task_handle.cpp
    src/task/task_history.cpp
    src/task/work_queue.cpp
    src/task/scheduler.cpp
    src/task/timer_scheduler.cpp
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
    set(RUNTIME_BUFFER_SRC src/net/buffer_ringbuf.cpp)
elseif(RUNTIME_BUFFER_IMPL STREQUAL "nginx")
    set(RUNTIME_BUFFER_SRC src/net/buffer_nginx.cpp)
else()
    set(RUNTIME_BUFFER_SRC src/net/buffer_muduo.cpp)
endif()

add_library(runtime_net STATIC
    ${RUNTIME_BUFFER_SRC}
    src/net/acceptor.cpp
    src/net/channel.cpp
    src/net/connector.cpp
    src/net/tcp_client.cpp
    src/net/epoll_poller.cpp
    src/net/event_loop.cpp
    src/net/event_loop_thread.cpp
    src/net/event_loop_thread_pool.cpp
    src/net/inet_address.cpp
    src/net/net_utils.cpp
    src/net/poller.cpp
    src/net/socket.cpp
    src/net/tcp_connection.cpp
    src/net/tcp_server.cpp
    src/net/timer_rbtree.cpp
    src/net/timer_queue.cpp
    src/net/poll_poller.cpp
    src/net/select.cpp
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

if(RUNTIME_ENABLE_SSL)
    target_compile_definitions(runtime_net PUBLIC RUNTIME_ENABLE_SSL)
    target_link_libraries(runtime_net PUBLIC OpenSSL::SSL OpenSSL::Crypto)
endif()

target_compile_features(runtime_net PUBLIC cxx_std_20)

set(RUNTIME_HTTP_SOURCES
    src/http/http_types.cpp
    src/http/http_request.cpp
    src/http/http_context.cpp
    src/http/http_response.cpp
    src/http/router.cpp
    src/http/http_server.cpp
    src/http/metrics_handler.cpp
    src/http/debug_handler.cpp
)

if(RUNTIME_ENABLE_HTTP2)
    list(APPEND RUNTIME_HTTP_SOURCES src/http/http2_session.cpp)
endif()

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

if(RUNTIME_ENABLE_HTTP2)
    target_compile_definitions(runtime_http PUBLIC RUNTIME_ENABLE_HTTP2)
    target_include_directories(runtime_http PUBLIC ${NGHTTP2_INCLUDE_DIRS})
    target_link_libraries(runtime_http PUBLIC ${NGHTTP2_LIBRARIES})
endif()

if(RUNTIME_ENABLE_SSL)
    target_compile_definitions(runtime_http PUBLIC RUNTIME_ENABLE_SSL)
endif()

target_compile_features(runtime_http PUBLIC cxx_std_20)

add_library(runtime_gateway STATIC
    src/gateway/proxy_pass.cpp
    src/gateway/health_checker.cpp
    src/gateway/gateway_server.cpp
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

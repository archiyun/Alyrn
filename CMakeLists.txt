cmake_minimum_required(VERSION 3.20)
project(high_concurrency_vexo
        VERSION 0.1.0
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(BUILD_TESTS "Build tests" ON)
option(BUILD_EXAMPLES "Build example programs" ON)
option(VEXO_ENABLE_URING "Enable io_uring proactor backend (requires liburing, Linux 5.19+)" OFF)
option(VEXO_ENABLE_GATEWAY_YAML_CONFIG "Build YAML gateway config support (requires yaml-cpp)" ON)

find_package(Threads REQUIRED)

if(VEXO_ENABLE_URING)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(URING REQUIRED liburing)
endif()

# Sanitizer toggle. Empty/OFF = no instrumentation (default). Accepts any
# value the compiler understands after -fsanitize=, e.g.:
#   cmake -B build-tsan -DVEXO_SANITIZER=thread
#   cmake -B build-asan -DVEXO_SANITIZER=address,undefined
# Applied globally to every target defined below (must precede add_library).
# address and thread are mutually exclusive — pick one build dir per sanitizer.
set(VEXO_SANITIZER "" CACHE STRING
    "Build with a sanitizer: address | thread | undefined | address,undefined (empty = off)")

if(VEXO_SANITIZER)
    message(STATUS "Building with sanitizer: -fsanitize=${VEXO_SANITIZER}")
    add_compile_options(-fsanitize=${VEXO_SANITIZER} -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=${VEXO_SANITIZER})
endif()

add_library(vexo_foundation STATIC
    src/log/async_logger.cc
    src/log/logger.cc
    src/memory/pool.cc
    src/time/timestamp.cc
)

target_include_directories(vexo_foundation
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(vexo_foundation
    PUBLIC
        Threads::Threads
)

target_compile_features(vexo_foundation PUBLIC cxx_std_23)

# ── Coroutine and IO contracts ──────────────────────────────────────────────
#
# vexo_coro —— backend-neutral 协程原语(Task<T>、Work、Scheduler、Spawn/
# JoinHandle、SyncWait)。Task<T> 不定义错误策略；I/O 层按需返回
# Task<base::Result<T>>。
add_library(vexo_coro INTERFACE)
target_include_directories(vexo_coro INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_features(vexo_coro INTERFACE cxx_std_23)

# vexo_io —— 协程可观察 I/O 契约和启动期 backend/capability profile。
# 这里不包含 fd、Channel、epoll_wait、SQE/CQE 等后端内部状态。
add_library(vexo_io INTERFACE)
target_include_directories(vexo_io INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(vexo_io INTERFACE vexo_coro vexo_foundation)
target_compile_features(vexo_io INTERFACE cxx_std_23)

set(VEXO_BUFFER_IMPL "muduo" CACHE STRING "Buffer implementation: muduo | ringbuf | nginx")

if(VEXO_BUFFER_IMPL STREQUAL "ringbuf")
    set(VEXO_BUFFER_SRC src/net/buffer_ringbuf.cc)
elseif(VEXO_BUFFER_IMPL STREQUAL "nginx")
    set(VEXO_BUFFER_SRC src/net/buffer_nginx.cc)
else()
    set(VEXO_BUFFER_SRC src/net/buffer_muduo.cc)
endif()

add_library(vexo_net STATIC
    ${VEXO_BUFFER_SRC}
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
    src/net/reactor_connect.cc
    src/net/reactor_backend.cc
    src/net/reactor_listener.cc
    src/net/reactor_stream.cc
    src/net/socket.cc
    src/net/tcp_connection.cc
    src/net/tcp_server.cc
    src/net/timer_queue.cc
)

target_include_directories(vexo_net
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(vexo_net
    PUBLIC
        vexo_io
        vexo_foundation
        Threads::Threads
)

target_compile_features(vexo_net PUBLIC cxx_std_23)

# memmem 是 GNU 扩展, buffer.h 内联用到, 通过 PUBLIC 让上层库也能 include buffer.h
target_compile_definitions(vexo_net PUBLIC _GNU_SOURCE)

# vexo_luring —— io_uring proactor 网络模块。它和 vexo_net/reactor 并列，
# 共同解释 vexo_io 的协程可观察 I/O 契约。
if(VEXO_ENABLE_URING)
    add_library(vexo_luring STATIC
        src/luring/capabilities.cc
        src/luring/connector.cc
        src/luring/listener.cc
        src/luring/loop.cc
        src/luring/ring.cc
        src/luring/server.cc
        src/luring/stream.cc
        src/luring/worker.cc
        src/luring/worker_group.cc
    )

    target_include_directories(vexo_luring
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${URING_INCLUDE_DIRS}
    )

    target_link_libraries(vexo_luring
        PUBLIC
            vexo_io
            ${URING_LIBRARIES}
    )

    target_compile_definitions(vexo_luring PUBLIC VEXO_ENABLE_URING)
    target_compile_features(vexo_luring PUBLIC cxx_std_23)
endif()

# vexo_coro_reactor —— 把协程绑到 epoll/reactor 后端,坐在 net 之上(用
# Socket/Channel/EventLoop)。当前是 header-only 骨架;awaitable 的 .cc 落地后
# 改成 add_library(vexo_coro_reactor STATIC src/coro/reactor/*.cc)。
add_library(vexo_coro_reactor INTERFACE)
target_include_directories(vexo_coro_reactor INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(vexo_coro_reactor INTERFACE vexo_coro vexo_net)
target_compile_features(vexo_coro_reactor INTERFACE cxx_std_23)

# vexo_coro_luring —— 把协程绑到 io_uring proactor 后端。头文件经 ring.h 引入
# <liburing.h>,故仅在 VEXO_ENABLE_URING 下构建;待 vexo_uring 复活后改 link 它。
if(VEXO_ENABLE_URING)
    add_library(vexo_coro_luring INTERFACE)
    target_include_directories(vexo_coro_luring INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}/include ${URING_INCLUDE_DIRS})
    target_link_libraries(vexo_coro_luring INTERFACE vexo_luring)
    target_compile_definitions(vexo_coro_luring INTERFACE VEXO_ENABLE_URING)
    target_compile_features(vexo_coro_luring INTERFACE cxx_std_23)
endif()

set(VEXO_HTTP_CORE_SOURCES
    src/http/http_types.cc
    src/http/http_request.cc
    src/http/http_response.cc
    src/http/http_parser.cc
    src/http/parse_status.cc
    src/http/router.cc
)

add_library(vexo_http_core STATIC ${VEXO_HTTP_CORE_SOURCES})

target_include_directories(vexo_http_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(vexo_http_core
    PUBLIC
        vexo_foundation
)

target_compile_features(vexo_http_core PUBLIC cxx_std_23)

set(VEXO_HTTP_SOURCES
    src/http/http_context.cc
    src/http/http_server.cc
)

add_library(vexo_http STATIC ${VEXO_HTTP_SOURCES})

target_include_directories(vexo_http
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(vexo_http
    PUBLIC
        vexo_http_core
        vexo_net
        vexo_foundation
        Threads::Threads
)

target_compile_features(vexo_http PUBLIC cxx_std_23)

add_library(vexo_gateway_core STATIC
    src/gateway/gateway_core.cc
)

target_include_directories(vexo_gateway_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(vexo_gateway_core PUBLIC
    vexo_http_core
    vexo_foundation
    Threads::Threads
)

target_compile_features(vexo_gateway_core PUBLIC cxx_std_23)

add_library(vexo_gateway STATIC
    src/gateway/proxy_pass.cc
    src/gateway/health_checker.cc
)

target_include_directories(vexo_gateway PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(vexo_gateway PUBLIC
    vexo_gateway_core
    vexo_http
    vexo_net
    vexo_foundation
    Threads::Threads
)

target_compile_features(vexo_gateway PUBLIC cxx_std_23)

if(VEXO_ENABLE_GATEWAY_YAML_CONFIG)
    find_package(yaml-cpp QUIET CONFIG)
    if(TARGET yaml-cpp::yaml-cpp)
        set(VEXO_YAML_CPP_TARGET yaml-cpp::yaml-cpp)
    elseif(TARGET yaml-cpp)
        set(VEXO_YAML_CPP_TARGET yaml-cpp)
    else()
        find_package(PkgConfig QUIET)
        if(PkgConfig_FOUND)
            pkg_check_modules(YAML_CPP QUIET IMPORTED_TARGET yaml-cpp)
            if(TARGET PkgConfig::YAML_CPP)
                set(VEXO_YAML_CPP_TARGET PkgConfig::YAML_CPP)
            endif()
        endif()
    endif()

    if(VEXO_YAML_CPP_TARGET)
        add_library(vexo_gateway_config STATIC
            src/gateway/gateway_config_yaml.cc
        )

        target_include_directories(vexo_gateway_config PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/include
        )

        target_link_libraries(vexo_gateway_config
            PUBLIC
                vexo_gateway
            PRIVATE
                ${VEXO_YAML_CPP_TARGET}
        )

        target_compile_features(vexo_gateway_config PUBLIC cxx_std_23)
        target_compile_definitions(vexo_gateway_config PUBLIC VEXO_HAS_GATEWAY_YAML_CONFIG=1)
    else()
        message(WARNING "yaml-cpp was not found; vexo_gateway_config will not be built")
    endif()
endif()


if(BUILD_TESTS)
    enable_testing()
endif()

if(BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(BUILD_TESTS)
    add_subdirectory(tests)
endif()

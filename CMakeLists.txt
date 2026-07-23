cmake_minimum_required(VERSION 3.20)
project(high_concurrency_coropact
        VERSION 0.1.0
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(BUILD_TESTS "Build tests" ON)
option(BUILD_EXAMPLES "Build example programs" ON)
option(COROPACT_ENABLE_URING "Enable io_uring proactor backend (requires liburing, Linux 5.19+)" OFF)
option(COROPACT_ENABLE_GATEWAY_YAML_CONFIG "Build YAML gateway config support (requires yaml-cpp)" ON)
option(COROPACT_ENABLE_CTRACK "Enable optional CTRACK instrumentation" OFF)
set(COROPACT_CTRACK_ROOT "" CACHE PATH "Path to a CTRACK checkout containing include/ctrack.hpp")

find_package(Threads REQUIRED)

if(COROPACT_ENABLE_URING)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(URING REQUIRED liburing)
endif()

# Sanitizer toggle. Empty/OFF = no instrumentation (default). Accepts any
# value the compiler understands after -fsanitize=, e.g.:
#   cmake -B build-tsan -DCOROPACT_SANITIZER=thread
#   cmake -B build-asan -DCOROPACT_SANITIZER=address,undefined
# Applied globally to every target defined below (must precede add_library).
# address and thread are mutually exclusive — pick one build dir per sanitizer.
set(COROPACT_SANITIZER "" CACHE STRING
    "Build with a sanitizer: address | thread | undefined | address,undefined (empty = off)")

if(COROPACT_SANITIZER)
    message(STATUS "Building with sanitizer: -fsanitize=${COROPACT_SANITIZER}")
    add_compile_options(-fsanitize=${COROPACT_SANITIZER} -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=${COROPACT_SANITIZER})
endif()

add_library(coropact_foundation STATIC
    src/log/async_logger.cc
    src/log/logger.cc
    src/time/timestamp.cc
)

target_include_directories(coropact_foundation
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(coropact_foundation
    PUBLIC
        Threads::Threads
)

target_compile_features(coropact_foundation PUBLIC cxx_std_23)

# ── Coroutine and IO contracts ──────────────────────────────────────────────
#
# coropact_coro —— backend-neutral 协程原语(Task<T>、Work、Scheduler、Spawn/
# JoinHandle、SyncWait、可选 FrameAllocatorScope)。Task<T> 不定义错误策略；
# I/O 层按需返回 Task<base::Result<T>>。
add_library(coropact_coro INTERFACE)
target_include_directories(coropact_coro INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_features(coropact_coro INTERFACE cxx_std_23)

# Optional profiling dependency. CTRACK is linked only into targets that use
# the luring instrumentation, and remains absent from normal builds.
add_library(coropact_ctrack INTERFACE)
if(COROPACT_ENABLE_CTRACK)
    if(NOT COROPACT_CTRACK_ROOT OR NOT EXISTS "${COROPACT_CTRACK_ROOT}/include/ctrack.hpp")
        message(FATAL_ERROR
            "COROPACT_ENABLE_CTRACK requires COROPACT_CTRACK_ROOT/include/ctrack.hpp")
    endif()
    target_include_directories(coropact_ctrack SYSTEM INTERFACE
        ${COROPACT_CTRACK_ROOT}/include)
    target_compile_definitions(coropact_ctrack INTERFACE
        COROPACT_ENABLE_CTRACK
        CTRACK_DISABLE_EXECUTION_POLICY)
endif()

# coropact_io —— 协程可观察 I/O 契约和启动期 backend/capability profile。
# 这里不包含 fd、Channel、epoll_wait、SQE/CQE 等后端内部状态。
add_library(coropact_io INTERFACE)
target_include_directories(coropact_io INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(coropact_io INTERFACE coropact_coro coropact_foundation)
target_compile_features(coropact_io INTERFACE cxx_std_23)

add_library(coropact_net STATIC
    src/net/channel.cc
    src/net/epoll_poller.cc
    src/net/event_loop.cc
    src/net/event_loop_scheduler.cc
    src/net/net_utils.cc
    src/net/poller.cc
    src/net/reactor_connect.cc
    src/net/reactor_listener.cc
    src/net/reactor_stream.cc
    src/net/reactor_worker.cc
    src/net/reactor_worker_group.cc
    src/net/socket.cc
    src/net/timer_queue.cc
    src/net/inet_address.cc
)

target_include_directories(coropact_net
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(coropact_net
    PUBLIC
        coropact_io
        coropact_foundation
        Threads::Threads
)

target_compile_features(coropact_net PUBLIC cxx_std_23)

# coropact_luring —— io_uring proactor 网络模块。它和 coropact_net/reactor 并列，
# 共同解释 coropact_io 的协程可观察 I/O 契约。
if(COROPACT_ENABLE_URING)
    add_library(coropact_luring STATIC
        src/luring/capabilities.cc
        src/luring/connector.cc
        src/luring/listener.cc
        src/luring/loop.cc
        src/luring/ring.cc
        src/luring/server.cc
        src/luring/timer_queue.cc
        src/luring/stream.cc
        src/luring/worker.cc
        src/luring/worker_group.cc
    )

    target_include_directories(coropact_luring
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${URING_INCLUDE_DIRS}
    )

    target_link_libraries(coropact_luring
        PUBLIC
            coropact_io
            coropact_ctrack
            ${URING_LIBRARIES}
    )

    target_compile_definitions(coropact_luring PUBLIC COROPACT_ENABLE_URING)
    target_compile_features(coropact_luring PUBLIC cxx_std_23)
endif()

# coropact_coro_reactor —— 把协程绑到 epoll/reactor 后端,坐在 net 之上(用
# Socket/Channel/EventLoop)。当前是 header-only 骨架;awaitable 的 .cc 落地后
# 改成 add_library(coropact_coro_reactor STATIC src/coro/reactor/*.cc)。
add_library(coropact_coro_reactor INTERFACE)
target_include_directories(coropact_coro_reactor INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(coropact_coro_reactor INTERFACE coropact_coro coropact_net)
target_compile_features(coropact_coro_reactor INTERFACE cxx_std_23)

# coropact_coro_luring —— 把协程绑到 io_uring proactor 后端。头文件经 ring.h 引入
# <liburing.h>,故仅在 COROPACT_ENABLE_URING 下构建;待 coropact_uring 复活后改 link 它。
if(COROPACT_ENABLE_URING)
    add_library(coropact_coro_luring INTERFACE)
    target_include_directories(coropact_coro_luring INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}/include ${URING_INCLUDE_DIRS})
    target_link_libraries(coropact_coro_luring INTERFACE coropact_luring)
    target_compile_definitions(coropact_coro_luring INTERFACE COROPACT_ENABLE_URING)
    target_compile_features(coropact_coro_luring INTERFACE cxx_std_23)
endif()

set(COROPACT_HTTP_CORE_SOURCES
    src/http/http_types.cc
    src/http/http_request.cc
    src/http/http_response.cc
    src/http/http_parser.cc
    src/http/parse_status.cc
    src/http/router.cc
)

add_library(coropact_http_core STATIC ${COROPACT_HTTP_CORE_SOURCES})

target_include_directories(coropact_http_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(coropact_http_core
    PUBLIC
        coropact_foundation
)

target_compile_features(coropact_http_core PUBLIC cxx_std_23)

add_library(coropact_gateway_core STATIC
    src/gateway/gateway_core.cc
)

target_include_directories(coropact_gateway_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(coropact_gateway_core PUBLIC
    coropact_http_core
    coropact_foundation
    Threads::Threads
)

target_compile_features(coropact_gateway_core PUBLIC cxx_std_23)

add_library(coropact_gateway STATIC
    src/gateway/proxy_pass.cc
)

target_include_directories(coropact_gateway PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(coropact_gateway PUBLIC
    coropact_gateway_core
    coropact_net
    coropact_foundation
    Threads::Threads
)

target_compile_features(coropact_gateway PUBLIC cxx_std_23)

if(COROPACT_ENABLE_GATEWAY_YAML_CONFIG)
    find_package(yaml-cpp QUIET CONFIG)
    if(TARGET yaml-cpp::yaml-cpp)
        set(COROPACT_YAML_CPP_TARGET yaml-cpp::yaml-cpp)
    elseif(TARGET yaml-cpp)
        set(COROPACT_YAML_CPP_TARGET yaml-cpp)
    else()
        find_package(PkgConfig QUIET)
        if(PkgConfig_FOUND)
            pkg_check_modules(YAML_CPP QUIET IMPORTED_TARGET yaml-cpp)
            if(TARGET PkgConfig::YAML_CPP)
                set(COROPACT_YAML_CPP_TARGET PkgConfig::YAML_CPP)
            endif()
        endif()
    endif()

    if(COROPACT_YAML_CPP_TARGET)
        add_library(coropact_gateway_config STATIC
            src/gateway/gateway_config_yaml.cc
        )

        target_include_directories(coropact_gateway_config PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/include
        )

        target_link_libraries(coropact_gateway_config
            PUBLIC
                coropact_gateway
            PRIVATE
                ${COROPACT_YAML_CPP_TARGET}
        )

        target_compile_features(coropact_gateway_config PUBLIC cxx_std_23)
        target_compile_definitions(coropact_gateway_config PUBLIC COROPACT_HAS_GATEWAY_YAML_CONFIG=1)
    else()
        message(WARNING "yaml-cpp was not found; coropact_gateway_config will not be built")
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

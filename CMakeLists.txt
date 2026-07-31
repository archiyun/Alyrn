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
option(BUILD_BENCHMARKS "Build standalone microbenchmarks" OFF)
option(COROPACT_ENABLE_URING "Enable io_uring proactor backend (requires liburing, Linux 5.19+)" OFF)
option(COROPACT_ENABLE_CTRACK "Enable optional CTRACK instrumentation" OFF)
option(COROPACT_ENABLE_TEST_HOOKS
       "Enable deterministic fault-injection hooks for runtime tests" OFF)
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

add_library(coropact_foundation INTERFACE)

target_include_directories(coropact_foundation
    INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(coropact_foundation
    INTERFACE
        Threads::Threads
)

target_compile_features(coropact_foundation INTERFACE cxx_std_23)

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

# coropact_io_contract —— 后端实现使用的 I/O concepts、Result 算法和语义 profile
# profile。它不包含具体 backend，也不依赖具体 backend。
add_library(coropact_io_contract INTERFACE)
target_include_directories(coropact_io_contract INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(coropact_io_contract INTERFACE coropact_coro coropact_foundation)
target_compile_features(coropact_io_contract INTERFACE cxx_std_23)

# coropact_io —— 面向应用的 I/O facade。具体 backend 在各自 target 定义完成后
# 通过 adapter 注入到这个 facade，backend 核心不反向依赖 facade。
add_library(coropact_io INTERFACE)
target_include_directories(coropact_io INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(coropact_io INTERFACE coropact_io_contract)
target_compile_features(coropact_io INTERFACE cxx_std_23)

add_library(coropact_net STATIC
    src/net/net_utils.cc
    src/net/socket.cc
)

target_include_directories(coropact_net
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(coropact_net
    PUBLIC
        coropact_foundation
        Threads::Threads
)

target_compile_features(coropact_net PUBLIC cxx_std_23)

# I/O contracts expose lower net primitives through lightweight public
# spellings, while remaining independent of concrete backend adapters.
target_link_libraries(coropact_io_contract INTERFACE coropact_net)

# coropact_reactor —— 基于 epoll 的 Reactor 网络后端。它依赖 coropact_net
# 中的地址和 socket 基础类型，但与 io_uring 后端保持并列。
add_library(coropact_reactor STATIC
    src/reactor/channel.cc
    src/reactor/epoll_poller.cc
    src/reactor/event_loop.cc
    src/reactor/event_loop_scheduler.cc
    src/reactor/poller.cc
    src/reactor/reactor_connect.cc
    src/reactor/reactor_listener.cc
    src/reactor/reactor_recv_source.cc
    src/reactor/reactor_stream.cc
    src/reactor/reactor_worker.cc
    src/reactor/reactor_worker_group.cc
    src/reactor/timer_queue.cc
)

target_include_directories(coropact_reactor
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(coropact_reactor
    PUBLIC
        coropact_net
        coropact_coro
        coropact_foundation
        Threads::Threads
)

target_compile_features(coropact_reactor PUBLIC cxx_std_23)

# The facade depends on the concrete Reactor backend only at the composition
# root; the Reactor target itself remains independent of io targets.
target_link_libraries(coropact_io INTERFACE coropact_reactor)

# coropact_luring —— io_uring proactor 网络模块。它与 coropact_reactor 并列，
# 共同解释 coropact_io 的协程可观察 I/O 契约。
if(COROPACT_ENABLE_URING)
    add_library(coropact_luring STATIC
        src/luring/capabilities.cc
        src/luring/connector.cc
        src/luring/listener.cc
        src/luring/recv_source.cc
        src/luring/recv_source_stream.cc
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
            coropact_net
            coropact_io_contract
            coropact_ctrack
            ${URING_LIBRARIES}
    )

    target_compile_definitions(coropact_luring PUBLIC COROPACT_ENABLE_URING)
    if(COROPACT_ENABLE_TEST_HOOKS)
        target_compile_definitions(coropact_luring PUBLIC COROPACT_ENABLE_TEST_HOOKS)
    endif()
    target_compile_features(coropact_luring PUBLIC cxx_std_23)

    # The luring core remains independent of the application facade. Only the
    # separate adapter is added to the higher-level facade below.
    target_link_libraries(coropact_io INTERFACE coropact_luring)

    add_library(coropact_io_luring_adapter STATIC
        src/io/luring_backend.cc
    )
    target_include_directories(coropact_io_luring_adapter
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${URING_INCLUDE_DIRS}
    )
    target_link_libraries(coropact_io_luring_adapter
        PUBLIC
            coropact_io_contract
            coropact_luring
    )
    target_compile_definitions(coropact_io_luring_adapter PUBLIC COROPACT_ENABLE_URING)
    target_compile_features(coropact_io_luring_adapter PUBLIC cxx_std_23)
    target_link_libraries(coropact_io INTERFACE coropact_io_luring_adapter)
endif()

# coropact_coro_reactor —— 把协程绑定到 coropact_reactor 后端。
add_library(coropact_coro_reactor INTERFACE)
target_include_directories(coropact_coro_reactor INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(coropact_coro_reactor INTERFACE coropact_reactor)
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

if(BUILD_TESTS)
    enable_testing()
endif()

if(BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

if(BUILD_TESTS)
    add_subdirectory(tests)
endif()

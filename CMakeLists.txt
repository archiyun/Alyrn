cmake_minimum_required(VERSION 3.20)
project(high_concurrency_coropact
        VERSION 0.1.0
        LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(BUILD_TESTS "Build tests" ON)
option(BUILD_EXPERIMENTAL_TESTS "Build experimental data-structure tests" OFF)
option(BUILD_EXAMPLES "Build example programs" ON)
option(BUILD_BENCHMARKS "Build standalone microbenchmarks" OFF)
option(COROPACT_ENABLE_URING "Enable io_uring proactor backend (requires liburing, Linux 5.19+)" OFF)
option(COROPACT_ENABLE_KQUEUE "Enable kqueue readiness backend (requires a BSD kqueue host)" OFF)
option(COROPACT_ENABLE_KQUEUE_SHIM_TESTS
       "Build kqueue backend tests against an in-memory kqueue shim (developer aid, no BSD host)"
       OFF)
option(COROPACT_STRICT_WARNINGS
       "Treat common GCC/Clang warnings as errors for CoroPact sources" OFF)

find_package(Threads REQUIRED)

set(COROPACT_HAS_EPOLL OFF)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(COROPACT_HAS_EPOLL ON)
endif()

set(COROPACT_HAS_KQUEUE OFF)
if(CMAKE_SYSTEM_NAME MATCHES "^(FreeBSD|NetBSD|OpenBSD|Darwin)$")
    set(COROPACT_HAS_KQUEUE ON)
endif()

if(COROPACT_ENABLE_URING AND NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "COROPACT_ENABLE_URING requires Linux")
endif()

if(COROPACT_ENABLE_KQUEUE AND NOT COROPACT_HAS_KQUEUE)
    message(FATAL_ERROR "COROPACT_ENABLE_KQUEUE requires a BSD or Darwin kqueue host")
endif()

if(COROPACT_ENABLE_URING)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(URING REQUIRED IMPORTED_TARGET liburing>=2.6)
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

# Keep strict diagnostics opt-in for local development and CI experiments.
# Project consumers do not inherit these flags through exported targets.
if(COROPACT_STRICT_WARNINGS)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        add_compile_options(-Wall -Wextra -Wpedantic -Werror)
    else()
        message(WARNING
            "COROPACT_STRICT_WARNINGS is supported only for GCC and Clang")
    endif()
endif()

add_library(coropact_foundation INTERFACE)

target_include_directories(coropact_foundation
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
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
# I/O 层按需返回 Task<Result<T>>。
# Scheduler TLS lives in one translation unit so macOS static links do not see
# duplicate inline thread_local symbols across backend object files.
add_library(coropact_coro STATIC
    src/coro/scheduler.cc
)
target_include_directories(coropact_coro PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_compile_features(coropact_coro PUBLIC cxx_std_23)

# coropact_io_contract —— 后端实现使用的 I/O concepts、Result 算法和语义 profile
# profile。它不包含具体 backend，也不依赖具体 backend。
add_library(coropact_io_contract INTERFACE)
target_include_directories(coropact_io_contract INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(coropact_io_contract INTERFACE coropact_coro coropact_foundation)
target_compile_features(coropact_io_contract INTERFACE cxx_std_23)

# coropact_io —— 面向应用的 I/O facade。它只依赖 backend-neutral contract；
# 最终可执行文件显式选择一个具体 backend，避免传递引入平台实现。
add_library(coropact_io INTERFACE)
target_include_directories(coropact_io INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(coropact_io INTERFACE coropact_io_contract)
target_compile_features(coropact_io INTERFACE cxx_std_23)

# coropact_net —— backend-shared socket/address/buffer value layer.  Its
# implementation is entirely inline so consumers do not pay for a separate
# transport utility library.
add_library(coropact_net INTERFACE)

target_include_directories(coropact_net
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(coropact_net
    INTERFACE
        coropact_foundation
)

target_compile_features(coropact_net INTERFACE cxx_std_23)

# I/O contracts expose lower net primitives through lightweight public
# spellings, while remaining independent of concrete backend adapters.
target_link_libraries(coropact_io_contract INTERFACE coropact_net)

# coropact_reactor —— Linux epoll readiness backend. kqueue remains a parallel
# backend rather than a preprocessor branch inside this implementation.
if(COROPACT_HAS_EPOLL)
    add_library(coropact_reactor STATIC
        src/reactor/channel.cc
        src/reactor/epoll_poller.cc
        src/reactor/event_loop.cc
        src/reactor/poller.cc
        src/reactor/reactor_connect.cc
        src/reactor/reactor_listener.cc
        src/reactor/reactor_recv_source.cc
        src/reactor/reactor_stream.cc
        src/reactor/reactor_worker.cc
        src/reactor/reactor_worker_group.cc
        src/reactor/runtime.cc
        src/reactor/timer_queue.cc
    )

    target_include_directories(coropact_reactor
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    )

    target_link_libraries(coropact_reactor
        PUBLIC
            coropact_net
            coropact_coro
            coropact_foundation
            Threads::Threads
    )

    target_compile_features(coropact_reactor PUBLIC cxx_std_23)
endif()

# coropact_kqueue —— BSD/Darwin kqueue readiness backend. It is parallel to
# the Linux epoll Reactor rather than a conditional implementation of Reactor.
# The initial source files intentionally remain separate while their kqueue
# event semantics are validated; only proven backend-neutral contracts may be
# extracted later.
if(COROPACT_ENABLE_KQUEUE)
    add_library(coropact_kqueue STATIC
        src/kqueue/channel.cc
        src/kqueue/kqueue_connect.cc
        src/kqueue/kqueue_listener.cc
        src/kqueue/kqueue_poller.cc
        src/kqueue/kqueue_recv_source.cc
        src/kqueue/kqueue_stream.cc
        src/kqueue/kqueue_worker.cc
        src/kqueue/kqueue_worker_group.cc
        src/kqueue/loop.cc
        src/kqueue/runtime.cc
        src/kqueue/timer_queue.cc
    )

    target_include_directories(coropact_kqueue
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    )

    target_link_libraries(coropact_kqueue
        PUBLIC
            coropact_net
            coropact_coro
            coropact_foundation
            Threads::Threads
    )

    target_compile_features(coropact_kqueue PUBLIC cxx_std_23)

    add_library(coropact_coro_kqueue INTERFACE)
    target_include_directories(coropact_coro_kqueue INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    )
    target_link_libraries(coropact_coro_kqueue INTERFACE coropact_kqueue)
    target_compile_features(coropact_coro_kqueue INTERFACE cxx_std_23)
endif()

# coropact_luring —— io_uring proactor 网络模块。它与 coropact_reactor 并列，
# 共同解释 coropact_io 的协程可观察 I/O 契约。
if(COROPACT_ENABLE_URING)
    add_library(coropact_luring STATIC
        src/luring/connector.cc
        src/luring/listener.cc
        src/luring/recv_source.cc
        src/luring/loop.cc
        src/luring/ring.cc
        src/luring/runtime.cc
        src/luring/server.cc
        src/luring/timer_queue.cc
        src/luring/stream.cc
        src/luring/worker.cc
        src/luring/worker_group.cc
    )

    target_include_directories(coropact_luring
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    )

    target_link_libraries(coropact_luring
        PUBLIC
            coropact_net
            coropact_io_contract
            PkgConfig::URING
    )

    target_compile_definitions(coropact_luring PUBLIC COROPACT_ENABLE_URING)
    target_compile_features(coropact_luring PUBLIC cxx_std_23)

endif()

# coropact_coro_reactor —— 把协程绑定到 coropact_reactor 后端。
if(COROPACT_HAS_EPOLL)
    add_library(coropact_coro_reactor INTERFACE)
    target_include_directories(coropact_coro_reactor INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    )
    target_link_libraries(coropact_coro_reactor INTERFACE coropact_reactor)
    target_compile_features(coropact_coro_reactor INTERFACE cxx_std_23)
endif()

# coropact_coro_luring —— 把协程绑定到 io_uring proactor 后端。
# It is available only when COROPACT_ENABLE_URING is enabled.
if(COROPACT_ENABLE_URING)
    add_library(coropact_coro_luring INTERFACE)
    target_include_directories(coropact_coro_luring INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
    target_link_libraries(coropact_coro_luring INTERFACE coropact_luring)
    target_compile_definitions(coropact_coro_luring INTERFACE COROPACT_ENABLE_URING)
    target_compile_features(coropact_coro_luring INTERFACE cxx_std_23)
endif()

if(BUILD_TESTS)
    enable_testing()
endif()

if(BUILD_EXAMPLES AND COROPACT_HAS_EPOLL)
    add_subdirectory(examples)
elseif(BUILD_EXAMPLES)
    message(STATUS "Examples are disabled until a native readiness backend is enabled")
endif()

if(BUILD_BENCHMARKS AND COROPACT_HAS_EPOLL)
    add_subdirectory(benchmarks)
elseif(BUILD_BENCHMARKS)
    message(STATUS "Benchmarks are disabled until a native readiness backend is enabled")
endif()

if(BUILD_TESTS AND (COROPACT_HAS_EPOLL OR COROPACT_ENABLE_KQUEUE))
    add_subdirectory(tests)
elseif(BUILD_TESTS)
    message(STATUS "Backend tests are disabled until a native readiness backend is enabled")
endif()

# ── Installation and package metadata ──────────────────────────────────────
# Keep the build-tree and install-tree include paths separate so the exported
# targets remain relocatable after the source tree is removed.
set(COROPACT_INSTALL_CMAKEDIR
    "${CMAKE_INSTALL_LIBDIR}/cmake/CoroPact")
set(COROPACT_PACKAGE_HAS_URING ${COROPACT_ENABLE_URING})

set(COROPACT_INSTALL_TARGETS
    coropact_foundation
    coropact_coro
    coropact_io_contract
    coropact_io
    coropact_net)

if(COROPACT_HAS_EPOLL)
    list(APPEND COROPACT_INSTALL_TARGETS
        coropact_reactor
        coropact_coro_reactor)
endif()

if(COROPACT_ENABLE_KQUEUE)
    list(APPEND COROPACT_INSTALL_TARGETS
        coropact_kqueue
        coropact_coro_kqueue)
endif()

if(COROPACT_ENABLE_URING)
    list(APPEND COROPACT_INSTALL_TARGETS
        coropact_luring
        coropact_coro_luring)
endif()

install(TARGETS ${COROPACT_INSTALL_TARGETS}
    EXPORT CoroPactTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(DIRECTORY include/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT CoroPactTargets
    FILE CoroPactTargets.cmake
    NAMESPACE CoroPact::
    DESTINATION ${COROPACT_INSTALL_CMAKEDIR})

configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/CoroPactConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/CoroPactConfig.cmake
    INSTALL_DESTINATION ${COROPACT_INSTALL_CMAKEDIR}
    PATH_VARS CMAKE_INSTALL_INCLUDEDIR CMAKE_INSTALL_LIBDIR)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/CoroPactConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/CoroPactConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/CoroPactConfigVersion.cmake
    DESTINATION ${COROPACT_INSTALL_CMAKEDIR})

set(CPACK_PACKAGE_NAME "coropact")
set(CPACK_PACKAGE_VENDOR "CoroPact contributors")
set(CPACK_PACKAGE_CONTACT "archiyun/CoroPact maintainers")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY
    "C++23 coroutine networking runtime with Reactor and io_uring backends")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_RESOURCE_FILE_LICENSE ${CMAKE_CURRENT_SOURCE_DIR}/LICENSE)
set(CPACK_GENERATOR "TGZ;DEB")
set(CPACK_PACKAGING_INSTALL_PREFIX "/usr")
if(COROPACT_ENABLE_URING)
    set(CPACK_DEBIAN_PACKAGE_DEPENDS "liburing-dev (>= 2.6)")
endif()
include(CPack)

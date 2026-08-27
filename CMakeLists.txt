cmake_minimum_required(VERSION 3.20)
project(Alyrn
        VERSION 0.1.0
        LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(BUILD_TESTS "Build tests" ON)
option(BUILD_EXAMPLES "Build example programs" ON)
option(BUILD_BENCHMARKS "Build standalone microbenchmarks" OFF)
option(BUILD_FUZZERS "Build libFuzzer targets (requires Clang)" OFF)
option(ALYRN_ENABLE_URING "Enable io_uring proactor backend (requires liburing, Linux 5.19+)" OFF)
option(ALYRN_ENABLE_KQUEUE "Enable kqueue readiness backend (requires a BSD kqueue host)" OFF)
option(ALYRN_ENABLE_KQUEUE_SHIM_TESTS
       "Build kqueue backend tests against an in-memory kqueue shim (developer aid, no BSD host)"
       OFF)
option(ALYRN_STRICT_WARNINGS
       "Treat common GCC/Clang warnings as errors for Alyrn sources" OFF)

find_package(Threads REQUIRED)

set(ALYRN_HAS_EPOLL OFF)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(ALYRN_HAS_EPOLL ON)
endif()

set(ALYRN_HAS_KQUEUE OFF)
if(CMAKE_SYSTEM_NAME MATCHES "^(FreeBSD|NetBSD|OpenBSD|Darwin)$")
    set(ALYRN_HAS_KQUEUE ON)
endif()

if(ALYRN_ENABLE_URING AND NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "ALYRN_ENABLE_URING requires Linux")
endif()

if(ALYRN_ENABLE_KQUEUE AND NOT ALYRN_HAS_KQUEUE)
    message(FATAL_ERROR "ALYRN_ENABLE_KQUEUE requires a BSD or Darwin kqueue host")
endif()

if(ALYRN_ENABLE_URING)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(URING REQUIRED IMPORTED_TARGET liburing>=2.6)
endif()

# Sanitizer toggle. Empty/OFF = no instrumentation (default). Accepts any
# value the compiler understands after -fsanitize=, e.g.:
#   cmake -B build-tsan -DALYRN_SANITIZER=thread
#   cmake -B build-asan -DALYRN_SANITIZER=address,undefined
# Applied globally to every target defined below (must precede add_library).
# address and thread are mutually exclusive — pick one build dir per sanitizer.
set(ALYRN_SANITIZER "" CACHE STRING
    "Build with a sanitizer: address | thread | undefined | address,undefined (empty = off)")

if(ALYRN_SANITIZER)
    message(STATUS "Building with sanitizer: -fsanitize=${ALYRN_SANITIZER}")
    add_compile_options(-fsanitize=${ALYRN_SANITIZER} -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=${ALYRN_SANITIZER})
endif()

# Keep strict diagnostics opt-in for local development and CI experiments.
# Project consumers do not inherit these flags through exported targets.
if(ALYRN_STRICT_WARNINGS)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        add_compile_options(-Wall -Wextra -Wpedantic -Werror)
    else()
        message(WARNING
            "ALYRN_STRICT_WARNINGS is supported only for GCC and Clang")
    endif()
endif()

add_library(alyrn_foundation INTERFACE)

target_include_directories(alyrn_foundation
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(alyrn_foundation
    INTERFACE
        Threads::Threads
)

target_compile_features(alyrn_foundation INTERFACE cxx_std_23)

# ── Coroutine and IO contracts ──────────────────────────────────────────────
#
# alyrn_coro —— backend-neutral 协程原语(Task<T>、Work、Scheduler、Spawn/
# JoinHandle、SyncWait、可选 FrameAllocatorScope)。Task<T> 不定义错误策略；
# I/O 层按需返回 Task<Result<T>>。
# Scheduler TLS lives in one translation unit so macOS static links do not see
# duplicate inline thread_local symbols across backend object files.
add_library(alyrn_coro STATIC
    src/coro/scheduler.cc
)
target_include_directories(alyrn_coro PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_compile_features(alyrn_coro PUBLIC cxx_std_23)

# alyrn_io_contract —— 后端实现使用的 I/O concepts、Result 算法和语义 profile
# profile。它不包含具体 backend，也不依赖具体 backend。
add_library(alyrn_io_contract INTERFACE)
target_include_directories(alyrn_io_contract INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(alyrn_io_contract INTERFACE alyrn_coro alyrn_foundation)
target_compile_features(alyrn_io_contract INTERFACE cxx_std_23)

# alyrn_io —— 面向应用的 I/O facade。它只依赖 backend-neutral contract；
# 最终可执行文件显式选择一个具体 backend，避免传递引入平台实现。
add_library(alyrn_io INTERFACE)
target_include_directories(alyrn_io INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(alyrn_io INTERFACE alyrn_io_contract)
target_compile_features(alyrn_io INTERFACE cxx_std_23)

# alyrn_net —— backend-shared socket/address/buffer value layer.  Its
# implementation is entirely inline so consumers do not pay for a separate
# transport utility library.
add_library(alyrn_net INTERFACE)

target_include_directories(alyrn_net
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(alyrn_net
    INTERFACE
        alyrn_foundation
)

target_compile_features(alyrn_net INTERFACE cxx_std_23)

# I/O contracts expose lower net primitives through lightweight public
# spellings, while remaining independent of concrete backend adapters.
target_link_libraries(alyrn_io_contract INTERFACE alyrn_net)

# alyrn_epoll —— Linux epoll readiness backend. kqueue remains a parallel
# backend rather than a preprocessor branch inside this implementation.
if(ALYRN_HAS_EPOLL)
    add_library(alyrn_epoll STATIC
        src/epoll/channel.cc
        src/epoll/epoll_poller.cc
        src/epoll/loop.cc
        src/epoll/poller.cc
        src/epoll/connector.cc
        src/epoll/listener.cc
        src/epoll/recv_source.cc
        src/epoll/stream.cc
        src/epoll/worker.cc
        src/epoll/worker_group.cc
        src/epoll/runtime.cc
        src/epoll/timer_queue.cc
    )

    target_include_directories(alyrn_epoll
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    )

    target_link_libraries(alyrn_epoll
        PUBLIC
            alyrn_net
            alyrn_coro
            alyrn_foundation
            Threads::Threads
    )

    target_compile_features(alyrn_epoll PUBLIC cxx_std_23)
endif()

# alyrn_kqueue —— BSD/Darwin kqueue readiness backend. It is parallel to
# the Linux epoll backend rather than a conditional implementation shared with
# the other readiness backends.
# The initial source files intentionally remain separate while their kqueue
# event semantics are validated; only proven backend-neutral contracts may be
# extracted later.
if(ALYRN_ENABLE_KQUEUE)
    add_library(alyrn_kqueue STATIC
        src/kqueue/channel.cc
        src/kqueue/connector.cc
        src/kqueue/listener.cc
        src/kqueue/poller.cc
        src/kqueue/recv_source.cc
        src/kqueue/stream.cc
        src/kqueue/worker.cc
        src/kqueue/worker_group.cc
        src/kqueue/loop.cc
        src/kqueue/runtime.cc
        src/kqueue/timer_queue.cc
    )

    target_include_directories(alyrn_kqueue
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    )

    target_link_libraries(alyrn_kqueue
        PUBLIC
            alyrn_net
            alyrn_coro
            alyrn_foundation
            Threads::Threads
    )

    target_compile_features(alyrn_kqueue PUBLIC cxx_std_23)

    add_library(alyrn_coro_kqueue INTERFACE)
    target_include_directories(alyrn_coro_kqueue INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    )
    target_link_libraries(alyrn_coro_kqueue INTERFACE alyrn_kqueue)
    target_compile_features(alyrn_coro_kqueue INTERFACE cxx_std_23)
endif()

# alyrn_luring —— io_uring proactor 网络模块。它与 alyrn_epoll 并列，
# 共同解释 alyrn_io 的协程可观察 I/O 契约。
if(ALYRN_ENABLE_URING)
    add_library(alyrn_luring STATIC
        src/uring/connector.cc
        src/uring/listener.cc
        src/uring/recv_source.cc
        src/uring/loop.cc
        src/uring/ring.cc
        src/uring/runtime.cc
        src/uring/server.cc
        src/uring/timer_queue.cc
        src/uring/stream.cc
        src/uring/worker.cc
        src/uring/worker_group.cc
    )

    target_include_directories(alyrn_luring
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    )

    target_link_libraries(alyrn_luring
        PUBLIC
            alyrn_net
            alyrn_io_contract
            PkgConfig::URING
    )

    target_compile_definitions(alyrn_luring PUBLIC ALYRN_ENABLE_URING)
    target_compile_features(alyrn_luring PUBLIC cxx_std_23)

endif()

# alyrn_coro_epoll —— 把协程绑定到 alyrn_epoll 后端。
if(ALYRN_HAS_EPOLL)
    add_library(alyrn_coro_epoll INTERFACE)
    target_include_directories(alyrn_coro_epoll INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    )
    target_link_libraries(alyrn_coro_epoll INTERFACE alyrn_epoll)
    target_compile_features(alyrn_coro_epoll INTERFACE cxx_std_23)
endif()

# alyrn_coro_luring —— 把协程绑定到 io_uring proactor 后端。
# It is available only when ALYRN_ENABLE_URING is enabled.
if(ALYRN_ENABLE_URING)
    add_library(alyrn_coro_luring INTERFACE)
    target_include_directories(alyrn_coro_luring INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
    target_link_libraries(alyrn_coro_luring INTERFACE alyrn_luring)
    target_compile_definitions(alyrn_coro_luring INTERFACE ALYRN_ENABLE_URING)
    target_compile_features(alyrn_coro_luring INTERFACE cxx_std_23)
endif()

if(BUILD_TESTS)
    enable_testing()
endif()

if(BUILD_EXAMPLES AND ALYRN_HAS_EPOLL)
    add_subdirectory(examples)
elseif(BUILD_EXAMPLES)
    message(STATUS "Examples are disabled until a native readiness backend is enabled")
endif()

if(BUILD_BENCHMARKS AND ALYRN_HAS_EPOLL)
    add_subdirectory(benchmarks)
elseif(BUILD_BENCHMARKS)
    message(STATUS "Benchmarks are disabled until a native readiness backend is enabled")
endif()

if(BUILD_FUZZERS)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR "BUILD_FUZZERS requires Clang and libFuzzer")
    endif()
    if(ALYRN_SANITIZER)
        message(FATAL_ERROR "BUILD_FUZZERS selects its own fuzzer,address,undefined sanitizers")
    endif()
    add_subdirectory(fuzz)
endif()

if(BUILD_TESTS AND (ALYRN_HAS_EPOLL OR ALYRN_ENABLE_KQUEUE))
    add_subdirectory(tests)
elseif(BUILD_TESTS)
    message(STATUS "Backend tests are disabled until a native readiness backend is enabled")
endif()

# ── Installation and package metadata ──────────────────────────────────────
# Keep the build-tree and install-tree include paths separate so the exported
# targets remain relocatable after the source tree is removed.
set(ALYRN_INSTALL_CMAKEDIR
    "${CMAKE_INSTALL_LIBDIR}/cmake/Alyrn")
set(ALYRN_PACKAGE_HAS_URING ${ALYRN_ENABLE_URING})

set(ALYRN_INSTALL_TARGETS
    alyrn_foundation
    alyrn_coro
    alyrn_io_contract
    alyrn_io
    alyrn_net)

if(ALYRN_HAS_EPOLL)
    list(APPEND ALYRN_INSTALL_TARGETS
        alyrn_epoll
        alyrn_coro_epoll)
endif()

if(ALYRN_ENABLE_KQUEUE)
    list(APPEND ALYRN_INSTALL_TARGETS
        alyrn_kqueue
        alyrn_coro_kqueue)
endif()

if(ALYRN_ENABLE_URING)
    list(APPEND ALYRN_INSTALL_TARGETS
        alyrn_luring
        alyrn_coro_luring)
endif()

install(TARGETS ${ALYRN_INSTALL_TARGETS}
    EXPORT AlyrnTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# The source tree contains implementation support headers under
# include/alyrn/detail.  They are installed only as dependencies of inline
# public adapters; callers should include the stable manifest below instead
# of depending on implementation module names such as base, ds, memory, or
# backend.  Keeping this list explicit prevents a new internal header from
# silently becoming part of the package interface.
set(ALYRN_PUBLIC_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/include/alyrn/alyrn.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/alyrn/coro.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/alyrn/io.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/alyrn/net.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/alyrn/result.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/alyrn/runtime.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/alyrn/spawn.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/alyrn/task.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/alyrn/time.h)

if(ALYRN_HAS_EPOLL)
    list(APPEND ALYRN_PUBLIC_HEADERS
        ${CMAKE_CURRENT_SOURCE_DIR}/include/alyrn/epoll.h)
endif()

if(ALYRN_ENABLE_KQUEUE)
    list(APPEND ALYRN_PUBLIC_HEADERS
        ${CMAKE_CURRENT_SOURCE_DIR}/include/alyrn/kqueue.h)
endif()

if(ALYRN_ENABLE_URING)
    list(APPEND ALYRN_PUBLIC_HEADERS
        ${CMAKE_CURRENT_SOURCE_DIR}/include/alyrn/uring.h)
endif()

install(FILES ${ALYRN_PUBLIC_HEADERS}
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/alyrn)

set(ALYRN_PUBLIC_DIRS coro io net time)
if(ALYRN_HAS_EPOLL)
    list(APPEND ALYRN_PUBLIC_DIRS epoll)
endif()
if(ALYRN_ENABLE_KQUEUE)
    list(APPEND ALYRN_PUBLIC_DIRS kqueue)
endif()
if(ALYRN_ENABLE_URING)
    list(APPEND ALYRN_PUBLIC_DIRS uring)
endif()

foreach(ALYRN_PUBLIC_DIR IN LISTS ALYRN_PUBLIC_DIRS)
    install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/alyrn/${ALYRN_PUBLIC_DIR}/
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/alyrn/${ALYRN_PUBLIC_DIR}
        FILES_MATCHING PATTERN "*.h")
endforeach()

# These headers are implementation dependencies of the header-defined
# adapters.  They intentionally have no stable namespace contract.
set(ALYRN_DETAIL_DIRS backend base coro ds memory net operation time utils)
if(ALYRN_HAS_EPOLL)
    list(APPEND ALYRN_DETAIL_DIRS epoll)
endif()
if(ALYRN_ENABLE_KQUEUE)
    list(APPEND ALYRN_DETAIL_DIRS kqueue)
endif()
if(ALYRN_ENABLE_URING)
    list(APPEND ALYRN_DETAIL_DIRS uring)
endif()

foreach(ALYRN_DETAIL_DIR IN LISTS ALYRN_DETAIL_DIRS)
    install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/alyrn/detail/${ALYRN_DETAIL_DIR}/
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/alyrn/detail/${ALYRN_DETAIL_DIR}
        FILES_MATCHING PATTERN "*.h")
endforeach()

install(EXPORT AlyrnTargets
    FILE AlyrnTargets.cmake
    NAMESPACE Alyrn::
    DESTINATION ${ALYRN_INSTALL_CMAKEDIR})

configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/AlyrnConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/AlyrnConfig.cmake
    INSTALL_DESTINATION ${ALYRN_INSTALL_CMAKEDIR}
    PATH_VARS CMAKE_INSTALL_INCLUDEDIR CMAKE_INSTALL_LIBDIR)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/AlyrnConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/AlyrnConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/AlyrnConfigVersion.cmake
    DESTINATION ${ALYRN_INSTALL_CMAKEDIR})

set(CPACK_PACKAGE_NAME "alyrn")
set(CPACK_PACKAGE_VENDOR "Alyrn contributors")
set(CPACK_PACKAGE_CONTACT "archiyun/Alyrn maintainers")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY
    "C++23 coroutine networking runtime with Epoll and io_uring backends")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_RESOURCE_FILE_LICENSE ${CMAKE_CURRENT_SOURCE_DIR}/LICENSE)
set(CPACK_GENERATOR "TGZ;DEB")
set(CPACK_PACKAGING_INSTALL_PREFIX "/usr")
if(ALYRN_ENABLE_URING)
    set(CPACK_DEBIAN_PACKAGE_DEPENDS "liburing-dev (>= 2.6)")
endif()
include(CPack)

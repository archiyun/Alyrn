# net 层 examples —— 网络基础能力和原始 TCP 基准

add_executable(demo_bench_http_reactor
    demo_bench_http_reactor.cc
)
target_link_libraries(demo_bench_http_reactor
    PRIVATE
        coropact_reactor
)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBUV REQUIRED libuv)
pkg_check_modules(LIBEVENT REQUIRED libevent)
pkg_check_modules(LIBEVENT_PTHREADS REQUIRED libevent_pthreads)
find_library(LIBEV_LIBRARY NAMES ev REQUIRED)
find_library(LIBAIO_LIBRARY NAMES aio REQUIRED)

# UV_TCP_REUSEPORT is not present in older distribution libuv headers.  Probe
# the actual header instead of coupling the benchmark to a libuv version.
include(CheckCXXSourceCompiles)
set(_coropact_required_includes "${CMAKE_REQUIRED_INCLUDES}")
set(CMAKE_REQUIRED_INCLUDES ${LIBUV_INCLUDE_DIRS})
check_cxx_source_compiles(
    "#include <uv.h>\nint main() { return UV_TCP_REUSEPORT; }"
    COROPACT_LIBUV_HAS_TCP_REUSEPORT)
set(CMAKE_REQUIRED_INCLUDES "${_coropact_required_includes}")

add_executable(demo_bench_http_libuv
    demo_bench_http_libuv.cc
)
target_include_directories(demo_bench_http_libuv PRIVATE ${LIBUV_INCLUDE_DIRS})
target_link_libraries(demo_bench_http_libuv PRIVATE ${LIBUV_LIBRARIES} Threads::Threads)
if(COROPACT_LIBUV_HAS_TCP_REUSEPORT)
    target_compile_definitions(demo_bench_http_libuv PRIVATE COROPACT_LIBUV_HAS_TCP_REUSEPORT)
else()
    message(STATUS "libuv lacks UV_TCP_REUSEPORT; libuv benchmark will use one worker")
endif()

add_executable(demo_bench_http_libevent
    demo_bench_http_libevent.cc
)
target_include_directories(demo_bench_http_libevent PRIVATE ${LIBEVENT_INCLUDE_DIRS}
    ${LIBEVENT_PTHREADS_INCLUDE_DIRS})
target_link_libraries(demo_bench_http_libevent PRIVATE ${LIBEVENT_LIBRARIES}
    ${LIBEVENT_PTHREADS_LIBRARIES} Threads::Threads)

add_executable(demo_bench_http_libev
    demo_bench_http_libev.cc
)
target_link_libraries(demo_bench_http_libev PRIVATE ${LIBEV_LIBRARY} Threads::Threads)

add_executable(demo_bench_http_libaio
    demo_bench_http_libaio.cc
)
target_link_libraries(demo_bench_http_libaio PRIVATE ${LIBAIO_LIBRARY} Threads::Threads)

set(ASIO_ROOT "/tmp/asio-1-38-2" CACHE PATH "Standalone Asio checkout")
if(EXISTS "${ASIO_ROOT}/include/asio.hpp")
    add_executable(demo_bench_http_asio
        demo_bench_http_asio.cc
    )
    target_include_directories(demo_bench_http_asio PRIVATE ${ASIO_ROOT}/include)
    target_link_libraries(demo_bench_http_asio PRIVATE Threads::Threads)
endif()

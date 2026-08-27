# Standalone benchmarks intentionally do not register with CTest: they are
# workload probes whose output depends on compiler flags and machine state.
add_executable(memory_pool_bench
    bench_memory_pool.cc
)

target_include_directories(memory_pool_bench
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_compile_options(memory_pool_bench PRIVATE -O2)

add_executable(coro_spawn_microbenchmark
    coro_spawn_microbenchmark.cc
)

target_link_libraries(coro_spawn_microbenchmark
    PRIVATE
        alyrn_coro
)

target_compile_definitions(coro_spawn_microbenchmark
    PRIVATE
        ALYRN_ENABLE_SPAWN_STATS
)

target_compile_features(coro_spawn_microbenchmark PRIVATE cxx_std_23)

add_executable(coro_spawn_join_microbenchmark
    coro_spawn_join_microbenchmark.cc
)

target_link_libraries(coro_spawn_join_microbenchmark
    PRIVATE
        alyrn_coro
)

target_compile_definitions(coro_spawn_join_microbenchmark
    PRIVATE
        ALYRN_ENABLE_SPAWN_STATS
)

target_compile_features(coro_spawn_join_microbenchmark PRIVATE cxx_std_23)

add_executable(coro_channel_microbenchmark
    coro_channel_microbenchmark.cc
)

target_link_libraries(coro_channel_microbenchmark
    PRIVATE
        alyrn_coro
        alyrn_foundation
)

target_compile_features(coro_channel_microbenchmark PRIVATE cxx_std_23)

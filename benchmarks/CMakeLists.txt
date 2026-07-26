# Standalone benchmarks intentionally do not register with CTest: they are
# workload probes whose output depends on compiler flags and machine state.
add_executable(coro_spawn_microbenchmark
    coro_spawn_microbenchmark.cc
)

target_link_libraries(coro_spawn_microbenchmark
    PRIVATE
        coropact_coro
)

target_compile_definitions(coro_spawn_microbenchmark
    PRIVATE
        COROPACT_ENABLE_SPAWN_STATS
)

target_compile_features(coro_spawn_microbenchmark PRIVATE cxx_std_23)

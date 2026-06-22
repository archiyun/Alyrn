# Repository Guidelines

## Project Structure & Module Organization

This is a Linux-focused C++20 runtime built with CMake. Public headers live in `include/runtime/`, grouped by layer: `base`, `ds`, `gateway`, `http`, `log`, `memory`, `metrics`, `net`, `task`, and `time`. Implementations live in matching `src/` subdirectories. Examples are under `examples/` by feature area, while tests are split into `tests/unit/`, `tests/integration/`, and `tests/adversarial/`. Project docs and benchmark scripts live in `docs/`; benchmark output is kept under `docs/benchmark/results/`.

## Build, Test, and Development Commands

Configure a normal release build:

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

Useful CMake options include `-DBUILD_TESTS=ON`, `-DBUILD_EXAMPLES=ON`, `-DRUNTIME_BUFFER_IMPL=muduo|ringbuf|nginx`, and `-DRUNTIME_SANITIZER=address,undefined` or `thread`. Run the demo HTTP server with `./build/examples/demo_http_server`; run the gateway with `./build/examples/demo_gateway` after starting backend demo servers.

## Coding Style & Naming Conventions

Use C++20 and keep includes rooted at `runtime/...`. Format C++ code with `.clang-format`, which is based on Google style with a 100-column limit and access modifiers at column 0. Follow existing naming: types use `PascalCase`, functions and methods generally use `PascalCase` or existing local style, private data members use trailing underscores where already established, and files use `snake_case.{h,cc}`. Keep layer dependencies acyclic: gateway depends on HTTP/net/foundation, HTTP depends on net/task/foundation, and lower layers must not include upper-layer headers.

## Testing Guidelines

Run all registered tests with:

```bash
ctest --test-dir build --output-on-failure
```

Run a subset with `ctest --test-dir build -R rbtree_validator --output-on-failure`. Some smoke tests are standalone executables; GoogleTest-based `runtime_unit_tests` and `runtime_integration_tests` are built only when CMake finds GTest. Name new tests `test_<component>.cc` or `test_<component>_smoke.cc`, place them in the matching test directory, and register them in `tests/CMakeLists.txt`.

## Commit & Pull Request Guidelines

Recent history uses Conventional Commit-style subjects such as `feat(gateway): ...`, `fix(gateway): ...`, `refactor(net): ...`, and `chore: ...`. Keep subjects imperative and scoped to the touched layer when possible. Pull requests should describe the behavior change, list validation commands run, link related issues, and call out concurrency, networking, or sanitizer implications. Include screenshots only for documentation or externally visible examples.

## Security & Configuration Tips

Do not commit local build directories, generated `compile_commands.json`, benchmark scratch output, or machine-specific configuration. Use separate build directories for sanitizer variants because `address` and `thread` sanitizers are mutually exclusive.

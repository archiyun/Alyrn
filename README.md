# Alyrn⚡

![C++](https://img.shields.io/badge/C++-23-blue)
![Platform](https://img.shields.io/badge/platform-Linux-lightgrey)
![License](https://img.shields.io/github/license/archiyun/Alyrn)
![Stars](https://img.shields.io/github/stars/archiyun/Alyrn?style=social)

***A C++23 coroutine networking runtime with parallel epoll and io_uring backends.***

Alyrn provides a unified, explicit, and high-performance C++23 coroutine
model over independent networking backends. Its default path hides
event-mechanism details much like a conventional networking library, while
`Runtime` offers Tokio-like server startup without preventing explicit
backend-native configuration and extensions.

The backends are **parallel adapters**, not one implementation with
preprocessor branches:

| Backend | Host | Dispatcher | Multi-worker topology |
|---|---|---|---|
| `epoll` | Linux | `epoll` readiness | Independent listeners with `SO_REUSEPORT` |
| `uring` | Linux | `io_uring` completion | Thread-per-ring Proactor |

Alyrn uses [Lifecycle-Refined Coroutine I/O (LRCI)](docs/design/zh-CN/network/lifecycle-refined-coroutine-io.md): backend events such as readiness notifications and CQEs are not treated directly as coroutine completion. They are refined into a shared logical lifecycle that separately determines result readiness, continuation resumption, and resource release.

* 🔀 **A unified asynchronous I/O contract**
  Each backend keeps its own threading, event-loop, and completion model, but exposes the same application-observable semantics through the `backend` concepts `AsyncStream`, `AsyncListener`, and `AsyncConnector` (application code uses the `io` aliases). `coro` expresses asynchronous control flow in synchronous-looking code while hiding frame, suspension, resumption, and lifetime mechanics; application code need not handle `epoll_event`, SQEs, or CQEs.

* 🧩 **Explicit ownership and completion semantics**
  Each Worker owns its thread, event loop, connections, and I/O operations. Operations complete in their owning execution context and coroutine continuations resume in that same context, with explicit rules for buffer lifetimes, cancellation, and asynchronous close. Coroutine frames are not moved across loops.

* 🚀 **Core operations and native extensions**
  Alyrn provides asynchronous accept, connect, read, write, close, and timers. Epoll can select LT or ET; uring additionally exposes extensions such as multishot receive and zero-copy send. HTTP and gateway policy live in [CoroGateway](https://github.com/archiyun/CoroGateway).

Linux is the CI-validated host. IOCP is not implemented.

## Quick Start

### 1. Choose headers

Applications normally include the backend-neutral modules and one concrete backend:

```cpp
#include "alyrn/coro.h"
#include "alyrn/io.h"
#include "alyrn/net.h"
#include "alyrn/epoll.h"  // Default Linux Epoll backend
```

Include only the modules your application uses.

| Backend | Umbrella header | Runtime tag | CMake option |
|---|---|---|---|
| Epoll | `alyrn/epoll.h` | `runtime::Epoll` | default on Linux |
| uring / io_uring | `alyrn/uring.h` | `runtime::Uring` | `-DALYRN_ENABLE_URING=ON` |

### 2. Write backend-neutral connection code

This echo session depends only on `AsyncStream`, so it works with
`epoll::Stream` and `uring::Stream`. See
[`examples/simple_echo`](examples/simple_echo) for the runnable Linux version.

```cpp
#include <array>
#include <cstddef>
#include <cstdio>
#include <expected>
#include <print>
#include <span>
#include <utility>

namespace cp = alyrn;

template <cp::io::AsyncStream Stream>
auto EchoSession(Stream stream) -> cp::Task<cp::Result<void>> {
  std::array<std::byte, 4096> buffer{};

  for (;;) {
    auto read = co_await stream.Read(buffer);
    if (!read.HasValue()) {
      co_return std::unexpected(read.Error());
    }
    if (*read == 0) {  // EOF
      co_return cp::Result<void>{};
    }

    auto payload = std::span<const std::byte>(buffer.data(), *read);
    auto written = co_await stream.Write(payload);
    if (!written.HasValue()) {
      co_return std::unexpected(written.Error());
    }
  }
}

template <cp::io::AsyncStream Stream>
auto HandleConnection(Stream stream) -> cp::DetachedTask {
  auto result = co_await EchoSession(std::move(stream));
  if (!result.HasValue()) {
    std::println(stderr, "session failed: {}", result.Error().message());
  }
  co_return;
}
```

### 3. Start a Runtime

```cpp
#include <stop_token>

int main() {
  constexpr int kPort = 19090;
  std::stop_source stop_source;

  auto runtime = cp::Runtime::Create<cp::runtime::Epoll>(
      cp::net::Endpoint::Loopback(kPort),
      [](auto stream) { return HandleConnection(std::move(stream)); });

  // A signal handler, management thread, or test later calls request_stop().
  auto result = runtime.Run(stop_source.get_token());
  return result.HasValue() ? 0 : 1;
}
```

For io_uring, build with `ALYRN_ENABLE_URING=ON`, include `alyrn/uring.h`, and change the tag to `cp::runtime::Uring`. The handler's stream remains statically typed as the selected backend type; no virtual call enters the connection data path.

### 4. Configure the default server explicitly

`Create` uses conservative defaults (one worker). Use the same Runtime's backend-specific Builder when worker count needs explicit control:

```cpp
auto runtime = cp::Runtime::Builder<cp::runtime::Epoll>{
                   cp::net::Endpoint::Loopback(19090)}
                   .AutoWorkers()
                   .OnConnection([](auto stream) {
                     return HandleConnection(std::move(stream));
                   })
                   .Build();
```

The backend tag still selects the implementation at compile time. Options that alter backend resources or lifecycle semantics—ring depth, provided buffers, and zero-copy, for example—are not disguised as cross-backend Runtime settings.

`Workers(n)` always means *n threads*. The topology behind that number is backend-specific: Epoll shares the listen port with `SO_REUSEPORT` when `n > 1`; uring keeps one ring per worker.

### 5. Use uring-native capabilities

`Runtime` owns the default TCP server's worker lifecycle; it is not a general io_uring configuration API. It may select safe defaults, such as multishot accept with fallback, but applications that need explicit control of ring depth, SQPOLL, provided-buffer rings, multishot receive, or zero-copy send should compose `uring::Loop`, `Options`, and the relevant listener, stream, or source directly:

```cpp
alyrn::uring::Loop loop;
alyrn::uring::Options options;
options.entries = 8192;
options.shared_buffer_capacity = 256;  // Provided buffers for RecvSource.

auto initialized = loop.Init(options);
// On the loop's owner thread: create listener/source, SpawnDetach(...), then loop.Run(...).
```

This native path makes ownership of each ring, buffer lease, and operation lifecycle explicit. See [`examples/uring`](examples/uring) and the uring public headers. Do not add these capabilities as cross-backend Runtime switches.

## Build

## Run the container demo

The published container runs a TCP echo server based on Alyrn's epoll
backend. Its port is available to the host when published with Docker:

```bash
docker run --rm -p 9090:9090 ghcr.io/archiyun/alyrn:latest
```

In another terminal:

```bash
printf 'hello\n' | nc 127.0.0.1 9090
```

To build the same image from a checkout, run `docker build -t alyrn:local .`.
The image is a runnable demonstration, not a replacement for an application
image that links Alyrn.

### Makefile shortcuts (Linux)

The repository Makefile configures Ninja builds and keeps
`compile_commands.json` pointed at the active build directory for clangd.

```bash
make build                  # configure and build the default Debug epoll build
make test                   # build, then run its tests
make run                    # build, then start examples/simple_echo
make run EXAMPLE=epoll/demo_epoll_coro_echo
                            # build, then start a chosen epoll example
make release                # configure and build Release epoll

# Requires liburing >= 2.6.
make uring                  # configure and build the Debug io_uring backend
make test-uring             # build, then run the io_uring-enabled test suite
make run-uring              # build, then start examples/simple_echo_luring
make run-uring URING_EXAMPLE=uring/demo_luring_recv_echo
                            # build, then start a chosen io_uring example
make uring TYPE=Release     # Release io_uring build
```

Use the CMake commands below when you need to set additional cache options.

Build the default Linux Epoll backend:

```bash
cmake -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=ON \
  -DBUILD_EXAMPLES=ON

cmake --build build -j"$(nproc)"
ctest --test-dir build --output-on-failure
```

For an opt-in strict diagnostic build on GCC or Clang, add
`-DALYRN_STRICT_WARNINGS=ON`.

Build with the io_uring backend enabled:

```bash
# Make sure liburing >= 2.6 is installed first.

cmake -B build-uring \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=ON \
  -DBUILD_EXAMPLES=ON \
  -DALYRN_ENABLE_URING=ON

cmake --build build-uring -j"$(nproc)"
ctest --test-dir build-uring --output-on-failure
```

### CMake options

| Option | Default | Effect |
|---|---|---|
| `ALYRN_ENABLE_URING` | `OFF` | Linux io_uring backend (`liburing >= 2.6`) |
| `ALYRN_STRICT_WARNINGS` | `OFF` | `-Wall -Wextra -Wpedantic -Werror` on GCC/Clang |
| `ALYRN_SANITIZER` | empty | e.g. `address,undefined` or `thread` |
| `BUILD_TESTS` | `ON` | Unit and smoke tests |
| `BUILD_EXAMPLES` | `ON` | Linux examples; disabled until a native readiness backend exists |
| `BUILD_BENCHMARKS` | `OFF` | Standalone microbenchmarks |
| `BUILD_FUZZERS` | `OFF` | Clang/libFuzzer state-machine fuzz targets; selects ASan/UBSan |

### Fuzzing and microbenchmarks

Build the receive-source lifecycle fuzzer with Clang. The target includes
AddressSanitizer and UndefinedBehaviorSanitizer; do not also set
`ALYRN_SANITIZER`:

```bash
CC=clang CXX=clang++ cmake -S . -B build-fuzz -G Ninja \
  -DBUILD_TESTS=OFF -DBUILD_EXAMPLES=OFF -DBUILD_FUZZERS=ON
cmake --build build-fuzz --target recv_source_state_fuzzer -j"$(nproc)"
ASAN_OPTIONS=detect_leaks=0 build-fuzz/fuzz/recv_source_state_fuzzer -runs=100000
```

`detect_leaks=0` is useful only in restricted or ptrace-based environments
where LeakSanitizer cannot inspect processes; leave it enabled in normal CI.

For the owner-thread channel buffer probe:

```bash
cmake -S . -B build-bench -DCMAKE_BUILD_TYPE=Release -DBUILD_BENCHMARKS=ON
cmake --build build-bench --target coro_channel_microbenchmark -j"$(nproc)"
ITERATIONS=1000000 build-bench/benchmarks/coro_channel_microbenchmark
```

### Requirements

* CMake 3.20+ and a compiler with C++23 coroutine support.
* Epoll uses `epoll` and is the default Linux backend. It has no extra networking-library dependency.
* uring requires Linux, `liburing >= 2.6` (Linux 5.19 or newer is recommended).
* `net` and the backend-neutral contracts use portable POSIX socket facilities.

Installable Debian/tarball artifacts and Docker release builds are described
in [Packaging and installation](docs/packaging.md).

## Architecture

```text
Custom Session / Application
               |
               v
 Task<T> + Scheduler + AsyncStream
               |
       Submit -> Suspend
       Complete -> Resume
               |
        +------+------+
        |             |
        v             v
   Epoll/epoll      uring
   alyrn::epoll     alyrn::uring
```

The backends do not share an event loop, and their internal state machines do not need to be identical. They only need to satisfy the same business-observable asynchronous I/O contract. [`docs/SUBSYSTEMS.md`](docs/SUBSYSTEMS.md) is the normative dependency policy.

Epoll multi-worker (`Workers(n>1)`) uses independent listeners on the same port:

```text
WorkerGroup
  |
  +-- Worker 0 -> Thread 0 -> Loop 0 -> listen :port (SO_REUSEPORT)
  +-- Worker 1 -> Thread 1 -> Loop 1 -> listen :port (SO_REUSEPORT)
  `-- Worker N -> Thread N -> Loop N -> listen :port (SO_REUSEPORT)
```

The io_uring server uses a thread-per-ring topology:

```text
Server
  |
  +-- Worker 0 -> Thread 0 -> Loop 0 -> Ring 0
  +-- Worker 1 -> Thread 1 -> Loop 1 -> Ring 1
  `-- Worker N -> Thread N -> Loop N -> Ring N
```

Connections, I/O operations, and coroutine continuations remain owned by the Worker and loop that run them. `Stream` cannot be moved across loops.

## Performance Benchmarks

Alyrn includes reproducible `wrk` benchmarks covering:

* Epoll and io_uring backends
* raw liburing
* standalone Asio
* Monoio
* Compio
* the libaio poll compatibility path
* libuv, libevent, and libev reference adapters
* an Nginx reference configuration

Results depend strongly on the workload and must not be interpreted as a universal ranking of networking frameworks. The latest current-source C++ baseline is the [2026-08-10 network-library baseline](docs/benchmark/network-libraries-20260810.md); it records the `wrk` file-descriptor precondition and invalid samples. The complete ten-target fixed-HTTP comparison, including charts, summary data, per-round data, latency anomalies, CPU usage, memory usage, and error counts, is available in the [unified network-library benchmark report](docs/benchmark/network-libraries.md). Other benchmark scripts, raw results, and optimization records are under [`docs/benchmark`](docs/benchmark/).

## Documentation

The documentation map is [`docs/index.md`](docs/index.md). Design notes are currently written in Chinese; they are the source of truth for contracts and ownership. `CONTEXT.md` is a local developer glossary and is intentionally not distributed.

* **[Networking architecture](docs/design/zh-CN/network/index.md)**: runtime layering, backend boundaries, and ownership models.
* **[Runtime Builder](docs/design/zh-CN/network/runtime-builder.md)**: compile-time backend tags and start/stop lifecycle.
* **[Lifecycle-refined coroutine I/O](docs/design/zh-CN/network/lifecycle-refined-coroutine-io.md)**: logical I/O specification, three authorization boundaries, and epoll / io_uring refinement.
* **[AsyncStream semantics](docs/design/zh-CN/network/async-stream-contract.md)**: read, write, close, cancellation, and buffer-lifetime semantics.
* **[Data structures](docs/design/zh-CN/datastructure/index.md)**: modern C++ intrusive data structures, intrusive red-black trees, intrusive lists, MPSC queues, and their use in the project. QuadHeap is a first-class timer-index adapter injected through `time::TimerIndex`.
* **[Performance benchmarks](docs/benchmark/network-libraries-20260810.md)**: the latest current-source C++ baseline; the broader unified network-library report and supporting material are in [`docs/benchmark`](docs/benchmark/).
* **[Examples](examples/)**: Epoll and io_uring examples on Linux.
* **[Tests](tests/)**: coroutine, networking, lifecycle, and backend validation.

## Current Status

Alyrn is still an experimental networking runtime and is not yet a production-ready replacement for mature networking frameworks.

Current work includes:

* Formal state-machine proofs, invariant tests, and concurrency validation for additional backends.
* Modern liburing networking options and io_uring optimizations.
* More realistic workload benchmarks and bottleneck analysis.

## Contributing

* Please open an [Issue](https://github.com/archiyun/Alyrn/issues) for bugs, questions, or feature requests.
* Pull Requests are welcome: [open a PR](https://github.com/archiyun/Alyrn/pulls).
* This project is released under the [MIT License](LICENSE).

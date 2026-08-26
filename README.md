# CoroPact⚡

![C++](https://img.shields.io/badge/C++-23-blue)
![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20FreeBSD%20%7C%20macOS-lightgrey)
![License](https://img.shields.io/github/license/archiyun/CoroPact)
![Stars](https://img.shields.io/github/stars/archiyun/CoroPact?style=social)

***A C++23 coroutine networking runtime with parallel epoll, io_uring, and kqueue backends.***

CoroPact provides a unified, explicit, and high-performance C++23 coroutine
model over independent networking backends. Its default path hides
event-mechanism details much like a conventional networking library, while
`Runtime` offers Tokio-like server startup without preventing explicit
backend-native configuration and extensions.

The backends are **parallel adapters**, not one implementation with
preprocessor branches:

| Backend | Host | Dispatcher | Multi-worker topology |
|---|---|---|---|
| `reactor` | Linux | `epoll` readiness | Independent listeners with `SO_REUSEPORT` |
| `luring` | Linux | `io_uring` completion | Thread-per-ring Proactor |
| `kqueue` | FreeBSD, NetBSD, OpenBSD, Darwin | `kqueue` readiness | Master-slave: one acceptor, user-space fd handoff |

CoroPact uses [Lifecycle-Refined Coroutine I/O (LRCI)](docs/design/zh-CN/network/lifecycle-refined-coroutine-io.md): backend events such as readiness notifications and CQEs are not treated directly as coroutine completion. They are refined into a shared logical lifecycle that separately determines result readiness, continuation resumption, and resource release.

* 🔀 **A unified asynchronous I/O contract**
  Each backend keeps its own threading, event-loop, and completion model, but exposes the same application-observable semantics through the `io` concepts `AsyncStream`, `AsyncListener`, and `AsyncConnector`. `coro` expresses asynchronous control flow in synchronous-looking code while hiding frame, suspension, resumption, and lifetime mechanics; application code need not handle `epoll_event`, SQEs, CQEs, or `kevent`.

* 🧩 **Explicit ownership and completion semantics**
  Each Worker owns its thread, event loop, connections, and I/O operations. Operations complete in their owning execution context and coroutine continuations resume in that same context, with explicit rules for buffer lifetimes, cancellation, and asynchronous close. Coroutine frames are not moved across loops.

* 🚀 **Core operations and native extensions**
  CoroPact provides asynchronous accept, connect, read, write, close, and timers. Reactor can select LT or ET; kqueue currently ships one-shot readiness as the stream mode; luring additionally exposes extensions such as multishot receive and zero-copy send. HTTP and gateway policy live in [CoroGateway](https://github.com/archiyun/CoroGateway).

Linux is the CI-validated host for Reactor and the optional io_uring backend. kqueue is implemented as a third adapter on BSD and Darwin; Linux can compile its loop/poller tests against an in-memory shim, which does not replace a native `kevent` host. IOCP is not implemented.

## Quick Start

### 1. Choose headers

Applications normally include the backend-neutral modules and one concrete backend:

```cpp
#include "coropact/coro.h"
#include "coropact/io.h"
#include "coropact/net.h"
#include "coropact/reactor.h"  // Default Linux Reactor backend
```

Include only the modules your application uses.

| Backend | Umbrella header | Runtime tag | CMake option |
|---|---|---|---|
| Reactor / epoll | `coropact/reactor.h` | `runtime::Reactor` | default on Linux |
| luring / io_uring | `coropact/luring.h` | `runtime::LUring` | `-DCOROPACT_ENABLE_URING=ON` |
| kqueue | `coropact/kqueue.h` | `runtime::Kqueue` | `-DCOROPACT_ENABLE_KQUEUE=ON` |

The kqueue umbrella header is rejected at compile time on non-BSD hosts.

### 2. Write backend-neutral connection code

This echo session depends only on `AsyncStream`, so it works with
`ReactorStream`, `LUringStream`, and `KqueueStream`. See
[`examples/simple_echo`](examples/simple_echo) for the runnable Linux version.

```cpp
#include <array>
#include <cstddef>
#include <cstdio>
#include <expected>
#include <print>
#include <span>
#include <utility>

namespace cp = coropact;

template <cp::io::AsyncStream Stream>
auto EchoSession(Stream stream) -> cp::coro::Task<cp::Result<void>> {
  std::array<std::byte, 4096> buffer{};
  cp::Result<void> session_result{};

  for (;;) {
    auto read = co_await stream.ReadSome(buffer);
    if (!read.has_value()) {
      session_result = std::unexpected(read.error());
      break;
    }
    if (*read == 0) {  // EOF
      break;
    }

    auto payload = std::span<const std::byte>(buffer.data(), *read);
    auto written = co_await stream.WriteAll(payload);
    if (!written.has_value()) {
      session_result = std::unexpected(written.error());
      break;
    }
  }

  auto closed = co_await stream.Close();
  if (!closed.has_value()) {
    if (session_result.has_value()) {
      session_result = std::unexpected(closed.error());
    } else {
      std::println(stderr, "close failed: {}", closed.error().message());
    }
  }
  co_return session_result;
}

template <cp::io::AsyncStream Stream>
auto HandleConnection(Stream stream) -> cp::coro::DetachedTask {
  auto result = co_await EchoSession(std::move(stream));
  if (!result.has_value()) {
    std::println(stderr, "session failed: {}", result.error().message());
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

  auto runtime = cp::Runtime::Create<cp::runtime::Reactor>(
      cp::net::Endpoint::Loopback(kPort),
      [](auto stream) { return HandleConnection(std::move(stream)); });

  // A signal handler, management thread, or test later calls request_stop().
  auto result = runtime.Run(stop_source.get_token());
  return result.has_value() ? 0 : 1;
}
```

For io_uring, build with `COROPACT_ENABLE_URING=ON`, include `coropact/luring.h`, and change the tag to `cp::runtime::LUring`. On a kqueue host, include `coropact/kqueue.h` and use `cp::runtime::Kqueue`. The handler's stream remains statically typed as the selected backend type; no virtual call enters the connection data path.

### 4. Configure the default server explicitly

`Create` uses conservative defaults (one worker). Use the same Runtime's backend-specific Builder when worker count needs explicit control:

```cpp
auto runtime = cp::Runtime::Builder<cp::runtime::Reactor>{
                   cp::net::Endpoint::Loopback(19090)}
                   .AutoWorkers()
                   .OnConnection([](auto stream) {
                     return HandleConnection(std::move(stream));
                   })
                   .Build();
```

The backend tag still selects the implementation at compile time. Options that alter backend resources or lifecycle semantics—ring depth, provided buffers, and zero-copy, for example—are not disguised as cross-backend Runtime settings.

`Workers(n)` always means *n threads*. The topology behind that number is backend-specific: Reactor shares the listen port with `SO_REUSEPORT` when `n > 1`; kqueue binds a single listener on worker 0 and posts accepted descriptors onto the other loops; luring keeps one ring per worker.

### 5. Use luring-native capabilities

`Runtime` owns the default TCP server's worker lifecycle; it is not a general io_uring configuration API. It may select safe defaults, such as multishot accept with fallback, but applications that need explicit control of ring depth, SQPOLL, provided-buffer rings, multishot receive, or zero-copy send should compose `luring::LUringLoop`, `LUringOptions`, and the relevant listener, stream, or source directly:

```cpp
coropact::luring::LUringLoop loop;
coropact::luring::LUringOptions options;
options.entries = 8192;
options.shared_buffer_capacity = 256;  // Provided buffers for RecvSource.

auto initialized = loop.Init(options);
// On the loop's owner thread: create listener/source, SpawnDetach(...), then loop.Run(...).
```

This native path makes ownership of each ring, buffer lease, and operation lifecycle explicit. See [`examples/luring`](examples/luring) and the luring public headers. Do not add these capabilities as cross-backend Runtime switches.

## Build

## Run the container demo

The published container runs a TCP echo server based on CoroPact's Reactor
backend. Its port is available to the host when published with Docker:

```bash
docker run --rm -p 9090:9090 ghcr.io/archiyun/coropact:latest
```

In another terminal:

```bash
printf 'hello\n' | nc 127.0.0.1 9090
```

To build the same image from a checkout, run `docker build -t coropact:local .`.
The image is a runnable demonstration, not a replacement for an application
image that links CoroPact.

Build the default Linux Reactor backend:

```bash
cmake -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=ON \
  -DBUILD_EXAMPLES=ON

cmake --build build -j"$(nproc)"
ctest --test-dir build --output-on-failure
```

For an opt-in strict diagnostic build on GCC or Clang, add
`-DCOROPACT_STRICT_WARNINGS=ON`.

Build with the io_uring backend enabled:

```bash
# Make sure liburing >= 2.6 is installed first.

cmake -B build-uring \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=ON \
  -DBUILD_EXAMPLES=ON \
  -DCOROPACT_ENABLE_URING=ON

cmake --build build-uring -j"$(nproc)"
ctest --test-dir build-uring --output-on-failure
```

Build the kqueue backend on FreeBSD, NetBSD, OpenBSD, or macOS:

```bash
cmake -B build-kqueue \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=ON \
  -DCOROPACT_ENABLE_KQUEUE=ON

cmake --build build-kqueue -j"$(sysctl -n hw.ncpu)"
ctest --test-dir build-kqueue --output-on-failure
```

On Linux, the kqueue *library* cannot be enabled. The in-memory kevent shim is a developer aid for loop and one-shot registration tests:

```bash
cmake -B build-kqueue-shim \
  -DCOROPACT_ENABLE_KQUEUE_SHIM_TESTS=ON \
  -DBUILD_TESTS=ON
```

That shim does not watch real sockets and does not run the native worker-group smoke test.

### CMake options

| Option | Default | Effect |
|---|---|---|
| `COROPACT_ENABLE_URING` | `OFF` | Linux io_uring backend (`liburing >= 2.6`) |
| `COROPACT_ENABLE_KQUEUE` | `OFF` | BSD/Darwin kqueue backend; CMake fails on other hosts |
| `COROPACT_ENABLE_KQUEUE_SHIM_TESTS` | `OFF` | Compile kqueue loop/poller tests against a fake `kevent` |
| `COROPACT_STRICT_WARNINGS` | `OFF` | `-Wall -Wextra -Wpedantic -Werror` on GCC/Clang |
| `COROPACT_SANITIZER` | empty | e.g. `address,undefined` or `thread` |
| `BUILD_TESTS` | `ON` | Unit and smoke tests |
| `BUILD_EXAMPLES` | `ON` | Linux examples; disabled until a native readiness backend exists |
| `BUILD_BENCHMARKS` | `OFF` | Standalone microbenchmarks |
| `BUILD_FUZZERS` | `OFF` | Clang/libFuzzer state-machine fuzz targets; selects ASan/UBSan |
| `BUILD_EXPERIMENTAL_TESTS` | `OFF` | SplayTree / QuadHeap validators |

### Fuzzing and microbenchmarks

Build the receive-source lifecycle fuzzer with Clang. The target includes
AddressSanitizer and UndefinedBehaviorSanitizer; do not also set
`COROPACT_SANITIZER`:

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
* Reactor uses `epoll` and is the default Linux backend. It has no extra networking-library dependency.
* luring requires Linux, `liburing >= 2.6` (Linux 5.19 or newer is recommended).
* kqueue requires a BSD or Darwin host with a native `kqueue(2)`.
* `net` and the backend-neutral contracts use portable POSIX socket facilities. Do not implement BSD readiness as conditional code inside the epoll Reactor.

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
        +------+------+------+
        |             |      |
        v             v      v
   Reactor/epoll   luring   kqueue
   coropact::reactor  ::luring  ::kqueue
```

The backends do not share an event loop, and their internal state machines do not need to be identical. They only need to satisfy the same business-observable asynchronous I/O contract. [`docs/SUBSYSTEMS.md`](docs/SUBSYSTEMS.md) is the normative dependency policy.

Reactor multi-worker (`Workers(n>1)`) uses independent listeners on the same port:

```text
ReactorWorkerGroup
  |
  +-- Worker 0 -> Thread 0 -> EventLoop 0 -> listen :port (SO_REUSEPORT)
  +-- Worker 1 -> Thread 1 -> EventLoop 1 -> listen :port (SO_REUSEPORT)
  `-- Worker N -> Thread N -> EventLoop N -> listen :port (SO_REUSEPORT)
```

The io_uring server uses a thread-per-ring topology:

```text
LUringServer
  |
  +-- Worker 0 -> Thread 0 -> LUringLoop 0 -> Ring 0
  +-- Worker 1 -> Thread 1 -> LUringLoop 1 -> Ring 1
  `-- Worker N -> Thread N -> LUringLoop N -> Ring N
```

kqueue multi-worker is master-slave. Only worker 0 binds the listener. I/O workers start first so their loops exist; the acceptor then `Release()`s the descriptor and `Post()`s it onto a round-robin owner loop, which reconstructs `KqueueStream` on that thread:

```text
KqueueWorkerGroup
  |
  +-- Worker 0 (acceptor) -> KqueueLoop 0 -> listen :port
  |         |
  |         +-- accept -> Release(fd) -> Post --> reconstruct stream
  |
  +-- Worker 1 (I/O) -> KqueueLoop 1
  `-- Worker N (I/O) -> KqueueLoop N
```

Connections, I/O operations, and coroutine continuations remain owned by the Worker and loop that run them. `KqueueStream` cannot be moved across loops; the handoff is a raw fd, not a live stream or a `Work*`.

## Performance Benchmarks

CoroPact includes reproducible `wrk` benchmarks covering:

* Reactor and io_uring backends
* raw liburing
* standalone Asio
* Monoio
* Compio
* the libaio poll compatibility path
* libuv, libevent, and libev reference adapters
* an Nginx reference configuration

Results depend strongly on the workload and must not be interpreted as a universal ranking of networking frameworks. The latest current-source C++ baseline is the [2026-08-10 network-library baseline](docs/benchmark/network-libraries-20260810.md); it records the `wrk` file-descriptor precondition and invalid samples. The complete ten-target fixed-HTTP comparison, including charts, summary data, per-round data, latency anomalies, CPU usage, memory usage, and error counts, is available in the [unified network-library benchmark report](docs/benchmark/network-libraries.md). Other benchmark scripts, raw results, and optimization records are under [`docs/benchmark`](docs/benchmark/).

## Documentation

The documentation map is [`docs/index.md`](docs/index.md). Design notes are currently written in Chinese; they are the source of truth for contracts and ownership. [`CONTEXT.md`](CONTEXT.md) is the domain glossary.

* **[Networking architecture](docs/design/zh-CN/network/index.md)**: runtime layering, backend boundaries, and ownership models.
* **[kqueue backend](docs/design/zh-CN/network/kqueue/index.md)**: one-shot readiness, `Post`, and master-slave handoff.
* **[Runtime Builder](docs/design/zh-CN/network/runtime-builder.md)**: compile-time backend tags and start/stop lifecycle.
* **[Coroutine state-machine models](docs/design/zh-CN/network/lamport-hot-swap-runtime.md)**: abstract stream invariants and backend refinement notes.
* **[AsyncStream semantics](docs/design/zh-CN/network/async-stream-contract.md)**: read, write, close, cancellation, and buffer-lifetime semantics.
* **[Data structures](docs/design/zh-CN/datastructure/index.md)**: modern C++ intrusive data structures, intrusive red-black trees, intrusive lists, MPSC queues, and their use in the project. SplayTree and QuadHeap are experimental explicit-header APIs; build their validators with `-DBUILD_EXPERIMENTAL_TESTS=ON`.
* **[Performance benchmarks](docs/benchmark/network-libraries-20260810.md)**: the latest current-source C++ baseline; the broader unified network-library report and supporting material are in [`docs/benchmark`](docs/benchmark/).
* **[Examples](examples/)**: Reactor and io_uring examples on Linux.
* **[Tests](tests/)**: coroutine, networking, lifecycle, and backend validation.

## Current Status

CoroPact is still an experimental networking runtime and is not yet a production-ready replacement for mature networking frameworks.

Current work includes:

* Hardening the kqueue adapter on real BSD/Darwin hosts (native worker-group and Runtime smoke already exist there).
* Formal state-machine proofs, invariant tests, and concurrency validation for additional backends.
* Modern liburing networking options and io_uring optimizations.
* More realistic workload benchmarks and bottleneck analysis.

## Contributing

* Please open an [Issue](https://github.com/archiyun/CoroPact/issues) for bugs, questions, or feature requests.
* Pull Requests are welcome: [open a PR](https://github.com/archiyun/CoroPact/pulls).
* This project is released under the [MIT License](LICENSE).

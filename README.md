# CoroPact⚡

![C++](https://img.shields.io/badge/C++-23-blue)
![Platform](https://img.shields.io/badge/platform-Linux-lightgrey)
![License](https://img.shields.io/github/license/archiyun/CoroPact)
![Stars](https://img.shields.io/github/stars/archiyun/CoroPact?style=social)

***A C++23 asynchronous networking runtime for Linux, powered by coroutines, epoll, and io_uring.***

CoroPact provides a unified, explicit, and high-performance C++23 coroutine model over independent Reactor and io_uring networking backends. Its default path hides event-mechanism details much like a conventional networking library, while `Runtime` offers Tokio-like server startup without preventing explicit backend-native configuration and extensions.

* 🔀 **A unified asynchronous I/O contract**
  epoll and io_uring retain their own threading, event-loop, and completion models, but expose the same application-observable semantics through the `io` concepts `AsyncStream`, `AsyncListener`, and `AsyncConnector`. `coro` expresses asynchronous control flow in synchronous-looking code while hiding frame, suspension, resumption, and lifetime mechanics; application code need not handle `epoll_event`, SQEs, or CQEs.

* 🧩 **Explicit ownership and completion semantics**
  Each Worker owns its thread, event loop, connections, and I/O operations. Operations complete in their owning execution context and coroutine continuations resume in that same context, with explicit rules for buffer lifetimes, cancellation, and asynchronous close.

* 🚀 **Core operations and native extensions**
  CoroPact provides asynchronous accept, connect, read, write, close, and timers. Reactor can select LT or ET; luring additionally exposes extensions such as multishot receive and zero-copy send. HTTP and gateway policy live in [CoroGateway](https://github.com/archiyun/CoroGateway).

Linux is the currently implemented and validated platform. The contracts leave an extension point for future kqueue or IOCP backends, but those backends are not implemented or supported today.

## Quick Start

### 1. Choose headers

Applications normally include the backend-neutral modules and one concrete backend:

```cpp
#include "coropact/coro.h"
#include "coropact/io.h"
#include "coropact/net.h"
#include "coropact/reactor.h"  // Default Reactor backend
```

For prototypes, the aggregate header `#include "coropact/coropact.h"` is also available. Projects sensitive to compile time should include only the modules they use. In an io_uring-enabled build, include `coropact/luring.h` instead of `coropact/reactor.h`.

### 2. Write backend-neutral connection code

This echo session depends only on `AsyncStream`, so it works with both ReactorStream and LUringStream. See [`examples/simple_echo`](examples/simple_echo) for the runnable version.

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
auto EchoSession(Stream stream) -> cp::coro::Task<cp::base::Result<void>> {
  std::array<std::byte, 4096> buffer{};
  cp::base::Result<void> session_result{};

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
    auto written = co_await cp::io::WriteAll(stream, payload);
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

For io_uring, build with `COROPACT_ENABLE_URING=ON`, include `coropact/luring.h`, and change the tag to `cp::runtime::LUring`. The handler's stream remains statically typed as the selected backend type; no virtual call enters the connection data path.

### 4. Configure the default server explicitly

`Create` uses conservative defaults. Use the same Runtime's backend-specific Builder when worker count needs explicit control:

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

### 5. Use luring-native capabilities

`Runtime` owns the default TCP server's worker lifecycle; it is not a general io_uring configuration API. It may select safe defaults, such as multishot accept with fallback, but applications that need explicit control of ring depth, SQPOLL, submission batching, provided-buffer rings, multishot receive, or zero-copy send should compose `luring::LUringLoop`, `LUringOptions`, and the relevant listener, stream, or source directly:

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

Build the Reactor backend:

```bash
cmake -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=ON \
  -DBUILD_EXAMPLES=ON

cmake --build build -j"$(nproc)"
ctest --test-dir build --output-on-failure
```

Build with the io_uring backend enabled:

```bash
# Make sure liburing is installed first.

cmake -B build-uring \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=ON \
  -DBUILD_EXAMPLES=ON \
  -DCOROPACT_ENABLE_URING=ON

cmake --build build-uring -j"$(nproc)"
ctest --test-dir build-uring --output-on-failure
```

### Requirements

* Linux, CMake 3.20+, and a compiler with C++23 coroutine support.
* The default Reactor build has no extra networking-library dependency.
* luring requires `liburing >= 2.6`; Linux 5.19 or newer is recommended.

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
Reactor / epoll   luring / io_uring
  coropact::reactor     coropact::luring
```

The two backends do not share an event loop, and their internal state machines do not need to be identical. They only need to satisfy the same business-observable asynchronous I/O contract.

The io_uring server uses a thread-per-ring topology:

```text
LUringServer
  |
  +-- Worker 0 -> Thread 0 -> LUringLoop 0 -> Ring 0
  +-- Worker 1 -> Thread 1 -> LUringLoop 1 -> Ring 1
  `-- Worker N -> Thread N -> LUringLoop N -> Ring N
```

Connections, I/O operations, and coroutine continuations remain owned by the Worker and Ring that created them; they do not migrate between Rings during execution.

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

Results depend strongly on the workload and must not be interpreted as a universal ranking of networking frameworks. The complete ten-target fixed-HTTP comparison, including charts, summary data, per-round data, latency anomalies, CPU usage, memory usage, and error counts, is available in the [unified network-library benchmark report](docs/benchmark/network-libraries.md). Other benchmark scripts, raw results, and optimization records are under [`docs/benchmark`](docs/benchmark/).

## Documentation

Most documentation is still being written and may lag behind the current implementation. Treat it as design and development reference material.

* **[Networking architecture](docs/design/zh-CN/network/index.md)**: runtime layering, backend boundaries, and ownership models.
* **[Coroutine state-machine models](docs/design/zh-CN/network/lamport-hot-swap-runtime.md)**: abstract stream invariants and backend refinement notes.
* **[AsyncStream semantics](docs/design/zh-CN/network/async-stream-contract.md)**: read, write, close, cancellation, and buffer-lifetime semantics.
* **[Data structures](docs/design/zh-CN/datastructure/index.md)**: modern C++ intrusive data structures, intrusive red-black trees, intrusive lists, MPSC queues, and their use in the project. SplayTree and QuadHeap are experimental explicit-header APIs; build their validators with `-DBUILD_EXPERIMENTAL_TESTS=ON`.
* **[Performance benchmarks](docs/benchmark/network-libraries.md)**: the unified network-library report; additional methods, raw results, and optimization records are in [`docs/benchmark`](docs/benchmark/).
* **[Examples](examples/)**: Reactor and io_uring examples.
* **[Tests](tests/)**: coroutine, networking, lifecycle, and backend validation.

## Current Status

CoroPact is still an experimental networking runtime and is not yet a production-ready replacement for mature networking frameworks.

Current work includes:

* Formal state-machine proofs, invariant tests, and concurrency validation for additional backends.
* Modern liburing networking options and io_uring optimizations.
* More realistic workload benchmarks and bottleneck analysis.

## Contributing

* Please open an [Issue](https://github.com/archiyun/CoroPact/issues) for bugs, questions, or feature requests.
* Pull Requests are welcome: [open a PR](https://github.com/archiyun/CoroPact/pulls).
* This project is released under the [MIT License](LICENSE).

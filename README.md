# Vexo⚡

![C++](https://img.shields.io/badge/C++-23-blue)
![Platform](https://img.shields.io/badge/platform-Linux-lightgrey)
![License](https://img.shields.io/github/license/akiba-miku/high-concurrency-runtime)
![Stars](https://img.shields.io/github/stars/akiba-miku/high-concurrency-runtime?style=social)

***A C++23 asynchronous networking runtime and L7 gateway for Linux, powered by coroutines, epoll, and io_uring.***

Vexo provides a unified and explicit coroutine programming model over independent Reactor and io_uring networking backends:

* 🔀 **A unified asynchronous I/O contract**
  The epoll and io_uring modules own independent event loops while exposing consistent coroutine Stream semantics. Application code depends on `Task<T>`, `AsyncStream`, and `AsyncListener` rather than backend details such as `epoll_event`, SQE, or CQE.

* 🧩 **Explicit ownership and completion semantics**
  Each Worker owns its thread, event loop, connections, and I/O operations. Operations complete in their owning execution context and coroutine continuations resume in that same context, with explicit rules for buffer lifetimes, cancellation, and asynchronous close.

* 🚀 **Networking runtime and L7 gateway**
  Vexo provides asynchronous accept, connect, read, write, and close, together with an incremental HTTP/1.1 parser, reverse proxy, upstream connection pool, load balancing, health checking, circuit breaking, and rate limiting.

Although Linux is the current primary platform, the coroutine and I/O contracts can be used to add other backends such as macOS kqueue and Windows IOCP.

## Quick Start

```cpp
#include <array>
#include <cstddef>

#include "vexo/coro/task.h"
#include "vexo/io/stream_algorithms.h"

vexo::coro::Task<void> Echo(vexo::io::AsyncStream auto& stream) {
    std::array<std::byte, 4096> buffer{};

    for (;;) {
        auto read = co_await stream.ReadSome(buffer);
        if (!read.has_value() || *read == 0) {
            break;
        }

        auto written =
            co_await vexo::io::WriteAll(stream, buffer.first(*read));

        if (!written.has_value()) {
            break;
        }
    }

    co_await stream.Close();
}
```

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
  -DVEXO_ENABLE_URING=ON

cmake --build build-uring -j"$(nproc)"
ctest --test-dir build-uring --output-on-failure
```

## Architecture

```text
HTTP / Gateway / Custom Session
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
   vexo::net         vexo::luring
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

Vexo includes reproducible `wrk` benchmarks covering:

* Reactor and io_uring backends
* raw liburing
* standalone Asio
* Monoio
* Compio
* the libaio poll compatibility path
* an Nginx reference configuration

Results depend strongly on the workload and must not be interpreted as a universal ranking of networking frameworks. The complete seven-target fixed-HTTP comparison, including charts, summary data, per-round data, latency anomalies, CPU usage, memory usage, and error counts, is available in the [unified network-library benchmark report](docs/benchmark/network-libraries.md). Other benchmark scripts, raw results, and optimization records are under [`docs/benchmark`](docs/benchmark/).

## Documentation

Most documentation is still being written and may lag behind the current implementation. Treat it as design and development reference material.

* **[Networking architecture](docs/design/zh-CN/network/index.md)**: runtime layering, backend boundaries, and ownership models.
* **Coroutine state-machine formalization**: planned documentation under the design tree.
* **[AsyncStream semantics](docs/design/zh-CN/network/async-stream-contract.md)**: read, write, close, cancellation, and buffer-lifetime semantics.
* **[Gateway architecture](docs/design/zh-CN/gateway/index.md)**: HTTP handling, reverse proxying, and upstream management.
* **[Data structures](docs/design/zh-CN/datastructure/index.md)**: modern C++ intrusive data structures, intrusive red-black trees, intrusive lists, MPSC queues, and their use in the project.
* **[Performance benchmarks](docs/benchmark/network-libraries.md)**: the unified network-library report; additional methods, raw results, and optimization records are in [`docs/benchmark`](docs/benchmark/).
* **[Examples](examples/)**: Reactor, io_uring, and Gateway examples.
* **[Tests](tests/)**: coroutine, networking, lifecycle, and gateway validation.

## Current Status

Vexo is still an experimental networking runtime and is not yet a production-ready replacement for mature networking frameworks.

Current work includes:

* Formal state-machine proofs, invariant tests, and concurrency validation for additional backends.
* Modern liburing networking options and io_uring optimizations.
* More realistic workload benchmarks and bottleneck analysis.

## Contributing

* Please open an [Issue](https://github.com/akiba-miku/high-concurrency-runtime/issues) for bugs, questions, or feature requests.
* Pull Requests are welcome: [open a PR](https://github.com/akiba-miku/high-concurrency-runtime/pulls).
* This project is released under the [MIT License](LICENSE).

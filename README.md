# CoroPact⚡

![C++](https://img.shields.io/badge/C++-23-blue)
![Platform](https://img.shields.io/badge/platform-Linux-lightgrey)
![License](https://img.shields.io/github/license/archiyun/CoroPact)
![Stars](https://img.shields.io/github/stars/archiyun/CoroPact?style=social)

***A C++23 asynchronous networking runtime for Linux, powered by coroutines, epoll, and io_uring.***

CoroPact provides a unified and explicit coroutine programming model over independent Reactor and io_uring networking backends:

* 🔀 **A unified asynchronous I/O contract**
  The epoll and io_uring modules own independent event loops while exposing consistent coroutine Stream semantics. Application code depends on `Task<T>`, `AsyncStream`, and `AsyncListener` rather than backend details such as `epoll_event`, SQE, or CQE.

* 🧩 **Explicit ownership and completion semantics**
  Each Worker owns its thread, event loop, connections, and I/O operations. Operations complete in their owning execution context and coroutine continuations resume in that same context, with explicit rules for buffer lifetimes, cancellation, and asynchronous close.

* 🚀 **Networking runtime**
  CoroPact provides asynchronous accept, connect, read, write, close, timers, and backend-specific extensions such as multishot receive and zero-copy send. HTTP and gateway policy live in [CoroGateway](https://github.com/archiyun/CoroGateway).

Although Linux is the current primary platform, the coroutine and I/O contracts can be used to add other backends such as macOS kqueue and Windows IOCP.

## Quick Start

```cpp
#include <array>
#include <cstddef>

#include "coropact/coro/task.h"
#include "coropact/io/stream_algorithms.h"

coropact::coro::Task<void> Echo(coropact::io::AsyncStream auto& stream) {
    std::array<std::byte, 4096> buffer{};

    for (;;) {
        auto read = co_await stream.ReadSome(buffer);
        if (!read.has_value() || *read == 0) {
            break;
        }

        auto written =
            co_await coropact::io::WriteAll(stream, buffer.first(*read));

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
  -DCOROPACT_ENABLE_URING=ON

cmake --build build-uring -j"$(nproc)"
ctest --test-dir build-uring --output-on-failure
```

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

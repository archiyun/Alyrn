# high-concurrency-runtime

**English** | [中文](README.zh-CN.md)

`high-concurrency-runtime` is a C++20 high-concurrency network runtime and HTTP server framework.
It is built around a Reactor model and a One-Loop-Per-Thread I/O architecture, and includes a TCP networking layer, HTTP routing, asynchronous logging, task scheduling, memory pools, and metrics utilities. It can be used as a lightweight Linux server framework, or as a codebase for learning WebSocket, RPC, distributed systems, and database-access layers.

## Features

- Linux `epoll` based event-driven networking
- One-Loop-Per-Thread I/O threading model
- TCP server, connection, buffer, timer, poller, and related primitives
- HTTP server, request parsing, response building, Trie router, and path parameters
- Cooperative task cancellation, priority work queue, and thread-pool scheduler
- Asynchronous logger and timestamp utilities
- MemoryPool / ObjectPool allocators
- Counter / Gauge / Histogram / Registry metrics interfaces
- Example servers, smoke tests, unit tests, integration tests, and wrk benchmark scripts

## Requirements

Linux is recommended for building and running this project. The networking layer uses `epoll`; macOS or other platforms need networking-layer changes before they can run the server components.

Required:

- Linux environment, for `epoll`
- CMake 3.20 or newer
- A C++20 compiler, preferably GCC 12+ or Clang 15+
- POSIX threads

Optional:

- `liburing`: required for `examples/io_uring_echo`
- GoogleTest: if found by CMake, additional gtest-based tests are built automatically
- `wrk`: used by the HTTP benchmark scripts

On Ubuntu / Debian:

```bash
sudo apt update
sudo apt install -y build-essential cmake git liburing2 liburing-dev
```

If you only want to build the core libraries and tests, and do not need the `io_uring` example, you can skip `liburing` and disable examples:

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_EXAMPLES=OFF
cmake --build build -j"$(nproc)"
```

## Download

Clone with HTTPS:

```bash
git clone https://github.com/akiba-miku/high-concurrency-runtime.git
cd high-concurrency-runtime
```

Or clone with SSH:

```bash
git clone git@github.com:akiba-miku/high-concurrency-runtime.git
cd high-concurrency-runtime
```

## Build

By default, CMake builds the libraries, examples, and tests:

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

Common CMake options:

| Option | Default | Description |
|---|---:|---|
| `BUILD_EXAMPLES` | `ON` | Build programs under `examples/` |
| `BUILD_TESTS` | `ON` | Build tests under `tests/` |
| `CMAKE_BUILD_TYPE` | empty | For single-config generators, use `Debug` or `Release` |

Build only the core libraries:

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_EXAMPLES=OFF -DBUILD_TESTS=OFF
cmake --build build -j"$(nproc)"
```

Debug build:

```bash
cmake -B build-debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build-debug -j"$(nproc)"
```

## Run The HTTP Demo

After building, start the demo HTTP server:

```bash
./build/examples/demo_http_server
```

Default listen address:

```text
127.0.0.1:18080
```

Runtime environment variables:

| Variable | Default | Description |
|---|---:|---|
| `HOST` | `127.0.0.1` | Bind address |
| `PORT` | `18080` | Listen port |
| `IO_THREADS` | auto, up to 4 | Number of I/O worker threads |
| `ET_MODE` | unset | Set to any value to enable edge-triggered epoll |

Example:

```bash
HOST=0.0.0.0 PORT=18080 IO_THREADS=4 ./build/examples/demo_http_server
```

In another terminal:

```bash
curl http://127.0.0.1:18080/
curl http://127.0.0.1:18080/api/health
curl -X POST http://127.0.0.1:18080/api/echo -d "hello runtime"
```

KV API:

```bash
curl -X POST http://127.0.0.1:18080/api/kv/name -d "miku"
curl http://127.0.0.1:18080/api/kv/name
curl http://127.0.0.1:18080/api/kv
```

404 / 405 behavior:

```bash
curl http://127.0.0.1:18080/notfound
curl -X DELETE http://127.0.0.1:18080/api/health
```

## Run The TCP Echo Demo

Start the TCP echo server:

```bash
./build/examples/simple_echo_server
```

It listens on `127.0.0.1:8080` by default. Test it with `nc`:

```bash
nc 127.0.0.1 8080
hello
```

The server writes received data back to the client.

## Use In Your Own Project

The simplest integration path is to add this repository as a CMake subdirectory and link the target you need:

```cmake
add_subdirectory(high-concurrency-runtime)

add_executable(my_server main.cpp)
target_link_libraries(my_server PRIVATE runtime_http)
```

Minimal HTTP server:

```cpp
#include "runtime/http/http_server.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"

int main() {
    runtime::net::EventLoop loop;
    runtime::http::HttpServer server(
        &loop,
        runtime::net::InetAddress(8080, "0.0.0.0"),
        "my-server");

    server.SetThreadNum(4);

    server.Get("/api/users/:id",
        [](const runtime::http::HttpRequest& req,
           runtime::http::HttpResponse& resp) {
            resp.SetContentType("application/json; charset=utf-8");
            resp.SetBody("{\"id\":\"" + std::string(req.PathParam("id")) + "\"}");
        });

    server.Post("/api/echo",
        [](const runtime::http::HttpRequest& req,
           runtime::http::HttpResponse& resp) {
            resp.SetContentType("text/plain; charset=utf-8");
            resp.SetBody(std::string(req.Body()));
        });

    server.Start();
    loop.Loop();
    return 0;
}
```

If you only need the task scheduler, link `runtime_task`:

```cmake
target_link_libraries(my_app PRIVATE runtime_task)
```

Example:

```cpp
#include "runtime/task/scheduler.h"

#include <iostream>

int main() {
    runtime::task::Scheduler scheduler(4);

    auto handle = scheduler.Submit([] {
        std::cout << "run task\n";
    });

    handle.Wait();
    return 0;
}
```

Library targets:

| Target | Description |
|---|---|
| `runtime_foundation` | Logging, time, and base utilities |
| `runtime_task` | Task, thread pool, scheduler |
| `runtime_net` | TCP networking layer and event loop |
| `runtime_http` | HTTP layer, depends on net/task/foundation |

## Run Tests

When configured with `BUILD_TESTS=ON`, run:

```bash
ctest --test-dir build --output-on-failure
```

Run one test:

```bash
ctest --test-dir build -R http_smoke_test --output-on-failure
```

Notes:

- Smoke tests do not require GoogleTest
- If CMake finds GoogleTest, it also builds `runtime_unit_tests` and `runtime_integration_tests`
- If GoogleTest is unavailable, CMake prints `GTest not found; skipping gtest-based unit tests.`

## Benchmark

Build a Release version and start the HTTP demo:

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
IO_THREADS=2 ./build/examples/demo_http_server
```

In another terminal, run `wrk`:

```bash
wrk -t1 -c128 -d5s http://127.0.0.1:18080/api/health
wrk -t1 -c128 -d5s -s benchmarks/wrk/post_echo.lua http://127.0.0.1:18080/api/echo
```

You can also use the included script:

```bash
bash benchmarks/wrk/run_wrk.sh
```

## Repository Layout

```text
.
├── include/runtime/
│   ├── base/        # NonCopyable, CurrentThread, base utilities
│   ├── config/      # Configuration loading interfaces
│   ├── http/        # HTTP server, router, request, response, context
│   ├── inference/   # Inference-related interfaces
│   ├── log/         # Logger, AsyncLogger
│   ├── memory/      # MemoryPool, ObjectPool, cache structures
│   ├── metrics/     # Counter, Gauge, Histogram, Registry
│   ├── net/         # EventLoop, TcpServer, Channel, Poller, Buffer, Timer
│   ├── task/        # Scheduler, ThreadPool, Task, WorkQueue
│   ├── time/        # Timestamp
│   └── trace/       # TraceId, LifecycleTrace
├── src/             # Implementations
├── examples/        # HTTP/TCP/logger/io_uring examples
├── tests/           # Unit and integration tests
├── benchmarks/      # wrk benchmark scripts and result archives
├── config/          # Example configuration files
├── docs/            # Design notes
└── third_party/     # Third-party headers
```

## Architecture

```text
HTTP Layer:        runtime::http
  HttpServer, Router, HttpContext, HttpRequest, HttpResponse

Net Layer:         runtime::net
  TcpServer, TcpConnection, EventLoop, Poller, Channel, Buffer, TimerQueue

Foundation Layer:  runtime::base / log / time / task / memory / metrics
  AsyncLogger, Scheduler, ThreadPool, MemoryPool, ObjectPool, Timestamp
```

Typical HTTP request flow:

```text
kernel
  -> Buffer::ReadFd()
  -> TcpConnection message callback
  -> HttpContext::ParseRequest()
  -> Router::Match()
  -> user handler
  -> HttpResponse::ToString()
  -> TcpConnection::Send()
```

## License

This project is licensed under the [MIT License](LICENSE).

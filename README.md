# high-concurrency-runtime

**English** | [中文](README.zh-CN.md)

A C++20 high-concurrency network runtime for Linux. The project is layered — you can use it as a full **reverse-proxy gateway**, as a plain **HTTP application server**, as a raw **TCP/event-loop framework**, or as individual **data-structure / allocator / scheduler** libraries. Every upper layer is built on top of the lower ones with no circular dependencies.

```
Gateway Layer  ─── runtime::gateway
HTTP Layer     ─── runtime::http        (depends on net)
Net Layer      ─── runtime::net         (depends on foundation)
Foundation     ─── runtime::base / ds / log / time / task / memory / metrics
```

## Features

### Gateway (`runtime::gateway`)

- **Reverse proxy** — transparent HTTP forwarding to upstream backends over persistent connections
- **Upstream management** — `UpstreamRegistry` + `Upstream` + `UpstreamPeer`; add backends at runtime
- **Load balancing** — Round Robin, Smooth Weighted Round Robin, Least Connections, Random, Weighted Random; pluggable via `LoadBalancer` interface
- **Active health checking** — periodic HTTP probe per backend; automatic mark-down after N consecutive failures and mark-up after M consecutive successes
- **Passive failure tracking** — `ProxySession` records per-request failures; backends are fenced for `fail_timeout` after `max_fails`
- **Connection pooling** — `UpstreamConnPool` maintains persistent connections to each backend, one pool per I/O thread (no cross-thread contention)
- **Direct routes** — register synchronous handlers on the gateway without touching `HttpServer`
- **Static file serving** — serve a local directory under a URL prefix
- **Code-driven configuration** — no config files; upstreams and routes are wired in C++ at startup

### HTTP server (`runtime::http`)

- `epoll`-based event loop, One-Loop-Per-Thread I/O threading
- Incremental HTTP/1.1 parser (`HttpContext`) — zero intermediate copies, keep-alive via `Reset()`
- Trie router with static/dynamic segments and path parameters (`:param` syntax)

### Networking (`runtime::net`)

- `EventLoop`, `EpollPoller`, `Channel`, `TcpServer`, `TcpConnection`, `Buffer`
- Timer queue driven by `timerfd`, indexing `runtime::time::Timer` objects through `TimerTree`
- Level-triggered and edge-triggered epoll modes

### Foundation

- Asynchronous double-buffered logger
- `MemoryPool`, `ObjectPool` allocators
- `Scheduler`, `ThreadPool`, `WorkQueue` with cooperative cancellation
- `runtime::ds::IntrusiveRBTree<T, kMember, kLess>` and `IntrusiveQuadHeap` — generic intrusive data structures with zero per-node heap allocation
- `Counter`, `Gauge`, `Histogram`, `Registry` metrics interfaces

## Requirements

Required:

- Linux (uses `epoll`)
- CMake ≥ 3.20
- GCC 12+ or Clang 15+ with C++20 support
- POSIX threads

Optional:

- GoogleTest — if found by CMake, `runtime_unit_tests` and `runtime_integration_tests` are built automatically
- `liburing` — for the `io_uring_echo` example only
On Ubuntu / Debian:

```bash
sudo apt update
sudo apt install -y build-essential cmake git
```

## Download

```bash
git clone https://github.com/akiba-miku/high-concurrency-runtime.git
cd high-concurrency-runtime
```

## Build

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

Common CMake options:

| Option | Default | Description |
|---|---:|---|
| `BUILD_EXAMPLES` | `ON` | Build programs under `examples/` |
| `BUILD_TESTS` | `ON` | Build tests under `tests/` |
| `CMAKE_BUILD_TYPE` | empty | Use `Debug` or `Release` for single-config generators |

## Run The Gateway

Start two upstream HTTP servers to act as backends:

```bash
PORT=9001 ./build/examples/demo_http_server &
PORT=9002 ./build/examples/demo_http_server &
```

Start the gateway (listens on `0.0.0.0:8080`):

```bash
./build/examples/demo_gateway
```

Exercise the gateway:

```bash
# Direct route — served by the gateway itself
curl -i http://127.0.0.1:8080/healthz

# Proxy routes — forwarded to 9001 / 9002 in round-robin
curl -i http://127.0.0.1:8080/api/health
curl -i http://127.0.0.1:8080/api/kv
```

Kill one backend and watch the health checker mark it down — requests stop going to it within one health-check interval.

## Run The HTTP Demo

```bash
./build/examples/demo_http_server
```

Default listen address: `127.0.0.1:18080`

Runtime environment variables:

| Variable | Default | Description |
|---|---:|---|
| `HOST` | `127.0.0.1` | Bind address |
| `PORT` | `18080` | Listen port |
| `IO_THREADS` | auto, up to 4 | Number of I/O worker threads |
| `ET_MODE` | unset | Set to any value to enable edge-triggered epoll |

```bash
curl http://127.0.0.1:18080/api/health
curl -X POST http://127.0.0.1:18080/api/echo -d "hello"
curl -X POST http://127.0.0.1:18080/api/kv/name -d "miku"
curl http://127.0.0.1:18080/api/kv/name
```

## Use In Your Own Project

Add the project as a CMake subdirectory and link only the layer you need:

```cmake
add_subdirectory(high-concurrency-runtime)

# Full gateway
target_link_libraries(my_gw    PRIVATE runtime_gateway)

# HTTP server only
target_link_libraries(my_http  PRIVATE runtime_http)

# Raw TCP / event loop only
target_link_libraries(my_tcp   PRIVATE runtime_net)

# Allocators / scheduler / data structures
target_link_libraries(my_util  PRIVATE runtime_foundation)
```

### Gateway example

```cpp
#include "runtime/gateway/gateway_server.h"
#include "runtime/gateway/upstream.h"
#include "runtime/gateway/upstream_peer.h"
#include "runtime/gateway/upstream_registry.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"

int main() {
    runtime::gateway::UpstreamRegistry reg;

    auto us = std::make_shared<runtime::gateway::Upstream>(
        runtime::gateway::UpstreamConfig{.name = "backend"});

    us->AddPeer(std::make_shared<runtime::gateway::UpstreamPeer>(
        runtime::gateway::UpstreamPeerConfig{.name = "127.0.0.1:9001",
                                              .host = "127.0.0.1", .port = 9001}));
    us->AddPeer(std::make_shared<runtime::gateway::UpstreamPeer>(
        runtime::gateway::UpstreamPeerConfig{.name = "127.0.0.1:9002",
                                              .host = "127.0.0.1", .port = 9002}));
    reg.Add(us);

    runtime::net::EventLoop loop;
    runtime::gateway::GatewayServer gw(&loop, runtime::net::InetAddress(8080),
                                       "gw", reg);
    gw.set_thread_num(4);

    // Direct route
    gw.Get("/healthz", [](const runtime::http::HttpRequest&,
                          runtime::http::HttpResponse& resp) {
        resp.set_content_type("application/json");
        resp.set_body("{\"status\":\"ok\"}");
    });

    // Proxy routes
    gw.AddProxyRoute("/api/", "backend", "round_robin");

    // Active health check
    gw.EnableHealthCheck({.path = "/api/health", .interval_sec = 10.0});

    gw.Start();
    loop.Loop();
}
```

### HTTP server example

```cpp
#include "runtime/http/http_server.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"

int main() {
    runtime::net::EventLoop loop;
    runtime::http::HttpServer server(&loop,
        runtime::net::InetAddress(8080, "0.0.0.0"), "my-server");
    server.set_thread_num(4);

    server.Get("/api/users/:id",
        [](const runtime::http::HttpRequest& req,
           runtime::http::HttpResponse& resp) {
            resp.set_content_type("application/json; charset=utf-8");
            resp.set_body("{\"id\":\"" + std::string(req.path_param("id")) + "\"}");
        });

    server.Start();
    loop.Loop();
}
```

### Task scheduler example

```cpp
#include "runtime/task/scheduler.h"
#include <iostream>

int main() {
    runtime::task::Scheduler scheduler(4);
    auto handle = scheduler.Submit([] { std::cout << "hello\n"; });
    handle.Wait();
}
```

Library targets:

| Target | Provides |
|---|---|
| `runtime_gateway` | Gateway, proxy, load balancers, health checker |
| `runtime_http` | HTTP server, Trie router, request/response, context |
| `runtime_net` | EventLoop, TcpServer, Channel, Poller, Buffer, TimerQueue |
| `runtime_task` | Scheduler, ThreadPool, Task, WorkQueue |
| `runtime_foundation` | Logger, Timestamp, MemoryPool, ObjectPool, `runtime::ds`, metrics |

## Run Tests

```bash
ctest --test-dir build --output-on-failure
```

Run one test:

```bash
ctest --test-dir build -R rbtree_validator --output-on-failure
```

Notable tests:

| Binary | What it tests |
|---|---|
| `rbtree_validator` | 10 M ops vs `std::set` oracle + `CheckRBInvariants()` every step |
| `http_smoke_test` | HTTP parsing and routing (no GTest required) |
| `buffer_smoke_test` | Buffer read / write / prepend |
| `runtime_unit_tests` | GTest suite: buffer, logger, memory pool, scheduler |
| `runtime_integration_tests` | GTest suite: event loop, TCP server, HTTP routing, trigger modes |

Notes:

- Smoke tests build without GoogleTest
- If CMake finds GoogleTest, it also builds `runtime_unit_tests` and `runtime_integration_tests`

## Repository Layout

```text
.
├── include/runtime/
│   ├── base/        # NonCopyable, CurrentThread
│   ├── ds/          # IntrusiveRBTree, IntrusiveQuadHeap, MurmurHash3
│   ├── gateway/     # GatewayServer, Upstream, LoadBalancer, HealthChecker, ProxyPass
│   ├── http/        # HttpServer, Router, HttpContext, HttpRequest, HttpResponse
│   ├── log/         # Logger, AsyncLogger
│   ├── memory/      # MemoryPool, ObjectPool
│   ├── metrics/     # Counter, Gauge, Histogram, Registry
│   ├── net/         # EventLoop, TcpServer, Channel, Poller, Buffer, TimerQueue
│   ├── task/        # Scheduler, ThreadPool, Task, WorkQueue, coro
│   ├── time/        # Timestamp, Timer, TimerId, TimerTree
│   └── trace/       # TraceId, LifecycleTrace
├── src/             # Implementations (mirrors include layout)
├── examples/        # demo_gateway, demo_http_server, demo_echo_server, demo_rbtree
├── tests/           # Unit, integration, smoke tests, oracle validator
└── docs/            # Design notes
```

## Architecture

```text
Gateway Layer   runtime::gateway
  GatewayServer, UpstreamRegistry, Upstream, UpstreamPeer
  LoadBalancer (RoundRobin / WeightedRoundRobin / LeastConn / Random)
  HealthChecker, ProxyPass, UpstreamConnPool, StaticHandler

HTTP Layer      runtime::http
  HttpServer, Router (Trie), HttpContext, HttpRequest, HttpResponse

Net Layer       runtime::net
  TcpServer, TcpConnection, EventLoop, EpollPoller, Channel
  Buffer, TimerQueue (timerfd-backed)

Foundation      runtime::base / ds / log / time / task / memory / metrics
  AsyncLogger, Scheduler, ThreadPool
  MemoryPool, ObjectPool
  runtime::ds::IntrusiveRBTree<T, kMember, kLess>, IntrusiveQuadHeap
  Timestamp, Timer, TimerTree, Counter, Gauge, Histogram
```

Typical gateway request flow:

```text
kernel
  → Buffer::ReadFd()
  → TcpConnection message callback (Sub Loop)
  → HttpContext::ParseRequest()       (incremental state machine)
  → GatewayServer::MatchRoute()
      ├── Direct  → synchronous Handler → HttpResponse
      ├── Proxy   → LoadBalancer::Select() → UpstreamConnPool
      │             → ProxySession (async upstream I/O on same Sub Loop)
      └── Static  → ThreadPool file I/O → HttpResponse
  → TcpConnection::Send()
```

Typical HTTP request flow:

```text
kernel
  → Buffer::ReadFd()
  → TcpConnection message callback
  → HttpContext::ParseRequest()
  → Router::Match()
  → user handler
  → HttpResponse::ToString()
  → TcpConnection::Send()
```

## License

This project is licensed under the [MIT License](LICENSE).

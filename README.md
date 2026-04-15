# high-concurrency-runtime

> A high-performance HTTP server framework in C++20, built on a Reactor + One-Loop-Per-Thread event model.

**[中文文档](README.zh-CN.md)**

---

## Highlights

- **Reactor model** — `epoll`-based event loop, zero-copy buffer reads via `readv` scatter-gather
- **One-Loop-Per-Thread** — Main Loop accepts only; Sub Loop thread pool owns each connection exclusively, no shared IO state between threads
- **Trie router** — `O(k)` path matching (k = path segments), dynamic path params (`:param`), static segments preferred over param segments, automatic 404 / 405 distinction
- **Incremental HTTP parser** — stateful `HttpContext` state machine consumes `Buffer&` directly with no intermediate copies; `Reset()` reuses context across keep-alive requests
- **Async logger** — double-buffered, batch-flush to disk; IO threads never block on log writes
- **Memory pool** — intrusive free-list allocator; `ObjectPool` adds RAII `ScopedPtr`, `NullMutex` variant for per-thread use (33× faster than `new/delete` in single-threaded benchmarks)
- **C++20** — header-only templates, `std::any` for typed connection context, structured bindings

---

## Architecture

```
┌──────────────────────────────────────────────┐
│               HTTP Layer                     │  runtime::http
│   HttpServer · Router · HttpContext           │
│   HttpRequest · HttpResponse                 │
└──────────────────┬───────────────────────────┘
                   │ depends on
┌──────────────────▼───────────────────────────┐
│              Net Layer                       │  runtime::net
│   TcpServer · TcpConnection · EventLoop      │
│   EpollPoller · Channel · Buffer             │
│   Acceptor · TimerQueue · EventLoopThreadPool│
└──────────────────┬───────────────────────────┘
                   │ depends on
┌──────────────────▼───────────────────────────┐
│            Foundation Layer                  │  runtime::log / time / task / memory
│   AsyncLogger · Timestamp                    │
│   ThreadPool · MemoryPool · ObjectPool       │
└──────────────────────────────────────────────┘
```

Dependency is strictly downward. Upper layers never include lower-layer headers in reverse.

---

## Request Flow

```
kernel data
  └─ Buffer::ReadFd()              ← scatter-gather read, no intermediate copy
       └─ MessageCallback(conn, buf, ts)
            └─ HttpContext::ParseRequest(buf)   ← state machine, consumes bytes in-place
                 └─ HttpRequest (parse complete)
                      └─ Router::Match(method, path)
                           └─ Handler(req, resp)
                                └─ conn->Send(resp.ToString())
```

---

## Quick Start

### Requirements

- Linux (epoll)
- GCC 12+ or Clang 15+ with C++20 support
- CMake 3.20+

### Build

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

### Run the demo server

```bash
./build/examples/demo_http_server
# Listens on 127.0.0.1:18080 by default
```

Environment variables:

| Variable | Default | Description |
|---|---|---|
| `HOST` | `127.0.0.1` | Bind address |
| `PORT` | `18080` | Listen port |
| `IO_THREADS` | auto (≤ 4) | Sub Loop thread count |
| `ET_MODE` | unset | Enable edge-triggered epoll |

```bash
# Health check
curl http://127.0.0.1:18080/api/health

# Echo POST body
curl -X POST http://127.0.0.1:18080/api/echo -d "hello"

# KV store
curl -X POST http://127.0.0.1:18080/api/kv/foo -d "bar"
curl http://127.0.0.1:18080/api/kv/foo
curl http://127.0.0.1:18080/api/kv

# 404 / 405
curl http://127.0.0.1:18080/notfound
curl -X DELETE http://127.0.0.1:18080/api/health
```

### Embed in your own server

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

    server.Get("/api/users/:id", [](const runtime::http::HttpRequest& req,
                                    runtime::http::HttpResponse& resp) {
        resp.SetContentType("application/json; charset=utf-8");
        resp.SetBody("{\"id\":\"" + std::string(req.PathParam("id")) + "\"}");
    });

    server.Post("/api/echo", [](const runtime::http::HttpRequest& req,
                                runtime::http::HttpResponse& resp) {
        resp.SetContentType("text/plain");
        resp.SetBody(std::string(req.Body()));
    });

    server.Start();
    loop.Loop();
}
```

---

## Benchmarks

**Environment:** AMD EPYC 9754 (2 vCPU, KVM) · 3.6 GiB RAM · Debian 12 · Release build (`-O3`)  
**Tool:** [wrk](https://github.com/wg/wrk) — 5 s duration, 1 wrk thread, Keep-Alive enabled, `IO_THREADS=2`

### HTTP throughput

| Endpoint | Connections | RPS | P50 | P99 |
|---|---|---|---|---|
| `GET /api/health` | 64 | 64,750 | 0.47 ms | 9.90 ms |
| `GET /api/health` | 128 | **69,481** | 0.99 ms | 12.04 ms |
| `POST /api/echo` | 64 | 62,895 | 0.49 ms | 10.30 ms |
| `POST /api/echo` | 128 | **62,756** | 1.08 ms | 11.13 ms |
| `GET /static` (file) | 128 | 22,202 | 4.28 ms | 16.02 ms |

Keep-Alive off drops throughput ~7× (9,255 RPS at c=128) — new TCP handshake per request dominates.

### Memory allocator comparison (single machine, `-O2`)

| Scenario | Pool (ns/op) | new/delete (ns/op) | Speedup |
|---|---|---|---|
| Sequential fill + drain | 28.3 | 27.3 | 1.0× |
| Interleaved alloc/free (std::mutex) | 35.2 | 24.9 | 0.7× |
| Batch-32 alloc + free | 19.2 | 37.3 | **1.9×** |
| ObjectPool with ctor/dtor (Task) | 35.8 | 188.8 | **5.3×** |
| Interleaved, NullMutex (no lock) | 1.2 | 40.3 | **33×** |
| 8-thread contention (std::mutex) | 50.9 | 56.9 | 1.1× |

Takeaway: `std::mutex` erases single-slot interleaved gains; `NullMutex` + per-thread pool delivers 33× in lock-free paths. Object construction/destruction is the dominant cost in typed allocation — `ObjectPool` with placement new is 5× faster than `new/delete`.

---

## Repository Layout

```
.
├── include/runtime/
│   ├── http/        # HTTP layer (server, router, context, request, response)
│   ├── net/         # TCP/net layer (event loop, channel, buffer, timers)
│   ├── log/         # Async logger
│   ├── memory/      # MemoryPool, ObjectPool, SegmentLRUCache
│   ├── metrics/     # Counter, Gauge, Histogram, Registry
│   ├── task/        # ThreadPool, Scheduler
│   ├── time/        # Timestamp
│   ├── trace/       # TraceId, LifecycleTrace
│   ├── inference/   # LLM inference integration (llama engine, SSE streaming)
│   ├── config/      # Config loader
│   └── base/        # NonCopyable, CurrentThread
├── src/             # Corresponding .cpp implementations
├── examples/
│   ├── demo_http_server.cpp   # KV store demo (REST API)
│   └── simple_echo_server.cpp
├── tests/
│   ├── unit/        # Unit tests (GTest + smoke tests)
│   └── integration/ # Integration tests
├── benchmarks/      # wrk scripts and result archives
└── docs/            # Design notes
```

---

## Status

| Module | Status |
|---|---|
| Net layer (Reactor, TcpServer, Buffer, Timer) | Done |
| HTTP layer (parser, Trie router, response) | Done |
| Async logger | Done |
| ThreadPool | Done |
| MemoryPool / ObjectPool | Done |
| Metrics (Counter / Gauge / Histogram) | Headers done, export integration pending |
| Middleware chain | Planned |
| HTTPS / TLS | Planned |
| LLM inference integration | In progress |

---

## License

[MIT](LICENSE)

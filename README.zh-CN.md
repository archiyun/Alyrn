# high-concurrency-runtime

> 基于 C++20 的高性能 HTTP 服务框架，采用 Reactor + One-Loop-Per-Thread 事件驱动模型。

**[English](README.md)**

---

## 核心特性

- **Reactor 模型** — 基于 `epoll` 的事件驱动循环，`readv` scatter-gather 零拷贝读取
- **One-Loop-Per-Thread** — Main Loop 只负责 accept，每条连接由 Sub Loop 线程独占，IO 线程间无共享状态，无全局锁
- **Trie 路由器** — 路径匹配 `O(k)`（k = 路径段数），支持动态参数（`:param`），静态段优先于参数段，自动区分 404 / 405
- **增量 HTTP 解析器** — `HttpContext` 状态机直接消费 `Buffer&` 字节，零中间拷贝；keep-alive 场景下调用 `Reset()` 复用同一上下文
- **异步日志** — 双缓冲批量落盘，IO 线程写日志不阻塞
- **内存池** — 侵入式 free-list 分配器；`ObjectPool` 封装 RAII `ScopedPtr`，`NullMutex` 变体适用于单线程路径（基准测试中比 `new/delete` 快 33×）
- **C++20** — 纯头文件模板，`std::any` 承载类型化连接上下文，结构化绑定

---

## 架构总览

```
┌──────────────────────────────────────────────┐
│               HTTP 层                        │  runtime::http
│   HttpServer · Router · HttpContext           │
│   HttpRequest · HttpResponse                 │
└──────────────────┬───────────────────────────┘
                   │ 依赖
┌──────────────────▼───────────────────────────┐
│               Net 层                         │  runtime::net
│   TcpServer · TcpConnection · EventLoop      │
│   EpollPoller · Channel · Buffer             │
│   Acceptor · TimerQueue · EventLoopThreadPool│
└──────────────────┬───────────────────────────┘
                   │ 依赖
┌──────────────────▼───────────────────────────┐
│             基础层                           │  runtime::log / time / task / memory
│   AsyncLogger · Timestamp                    │
│   ThreadPool · MemoryPool · ObjectPool       │
└──────────────────────────────────────────────┘
```

依赖关系严格单向向下，上层模块不得反向引用下层头文件。

---

## 请求处理链路

```
内核数据
  └─ Buffer::ReadFd()                 ← scatter-gather 读，无中间拷贝
       └─ MessageCallback(conn, buf, ts)
            └─ HttpContext::ParseRequest(buf)   ← 状态机，原地消费字节
                 └─ HttpRequest（解析完成）
                      └─ Router::Match(method, path)
                           └─ Handler(req, resp)
                                └─ conn->Send(resp.ToString())
```

---

## 快速开始

### 依赖

- Linux（需要 epoll）
- GCC 12+ 或 Clang 15+（C++20）
- CMake 3.20+

### 构建

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

### 运行 Demo 服务

```bash
./build/examples/demo_http_server
# 默认监听 127.0.0.1:18080
```

支持的环境变量：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `HOST` | `127.0.0.1` | 绑定地址 |
| `PORT` | `18080` | 监听端口 |
| `IO_THREADS` | 自动（≤ 4） | Sub Loop 线程数 |
| `ET_MODE` | 未设置 | 启用边沿触发 epoll |

```bash
# 健康检查
curl http://127.0.0.1:18080/api/health

# Echo POST body
curl -X POST http://127.0.0.1:18080/api/echo -d "hello"

# 内置 KV 存储
curl -X POST http://127.0.0.1:18080/api/kv/foo -d "bar"
curl http://127.0.0.1:18080/api/kv/foo
curl http://127.0.0.1:18080/api/kv

# 404 / 405 验证
curl http://127.0.0.1:18080/notfound
curl -X DELETE http://127.0.0.1:18080/api/health
```

### 接入自己的服务

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

    // 动态路径参数
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

## 基准测试

**测试环境：** AMD EPYC 9754（2 vCPU，KVM 虚拟化）· 3.6 GiB RAM · Debian 12 · Release 构建（`-O3`）  
**测试工具：** [wrk](https://github.com/wg/wrk) — 持续 5 秒，1 个 wrk 线程，Keep-Alive 开启，`IO_THREADS=2`

### HTTP 吞吐量

| 接口 | 并发连接 | QPS | P50 延迟 | P99 延迟 |
|---|---|---|---|---|
| `GET /api/health` | 64 | 64,750 | 0.47 ms | 9.90 ms |
| `GET /api/health` | 128 | **69,481** | 0.99 ms | 12.04 ms |
| `POST /api/echo` | 64 | 62,895 | 0.49 ms | 10.30 ms |
| `POST /api/echo` | 128 | **62,756** | 1.08 ms | 11.13 ms |
| `GET /static`（静态文件） | 128 | 22,202 | 4.28 ms | 16.02 ms |

关闭 Keep-Alive 后吞吐量下降约 7×（c=128 时 QPS 降至 9,255）——每请求建立新 TCP 连接的握手开销成为主要瓶颈。

### 内存分配器对比（单机，`-O2`）

| 场景 | Pool（ns/op） | new/delete（ns/op） | 加速比 |
|---|---|---|---|
| 顺序 fill + drain | 28.3 | 27.3 | 1.0× |
| 交错 alloc/free（std::mutex） | 35.2 | 24.9 | 0.7× |
| 批量 32 alloc + free | 19.2 | 37.3 | **1.9×** |
| ObjectPool 含 ctor/dtor（Task） | 35.8 | 188.8 | **5.3×** |
| 交错，NullMutex（无锁） | 1.2 | 40.3 | **33×** |
| 8 线程竞争（std::mutex） | 50.9 | 56.9 | 1.1× |

**分析：**
- `std::mutex` 的加锁开销完全抵消了单槽交错场景的收益（场景 2 甚至比 `new/delete` 慢）
- 去掉锁（`NullMutex`）后可达 33× 加速——适合 One-Loop-Per-Thread 架构下的单线程对象池
- 含构造/析构的对象分配收益最明显（5.3×）：`new` 需经过 malloc + ctor，对象池只需 free-list pop + placement new

---

## 目录结构

```
.
├── include/runtime/
│   ├── http/        # HTTP 层（server、router、context、request、response）
│   ├── net/         # TCP/网络层（event loop、channel、buffer、timer）
│   ├── log/         # 异步日志
│   ├── memory/      # MemoryPool、ObjectPool、SegmentLRUCache
│   ├── metrics/     # Counter、Gauge、Histogram、Registry
│   ├── task/        # ThreadPool、Scheduler
│   ├── time/        # Timestamp
│   ├── trace/       # TraceId、LifecycleTrace
│   ├── inference/   # LLM 推理集成（llama engine、SSE 流式响应）
│   ├── config/      # 配置加载
│   └── base/        # NonCopyable、CurrentThread
├── src/             # 对应的 .cpp 实现
├── examples/
│   ├── demo_http_server.cpp   # KV store demo（REST API）
│   └── simple_echo_server.cpp
├── tests/
│   ├── unit/        # 单元测试（GTest + smoke tests）
│   └── integration/ # 集成测试
├── benchmarks/      # wrk 脚本与测试结果归档
└── docs/            # 设计文档
```

---

## 开发状态

| 模块 | 状态 |
|---|---|
| Net 层（Reactor、TcpServer、Buffer、定时器） | 完成 |
| HTTP 层（解析器、Trie 路由、响应） | 完成 |
| 异步日志 | 完成 |
| 线程池 | 完成 |
| 内存池 / 对象池 | 完成 |
| Metrics（Counter / Gauge / Histogram） | 头文件完成，导出接入待实现 |
| 中间件链 | 规划中 |
| HTTPS / TLS | 规划中 |
| LLM 推理集成 | 进行中 |

---

## License

[MIT](LICENSE)

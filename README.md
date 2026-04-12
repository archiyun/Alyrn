# high-concurrency-runtime

> 基于 C++20 实现的高并发 HTTP 服务框架，采用 Reactor + One-Loop-Per-Thread 事件驱动模型。

---

## 架构总览

```
┌─────────────────────────────────────────┐
│              HTTP Layer                 │  runtime::http
│   HttpServer / Router                   │
│   HttpRequest / HttpResponse            │
│   HttpContext（状态机解析器）            │
└──────────────────┬──────────────────────┘
                   │ 依赖
┌──────────────────▼──────────────────────┐
│            TCP / Net Layer              │  runtime::net
│   TcpServer / TcpConnection             │
│   EventLoop / Channel / EpollPoller     │
│   Buffer / Socket / Acceptor            │
│   TimerQueue / EventLoopThreadPool      │
└──────────────────┬──────────────────────┘
                   │ 依赖
┌──────────────────▼──────────────────────┐
│           Foundation Layer              │  runtime::log / runtime::time
│   AsyncLogger / Timestamp               │
│   ThreadPool / MemoryPool / ObjectPool  │
└─────────────────────────────────────────┘
```

---

## 核心设计

### Reactor 事件循环

采用 **One-Loop-Per-Thread** 模型，Main Loop 只负责 accept，Sub Loop 线程池负责已建立连接的 IO：

| 组件 | 职责 |
|---|---|
| `EventLoop` | 事件驱动主循环，持有 Poller / Channel / TimerQueue |
| `Channel` | fd 的事件代理，封装感兴趣的事件与回调分发 |
| `EpollPoller` | epoll 的具体封装，实现 Poller 接口 |
| `EventLoopThreadPool` | Sub Loop 线程池，管理连接 IO |

### 连接管理

| 组件 | 职责 |
|---|---|
| `TcpServer` | 监听端口，accept 新连接，分配 IO 线程 |
| `TcpConnection` | 单条连接的全生命周期管理（状态机 + 读写缓冲） |
| `Buffer` | 三段滑动索引缓冲区，支持 `ReadFd` 零拷贝读取 |
| `Acceptor` | 专门监听 accept，建立连接后通知 TcpServer |

### HTTP 层

HTTP 模块构建在 TcpServer 之上，连接状态通过 `TcpConnection::SetContext(std::any)` 绑定，**每条连接的解析状态自治，IO 线程间无共享数据，无全局锁**。

| 组件 | 职责 |
|---|---|
| `HttpContext` | 状态机解析器，直接消费 `Buffer&`，零中间拷贝 |
| `HttpRequest` | 已解析的请求数据，Header 查找大小写不敏感 |
| `HttpResponse` | 响应序列化，`Content-Length` 在 `ToString()` 时动态计算 |
| `Router` | 静态路由表，`(Method, path)` → `Handler`，区分 404 / 405 |
| `HttpServer` | 组装层，注册回调，驱动解析 → 路由 → 发送循环 |

请求处理数据流：

```
内核数据
  └─ Buffer::ReadFd()
       └─ MessageCallback(conn, Buffer&, ts)    ← 零拷贝
            └─ HttpContext::ParseRequest(buf)   ← 直接消费字节
                 └─ HttpRequest（解析完成）
                      └─ Router::Match()
                           └─ Handler(req, resp)
                                └─ conn->Send(resp.ToString())
```

### 异步日志

`Logger` 单例 + RAII LogMessage + 宏实现流式输出，底层 `AsyncLogger` 异步批量落盘，支持滚动日志文件，IO 线程不阻塞。

---

## 快速开始

### 依赖

- Linux（epoll）
- GCC 12+ 或 Clang 15+（C++20）
- CMake 3.20+

### 构建

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

### 运行 demo

```bash
./build/examples/demo_http_server
```

服务启动后监听 `127.0.0.1:18080`，可用 curl 验证：

```bash
# 基础路由
curl http://127.0.0.1:18080/
curl http://127.0.0.1:18080/health

# POST + Body
curl -X POST http://127.0.0.1:18080/echo -d "hello"

# 404 / 405
curl http://127.0.0.1:18080/notfound
curl -X POST http://127.0.0.1:18080/
```

### 在代码中使用

```cpp
#include "runtime/http/http_server.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"

int main() {
    runtime::net::EventLoop loop;
    runtime::http::HttpServer server(&loop,
        runtime::net::InetAddress(8080, "0.0.0.0"), "my-server");

    server.SetThreadNum(4);

    server.Get("/api/hello", [](const runtime::http::HttpRequest& req,
                                runtime::http::HttpResponse& resp) {
        resp.SetContentType("application/json; charset=utf-8");
        resp.SetBody("{\"hello\":\"world\"}");
    });

    server.Start();
    loop.Loop();
}
```

---

## 基准测试

测试环境：AMD EPYC 9754（2 vCPU）/ 3.6 GiB RAM / Debian 12 / Release 构建 / IO 线程数 = 2

| 场景 | 并发数 | QPS | P50 延迟 | P99 延迟 |
|---|---|---|---|---|
| GET /api/health | 128 | **69,481** | 0.99 ms | 12.04 ms |
| GET /api/echo | 128 | **62,756** | 1.08 ms | 11.13 ms |
| GET /api/echo | 64 | **62,895** | 0.49 ms | 10.30 ms |

测试工具：[wrk](https://github.com/wg/wrk)，持续 5 秒。

---

## 目录结构

```
.
├── include/runtime/
│   ├── http/          # HTTP 层头文件
│   ├── net/           # TCP / 网络层头文件
│   ├── log/           # 异步日志
│   ├── memory/        # 内存池 / 对象池
│   ├── metrics/       # 指标（Counter / Gauge / Histogram）
│   ├── task/          # 线程池 / 任务调度
│   ├── time/          # Timestamp
│   └── trace/         # 链路追踪（TraceId）
├── src/               # 对应实现
├── examples/          # 可直接运行的示例
│   └── demo_http_server.cpp
├── tests/
│   ├── unit/          # 单元测试
│   ├── integration/   # 集成测试
│   └── stress/        # 压力测试
├── benchmarks/        # 基准测试结果
└── docs/              # 设计文档
```

---

## 开发状态

| 模块 | 状态 |
|---|---|
| Net Layer（Reactor / TcpServer / Buffer） | 完成 |
| HTTP Layer（解析 / 路由 / 响应） | 完成 |
| 异步日志 | 完成 |
| 定时器（TimerQueue） | 完成 |
| 线程池 / 内存池 | 完成 |
| Metrics（Counter / Gauge / Histogram） | 头文件完成，导出待接入 |
| 动态路由（Trie） | 待实现 |
| 中间件链 | 待实现 |
| HTTPS（TLS） | 待实现 |

---

> 该项目仍在持续开发中，代码仅供参考学习。

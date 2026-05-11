# high-concurrency-runtime

[English](README.md) | **中文**

一个 C++20 高并发 Linux 网络运行时。项目采用分层设计——你可以把它当作完整的**反向代理网关**使用，也可以只用 **HTTP 应用服务器**层、裸 **TCP/事件循环**层，或者单独使用其中的**数据结构 / 内存分配器 / 调度器**库。上层依赖下层，无循环依赖。

```
Gateway Layer  ─── runtime::gateway
HTTP Layer     ─── runtime::http        (依赖 net)
Net Layer      ─── runtime::net         (依赖 foundation)
Foundation     ─── runtime::base / log / time / task / memory / metrics
```

## 功能概览

### 网关层（`runtime::gateway`）

- **反向代理** — 将 HTTP 请求透明转发到上游 backend，底层使用持久连接
- **上游管理** — `UpstreamRegistry` + `Upstream` + `UpstreamPeer`；支持运行时动态添加 backend
- **负载均衡** — 轮询、平滑加权轮询、最少连接、随机、加权随机；通过 `LoadBalancer` 接口可插拔
- **主动健康检查** — 定时 HTTP 探针；连续失败 N 次摘除 backend，连续成功 M 次恢复
- **被动故障追踪** — `ProxySession` 记录每次请求的失败；达到 `max_fails` 后隔离 `fail_timeout`
- **连接池** — `UpstreamConnPool` 对每个 backend 维护持久连接，每个 I/O 线程独立一个池，无跨线程竞争
- **直接路由** — 在网关上直接注册同步 handler，无需单独启动 `HttpServer`
- **静态文件服务** — 将本地目录挂载到 URL 前缀
- **代码驱动配置** — 无配置文件，上游和路由全部在 C++ 代码中注册

### HTTP 服务层（`runtime::http`）

- `epoll` 事件驱动，One-Loop-Per-Thread I/O 线程模型
- 增量式 HTTP/1.1 解析器（`HttpContext`）——零中间拷贝，keep-alive 通过 `Reset()` 复用上下文
- 前缀树路由，支持静态段和动态路径参数（`:param` 语法）
- 静态分支优先匹配，`/users/me` 不会被 `/users/:id` 错误捕获

### 网络层（`runtime::net`）

- `EventLoop`、`EpollPoller`、`Channel`、`TcpServer`、`TcpConnection`、`Buffer`
- 定时器队列基于泛型侵入式红黑树（`TimerTree`）实现
- 支持水平触发和边缘触发两种 epoll 模式

### 基础设施

- 异步双缓冲日志
- `MemoryPool`、`ObjectPool` 内存分配器
- `Scheduler`、`ThreadPool`、`WorkQueue`，支持协作式任务取消
- `IntrusiveRBTree<T, kMember, kLess>` — 泛型侵入式红黑树，节点零堆分配
- `Counter`、`Gauge`、`Histogram`、`Registry` 指标接口

## 环境要求

基础依赖：

- Linux（使用 `epoll`）
- CMake ≥ 3.20
- GCC 12+ 或 Clang 15+，支持 C++20
- POSIX threads

可选依赖：

- GoogleTest — 若 CMake 检测到则自动构建 `runtime_unit_tests` 和 `runtime_integration_tests`
- `liburing` — 仅 `io_uring_echo` 示例需要
- `wrk` — 用于运行 HTTP 压测脚本

Ubuntu / Debian 安装常用依赖：

```bash
sudo apt update
sudo apt install -y build-essential cmake git
```

## 下载项目

```bash
git clone https://github.com/akiba-miku/high-concurrency-runtime.git
cd high-concurrency-runtime
```

## 快速构建

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

常用 CMake 选项：

| 选项 | 默认值 | 说明 |
|---|---:|---|
| `BUILD_EXAMPLES` | `ON` | 构建 `examples/` 下的示例程序 |
| `BUILD_TESTS` | `ON` | 构建 `tests/` 下的测试 |
| `CMAKE_BUILD_TYPE` | 空 | 单配置生成器下建议设为 `Debug` 或 `Release` |

## 运行网关示例

先启动两个上游 HTTP server 模拟 backend：

```bash
PORT=9001 ./build/examples/demo_http_server &
PORT=9002 ./build/examples/demo_http_server &
```

启动网关（监听 `0.0.0.0:8080`）：

```bash
./build/examples/demo_gateway
```

验证各路由：

```bash
# 直接路由——由网关自己处理
curl -i http://127.0.0.1:8080/healthz

# 代理路由——轮询转发到 9001 / 9002
curl -i http://127.0.0.1:8080/api/health
curl -i http://127.0.0.1:8080/api/kv
```

关掉其中一个 backend，观察健康检查在一个探测周期内将其摘除，请求不再转发给它。

## 运行 HTTP 示例

```bash
./build/examples/demo_http_server
```

默认监听 `127.0.0.1:18080`，可以通过环境变量调整：

| 变量 | 默认值 | 说明 |
|---|---:|---|
| `HOST` | `127.0.0.1` | 监听地址 |
| `PORT` | `18080` | 监听端口 |
| `IO_THREADS` | 自动探测，最多 4 | I/O 子线程数量 |
| `ET_MODE` | 未设置 | 设置任意值启用 edge-triggered epoll |

```bash
curl http://127.0.0.1:18080/api/health
curl -X POST http://127.0.0.1:18080/api/echo -d "hello"
curl -X POST http://127.0.0.1:18080/api/kv/name -d "kunkun"
curl http://127.0.0.1:18080/api/kv/name
```

## 在自己的程序中使用

将本项目作为 CMake 子目录引入，按需链接对应层级：

```cmake
add_subdirectory(high-concurrency-runtime)

# 完整网关
target_link_libraries(my_gw    PRIVATE runtime_gateway)

# 只要 HTTP server
target_link_libraries(my_http  PRIVATE runtime_http)

# 只要 TCP 事件循环
target_link_libraries(my_tcp   PRIVATE runtime_net)

# 只要分配器 / 调度器 / 数据结构
target_link_libraries(my_util  PRIVATE runtime_foundation)
```

### 网关示例

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
    gw.SetThreadNum(4);

    // 直接路由——网关自己响应
    gw.Get("/healthz", [](const runtime::http::HttpRequest&,
                          runtime::http::HttpResponse& resp) {
        resp.SetContentType("application/json");
        resp.SetBody("{\"status\":\"ok\"}");
    });

    // 代理路由——转发到 backend
    gw.AddProxyRoute("/api/", "backend", "round_robin");

    // 主动健康检查
    gw.EnableHealthCheck({.path = "/api/health", .interval_sec = 10.0});

    gw.Start();
    loop.Loop();
}
```

### HTTP server 示例

```cpp
#include "runtime/http/http_server.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"

int main() {
    runtime::net::EventLoop loop;
    runtime::http::HttpServer server(&loop,
        runtime::net::InetAddress(8080, "0.0.0.0"), "my-server");
    server.SetThreadNum(4);

    server.Get("/api/users/:id",
        [](const runtime::http::HttpRequest& req,
           runtime::http::HttpResponse& resp) {
            resp.SetContentType("application/json; charset=utf-8");
            resp.SetBody("{\"id\":\"" + std::string(req.PathParam("id")) + "\"}");
        });

    server.Start();
    loop.Loop();
}
```

### 任务调度示例

```cpp
#include "runtime/task/scheduler.h"
#include <iostream>

int main() {
    runtime::task::Scheduler scheduler(4);
    auto handle = scheduler.Submit([] { std::cout << "hello\n"; });
    handle.Wait();
}
```

库目标关系：

| 目标 | 提供 |
|---|---|
| `runtime_gateway` | 网关、反向代理、负载均衡器、健康检查 |
| `runtime_http` | HTTP 服务器、Trie 路由、请求/响应、上下文 |
| `runtime_net` | EventLoop、TcpServer、Channel、Poller、Buffer、TimerQueue |
| `runtime_task` | Scheduler、ThreadPool、Task、WorkQueue |
| `runtime_foundation` | 日志、时间戳、MemoryPool、ObjectPool、IntrusiveRBTree、指标 |

## 运行测试

```bash
ctest --test-dir build --output-on-failure
```

只运行某个测试：

```bash
ctest --test-dir build -R rbtree_validator --output-on-failure
```

主要测试：

| 二进制 | 测试内容 |
|---|---|
| `rbtree_validator` | 1000 万次操作对比 `std::set` 对数器 + 每步 `CheckRBInvariants()` |
| `http_smoke_test` | HTTP 解析与路由（不依赖 GTest） |
| `buffer_smoke_test` | Buffer 读/写/预置 |
| `runtime_unit_tests` | GTest 套件：buffer、logger、内存池、调度器 |
| `runtime_integration_tests` | GTest 套件：事件循环、TCP 服务器、HTTP 路由、触发模式 |

说明：

- smoke 测试不依赖 GTest
- 如果 CMake 找到 GTest，会额外构建 `runtime_unit_tests` 和 `runtime_integration_tests`

## 运行压测

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
IO_THREADS=2 ./build/examples/demo_http_server
```

```bash
wrk -t1 -c128 -d5s http://127.0.0.1:18080/api/health
wrk -t1 -c128 -d5s -s benchmarks/wrk/post_echo.lua http://127.0.0.1:18080/api/echo
bash benchmarks/wrk/run_wrk.sh
```

## 目录结构

```text
.
├── include/runtime/
│   ├── base/        # NonCopyable、CurrentThread、ThreadPool、IntrusiveRBTree
│   ├── config/      # 配置加载接口
│   ├── gateway/     # GatewayServer、Upstream、LoadBalancer、HealthChecker、ProxyPass
│   ├── http/        # HttpServer、Router、HttpContext、HttpRequest、HttpResponse
│   ├── log/         # Logger、AsyncLogger
│   ├── memory/      # MemoryPool、ObjectPool
│   ├── metrics/     # Counter、Gauge、Histogram、Registry
│   ├── net/         # EventLoop、TcpServer、Channel、Poller、Buffer、Timer
│   ├── task/        # Scheduler、ThreadPool、Task、WorkQueue
│   ├── time/        # Timestamp
│   └── trace/       # TraceId、LifecycleTrace
├── src/             # 各模块实现（目录结构与 include 对称）
├── examples/        # demo_gateway、demo_http_server、demo_echo_server、demo_rbtree
├── tests/           # 单元测试、集成测试、smoke 测试、对数器验证
├── benchmarks/      # wrk 压测脚本和结果归档
├── config/          # 示例配置
├── docs/            # 设计文档
└── third_party/     # 第三方头文件
```

## 架构分层

```text
Gateway Layer   runtime::gateway
  GatewayServer、UpstreamRegistry、Upstream、UpstreamPeer
  LoadBalancer（RoundRobin / WeightedRoundRobin / LeastConn / Random）
  HealthChecker、ProxyPass、UpstreamConnPool、StaticHandler

HTTP Layer      runtime::http
  HttpServer、Router（Trie）、HttpContext、HttpRequest、HttpResponse

Net Layer       runtime::net
  TcpServer、TcpConnection、EventLoop、EpollPoller、Channel
  Buffer、TimerQueue（基于 IntrusiveRBTree）

Foundation      runtime::base / log / time / task / memory / metrics
  AsyncLogger、Scheduler、ThreadPool
  MemoryPool、ObjectPool
  IntrusiveRBTree<T, kMember, kLess>
  Timestamp、Counter、Gauge、Histogram
```

典型网关请求路径：

```text
kernel
  → Buffer::ReadFd()
  → TcpConnection 消息回调（Sub Loop）
  → HttpContext::ParseRequest()        增量状态机解析
  → GatewayServer::MatchRoute()
      ├── Direct  → 同步 Handler → HttpResponse
      ├── Proxy   → LoadBalancer::Select() → UpstreamConnPool
      │             → ProxySession（异步上游 I/O，同一 Sub Loop）
      └── Static  → ThreadPool 文件 I/O → HttpResponse
  → TcpConnection::Send()
```

典型 HTTP 请求路径：

```text
kernel
  → Buffer::ReadFd()
  → TcpConnection 消息回调
  → HttpContext::ParseRequest()
  → Router::Match()
  → 用户 handler
  → HttpResponse::ToString()
  → TcpConnection::Send()
```

## License

本项目使用 [MIT License](LICENSE)。

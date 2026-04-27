# high-concurrency-runtime

**中文** | [English](README.md)

`high-concurrency-runtime` 是一个 C++20 高并发网络运行时与 HTTP 服务框架。
项目基于 Reactor 模型和 One-Loop-Per-Thread 线程模型，提供 TCP 网络层、HTTP 路由层、异步日志、任务调度器、内存池、指标组件等基础设施，适合用来学习或搭建轻量级 Linux 服务端程序, 或者基于此框架来学习`WebSocket`, `RPC`, 分布式系统， 数据库连接半ORM。

## 功能概览

- Linux `epoll` 事件驱动网络层
- One-Loop-Per-Thread I/O 线程模型
- TCP server、connection、buffer、timer、poller 等基础组件
- HTTP server、请求解析、响应构造、Trie 路由、路径参数
- 协作式任务取消、优先级任务队列、线程池调度器
- 异步日志与时间戳工具
- MemoryPool / ObjectPool 内存池
- Counter / Gauge / Histogram / Registry 指标接口
- 示例服务、单元测试、集成测试和 wrk 压测脚本

## 环境要求

推荐在 Linux 环境下构建。
如果要在 MacOS 或者其它环境需要修改网络层来兼容。

基础依赖：

- Linux 环境 , for `epoll`
- CMake 3.20 或更高版本
- 支持 C++20 的编译器，推荐 GCC 12+ 或 Clang 15+
- POSIX threads

可选依赖：

- `liburing`：用于构建 `examples/io_uring_echo`
- GoogleTest：如果系统安装了 GTest，CMake 会自动构建更完整的 gtest 测试；未安装时会跳过这些测试
- `wrk`：用于运行 HTTP 压测脚本

Ubuntu / Debian 可以先安装常用依赖：

```bash
sudo apt update
sudo apt install -y build-essential cmake git liburing2 liburing-dev
```

如果只想构建核心库和测试，不运行 `io_uring` 示例，可以不安装 `liburing`，并在配置时关闭示例：

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_EXAMPLES=OFF
cmake --build build -j"$(nproc)"
```

## 下载项目

使用 HTTPS：

```bash
git clone https://github.com/akiba-miku/high-concurrency-runtime.git
cd high-concurrency-runtime
```

或者使用 SSH：

```bash
git clone git@github.com:akiba-miku/high-concurrency-runtime.git
cd high-concurrency-runtime
```

## 快速构建

默认会构建库、示例程序和测试：

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

只构建核心库：

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_EXAMPLES=OFF -DBUILD_TESTS=OFF
cmake --build build -j"$(nproc)"
```

Debug 构建：

```bash
cmake -B build-debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build-debug -j"$(nproc)"
```

## 运行 HTTP 示例

构建完成后运行 demo HTTP server：

```bash
./build/examples/demo_http_server
```

默认监听：

```text
127.0.0.1:18080
```

可以通过环境变量调整运行参数：

| 变量 | 默认值 | 说明 |
|---|---:|---|
| `HOST` | `127.0.0.1` | 监听地址 |
| `PORT` | `18080` | 监听端口 |
| `IO_THREADS` | 自动探测，最多 4 | I/O 子线程数量 |
| `ET_MODE` | 未设置 | 设置任意值后启用 edge-triggered epoll |

示例：

```bash
HOST=0.0.0.0 PORT=18080 IO_THREADS=4 ./build/examples/demo_http_server
```

另开一个终端访问接口：

```bash
curl http://127.0.0.1:18080/
curl http://127.0.0.1:18080/api/health
curl -X POST http://127.0.0.1:18080/api/echo -d "hello runtime"
```

KV 示例接口：

```bash
curl -X POST http://127.0.0.1:18080/api/kv/name -d "miku"
curl http://127.0.0.1:18080/api/kv/name
curl http://127.0.0.1:18080/api/kv
```

404 / 405 行为验证：

```bash
curl http://127.0.0.1:18080/notfound
curl -X DELETE http://127.0.0.1:18080/api/health
```

## 运行 TCP Echo 示例

启动 TCP echo server：

```bash
./build/examples/simple_echo_server
```

默认监听 `127.0.0.1:8080`。可以用 `nc` 测试：

```bash
nc 127.0.0.1 8080
hello
```

服务端会把收到的内容原样写回。

## 在自己的程序中使用

最简单的方式是把本项目作为子目录加入你的 CMake 工程，然后链接对应的静态库目标。

```cmake
add_subdirectory(high-concurrency-runtime)

add_executable(my_server main.cpp)
target_link_libraries(my_server PRIVATE runtime_http)
```

一个最小 HTTP 服务：

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

如果只需要任务调度器，可以链接 `runtime_task`：

```cmake
target_link_libraries(my_app PRIVATE runtime_task)
```

示例代码：

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

库目标关系：

| 目标 | 说明 |
|---|---|
| `runtime_foundation` | 日志、时间、基础工具 |
| `runtime_task` | 任务、线程池、调度器 |
| `runtime_net` | TCP 网络层和事件循环 |
| `runtime_http` | HTTP 服务层，依赖 net/task/foundation |

## 运行测试

默认配置 `BUILD_TESTS=ON` 时可以运行：

```bash
ctest --test-dir build --output-on-failure
```

只运行某个测试：

```bash
ctest --test-dir build -R http_smoke_test --output-on-failure
```

说明：

- smoke 测试不依赖 GTest
- 如果 CMake 找到了 GTest，会额外构建 `runtime_unit_tests` 和 `runtime_integration_tests`
- 如果没有 GTest，配置阶段会提示 `GTest not found; skipping gtest-based unit tests.`

## 运行压测

先构建 Release 版本并启动 HTTP demo：

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
IO_THREADS=2 ./build/examples/demo_http_server
```

另开终端使用 `wrk`：

```bash
wrk -t1 -c128 -d5s http://127.0.0.1:18080/api/health
wrk -t1 -c128 -d5s -s benchmarks/wrk/post_echo.lua http://127.0.0.1:18080/api/echo
```

也可以使用项目内脚本：

```bash
bash benchmarks/wrk/run_wrk.sh
```

## 目录结构

```text
.
├── include/runtime/
│   ├── base/        # NonCopyable、CurrentThread 等基础工具
│   ├── config/      # 配置加载接口
│   ├── http/        # HTTP server、router、request、response、context
│   ├── inference/   # 推理相关接口
│   ├── log/         # Logger、AsyncLogger
│   ├── memory/      # MemoryPool、ObjectPool、缓存结构
│   ├── metrics/     # Counter、Gauge、Histogram、Registry
│   ├── net/         # EventLoop、TcpServer、Channel、Poller、Buffer、Timer
│   ├── task/        # Scheduler、ThreadPool、Task、WorkQueue
│   ├── time/        # Timestamp
│   └── trace/       # TraceId、LifecycleTrace
├── src/             # 各模块实现
├── examples/        # HTTP/TCP/logger/io_uring 示例
├── tests/           # 单元测试与集成测试
├── benchmarks/      # wrk 压测脚本和结果归档
├── config/          # 示例配置
├── docs/            # 设计文档
└── third_party/     # 第三方头文件
```

## 架构分层

```text
HTTP Layer:        runtime::http
  HttpServer, Router, HttpContext, HttpRequest, HttpResponse

Net Layer:         runtime::net
  TcpServer, TcpConnection, EventLoop, Poller, Channel, Buffer, TimerQueue

Foundation Layer:  runtime::base / log / time / task / memory / metrics
  AsyncLogger, Scheduler, ThreadPool, MemoryPool, ObjectPool, Timestamp
```

典型 HTTP 请求路径：

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

本项目使用 [MIT License](LICENSE)。

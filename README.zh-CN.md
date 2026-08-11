# CoroPact⚡

![C++](https://img.shields.io/badge/C++-23-blue)
![Platform](https://img.shields.io/badge/platform-Linux-lightgrey)
![License](https://img.shields.io/github/license/archiyun/CoroPact)
![Stars](https://img.shields.io/github/stars/archiyun/CoroPact?style=social)

***面向 Linux 的 C++23 异步网络运行时，由协程、epoll 与 io_uring 驱动***

CoroPact 在相互独立的 Reactor 与 io_uring 网络后端之上，提供统一、直观且高性能的 C++23 协程编程模型。它让默认路径像 Go 的网络库一样不暴露底层事件机制，并通过 `Runtime` 提供类似 Tokio 的快速启动方式；需要时，应用仍可显式使用后端原生扩展与配置。

CoroPact 使用[生命周期精化协程 I/O（LRCI）](docs/design/zh-CN/network/lifecycle-refined-coroutine-io.md)：readiness 与 CQE 等后端事件不会直接等同于协程完成，而是被精化到一套共享逻辑生命周期，分别确定结果何时 ready、continuation 何时恢复、资源何时释放。

* 🔀 **统一的异步 I/O 契约**
  epoll 与 io_uring 保留各自的线程、事件循环与完成模型，但通过 `io` 的 `AsyncStream`、`AsyncListener` 与 `AsyncConnector` concept 提供一致的业务可观察语义。`coro` 以同步代码形式表达异步控制流，并隐藏协程帧、挂起、恢复与生命周期细节；业务代码无需接触 `epoll_event`、SQE 或 CQE。

* 🧩 **明确的所有权与完成语义**
  每个 Worker 独占自己的线程、事件循环、连接与 I/O 操作。操作在所属执行上下文中完成，协程 continuation 也在相同上下文中恢复，同时明确约束 buffer 生命周期、取消行为与异步关闭流程。

* 🚀 **基础功能与高级扩展**
  CoroPact 提供异步 accept、connect、read、write、close 与 timer；Reactor 可选择 LT/ET，luring 还提供 multishot receive、zero-copy send 等扩展。HTTP 与网关策略已迁移至 [CoroGateway](https://github.com/archiyun/CoroGateway)。

当前实现与验证目标是 Linux。抽象契约为未来的 kqueue 或 IOCP 后端保留扩展位置，但它们尚未实现或验证，不能视为当前支持的平台。

## 快速开始

### 1. 选择头文件

应用通常按需包含后端无关模块与一个具体 backend：

```cpp
#include "coropact/coro.h"
#include "coropact/io.h"
#include "coropact/net.h"
#include "coropact/reactor.h"  // 默认 Reactor backend
```

请按实际使用的模块包含头文件。启用 io_uring 后端时，包含
`coropact/luring.h` 替代 `coropact/reactor.h`。

### 2. 后端无关的连接处理协程

以 echo server 为例。该协程只依赖 `AsyncStream`，可同时服务 ReactorStream 与 LUringStream；可运行版本见 [`examples/simple_echo`](examples/simple_echo)。

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
    auto written = co_await stream.WriteAll(payload);
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

### 3. 通过 Runtime 快速启动

```cpp
#include <stop_token>

int main() {
  constexpr int kPort = 19090;
  std::stop_source stop_source;

  auto runtime = cp::Runtime::Create<cp::runtime::Reactor>(
      cp::net::Endpoint::Loopback(kPort),
      [](auto stream) { return HandleConnection(std::move(stream)); });

  // signal handler、管理线程或测试代码随后调用 stop_source.request_stop()。
  auto result = runtime.Run(stop_source.get_token());
  return result.has_value() ? 0 : 1;
}
```

要使用 io_uring，只需在启用 `COROPACT_ENABLE_URING=ON` 的构建中包含 `coropact/luring.h`，并将 tag 改为 `cp::runtime::LUring`。handler 中的 `stream` 仍保持对应后端的静态类型，不会引入虚调用。

### 4. 需要时显式配置

`Create` 使用保守默认值。需要控制 worker 数量时，使用同一个 Runtime 的 backend-specific Builder：

```cpp
auto runtime = cp::Runtime::Builder<cp::runtime::Reactor>{
                   cp::net::Endpoint::Loopback(19090)}
                   .AutoWorkers()
                   .OnConnection([](auto stream) {
                     return HandleConnection(std::move(stream));
                   })
                   .Build();
```

Backend tag 仍在编译期选择实现；ring 深度、provided buffer、zero-copy 等改变后端资源或生命周期语义的选项不伪装成通用 Runtime 配置。

### 5. 使用 luring 原生能力

`Runtime` 只负责默认 TCP server 的 worker 生命周期，不是通用的 io_uring 配置接口。它可以选择安全的默认策略（例如带 fallback 的 multishot accept），但应用若要**显式**控制 ring 深度、SQPOLL、提交批次、provided-buffer ring、multishot receive 或 zero-copy send，应直接组合 `luring::LUringLoop`、`LUringOptions` 与对应的 listener、stream 或 source：

```cpp
coropact::luring::LUringLoop loop;
coropact::luring::LUringOptions options;
options.entries = 8192;
options.shared_buffer_capacity = 256;  // RecvSource 的 provided buffers

auto initialized = loop.Init(options);
// 在 loop 所属线程上创建 listener/source，SpawnDetach(...) 后调用 loop.Run(...)
```

这条原生路径让应用明确承担每个 ring、buffer lease 与操作生命周期；参考 [`examples/luring`](examples/luring) 及 luring 的公开头文件。不要把这些能力增加为 `Runtime` 的跨后端开关。

## 构建

构建 Reactor 后端：

```bash
cmake -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=ON \
  -DBUILD_EXAMPLES=ON

cmake --build build -j"$(nproc)"
ctest --test-dir build --output-on-failure
```

构建并启用 io_uring 后端：

```bash
# 请确保系统已经安装 liburing。

cmake -B build-uring \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=ON \
  -DBUILD_EXAMPLES=ON \
  -DCOROPACT_ENABLE_URING=ON

cmake --build build-uring -j"$(nproc)"
ctest --test-dir build-uring --output-on-failure
```

### 环境要求

* Linux；CMake 3.20+；支持 C++23 coroutine 的编译器。
* 默认 Reactor 构建不依赖额外网络库。
* luring 构建需要 `liburing >= 2.6`，并建议使用 Linux 5.19 或更新内核。

## 架构

```text
自定义 Session / 应用
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

两个后端不共享事件循环，内部状态机也不需要完全一致。它们只需要遵守相同的业务可观察异步 I/O 契约。

io_uring Server 采用 thread-per-ring 模型：

```text
LUringServer
  |
  +-- Worker 0 -> Thread 0 -> LUringLoop 0 -> Ring 0
  +-- Worker 1 -> Thread 1 -> LUringLoop 1 -> Ring 1
  `-- Worker N -> Thread N -> LUringLoop N -> Ring N
```

连接、I/O 操作与协程 continuation 始终归属于创建它们的 Worker 和 Ring，不会在运行过程中跨 Ring 迁移。

## 性能测试

CoroPact 提供了可复现的 `wrk` 性能测试，用于比较：

* Reactor 与 io_uring 后端
* raw liburing
* standalone Asio
* Monoio
* Compio
* libaio poll 兼容路径
* libuv、libevent 与 libev 参考适配器
* Nginx 参考配置

性能测试结果与具体 workload 密切相关，不应被解释为网络框架的综合排名。测试报告会完整保留实验拓扑、原始轮次、延迟异常、CPU 使用率、内存占用与错误数量。

最新的当前源码 C++ 对照基线见[2026-08-10 网络库基线](docs/benchmark/network-libraries-20260810.md)；它记录了 `wrk` 的文件描述符前置条件和无效样本。完整的十个网络库统一公平压测报告（包含图表、汇总数据和每轮关键数据）请参阅[网络库统一公平压测](docs/benchmark/network-libraries.md)。其它测试脚本、原始结果和专项优化记录请参阅 [`docs/benchmark`](docs/benchmark/)。

## 文档

目前多数内容仍然在编写中, 且随版本更新内容会出现迟滞, 仅供参考.

* **[网络架构](docs/design/zh-CN/network/index.md)：** 运行时分层、后端边界与所有权模型。
* **[协程状态机模型](docs/design/zh-CN/network/lamport-hot-swap-runtime.md)：** 抽象 stream 不变量与后端 refinement 说明。
* **[AsyncStream 语义契约](docs/design/zh-CN/network/async-stream-contract.md)：** read、write、close、取消与 buffer 生命周期语义。
* **[数据结构](docs/design/zh-CN/datastructure/index.md):** C++现代风格的侵入式数据结构, 侵入式红黑树, 侵入式链表, MPSC队列的设计与实现, 以及它们在项目各处的应用。SplayTree 与 QuadHeap 属于显式 include 的实验性 API；构建其验证器时使用 `-DBUILD_EXPERIMENTAL_TESTS=ON`。
* **[性能测试](docs/benchmark/network-libraries-20260810.md)：** 最新的当前源码 C++ 对照基线；完整统一网络库报告、测试方法、原始结果与优化记录见 [`docs/benchmark`](docs/benchmark/)。
* **[示例](examples/)：** Reactor 与 io_uring 使用示例。
* **[测试](tests/)：** 协程、网络、生命周期与后端行为验证。

## 当前状态

CoroPact 目前仍是一个实验性网络运行时，尚不适合作为成熟网络框架的生产级替代方案。

当前正在推进的方向包括：

* 给出状态机的形式化证明, 并编写不变量测试和并发检验接入的后端.
* io_uring, 提供liburing库最新版本的网络选项配置和现代优化.
* 更贴近实际环境的数据压测和瓶颈分析.

## 参与项目

* 遇到问题、发现 Bug 或希望提出新功能，请创建 [Issue](https://github.com/archiyun/CoroPact/issues)。
* 欢迎提交 [Pull Request](https://github.com/archiyun/CoroPact/pulls)。
* 本项目使用 [MIT License](LICENSE)。

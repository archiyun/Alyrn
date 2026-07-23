# 用户指南

@brief 从协程、异步 I/O 到 luring 网络服务和 gateway 接入的中文入门。

本文面向第一次使用 high-concurrency-runtime 的开发者。示例只使用仓库当前已经存在的
C++23 API；设计推导和完整语义证明请阅读：

- [网络库总览](../design/zh-CN/network/index.md)
- [AsyncStream 与 AsyncListener 协程语义契约](../design/zh-CN/network/async-stream-contract.md)
- [网关总览](../design/zh-CN/gateway/index.md)

当前项目是 Linux 优先的 C++23 运行时。`coropact::net` 是 Reactor/epoll 网络模块，
`coropact::luring` 是 io_uring 网络模块；两者共享 `coropact::io` 中的异步 I/O 语义，但不共享
底层事件循环。

## 先理解分层

业务代码应该依赖协程和公共 I/O 契约，而不是依赖 epoll 或 io_uring 的内部对象：

```text
业务层：HTTP / gateway / 自定义 session
              |
协程语义：Task<T> + Scheduler + AsyncStream / AsyncListener
              |
公共契约：coropact::coro + coropact::io
              |
       +------+----------------+
       |                       |
Reactor / epoll            luring / io_uring
coropact::net                  coropact::luring
```

`luring` 不是 `net` 的 backend 枚举，也不是 `net` 内部的一个实现选项。应用选择一个
网络模块；HTTP 和 gateway session 只接触公共 stream 语义。

## 构建项目

### 构建基础模块

```bash
cmake -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=ON \
  -DBUILD_EXAMPLES=ON

cmake --build build -j"$(nproc)"
```

### 构建 io_uring 模块

`coropact_luring` 需要 liburing，并通过 `COROPACT_ENABLE_URING` 显式打开：

```bash
cmake -B build-uring \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=ON \
  -DBUILD_EXAMPLES=ON \
  -DCOROPACT_ENABLE_URING=ON

cmake --build build-uring -j"$(nproc)"
```

运行测试：

```bash
ctest --test-dir build-uring --output-on-failure
```

没有 liburing 时，基础 `coropact_coro`、`coropact_io`、`coropact_net` 和 gateway 的 Reactor 路径仍可
单独构建。`COROPACT_ENABLE_URING=ON` 时，CMake 会通过 pkg-config 查找 `liburing`。

### 可选配置

```bash
# 开启 AddressSanitizer 和 UndefinedBehaviorSanitizer
cmake -B build-asan \
  -DCOROPACT_SANITIZER=address,undefined
```

AddressSanitizer 和 ThreadSanitizer 应使用不同的 build 目录。

## 编写协程

### 定义 Task

`coropact::coro::Task<T>` 是惰性协程。函数返回 `Task` 并不会立即执行函数体；协程要么被另一个
协程 `co_await`，要么交给 `Spawn` 或 `SyncWait` 驱动。

```cpp
#include "coropact/coro/task.h"

coropact::coro::Task<int> GetAnswer() {
  co_return 42;
}

coropact::coro::Task<int> Caller() {
  const int answer = co_await GetAnswer();
  co_return answer * 2;
}
```

协程可以返回普通值，也可以返回 `coropact::base::Result<T>`：

```cpp
#include <cerrno>
#include <expected>

#include "coropact/base/error.h"
#include "coropact/coro/task.h"

coropact::coro::Task<coropact::base::Result<int>> MayFail(bool ok) {
  if (!ok) {
    co_return std::unexpected(coropact::base::make_errno(EINVAL));
  }
  co_return 42;
}
```

项目的 I/O 错误通过 `std::expected` / `coropact::base::Result` 传播，而不是通过异常传播。
协程 promise 中未处理的异常会调用 `std::terminate()`；不要让未处理异常进入运行时热路径。

### 在测试或纯计算代码中同步运行

`SyncWait` 会阻塞当前线程，适合测试和纯计算协程：

```cpp
#include "coropact/coro/sync_wait.h"

int main() {
  const int answer = coropact::coro::SyncWait(Caller());
  return answer == 84 ? 0 : 1;
}
```

不要在 Reactor 或 io_uring loop 线程中调用 `SyncWait`。它会阻塞负责处理 I/O completion 的
线程，等待的操作也就无法完成。

### Spawn、Wait 和 Detach

`Spawn` 需要一个具体的 `Scheduler`，返回可等待的 `JoinHandle<T>`：

```cpp
coropact::coro::Task<int> Work() {
  co_return 42;
}

// 在一个已经运行的 Scheduler 上：
auto handle = coropact::coro::Spawn(scheduler, Work());

// 同步等待只适合非 loop 线程。
int result = handle.Wait();

// 或者放弃结果，让任务独立运行到结束。
coropact::coro::Spawn(scheduler, Work()).Detach();
```

在协程内部可以异步等待 `JoinHandle`：

```cpp
coropact::coro::Task<int> Parent(coropact::coro::Scheduler& scheduler) {
  const int child = co_await coropact::coro::Spawn(scheduler, Work());
  co_return child + 1;
}
```

`Task`、`JoinHandle` 都是 move-only 的单消费者对象。一个 task 只能被消费一次；不要复制
或重复 `co_await` 同一个 task。

### 可选的协程帧内存池

默认情况下，`Task`、`SpawnRoot` 和 `SyncWaitRoot` 的 coroutine frame 使用
`std::pmr::new_delete_resource()`，因此不需要修改现有代码。需要降低 frame 的堆分配开销时，
可以使用专用的 worker-local size-class frame pool：

```cpp
#include <memory_resource>

#include "coropact/coro/frame_allocator.h"
#include "coropact/net/event_loop_scheduler.h"

coropact::coro::CoroFramePoolResource frame_pool;
coropact::net::EventLoopScheduler scheduler(&loop, &frame_pool);

{
  // 必须覆盖 Task 函数的调用；Task frame 在函数调用时就已经分配。
  coropact::coro::FrameAllocatorScope frames(frame_pool);
  coropact::coro::Spawn(scheduler, Serve(&connection)).Detach();
}
```

`FrameAllocatorScope` 记录在每个 frame 的分配元数据中，所以 frame 可以在作用域结束后恢复和
销毁。`Scheduler` 的 resource 会在每次 `Work` 恢复期间重新激活，协程运行中创建的嵌套 Task
也会使用同一个 resource。内存 resource 必须存活到所有相关 frame 销毁之后；
`CoroFramePoolResource` 会在自身析构时释放各个 size-class 的 chunk。

## Scheduler 和协程恢复

`coropact::coro::Scheduler` 只有一个核心职责：接收 `coro::Work*` 并在所属执行上下文中运行。
协程恢复时，运行时会设置 `coropact::coro::Scheduler::Current()`：

```cpp
coropact::coro::Scheduler* current = coropact::coro::Scheduler::Current();
```

具体实现有两个：

- `coropact::net::EventLoopScheduler`：把 `Work` 放入 Reactor 的 EventLoop。
- `coropact::luring::LUringLoop`：把 `Work` 放入当前 ring 的 ready queue。

业务协程不应该依赖具体 scheduler 的队列实现。后端 awaiter 负责把一次 I/O completion 转成
一次 `ResumeWork`，因此业务看到的核心链路始终是：

```text
co_await
  -> Submit
  -> Suspend
  -> Complete
  -> Resume
```

epoll readiness、SQE 入队、CQE reap、batch submit 和 `io_uring_enter` 都是后端内部事件，
不属于业务可观察语义。

## AsyncStream 契约

公共 stream 概念位于 `coropact::io`：

```cpp
template <class T>
concept AsyncStream =
    AsyncReadStream<T> &&
    AsyncWriteStream<T> &&
    AsyncClosableStream<T>;
```

核心接口是：

```cpp
// ReadSome/WriteSome 返回可 await 对象，await_resume() 的结果分别为：
// Result<std::size_t>
Task<Result<void>> Shutdown();
Task<Result<void>> Close();
```

`ReadSome` 和 `WriteSome` 可以返回惰性 `Task`，也可以返回直接执行 I/O 的底层
awaitable；调用方统一使用 `co_await`。当前两个后端的 span 重载都直接返回 awaitable，
避免为单次读写创建额外的子协程帧；`io::Buffer` 等扩展重载仍可返回 `Task`。

`coropact::net::ReactorStream` 和 `coropact::luring::LUringStream` 都满足这个概念：

```cpp
static_assert(coropact::io::AsyncStream<coropact::net::ReactorStream>);
static_assert(coropact::io::AsyncStream<coropact::luring::LUringStream>);
```

### ReadSome 的结果

```text
Result<N>, N > 0  读取到 N 字节
Result<0>         对端有序关闭，表示 EOF
unexpected(error) 读取失败，error 是 errno 风格错误码
```

### WriteSome 的结果

`WriteSome` 允许短写，不保证一次写完全部 buffer。通常使用 `coropact::io::WriteAll`：

```cpp
#include <array>
#include <cstddef>
#include <span>
#include <string_view>

#include "coropact/io/stream_algorithms.h"

coropact::coro::Task<void> SendHello(coropact::io::AsyncStream auto& stream) {
  constexpr std::string_view message = "hello\n";
  const auto bytes = std::as_bytes(
      std::span<const char>(message.data(), message.size()));

  auto written = co_await coropact::io::WriteAll(stream, bytes);
  if (!written.has_value()) {
    // 处理 written.error()
    co_return;
  }
}
```

如果 `WriteSome` 返回 `0`，`WriteAll` 会把它转换成 `EPIPE`，避免在没有进展的情况下死循环。

### buffer 生命周期

传给 `ReadSome` 或 `WriteSome` 的 buffer 必须存活到对应 I/O operation 完成：

```cpp
coropact::coro::Task<void> Correct(coropact::luring::LUringStream& stream) {
  std::array<std::byte, 4096> buffer{};
  auto result = co_await stream.ReadSome(buffer);
  // 从这里开始，buffer 不再被这一次 read operation 使用。
  (void)result;
}
```

不要在 `co_await` 之前销毁、移动或重新分配 buffer。io_uring 可能在 CQE 到达前仍持有这个
地址，即使某些 Reactor 快路径看起来是同步的，也不能依赖该实现细节。

### 并发规则

CoreStream 对每个 stream 默认只保证：

- 一个 pending read；
- 一个 pending write；
- read 和 write 可以同时 pending。

同一个 stream 上同时提交两个 read 或两个 write 会返回 `EBUSY` / 等价错误。需要读队列、写
队列、multishot 或 pipeline 时，应建立新的明确契约，不要把它们伪装成 `ReadSome`。

### Shutdown 和 Close

`Shutdown()` 通常表示 TCP 写方向 half-close；读方向仍可能有数据。

`Close()` 是本地资源关闭，并且允许异步完成：

```text
Close
  -> 标记 Closing / Closed
  -> 取消或收敛 pending read/write
  -> 关闭 fd
  -> Close Task 完成
```

`Close()` 不保证在调用点同步完成取消。关闭之后的新 I/O 应返回 `EBADF` 或等价的 closed
error。对端 EOF、写方向 `EPIPE` 和本地 `Close` 不是同一种事件，业务逻辑应该分别处理。

## AsyncListener 契约

listener 提供 single-shot `Accept`：

```cpp
Task<Result<Stream>> Accept();
Task<Result<void>> Close();
```

当前 `coropact::luring::LUringListener` 默认只保证一个 pending accept。`Close()` 会取消 pending
accept，并让等待中的协程恢复。multishot accept 尚未进入公共 listener 接口。

直接使用 `LUringListener` 时，listener 和 stream 必须在同一个 `LUringLoop` 所属线程创建和
使用。大多数服务应直接使用后面的 `LUringServer`，让它管理 accept loop 和 worker。

## 运行 luring Server

### 最小 session server

`LUringServer` 是 thread-per-ring 的服务封装：每个 worker 创建一个 `LUringLoop`，每个 loop
拥有一个 io_uring ring。`SO_REUSEPORT` 默认开启，使多个 worker 可以监听同一个 IPv4 地址。

```cpp
#include <array>
#include <cstddef>
#include <iostream>

#include "coropact/coro/task.h"
#include "coropact/io/stream_algorithms.h"
#include "coropact/luring/server.h"
#include "coropact/luring/stream.h"
#include "coropact/net/inet_address.h"

coropact::coro::Task<void> EchoSession(coropact::luring::LUringStream stream) {
  std::array<std::byte, 4096> buffer{};

  for (;;) {
    auto read = co_await stream.ReadSome(buffer);
    if (!read.has_value() || *read == 0) {
      break;
    }

    auto written = co_await coropact::io::WriteAll(stream, buffer.first(*read));
    if (!written.has_value()) {
      break;
    }
  }

  co_await stream.Close();
}

int main() {
  coropact::luring::LUringServerOptions options;
  options.worker_group_options.worker_num = 4;
  options.worker_group_options.worker_options.loop_options.entries = 4096;
  options.worker_group_options.worker_options.loop_options.submit_batch = 32;

  coropact::luring::LUringServer server(
      coropact::net::InetAddress(9090), options);

  server.set_session_handler(
      [](coropact::luring::LUringLoop&, coropact::luring::LUringStream stream) {
        return EchoSession(std::move(stream));
      });

  auto started = server.Start();
  if (!started.has_value()) {
    // 处理 started.error()，例如 EPERM、ENOSYS 或 EINVAL。
    return 1;
  }

  // 实际应用应在这里接入信号处理和生命周期管理。
  std::cin.get();
  server.Stop();
  return 0;
}
```

session handler 的第一个参数是当前 worker 的 `LUringLoop&`。它不是装饰信息，而是连接所有权
的一部分：连接、stream、connector 和针对该连接的协程必须留在同一个 ring 上。

### Server 的线程模型

```text
LUringServer
  -> LUringWorkerGroup
       -> worker 0: thread 0 -> LUringLoop 0 -> ring 0 -> listener 0
       -> worker 1: thread 1 -> LUringLoop 1 -> ring 1 -> listener 1
       -> ...
```

`worker_num` 是 OS 线程和 ring 的数量，不是协程数量。一个 loop 中可以运行很多 session
协程，但同一个 loop 的操作都必须在它的所属线程提交。

当前 luring 没有 eventfd 跨 ring 通信层。不要从其他线程直接调用某个 `LUringLoop` 的
`Schedule` 或向其 ring 提交 SQE；跨 ring 消息通道将使用 `msg_ring` 方向继续演进。

## LUringLoop 和 Ring 配置

`LUringOptions` 配置每个 ring：

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `entries` | `4096` | SQ/CQ 的基础队列深度 |
| `cq_entries` | `0` | 非零时请求独立 CQ 大小 |
| `setup_sqpoll` | `false` | 请求 SQPOLL，可能需要额外权限和内核条件 |
| `sqpoll_idle_ms` | `1000` | SQPOLL 线程空闲时间 |
| `setup_iopoll` | `false` | 面向 polling I/O；当前网络场景不要开启 |
| `setup_submit_all` | `true` | 使用 `IORING_SETUP_SUBMIT_ALL` |
| `setup_single_issuer` | `true` | 声明单 issuer，匹配一个 ring 一个 loop 线程 |
| `submit_batch` | `32` | loop 每次 flush 尽量批量提交的数量 |

`submit_batch` 是内部提交策略，不改变业务语义。应用只需要根据并发量和内核资源调整
`entries`；不要用它来推断一个 `co_await` 会对应多少个业务完成事件。

### 手动使用 LUringLoop

服务端通常使用 `LUringServer`。如果需要编写底层 awaiter 或单元测试，可以直接创建 loop：

```cpp
coropact::luring::LUringLoop loop;
coropact::luring::LUringOptions options;
options.entries = 64;
options.submit_batch = 1;

auto initialized = loop.Init(options);
if (!initialized.has_value()) {
  // 检查 initialized.error()
}

// Spawn 后，先运行 ready work；提交的 SQE 在 flush 时进入 ring。
// WaitCompletions() 取回 CQE，RunReady() 再恢复协程。
loop.RunReady();
loop.WaitCompletions();
loop.RunReady();
```

这些方法适合 backend 测试和自定义 loop 驱动。生产 server 应使用 `Loop(std::stop_token)`
和 `LUringWorker` 的线程封装。

## LUringConnector

`LUringConnector` 在当前 loop 上提交异步 TCP connect：

```cpp
coropact::coro::Task<void> ConnectOnce(coropact::luring::LUringLoop& loop) {
  coropact::luring::LUringConnector connector(&loop);

  auto connected = co_await connector.Connect("127.0.0.1", 9000);
  if (!connected.has_value()) {
    // 处理 connected.error()
    co_return;
  }

  coropact::luring::LUringStream stream = std::move(*connected);
  co_await stream.Close();
}
```

当前连接器只解析数字 IPv4 地址，例如 `127.0.0.1`；域名解析和 IPv6 不属于当前实现。
connector 是 loop-bound 对象，不应在 worker 之间共享。

## Capability 和启动期绑定

能力分为三类：

| 类别 | 含义 | 示例 |
|---|---|---|
| A：核心语义 | 业务可移植地板，必须满足 | read、write、accept、connect、close |
| B：实现标签 | 描述底层实现，业务不声明 | SQE submit、batch、SQPOLL |
| C：扩展语义 | 改变返回类型、完成基数或生命周期 | multishot、provided buffer、zero-copy |

可以在启动期探测和绑定 luring：

```cpp
#include "coropact/luring/capabilities.h"

coropact::luring::LUringOptions options;
auto binding = coropact::luring::BindLUring(
    options, coropact::io::CapabilitySet::CoreGateway());

if (!binding.has_value()) {
  // ENOTSUP：内核或 liburing 不满足 profile
  // EPERM / EINVAL：ring 配置或权限不满足
}
```

探测发生在启动期，绑定成功后 profile 应视为固定配置。业务依赖的是 `CoreGateway` 等
语义 profile，而不是 `kSubmitRead` 之类的实现标签。

### 当前能力边界

当前可以稳定使用的是 single-shot TCP stream/listener/connect/close，以及其协程恢复语义。
以下能力虽然在 capability 枚举或 probe 设计中预留，尚未作为公共业务接口提供：

- multishot accept / recv；
- provided buffer；
- send zero-copy；
- fixed file / registered buffer 的业务接口；
- 暴露给业务的 linked operation；
- `ReadSomeFor` 形式的统一超时 stream API。

当前 `ProbeCapabilities` 会把部分内核 opcode 的存在报告为扩展能力，但“内核支持 opcode”不
等于“coropact 已经提供对应的业务 concept”。应用不要仅凭 capability bit 直接构造尚未存在的
API。

## Gateway 接入

### 为什么不是直接套 GatewayServer

旧的 `GatewayServer<Listener, Connector>` 自己拥有 listener、accept loop 和 scheduler，主要
服务 Reactor 形状。`LUringServer` 已经拥有 accept loop 和 worker，因此两者不能互相接管同一
个 accept loop。

当前的拆分是：

```text
LUringServer
  accept、worker、ring、连接所有权

GatewaySessionService
  HTTP parser、路由、限流、代理和单连接 session
```

`GatewaySessionService` 是 backend-neutral 模板，不包含 `LUring` 头文件。应用层只在绑定
server handler 的位置选择具体 stream 和 connector 类型。

### 最小 gateway 示例

```cpp
#include <iostream>
#include <memory>
#include <utility>

#include "coropact/gateway/gateway_session_service.h"
#include "coropact/gateway/upstream.h"
#include "coropact/gateway/upstream_peer.h"
#include "coropact/gateway/upstream_registry.h"
#include "coropact/http/http_request.h"
#include "coropact/http/http_response.h"
#include "coropact/luring/connector.h"
#include "coropact/luring/server.h"
#include "coropact/luring/stream.h"
#include "coropact/net/inet_address.h"

using LuringGateway = coropact::gateway::GatewaySessionService<
    coropact::luring::LUringStream,
    coropact::luring::LUringConnector>;

int main() {
  coropact::gateway::UpstreamRegistry registry;

  auto backend = std::make_shared<coropact::gateway::Upstream>(
      coropact::gateway::UpstreamConfig{.name = "user_service"});
  backend->AddPeer(std::make_shared<coropact::gateway::UpstreamPeer>(
      coropact::gateway::UpstreamPeerConfig{
          .name = "user-1",
          .host = "127.0.0.1",
          .port = 9001,
      }));
  registry.Add(backend);

  LuringGateway gateway("gateway", registry);

  gateway.Get("/healthz", [](const coropact::http::HttpRequest&,
                              coropact::http::HttpResponse& response) {
    response.set_status_code(coropact::http::StatusCode::Ok);
    response.set_content_type("application/json");
    response.set_body(R"({"status":"ok"})");
  });
  gateway.AddProxyRoute("/api", "user_service", "round_robin");

  coropact::luring::LUringServerOptions options;
  options.worker_group_options.worker_num = 4;

  // 声明顺序保证 server 在 gateway 和 registry 之前析构。
  coropact::luring::LUringServer server(
      coropact::net::InetAddress(8080), options);
  server.set_session_handler(
      [&gateway](coropact::luring::LUringLoop& loop,
                 coropact::luring::LUringStream stream) {
        // connector 由当前 loop 构造，并按值进入 session 协程 frame。
        return gateway.Serve(
            std::move(stream),
            coropact::luring::LUringConnector(&loop));
      });

  auto started = server.Start();
  if (!started.has_value()) {
    return 1;
  }

  std::cin.get();
  server.Stop();
  return 0;
}
```

这个例子中：

1. gateway 只依赖 `AsyncStream` / `UpstreamConnector` 语义。
2. `LUringServer` 决定 accept 和 worker 归属。
3. 每个 session 使用当前 worker 的 `LUringLoop` 创建 connector。
4. 客户端 stream、upstream connector 和 I/O 协程不会跨 ring 移动。

当前 `GatewaySessionService` 的 upstream pool 是每个 client session 一个实例，保证 loop-bound
stream 不跨线程；跨 client session 的 per-ring keep-alive pool 尚未接入。`set_pool_config` 目前
只影响 session 内 pool 的配置。

### Gateway 的当前限制

- luring session service 已覆盖 direct route 和 proxy route；
- 旧的主动 health-check 实现仍绑定 Reactor 的 timer/connector 模型；
- YAML `ApplyGatewayConfig` 的 health-check 路径不能直接当作 luring health-check 使用；
- gateway 路由和 `UpstreamRegistry` 应在 `server.Start()` 前完成配置；
- 多 worker 下 direct handler、业务状态和自定义指标逻辑必须满足并发访问要求。

## HTTP 请求限制

公共 `HttpParser` 当前默认限制如下：

| 项目 | 限制 |
|---|---:|
| request line / URI | 8 KiB |
| 单个 header line | 8 KiB |
| header 总大小 | 32 KiB |
| header 数量 | 100 |
| `Content-Length` body | 8 MiB |

当前 parser 拒绝 `Transfer-Encoding`，只支持 HTTP/1.0 和 HTTP/1.1。解析错误通过
`ParseStatus` 映射到 400、413、414、431、501 或 505 等响应。

## 错误处理建议

I/O API 通常返回：

```cpp
coropact::base::Result<T>
// 等价于 std::expected<T, std::error_code>
```

建议在每个 await 边界立即处理错误：

```cpp
auto read = co_await stream.ReadSome(buffer);
if (!read.has_value()) {
  if (read.error() == std::errc::operation_canceled) {
    // 本地 Close 导致取消
  } else {
    // 记录 errno 并结束 session
  }
  co_return;
}

if (*read == 0) {
  // EOF，不是本地 Close
  co_return;
}
```

不要把 `Result<0>`、`ECANCELED`、`EBADF`、`EPIPE` 和任意网络 errno 统一吞掉。gateway 的
重试、连接池和熔断器需要依赖这些差异。

## 启动失败和排查

### `EPERM` 或 `operation_not_permitted`

常见原因是容器或沙箱禁止 `io_uring_setup`、网络 bind 或 SQPOLL。先关闭 `setup_sqpoll`，
再确认进程具备 loopback bind 和 io_uring 权限。

### `ENOSYS` 或 `operation_not_supported`

检查内核、liburing 和构建时使用的头文件/库版本。luring 是可选模块；如果环境不满足，使用
Reactor 构建或把该测试标记为环境跳过。

### `EINVAL`

通常表示 ring setup 参数、active capability profile 或 IPv4 地址格式不满足要求。当前
`LUringConnector` 和 listener 都按数字 IPv4 处理，不会自动做 DNS 解析。

### 停止服务

`LUringServer::Stop()` 会请求 worker 停止并关闭 listener。当前实现的重点是停止接收新连接；
应用需要确保 session 生命周期和外部资源在 server 停止前后满足自己的关闭策略。完整的 active
session graceful drain、超时和跨 ring 消息机制仍属于后续工作。

## 已实现与后续方向

当前建议依赖的能力：

- C++23 coroutine `Task<T>`；
- `Spawn` / `JoinHandle` / `SyncWait`；
- backend-neutral `AsyncStream` / `AsyncListener`；
- Reactor stream/listener；
- luring single-shot read/write/accept/connect/close；
- thread-per-ring `LUringServer`；
- gateway session 接入和 upstream proxy。

后续能力必须在新 concept 和 profile gate 下增加，而不是修改现有 `ReadSome` 的含义：

- `ReadSomeFor` / 统一 timeout；
- per-ring upstream connection pool；
- `msg_ring` 跨 ring 投递；
- multishot accept/recv；
- provided buffer；
- registered buffer / fixed file 优化；
- send zero-copy；
- full graceful session drain。

## 测试和示例

基础协程测试：

```bash
ctest --test-dir build -R '^(coro_smoke_test|reactor_stream_smoke_test)$' \
  --output-on-failure
```

luring 测试：

```bash
ctest --test-dir build-uring -R '^(luring_.*_smoke_test)$' \
  --output-on-failure
```

其中 `luring_gateway_smoke_test` 覆盖：

- luring accept 到 gateway session；
- gateway direct route；
- luring connector 到 upstream；
- gateway proxy route 和 response relay；
- session handler 使用正确的 worker loop。

开发新 backend-neutral 业务时，优先参考 `examples/net/demo_reactor_coro_echo.cc` 的
`AsyncStream` 写法；开发 luring 网络模块时，优先参考 `tests/unit/test_luring_*_smoke.cc`
中的 loop、completion 和生命周期验证。

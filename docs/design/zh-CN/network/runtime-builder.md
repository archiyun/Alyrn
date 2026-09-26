# Runtime Builder：简洁配置，不抹平后端

`alyrn::Runtime` 是应用的组合根：它管理启动与停止；编译期 backend tag 选择由哪个后端
创建 worker group，并将每个已接受的 stream 交给连接处理协程。

本页记录默认 interface 的稳定用法。backend 选择不是运行时 enum；ring/epoll tuning 与 main 宏
也不进入公共默认路径。

它不是新的统一 I/O backend，也不取代 `epoll::Loop` 或 `uring::Loop`。需要手动控制 loop、
定时器、跨 worker mailbox/`Post` 或特殊资源生命周期时，仍应使用对应后端的原生公开类型。

## Auto：平台默认 backend

`runtime::Auto` 是编译期别名，不会在运行时探测或切换 backend：Linux 映射到 `runtime::Epoll`。
`io_uring` 仍需显式选择，因为它依赖额外的库、内核能力和
不同的 I/O capability。

根据目标平台包含对应的 backend umbrella 后，即可使用相同的 Builder 入口：

```cpp
#include <alyrn/epoll.h>   // Linux

auto runtime = alyrn::Runtime::Builder<alyrn::runtime::Auto>{
                   alyrn::net::Endpoint::Any(8080)}
                   .AutoWorkers()
                   .OnConnection([](auto stream) -> alyrn::DetachedTask {
                     // 使用跨 backend 的 stream 操作
                     co_return;
                   })
                   .Build();
```

## Epoll

```cpp
alyrn::DetachedTask Handle(alyrn::epoll::Stream stream) {
  // co_await stream.Read(...)
  co_return;
}

auto runtime = alyrn::Runtime::Builder<alyrn::runtime::Epoll>{
                   alyrn::net::Endpoint::Any(8080)}
                   .AutoWorkers()
                   .OnConnection(Handle)
                   .Build();

auto started = runtime.Start();
if (!started) {
  // 检查 started.Error()
}

// 应用自己的 stop 协议完成后：
runtime.Stop();  // request stop、drain、join workers
```

默认只有一个 worker。`AutoWorkers()` 明确选择 `hardware_concurrency()`（至少为 1），避免
库在未声明的情况下占满机器。`Workers(n>1)` 时每个 worker 绑定自己的 listener，并打开
`SO_REUSEPORT`。

`Tcp(net::TcpOptions)` 透传给每个 worker 的 listener，作用于所有已接受的连接；未设置的字段
保持操作系统默认值，Runtime 不替应用改默认。请求/响应协议通常应开启 `no_delay`，否则紧跟在
另一次写之后的小回复可能被 Nagle 算法扣住，等待对端的延迟 ACK。两个后端的 Builder 都提供
这个选项，语义一致。

## luring

```cpp
alyrn::DetachedTask Handle(alyrn::uring::Stream stream) {
  co_return;
}

auto runtime = alyrn::Runtime::Builder<alyrn::runtime::Uring>{
                   alyrn::net::Endpoint::Any(8080)}
                   .AutoWorkers()
                   .OnConnection(Handle)
                   .Build();
```

Runtime 选择 luring 的默认 ring/batch/buffer 策略，并尝试 multishot accept；不可用时由
source 保持既有 capability fallback。业务不需要为这些物理执行细节分支。

`RecvSource`、`BufferLease`、`SendZeroCopy` 和固定资源仍是 stream/source 的显式扩展入口。
它们会改变所有权或生命周期，因此不能伪装成一个 server-wide 的 Runtime 开关。

## 生命周期

`Start()` 会创建 worker group 并启动每个 owner loop。它要求已经设置 `OnConnection()`，且
worker 数大于零；缺少 handler 或非法 worker 数时返回 `EINVAL`，而不是启动一个会静默关闭所有
连接的 server。bind、ring 初始化和线程创建等后端/OS 错误也由 `Start()` 返回。

`RequestStop()` 是跨线程、幂等的 admission/cancellation 请求。它通知 worker loop 开始后端
cancel 与 completion drain，但不等待 drain，也不 join worker。控制线程随后调用 `Stop()`，或正在
`Run()` 中等待该请求。

`Stop()` 是同步 owner-thread 边界：它再次请求所有 worker 停止，并在析构 worker group 时等待线程
退出。它继承各后端的 close/cancel/drain 协议：

```text
Runtime::RequestStop()
  -> WorkerGroup::RequestStop()
  -> each worker loop begins backend cancellation/drain

Runtime::Stop()
  -> WorkerGroup::RequestStop()
  -> backend cancel / completion drain
  -> WorkerGroup destruction joins worker threads
```

因此 `Runtime` 不可复制；它在不与生命周期调用并发时可移动，底层 control 的地址不随移动改变。
worker callback 可以安全引用 control 保存的 handler，直到 `Stop()` 完成。

`Start()` 与 `Stop()` 必须由一个应用控制线程串行调用；`RequestStop()` 是唯一允许从其他线程调用的
控制方法。其生命周期为：

```text
Created --Start--> Starting --success--> Running --RequestStop--> Stopping --Stop/join--> Stopped
   ^                    |                    |                                      |
   +---- Start fails ---+                    +--------------------------------------+-- Start() returns EALREADY
```

启动失败不会消耗 Runtime，因此仍可重试；但一次成功启动后的 `Stop()` 是终态。重复 `Stop()` 是
no-op。每个 `OnConnection` 调用按值接收 stream，handler 的 detached coroutine 独占该 stream，
直至协程结束。

## Worker 生命周期钩子

有状态的服务（例如每个 worker 一个聊天 hub）需要在 loop 线程上创建 owner-affine 状态，并在
loop 销毁前关闭它。`OnWorkerStart` / `OnWorkerStop` 提供这两个时机：

```cpp
thread_local std::unique_ptr<Hub> t_hub;  // Hub 内含 Channel，并在其 loop 上 SpawnDetach 事件循环

auto runtime = alyrn::Runtime::Builder<alyrn::runtime::Epoll>{endpoint}
                   .OnWorkerStart([](alyrn::epoll::Loop& loop, std::size_t) -> alyrn::Result<void> {
                     t_hub = std::make_unique<Hub>(loop);
                     return {};
                   })
                   .OnWorkerStop([](alyrn::epoll::Loop&, std::size_t) { t_hub->Close(); })
                   .OnConnection([](auto stream) { return HandleConnection(std::move(stream)); })
                   .Build();
```

时序与所有权：

```text
worker thread: Loop / listener 创建
  -> OnWorkerStart(loop, index)      在 loop 调度上下文中；返回错误则 Start()/Run() 失败
  -> accept 循环启动，连接 handler 开始运行
  ...
  -> RequestStop：listener 停止，pending I/O 取消，连接协程链排空
  -> OnWorkerStop(loop, index)       仅对 start hook 成功的 worker；在 loop 调度上下文中
  -> 排空 hook 调度的工作（例如 Channel::Close() 唤醒的等待者）
  -> listener / connector / Loop 销毁，线程退出（thread_local 析构）
```

- 两个 hook 都在 worker 自己的线程上、以该 worker 的 Loop 为当前 scheduler 执行，可以直接创建、
  关闭 Channel，`Spawn` / `SpawnDetach` 根任务。
- `OnWorkerStop` 应当**关闭**仍有等待者的 Channel，但不要**销毁**被唤醒协程仍在引用的对象：
  runtime 在 hook 返回之后才排空这些协程。销毁放在排空之后，例如交给 `thread_local` 析构。
- `OnWorkerStop` 不能发起新的 I/O：此时 loop 已经停止。
- `Workers(n > 1)` 时每个 worker 各有一份状态；跨 worker 共享需要跨线程投递。

## 默认阻塞入口

当应用希望由调用 `main()` 的线程拥有整个 server 生命周期时，可使用 `Run()`：

```cpp
std::stop_source stop_source;
// 应用的 signal / 管理平面 / 测试代码在适当时机调用：
// stop_source.request_stop();

auto result = runtime.Run(stop_source.get_token());
```

`Run()` 严格执行：

```text
Start() -> wait stop_token or RequestStop() -> Stop() -> drain and join -> return
```

它不安装 signal handler，也不会解释进程退出策略；这些是应用的职责。`stop_token` 已请求停止时
仍会先完成一次 `Start()`，再立即执行 `Stop()`，因此启动与关闭采用同一条已验证的收敛路径。
成功返回时 `Started()` 必为 `false`，所有 worker 都已退出。

## 默认短路径

不需要显式 worker 配置的服务可以省略 backend template 参数，使用平台默认 backend：

```cpp
auto runtime = alyrn::Runtime::Create(
    alyrn::net::Endpoint::Any(8080),
    [](auto stream) -> alyrn::DetachedTask {
      // stream 的静态类型仍是 epoll::Stream。
      co_return;
    });
```

此调用等价于 `Create<runtime::Auto>`，即 Linux 选择 Epoll。
`Create` 等价于对应 `Builder` 的默认配置加 `OnConnection`；它仍返回同一个
`alyrn::Runtime`。需要指定后端时可以显式写 `Create<runtime::Uring>` 或
`Create<runtime::Epoll>`，此时 handler 中的
`stream` 静态类型相应为所选 backend 的 `Stream`，没有虚调用或类型擦除进入连接数据路径。

## 为什么不做 C++ 宏

Tokio 的 `#[tokio::main]` 本质上是编译期生成 `Runtime::Builder` 调用。C++ 预处理宏无法提供
Rust attribute macro 的类型检查和诊断质量。先稳定 builder 的小 interface；未来若确实需要，
`ALYRN_MAIN(...)` 只能作为生成 `main()` 与 builder 调用的薄语法糖，不能承载后端语义。

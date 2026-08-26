# ADR-0010: 冻结 compile-time backend-selected Runtime composition root

## 状态

Accepted

## 背景

Reactor 与 luring 都需要为 TCP server 创建 listener、worker group、loop、scheduler 和连接
协程。若应用重复完成这些组装，便会重新暴露 `epoll`、ring、multishot、buffer 策略与 worker
shutdown 的后端细节。反过来，若引入一个运行时 `Backend(...)` enum，又会伪造两个后端配置与能力
完全可互换的抽象，并迫使 handler、stream 或 backend 配置进入不必要的类型擦除。

## 决策

冻结一个 backend-neutral application composition root：`coropact::Runtime`。后端通过编译期
tag 选择对应 Builder specialization：

- `Runtime::Builder<runtime::Reactor>`；
- `Runtime::Builder<runtime::LUring>`；
- `Runtime::Builder<runtime::Kqueue>`（见文末修订）；
- `Runtime::Create<Backend>(Endpoint, Handler)` 是默认配置短路径。

Runtime 的稳定 lifecycle interface 仅包含：

- `Runtime::Builder<Backend>{Endpoint}`；
- `Workers()` 或 `AutoWorkers()`；
- `OnConnection(ConnectionHandler)`；
- `Build()`、`Start()`、`Run(stop_token)`、`RequestStop()`、`Stop()`、`Started()`。

每个 Builder 的 `ConnectionHandler` 按值取得已接受 stream，并在返回的 `DetachedTask` 生命周期内
独占它。Runtime 只 type-erase cold lifecycle control；stream、awaiter、operation 和 worker-local
资源仍保持 backend 静态类型。Runtime 自己选择后端的默认策略：Reactor 使用既有 readiness 策略；
luring 尝试 multishot accept 并保留 capability fallback。ring 深度、provided buffer、zero-copy、
触发模式、CPU affinity 和 frame resource 不属于此 interface。

Runtime 是一次性生命周期对象：成功 `Start()` 后，`Stop()` 或 `Run()` 返回会进入终态；随后
`Start()` 返回 `EALREADY`。失败的 `Start()` 保持 `Created`，可以重试。`RequestStop()` 是唯一
允许跨线程调用的控制方法；它请求 cancellation/drain 而不 join。`Stop()` 与 `Start()` 由应用
控制线程串行调用，并负责最终 join。

## 结果

应用的默认路径不需要了解 worker bootstrap 或物理 I/O 策略，同时 backend seam 保持诚实：backend
tag 在编译期选择实现，而不是用运行时 enum 抹平配置。需要手动控制 loop、timer、mailbox、`Post`、资源注册
或能力选择的用户继续直接组合 `reactor::Loop`、`luring::Loop` 或 `kqueue::Loop` 及其公开 adapter。

不新增运行时 `Backend(...)` 配置、类型擦除的通用 stream handler、额外 Builder tuning 方法或 C++
main 宏。未来只有在各后端长期拥有相同、已验证且无需削弱原生能力的应用级语义时，才重新评估
这些便利层。
POSIX signal 处理属于应用策略；`examples/simple_echo` 可提供示范，但 Runtime 不安装 signal
handler。

## 验证

Runtime smoke test 覆盖配置错误、pre-cancelled token、`Run()` 被跨线程 `RequestStop()` 唤醒、
`RequestStop()` 与同步 join 的分离、启动失败重试及停止后的 restart 拒绝。两个 simple echo 示例
验证默认 Runtime 组合；其中 POSIX 示例通过 `sigwait` 将终止信号转为 `stop_source`。
kqueue 的 Runtime smoke 在 `COROPACT_ENABLE_KQUEUE` 的 BSD/Darwin 构建中运行。

## 修订（2026-08）

第三个编译期 tag `runtime::Kqueue` 加入同一套 lifecycle interface，不改变本 ADR 的决策：
backend 选择仍是编译期 tag，Runtime 仍只 type-erase cold start/stop。kqueue 的
`Workers(n>1)` 拓扑是 master-slave（单 listener + `Loop::Post` 移交 fd），不是
Reactor 的 `SO_REUSEPORT`，也不是 luring 的 thread-per-ring。这属于后端 bootstrap
细节，不进入跨后端 Builder 开关。

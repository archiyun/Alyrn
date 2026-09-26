# 用户试用问题修复计划

**来源：** 2026-09-27 以应用开发者身份试用 Alyrn：按 `docs/packaging.md` 安装到独立前缀，
用 `find_package(Alyrn)` 写了 5 个程序（KV 服务端、并发客户端、聊天室、select 定时汇报、
uring 原生回显），外加一组误用探针。

**目标：** 修复试用中暴露的崩溃与能力缺口，让超出 echo 形状的常见应用（有状态服务、客户端、
定时器驱动、多线程协作）不必依赖泄漏或绕路就能写出来。

**约束：**

- 不改变 LRCI 语义、后端独立性，以及"协程帧不跨 loop 迁移"的原则。
- Channel 保持 scheduler 亲和，不做成线程安全容器。
- 保持无异常的错误模型；契约违反仍然 fail-fast。
- 只提交本计划涉及的改动，不混入工作区里其他未提交的修改；不推送远程。
- 行号基于 2026-09-27 的工作区（含未提交改动），落地时以实际代码为准。

## 总览

| ID | 问题 | 级别 | 规模 | 依赖 |
|---|---|---|---|---|
| F1 | Loop 回调（`RunAfter`/`RunEvery`/`RunOnOwner`）里操作 Channel 会 panic | P0 | 小 | — |
| F2 | Runtime 没有 worker 生命周期钩子；有状态服务要么泄漏，要么退出时 panic | P0 | 中 | F1 |
| F3 | 没有跨线程投递 API | P1 | 大 | F1 |
| F4 | Builder 不能配置 TCP 选项，默认 Nagle 造成约 40ms 往返 | P1 | 小 | — |
| F5 | 客户端没有入口；`SyncWait` 做 I/O 的报错难以理解 | P1 | 小 | — |
| F6 | `Connect(host, port)` 不解析主机名，参数却叫 `host` | P2 | 小 | 解析器依赖 F3 |
| F7 | 错误传播样板多；异常策略没有写进文档 | P2 | 小 | — |
| F8 | 若干易用性小项 | P3 | 小 | — |
| F9 | 文档承诺了不存在的 mailbox / `Post` / MPSC 队列 | P2 | 小 | F3 |

P0：正常使用公开 API 就会崩溃。P1：常见场景缺能力或踩性能坑。P2：理解或使用上的摩擦。
P3：打磨。

## F1 Loop 回调中操作 Channel 会 panic

**现象**

```cpp
alyrn::epoll::Loop loop;
alyrn::Channel<int> ticks(loop, 1);
loop.RunEvery(alyrn::time::Milliseconds(50), [&] { (void)ticks.TrySend(1); });
// [ALYRN PANIC] Channel operation called outside its owning scheduler
```

uring 的 `RunAfter` 回调、epoll 的 `RunOnOwner`（即使在 `Run()` 之前调用）结果相同：线程正确，
Loop 就是 Channel 的 scheduler，仍然 panic。"定时器往 channel 里喂 tick"是 Go 风格
`select` + ticker 的标准写法，目前只能改成"协程里 `SleepFor` 再 `TrySend`"来绕过。

**根因**

- `Channel::CheckOwner()`（`include/alyrn/coro/channel.h:366`）判定
  `Scheduler::TryCurrent() == scheduler_`。
- `TryCurrent()` 只在 `Scheduler::ExecutionScope` 内有效，而这个 scope 目前只建立在
  "恢复协程 Work"的路径上：epoll 的 `DoPendingWork → RunBatch`（`src/epoll/loop.cc:161`），
  uring 的 `RunReady` / `DrainCompletionReady`（`src/uring/loop.cc:410`、`:386`）。
- 用户回调走的是另一条路径：
  - epoll：fd 就绪分发 `channel->HandleEventUnchecked()`（`src/epoll/loop.cc:87`，
    timerfd 定时器回调也从这里触发）；`RunOnOwner` 直接调用 `callback()`
    （`src/epoll/loop.cc:111`）。
  - uring：`HandleCqe → DispatchTimerDriverComplete → TimerQueue::ProcessExpired`
    （`src/uring/timer_queue.cc:162`）。
- 结果是"owner 线程"（`IsInLoopThread()`）与"owner 调度上下文"（`TryCurrent()`）两个判定
  不一致：Loop 自己的公开回调 API 反而不满足 Channel 的 owner 契约。

**方案：Loop 在整个 `Run()` 期间只建立一次执行域**

- epoll `Loop::Run`：状态检查之后、主循环之前构造 `ExecutionScope`，覆盖 `DoPendingWork`、
  poll 分发、`BeginShutdown` 和 `RunPending`。
- epoll `Loop::RunOnOwner`：在执行域内调用 `callback()`。
- uring `Loop::Run`：同样在循环外建立一次，覆盖 CQE 分发与 `DrainStoppedOperations`。

可行性：

- `CurrentScope`（`include/alyrn/coro/scheduler.h:84`）与 `FrameAllocatorScope`
  （`include/alyrn/coro/frame_allocator.h:551`）在目标值相同时不做切换，所以 `RunBatch` /
  `RunReady` 内原有的 scope 变成嵌套 no-op，`CheckExecutionScope()` 仍然成立。
- 回调里新建的协程帧会改用 Loop 选定的 frame resource（今天回调里 `SpawnDetach` 会退回
  `new_delete_resource`，结果正确但不一致）。帧释放按头部或 slab 反查资源，不会错配。
- 整个 `Run()` 只多一次 TLS 写入；嵌套 scope 只有一次 TLS 读和比较。

备选：只包裹用户回调调用点（TimerQueue 触发、`RunOnOwner`）。改动更小，但 `ExecutionScope`
是 `Scheduler` 的 protected 成员，TimerQueue 要经 `LoopAccess` 转发，而且以后 fd 分发里新增的
用户回调仍会遗漏。不采用。

**清单**

- [x] epoll：`Loop::Run`、`Loop::RunOnOwner` 建立执行域。
- [x] uring：`Loop::Run` 建立执行域。
- [x] `epoll/loop.h`、`uring/loop.h` 中定时器与 `RunOnOwner` 的注释写明：回调在 Loop 的调度
      上下文中执行，可以操作 Channel、`Spawn` / `SpawnDetach`。
- [x] `tests/unit/test_epoll_timer_smoke.cc`：`RunAfter` / `RunEvery` 回调中 `TrySend`，
      接收协程收到值；`Run()` 之前在 `RunOnOwner` 中 `TrySend` + `Close()` 不 panic。
- [x] `tests/unit/test_luring_timer_smoke.cc`：`RunAfter` 回调中 `TrySend`。

## F2 Runtime 缺少 worker 生命周期钩子

**现象**

聊天室需要一个 hub（持有成员表和事件 Channel）。在 `Runtime` 下拿到 Loop 的唯一途径是
handler 里的 `stream.OwnerLoop()`，最自然的写法是：

```cpp
template <class Stream>
alyrn::DetachedTask HandleConnection(Stream stream) {
  static Hub hub(*stream.OwnerLoop());  // hub 内有 Channel<Event>，并 SpawnDetach 了事件循环
  // ...
}
```

功能正确，关停时各连接的清理链也完整跑完；但 `runtime.Run()` 正常返回后，静态析构触发
`[ALYRN PANIC] Channel destroyed without Close()`，进程以 134 退出。此时 loop 线程已经结束，
用户没有任何合法时机调用 `Close()`，只能 `*new Hub(...)` 故意泄漏。

**根因**

- `Runtime::Builder` 只有 `Workers` / `AutoWorkers` / `OnConnection`
  （`include/alyrn/epoll/runtime.h:27`、`include/alyrn/uring/runtime.h:25`）。
- 底层 `detail::Worker` 已支持 `ThreadInitCallback` / `ThreadExitCallback`
  （`src/epoll/worker.cc:116`、`:134`；`src/uring/worker.cc:195`、`:222`），但 `RuntimeControl`
  传入的是空回调（`src/epoll/runtime.cc:63`、`src/uring/runtime.cc:65`）。
- 直接暴露还不够：exit 回调在 `loop.Run()` 返回之后执行，既不在执行域内（`Channel::Close()`
  会因为 F1 的原因 panic），也没人排空它调度的工作——epoll 的 `~Loop` 会以
  `Loop destroyed with pending owner work`（`src/epoll/loop.cc:47`）panic。

**方案**

两个 Builder 增加（以 epoll 为例）：

```cpp
// 在每个 worker 线程上、该 worker 的 Loop 调度上下文中调用。Loop 与 listener 已创建，
// 还没有 accept 第一个连接。返回错误会让 Start() / Run() 失败。
Builder& OnWorkerStart(std::function<Result<void>(epoll::Loop&, std::size_t worker_index)> hook);

// 在每个 worker 线程上调用：listener 已停止、I/O 已取消、连接协程链已排空，Loop 尚未销毁。
// hook 返回后，runtime 会继续排空它调度的工作（例如 Channel::Close() 唤醒的等待者），
// 然后才销毁 Loop。
Builder& OnWorkerStop(std::function<void(epoll::Loop&, std::size_t worker_index)> hook);
```

实现要点：

- `RuntimeControl::Start` 把 hook 适配成 `WorkerGroup` 的 init / exit 回调（`WorkerContext`
  里已有 `loop` 与 `index`）。
- `Worker::WorkLoop`（两个后端）在执行域内调用 init / exit 回调：epoll 在 F1 之后可以直接用
  `loop.RunOnOwner(...)`；uring 补一个与 epoll 对齐的 `RunOnOwner`。
- exit 回调之后排空：epoll 调用 `loop.RunPending()`；uring 循环调用
  `LoopAccess::RunReady(loop)`，直到 ready 队列为空。
- 文档写明：`OnWorkerStop` 里应当关闭仍有等待者的 Channel，但不要销毁刚被唤醒的协程仍在引用的
  对象——销毁要放在排空之后（例如 `thread_local` 析构）。

修复后的用法：

```cpp
thread_local std::unique_ptr<Hub> t_hub;

auto runtime = alyrn::Runtime::Builder<alyrn::runtime::Epoll>{endpoint}
                   .OnWorkerStart([](alyrn::epoll::Loop& loop, std::size_t) -> alyrn::Result<void> {
                     t_hub = std::make_unique<Hub>(loop);
                     return {};
                   })
                   .OnWorkerStop([](alyrn::epoll::Loop&, std::size_t) { t_hub->Close(); })
                   .OnConnection([](auto stream) { return HandleConnection(std::move(stream)); })
                   .Build();
```

`Workers(n > 1)` 时每个 worker 各有一个 hub，房间按 worker 分区；跨 worker 广播需要 F3。

以后可以在钩子之上叠加带类型的 per-worker 状态（如 `WithWorkerState<S>(factory)`，handler 签名
变为 `(Stream, S&)`）。它会改变 `ConnectionHandler` 的类型，本轮不做。

**清单**

- [ ] epoll / uring Builder 增加 `OnWorkerStart` / `OnWorkerStop`，`RuntimeControl` 透传。
- [ ] `Worker::WorkLoop` 在执行域内调用回调，exit 回调后排空 ready 工作。
- [ ] uring `Loop` 补 `RunOnOwner`。
- [ ] `tests/unit/test_runtime_builder_smoke.cc`：start hook 每个 worker 在其线程上各调用一次，
      并且先于首个连接；start hook 返回错误时 `Run()` 失败；stop hook 关闭 Channel 后，阻塞在
      该 Channel 上的协程完成，进程正常退出。两个后端都覆盖。
- [ ] `docs/design/zh-CN/network/runtime-builder.md` 补充钩子的时序与所有权说明。

## F3 没有跨线程投递 API

**现象与影响**

- 两个 `Loop` 上唯一线程安全的方法是 `RequestStop()`（`include/alyrn/epoll/loop.h:47`、
  `include/alyrn/uring/loop.h:71`）；`Schedule`、`RunOnOwner`、`RunAfter` 都要求在 owner 线程调用。
- 后果：多 worker 之间无法广播（聊天室只能单 worker）；无法把阻塞工作（`getaddrinfo`、文件 I/O、
  CPU 密集计算）交给线程池、再回到 loop 继续；Channel 无法跨 worker 使用。
- 历史：uring 曾有 `detail` 级 mailbox（有界 MPSC + `MSG_RING` 通知合并），在
  `735cd86 remove unused luring mailbox transport` 中作为未使用代码删除，但多处文档仍把它
  当作现有能力（见 F9）。

**方案**

两个后端的 `Loop` 增加：

```cpp
// 线程安全。把 callback 投递到本 Loop 的 owner 线程，在其调度上下文中、之后的某一轮执行。
// 返回成功的投递恰好执行一次；Loop 完成关停排空之后返回 operation_canceled，callback
// 不会执行，并在调用线程析构。
[[nodiscard]] Result<void> Post(Functor callback);
```

已定的语义：

- 无界队列（互斥锁 + 向量）。每个 producer 内 FIFO；队列由空变非空时唤醒一次，其余合并。
- "关闭"标志与队列在同一把锁下切换，保证每次投递要么执行、要么返回 `ECANCELED`，不会滞留。
- `Run()` 之前允许投递，第一轮执行；关停排空阶段仍接受投递并执行；排空结束后拒绝。
- Loop 从未运行就被销毁时，已接受的 callback 不执行，在 owner 线程析构。
- Loop 对象本身必须比所有 `Post` 调用活得久；这是调用方的同步责任，文档给出注册表示例。

实现：

- epoll：复用 `wakeup_fd_` eventfd（`Wakeup()` 已被 `RequestStop` 使用），在 wakeup 读回调里
  批量执行；执行时依赖 F1 的执行域。
- uring：复用 `wake_fd_` / `ArmWakePoll` 唤醒路径。`MSG_RING` 快路径留作后续优化，可参考
  `735cd86` 之前的 `detail::Mailbox`。
- 后续可叠加 `co_await RunOn(target_loop, fn)`：`fn` 在目标 loop 执行，结果再 `Post` 回原 loop
  恢复，协程帧不迁移。本轮不做。

**清单**

- [ ] epoll / uring 实现 `Loop::Post`。
- [ ] 压力测试：多个 producer 线程并发投递，每个 callback 恰好执行一次、在 owner 线程、
      producer 内 FIFO；关停后投递返回 `ECANCELED`；与关停并发的投递不丢不重。
- [ ] 设计文档：新增投递语义说明，替换 `luring/cross-worker-mailbox.md`。

## F4 Builder 不能配置 TCP 选项（Nagle 陷阱）

**现象**

KV 服务一次读到流水线化的 `SET` + `GET`，分两次 `Write` 回复；第二个小包被 Nagle 扣住，
要等客户端的延迟 ACK，每个往返约 40ms。单客户端 49 cmds/s；在 handler 里加
`SetNoDelay(true)` 后是 50,305 cmds/s（Debug 构建）。压测时看起来像死锁。

**根因**

- `Builder` 不暴露 `net::TcpOptions`；`RuntimeControl::Start` 只设置 `reuse_port`
  （`src/epoll/runtime.cc:57`、`src/uring/runtime.cc:59`）。
- 两个 listener 其实都会对 accept 得到的 fd 应用 `tcp_options`（`src/epoll/listener.cc:169`、
  `src/uring/listener.cc:278`），只差透传。

**方案**

- 两个 Builder 增加 `Builder& Tcp(net::TcpOptions options)`，透传到 epoll 的
  `listener_options.tcp_options` 与 uring 的 `listen_options.tcp_options`。
- README 与 `runtime-builder.md` 说明：请求/响应协议通常应开启 `no_delay`。
- 默认值保持操作系统默认，符合"保守默认、显式配置"。

**清单**

- [ ] 两个 Builder 增加 `Tcp()` 并透传。
- [ ] `test_runtime_builder_smoke.cc`：`Tcp({.no_delay = true})` 后，accept 得到的连接
      `getsockopt(TCP_NODELAY) == 1`，两个后端。
- [ ] README 与 `runtime-builder.md` 补充说明。

## F5 客户端没有入口；SyncWait 做 I/O 的报错难懂

**现象**

- 写客户端需要自己组装：创建 `Loop` → `Spawn` → 在任务末尾记得 `loop.RequestStop()` →
  `Run()` → `Wait()`。忘记 `RequestStop()`，程序会永远挂着。
- `coro::SyncWait` 看起来是答案，但只要内部有 I/O 就 panic：
  `SchedulerContinuation requires a current owner scheduler`
  （`include/alyrn/detail/scheduler_continuation.h:33`）。新手无法从这句话推断出
  "SyncWait 不能驱动 I/O"。

**方案**

1. 在 `io` 门面新增 `alyrn::io::BlockOn`（`include/alyrn/io/block_on.h`，由 `alyrn/io.h`
   导出），两个后端通用：

   ```cpp
   // 在调用线程（必须是 loop 的 owner）上把 task 作为根任务运行，task 完成后停止 loop 并
   // 返回结果。Loop 只能 Run 一次，BlockOn 之后 loop 不能再用。若 loop 被外部停止而 task
   // 仍未完成，属于契约违反，fail-fast。
   template <class L, Returnable T>
     requires ManagedLoop<L> && std::derived_from<L, coro::Scheduler>
   T BlockOn(L& loop, Task<T> task);
   ```

   实现：`Spawn` 一个包装根任务（`co_await` 用户任务后调用 `loop.RequestStop()`），然后
   `loop.Run()`，确认已完成后 `Wait()`。为此给 `JoinHandle` 增加公开的 `IsFinished()`
   （`SpawnState::IsFinished` 已存在，`include/alyrn/coro/detail/spawn_state.h:129`）。
   uring 的 loop 需要先 `Init()`。
2. 改进 `SchedulerContinuation::Bind` 的 panic 信息，直接说明"I/O 必须在其 Loop 上运行；
   用 Spawn + Run 或 BlockOn；SyncWait 不能驱动 I/O"。
3. README 增加"编写客户端"一节。

**清单**

- [ ] `JoinHandle::IsFinished()`；`io::BlockOn`（含 `Task<void>`）。
- [ ] 测试：返回值、`Task<void>`、在本地 listener 上完成 connect / write / read。
- [ ] 改进 panic 信息；`SyncWait` 注释指向 `BlockOn`。
- [ ] README 客户端一节。

## F6 `Connect(host, port)` 不解析主机名

**现象：** `connector.Connect("localhost", port)` 返回 `Invalid argument`；外面如果套了重试，
要重试若干次后才暴露。

**根因：** 两个后端都直接调用 `net::ParseIpAddress`（`src/epoll/connector.cc:229`、
`src/uring/connector.cc:198`），它只接受数字 IP（见 `net/endpoint.h` 注释）；但公开签名与
`AsyncConnector` concept 的参数名都是 `host`（`include/alyrn/epoll/connector.h:42`、
`include/alyrn/uring/connector.h:37`、`include/alyrn/backend/async_connector.h:18`）。

**方案**

- 本轮：参数改名为 `ip`，注释写明"只接受 IPv4 / IPv6 数字字面量，不解析主机名，失败返回
  `EINVAL`"。改名不影响 ABI，也不影响调用方源码。
- 以后：基于 F3 做异步解析（线程池 `getaddrinfo` + `Post` 回 loop），再决定是否提供按主机名
  连接的接口。

**清单**

- [ ] 头文件与 concept 参数改名、补注释。
- [ ] 测试：`Connect("localhost", port)` 返回 `EINVAL`（锁定已文档化的行为）。

## F7 错误传播样板多；异常策略没有写进文档

**现象**

- 5 个小程序里 `if (!r) co_return std::unexpected(r.Error());` 出现了十几次。`Result` 没有
  `and_then` / `transform`，也没有类似 TRY 的宏。
- 协程里逃逸的异常会直接 `std::terminate`（`include/alyrn/coro/detail/promise_base.h:40`；
  `coro/detached_task.h:28`、`coro/detail/spawn_root.h:77`、`coro/detail/sync_wait_root.h:24`
  同理），README 没有提到。`std::stoi` 解析失败、容器分配失败都会直接结束进程。

**方案**

1. 协程早返回宏，放在 `alyrn/result.h`。`co_await` 不能出现在 GNU 语句表达式里，所以采用
   "语句 + 赋值"的形式：

   ```cpp
   // expr 为 Result<void>：失败时从当前协程 co_return 其错误。
   ALYRN_CO_TRY(co_await stream.Write(bytes));

   // expr 为 Result<T>：成功时把值赋给 lhs，失败时 co_return 其错误。
   ALYRN_CO_TRY_ASSIGN(auto n, co_await stream.Read(buffer));
   ```

   同时提供非协程版本 `ALYRN_TRY` / `ALYRN_TRY_ASSIGN`（使用 `return`）。
2. `Result` 增加 `AndThen` / `Transform` / `OrElse` / `TransformError`，转发给内部的
   `std::expected`，命名沿用 PascalCase。
3. README 增加"错误模型"一节：`Result` 的用法、异常逃逸即 `std::terminate`、推荐的非抛出
   替代（如 `std::from_chars`）。

**清单**

- [ ] 宏与 `Result` 的单子操作，单元测试覆盖 `Result<void>`、`Result<T>` 与只可移动的 `T`。
- [ ] README 错误模型一节。

## F8 易用性小项

1. **Channel 容量必须是 0 或 2 的幂**（`include/alyrn/coro/channel.h:360`），传 3 在运行时
   panic，而 Go 没有这个限制。存储按 `std::bit_ceil(capacity)` 分配并新增 `mask_`，逻辑容量
   保持用户给的值；`PushBuffer` / `PopBuffer` 改用 `mask_`，所有 `size_ == capacity_` 判断不变。
2. **handler 必须返回 `DetachedTask`**，已有的 `Task<Result<void>>` 会话要多包一层。本轮不做：
   `DetachedTask` 让所有权显式，包装只需几行；错误如何处理（丢弃或交给错误回调）需要单独设计。
3. **没有 `std::string_view → std::span<const std::byte>` 的辅助函数**，每个用户都要写一个。
   在 `alyrn/net/buffer.h` 增加 `net::AsBytes()`。
4. **每个服务端都要重写 Ctrl+C 处理**，`examples/support/signal_stop.h` 不在安装内容里。
   本轮不做：示例目录正在调整；是否把信号处理纳入库的公开 API 需要单独决定。
5. **`RecvSource` 没有教程式示例**：`examples/uring/03_recv_echo.cc` 名为 Recv echo，演示的
   其实是 `stream.Recv(Buffer)`；`RecvSource` 只出现在压测 demo 里。新增
   `examples/uring/07_recv_source_echo.cc`（`AcceptSource` + `RecvSource` + `BufferLease` +
   `Stop()` + 优雅退出）。
6. **命名不一致**：epoll 叫 `ListenerOptions`，uring 叫 `ListenOptions`。uring 增加
   `ListenerOptions` 别名，旧名保留。

**清单**

- [ ] 1：Channel 任意容量，同步更新 `docs/design/zh-CN/coro/channel.md`。
- [ ] 3：`net::AsBytes`。
- [ ] 5：RecvSource 示例。
- [ ] 6：`uring::ListenerOptions` 别名。

## F9 文档与代码不一致

下列文档把已删除或从未公开的能力当作现有能力：

- `docs/design/zh-CN/network/luring/cross-worker-mailbox.md`：描述 `PostMessage` / `MSG_RING`
  mailbox（已在 `735cd86` 删除），仍在 `mkdocs.yml` 导航中，并被 `luring/index.md:81` 链接。
- `docs/design/zh-CN/network/luring/index.md:22`：能力表列出 `PostMessage` / `MSG_RING`。
- `docs/design/zh-CN/network/runtime-builder.md:10`：让用户"使用对应后端的原生公开类型"处理
  跨 worker mailbox / `Post`，但并不存在。
- `docs/design/zh-CN/coro/channel.md:110`：同上。
- `docs/SUBSYSTEMS.md:49`：称 `uring/detail` 拥有 "ring/mailbox transport"。
- `README.md:346`、`README.zh-CN.md:339`：提到 MPSC 队列，`include/alyrn/detail/` 中已经没有。
- `docs/releases/v0.1.0.md:19`：发布记录属于历史，保留原文。

**方案：** 以 F3 的 `Loop::Post` 为准更新上述文档；mailbox 页面改写为投递语义说明。

**清单**

- [ ] 更新上述文档与 `mkdocs.yml`。

## 落地顺序

1. **第一批：** F1、F4、F5、F6、F7（README 错误模型）。
2. **第二批：** F2、F7（宏与单子操作）、F8。
3. **第三批：** F3，随后 F9。

每一批完成后运行 epoll 与 uring 两套测试，并在只包含已提交内容的独立 worktree 中构建、测试，
确认提交本身完整。

## 验证

- 各项的单元测试见清单；`ctest` 在 epoll 与 uring 构建下全部通过。
- 用试用程序验收：
  - `RunEvery` + `TrySend` 的 ticker 版本正常运行（F1）；
  - 聊天室在 `OnWorkerStop` 中关闭 hub，Ctrl+C 后退出码为 0（F2）；
  - KV 服务配置 `Tcp({.no_delay = true})` 后，单客户端吞吐恢复到数万 cmds/s（F4）；
  - KV 客户端改用 `BlockOn` 后不再需要手写 `RequestStop()`（F5）。
- 对改动文件运行 `git diff --check`。

## 附录：试用程序概况

| 程序 | 主要 API | 结果 |
|---|---|---|
| KV 服务端（epoll / uring） | `Runtime::Builder`、`io::AsyncStream` 模板、`SetReadDeadline` | 功能、空闲超时、Ctrl+C 退出在两个后端上一致；踩到 F4 |
| KV 并发客户端 | `epoll::Loop`、`Connector`、`Spawn` / `JoinHandle`、`SleepFor` | 可用；踩到 F5、F6 |
| 聊天室 | `Runtime` + `Channel` hub | 功能正确，退出时 panic（F2） |
| select 定时汇报 | `Channel`、`Select`、`RunEvery` | 回调中 `TrySend` panic（F1），改用协程写法后正确 |
| uring 大写回显 | `uring::Loop`、`AcceptSource`、`RecvSource`、`BufferLease` | 50 个并发连接、每个约 44KB，全部正确，退出干净 |

试用中确认表现良好、无需改动的点：同一份模板会话代码在两个后端上，超时（`ETIMEDOUT`）与关停
取消（`ECANCELED`）出现的位置一致；关停时连接协程链完整排空；同一条流上并发两个 `Read`，第二个
返回 `EBUSY`；concept 不满足时，编译器能指出缺失的成员。

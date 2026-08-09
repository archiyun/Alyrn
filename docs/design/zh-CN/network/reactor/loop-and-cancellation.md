# EventLoop、readiness 与取消/关闭

这篇文档按一次 `ReadSome()` 的实际路径介绍 Reactor，并重点说明取消。

先记住一句话：

> Reactor 取消的是“等待下一次 readiness”的逻辑操作，不是取消一个内核仍在访问用户 buffer 的 I/O 请求。

因此 Reactor 与 luring 可以给业务层相同的 `Result`、恢复位置与 close 语义，却不应假装
它们有相同的物理取消过程。

## 1. 模块与职责

```text
reactor::EventLoop                  公开的单线程 dispatcher / Scheduler
reactor::ReactorStream              socket 的 read/write/close 适配器
reactor::ReactorListener            accept 与 AcceptSource 适配器
reactor::ReactorConnector           connect / SleepFor 适配器
reactor::ReactorRecvSource          readiness 驱动的多事件 receive source

reactor::detail::Channel            一个 fd 的 interest、事件与回调
reactor::detail::Poller             poller seam；默认实现是 EPollPoller
reactor::detail::TimerQueue         owner-loop timer
reactor::detail::LoopShutdownRegistry
                                  loop stop 时通知 loop-owned 资源
```

公开 seam 很小：业务代码只持有 `EventLoop`、stream、listener 或 connector。
`Channel`、`epoll_ctl`、pending awaiter 指针和关闭注册表都留在 Reactor 的实现中。

```text
业务协程
  │ co_await stream.ReadSome(buffer)
  ▼
ReactorStream awaiter
  │ 尝试 nonblocking read()
  ├─ 成功 / EOF / errno ───────────────► await_resume()
  │
  └─ EAGAIN
       │ 记录 pending_read_，开启 EPOLLIN
       ▼
EventLoop ──► EPollPoller::Poll() ──► Channel::HandleEvent()
                                            │
                                            ▼
                                  ReactorStream::HandleRead()
                                            │ 再尝试 read()
                                            ▼
                               CompleteRead() → Schedule continuation
```

源码入口：

- `include/coropact/reactor/loop.h`
- `src/reactor/event_loop.cc`
- `include/coropact/reactor/stream.h`
- `src/reactor/reactor_stream.cc`
- `include/coropact/reactor/detail/channel.h`
- `src/reactor/epoll_poller.cc`

## 2. EventLoop：一个线程，一个调度器

每个 `EventLoop` 创建时绑定当前线程，并且自身继承 `coro::Scheduler`：

```text
owner thread
  └── EventLoop
        ├── EPollPoller
        ├── TimerQueue
        ├── owner-local coroutine WorkQueue
        ├── eventfd wakeup Channel
        └── LoopShutdownRegistry
```

以下操作都必须在 owner thread：创建/移动/销毁 stream、提交 I/O、修改 `Channel` interest、
执行 `Run()`，以及 `Scheduler::Schedule()`。跨线程唯一的 loop 控制入口是
`RequestStop()`；它不是通用任务队列。

`EventLoop` 状态为：

```text
Created ──Run──► Running ──RequestStop──► Stopping ──drain──► Stopped
                    ▲                         │
                    └──── stop_token ─────────┘
```

`RequestStop()` 使用原子状态转换，并向 `eventfd` 写入一个计数。这个写入会唤醒可能阻塞在
`epoll_wait()` 的 owner thread；真正的资源取消和 coroutine 恢复仍只在 owner thread 进行。

## 3. 一次单次 read 的完整路径

以 borrowed buffer 的普通读为例：

```cpp
std::array<std::byte, 4096> storage;
auto read = co_await stream.ReadSome(storage);
```

### 3.1 首次尝试：不必挂起

`ReadSomeAwaiter::await_suspend()` 先检查：

```text
loop 不是 Stopping/Stopped
stream 尚未关闭
当前没有另一条 pending read
```

随后立即执行 nonblocking `read()`：

```text
bytes >= 0        完成；await_suspend() 返回 false
EOF (bytes == 0)  完成；await_suspend() 返回 false
其他 errno        完成；await_suspend() 返回 false
EAGAIN            建立逻辑等待
```

`EINTR` 会重试。只有 `EAGAIN/EWOULDBLOCK` 才会将 awaiter 写入 stream 的
`pending_read_` 槽位并启用 `EPOLLIN`。

每个 stream 最多同时拥有一个 pending read 和一个 pending write。这是当前 Reactor
单 fd 状态机的刻意限制，不是 `epoll` 的限制。

### 3.2 readiness 后重试

`epoll_wait()` 返回的是“现在值得再试一次”的提示，不是 read 的完成结果：

```text
EPOLLIN
  -> Channel::HandleEvent()
  -> ReactorStream::HandleRead()
  -> awaiter::OnReady()
  -> 再次 nonblocking read()
```

第二次 `read()` 仍可能得到 `EAGAIN`，此时 awaiter 保持 pending；否则
`ReactorStream::CompleteRead()` 会：

```text
1. 取走 pending_read_，所以同一 awaiter 不再是完成目标；
2. 对 EOF/错误取消 read interest；
3. 调用 awaiter::Complete(result)；
4. awaiter 的 CompletionGate 授权一次 SchedulerContinuation::Schedule()。
```

协程不是在 epoll callback 中直接 `resume()`，而是作为 `coro::Work` 放回 owner loop 的
ready queue。这样 callback 返回后，下一轮 `DoPendingWork()` 再恢复协程，避免在
`Channel::HandleEvent()` 的栈中递归恢复用户协程。

## 4. Reactor 的取消到底取消了什么

### 4.1 没有“在飞的 read request”

Reactor 使用普通 nonblocking syscall：

```text
read(buffer)
  ├── 立即把数据写入 buffer 并返回
  └── 返回 EAGAIN；内核没有保存 buffer 地址
```

第二种情况下，pending 的只是：

```text
awaiter 指针 + continuation + EPOLLIN interest
```

而不是内核已接受、稍后会继续写 `buffer` 的 request。因此当 owner thread 取消等待时：

```text
清除 pending_read_
  -> 从 epoll 移除/关闭 interest
  -> 用 ECANCELED 完成 awaiter
  -> 调度 continuation
```

到 `await_resume()` 返回时，Reactor 后端已经不会再访问 borrowed buffer。这里没有 luring
的“逻辑取消先可见、迟到 CQE 仍可能写 buffer”的额外释放边界。

这也是 Core contract 能要求 borrowed `span` 在 await 返回前保持有效的原因；它不意味着
调用者可以在协程真正恢复前提前释放 storage。

### 4.2 `CompletionGate`：ready、timeout、close 的唯一裁决

同一个 awaiter 可能同时遇到多个终态来源：

```text
socket readiness
timer timeout
stream Close()
loop RequestStop()
EPOLLERR / HUP
```

它们都必须经过 awaiter 的 `CompletionGate::TryComplete()`：

```text
第一个到达者：保存 result，安排 continuation
后续到达者：TryComplete() == false，什么也不做
```

Gate 是 owner-thread-confined 的 1-byte 状态；Reactor 不允许多个线程直接执行
`Complete()`。跨线程 stop 先通过 `eventfd` 回到 owner loop，避免把 gate 变成一把热路径原子锁。

### 4.3 timeout 是“解除等待”，不是内核取消

`ReadSomeFor()` 在 read 因 `EAGAIN` 挂起后，将一个 timer 放入 `TimerQueue`：

```text
read readiness  ─┐
                 ├─ CompleteRead(result) ─► CompletionGate
timer expiry ────┘
```

timer 首先检查该 awaiter 是否仍是 stream 的 `pending_read_`，随后以 `ETIMEDOUT` 走同一条
`CompleteRead()` 路径。正常 read、EOF、error、close 任一方先胜出时，awaiter 会取消 timer。

因此 timeout 后：

```text
pending slot 已清除
EPOLLIN 已按终态收敛
timer 已取消或已消费
协程恰好恢复一次
```

### 4.4 `ReactorStream::Close()`：资源级取消

`Close()` 必须在 owner loop 调用。它进入 `CloseNow()`，顺序为：

```text
closed_ = true
  -> pending read  完成 ECANCELED
  -> pending write 完成 ECANCELED
  -> Channel::DisableAll() / Remove()
  -> close(fd)
```

它有两个效果：

1. 已经挂起的 read/write 都得到一次 `ECANCELED`；
2. 后续新操作因 `closed_` 或无效 fd 得到 `EBADF`，不会重新注册 epoll interest。

`Shutdown()` 不等于 `Close()`：前者仅执行 socket 写方向 shutdown，不会取消本地 pending
read/write，也不释放 fd。

### 4.5 `EventLoop::RequestStop()`：dispatcher 级取消

`RequestStop()` 不直接从调用线程关 fd，也不直接恢复协程：

```text
foreign thread / stop_token
  -> EventLoop::RequestStop()
  -> state: Running -> Stopping
  -> eventfd write
  -> epoll_wait returns

owner thread
  -> EventLoop::BeginShutdown()
  -> LoopShutdownRegistry::RequestStop()
  -> each resource's loop-stop callback
  -> RunPending()
  -> Stopped
```

目前登记的 owner-loop 参与者包括：

| 参与者 | stop 时做什么 |
| --- | --- |
| `ReactorStream` | `CloseNow()`；pending read/write 为 `ECANCELED`，fd 关闭 |
| `ReactorListener` | `CloseNow()`；pending `Accept()` 为 `ECANCELED`，listener fd 关闭 |
| `ReactorRecvSource` | `RequestBackendStop()`；停止 readiness admission，结算 pending `Next/Stop` |
| pending `ConnectAwaiter` | detach 临时 Channel，返回 `ECANCELED` |
| pending `SleepAwaiter` | cancel timer，恢复等待协程 |

`Stopped` 只表示 EventLoop 已经退出 poll 并 drain 了它的 owner-local coroutine work。
它**不是**“所有 C++ 对象已经析构”的承诺：`BufferLease`、用户持有的 stream、coroutine owner
仍须由各自的生命周期规则收尾。

## 5. AcceptSource 与 RecvSource 的停止

它们不是单次操作，不能简单等同于 `ECANCELED`。

### AcceptSource

Reactor 通过：

```text
EPOLLIN -> accept() drain -> queued accepted streams -> Next()
```

模拟逻辑 event source。`Stop()` 解除 read interest、结算当前 armed readiness，然后让 source
进入 terminal；已排队的 accepted stream 仍先交付。队列耗尽后，`Next()` 返回：

```text
Result<std::optional<ReactorStream>>{std::nullopt}
```

listener 关闭时也走 source terminal 语义；真正的 listener 错误才经 `Result` 作为 error 返回。

### RecvSource 与 BufferLease

`ReactorRecvSource` 自己拥有一组 buffer slots。readiness 到来后它循环执行 `recv(MSG_DONTWAIT)`，
将每次成功 read 转成带 `BufferLease` 的事件。

`RequestStop()` 只停止后续 admission：

```text
disable EPOLLIN
  -> terminal readiness request
  -> 允许已排队 event 继续被 Next() 取走
  -> 等所有 BufferLease 归还
  -> Stop() 才完成
```

因此 source 的最终销毁前必须满足：没有 pending `Next/Stop`、事件队列为空、没有 outstanding
lease。这是资源归还协议，不应被 `EventLoop::Stopped` 偷偷绕过。

## 6. 必须保持的安全不变量

阅读或修改 Reactor 时，优先检查下面六条：

```text
1. 所有 Channel、pending slot、CompletionGate 都只由 owner loop 访问。
2. 一个 pending read/write/accept 至多完成一次。
3. 完成前 awaiter、continuation、stream/listener 与 borrowed buffer 都还存活。
4. 取消先清除 pending slot，再安排 continuation；之后的 readiness 不能再找到旧 awaiter。
5. Channel 必须在 owner 析构或 move 前 DisableAll()、Remove()。
6. loop stop 只触发关闭与 drain；对象/lease 的最终释放仍需各自授权。
```

对应抽象模型见：

- [`loop_stop_control.tla`](../formal/loop_stop_control.tla)
- [`resource_close_cancel.tla`](../formal/resource_close_cancel.tla)
- [AsyncStream 与 AsyncListener 协程语义契约](../async-stream-contract.md)

其中 `resource_close_cancel.tla` 是跨后端的更强模型。Reactor 对 borrowed read 的实现较简单，
因为 `EAGAIN` 后没有 kernel-owned buffer reference；luring 必须额外等待 target CQE 的 terminal
边界，才能得到同样的用户可观察安全性。

## 7. 建议的源码阅读顺序

第一次阅读建议只走单次 read：

1. `include/coropact/reactor/loop.h`
2. `src/reactor/event_loop.cc`
3. `include/coropact/reactor/detail/channel.h`
4. `src/reactor/channel.cc`
5. `src/reactor/epoll_poller.cc`
6. `src/reactor/reactor_stream.cc` 中 `ReadSomeAwaiter`、`HandleRead`、`CompleteRead`、`CloseNow`

再看两条更复杂的取消路径：

```text
timeout: ReadSomeAwaiter timer -> CompleteRead(ETIMEDOUT)
loop stop: RequestStop -> BeginShutdown -> LoopShutdownRegistry -> CloseNow
```

最后再读 event source：`src/reactor/reactor_listener.cc` 的 `ReactorAcceptSource` 与
`src/reactor/reactor_recv_source.cc`。这样不会一开始就被 multishot/lease 状态机淹没。

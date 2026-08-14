# KqueueLoop、Post 与主从移交

这篇文档说明 kqueue 多 worker 为什么不能照搬 Linux `SO_REUSEPORT`，以及 fd 如何
在 loop 之间移动而不带走协程帧。

先记住两句话：

> `KqueueStream` 绑定一个 `KqueueLoop`。它不能跨 loop 移动。
> 跨线程只能 `Post` 回调；不能 `Schedule` 别人的 `Work*`。

## 1. KqueueLoop

每个 `KqueueLoop` 创建时绑定当前线程，并且自身继承 `coro::Scheduler`：

```text
owner thread
  └── KqueueLoop
        ├── KqueuePoller          (kevent 变更与等待)
        ├── TimerQueue            (用户态 timer 树 + 一个 EVFILT_TIMER)
        ├── owner-local WorkQueue
        ├── posted_               (跨线程回调，mutex 保护)
        ├── wakeup pipe Channel
        └── LoopShutdownRegistry
```

必须在 owner thread 上的操作：创建/移动/销毁 stream、提交 I/O、修改 Channel
interest、`Run()`、`RunOnOwner()`、`Schedule()`。

跨线程允许的 loop API：

| 方法 | 作用 |
| --- | --- |
| `RequestStop()` | 幂等；唤醒阻塞中的 `kevent`，开始 cancel/drain |
| `Post(Functor)` | 把回调排到属主线程的下一轮；用于移交已接受的 fd |

`Post` **不是**通用任务队列，也不是把协程 `Work` 扔到另一个 scheduler 的入口。
continuation 仍然只能由记录下来的那个 `Scheduler` 恢复。

`Run()` 每轮先排空 `Post` 的回调，再跑 owner-local coroutine work，再 `kevent`。
`HasImmediateWork()` 在 `posted_` 非空时使用 0 超时，避免已排队的移交被一次阻塞
等待拖住。

## 2. 为什么 Wakeup 必须留在 posted mutex 里

`Post` 在锁内 `push_back` 之后立刻 `Wakeup()`（写 wakeup pipe），然后才解锁。
析构在同一把锁下检查 `posted_` 为空并关闭写端。

假 `kevent`（Linux shim）会立刻返回，0 超时 poll 也能在真实 kqueue 上发生。若
`Wakeup` 放在锁外，属主线程可能已经执行完这次回调里的 `RequestStop()`、退出
`Run()`、并在析构里 `close` 管道，而 `Post` 的 `write` 还没返回。这是数据竞争，
也会在管道已关闭时触发 `EBADF`。

`RequestStop()` 仍然可以在不持有 `posted_mutex_` 的情况下 `Wakeup()`：它只保证
“请停止”，不保证与对象析构并发。应用必须在没有未完成 `Post` 之后再销毁 loop；
worker group 先停 acceptor，再 join I/O worker。

## 3. 移交的是 fd，不是 stream

`KqueueStream` 的构造与移动都要求当前线程就是其 loop 的 owner。Channel 注册表、
pending read/write 与 shutdown participant 都是 owner-local 的。

因此 acceptor 不能把一个活的 `KqueueStream` 搬到另一个 worker。正确顺序是：

```text
acceptor (worker 0)
  Accept() -> KqueueStream on loop 0
  PeerAddress()                 // Release 之前读出
  fd = stream.Release()         // 解注册 Channel，交出 fd，stream 不再拥有 socket
  target->Loop()->Post({
      reconstruct KqueueStream(owner, fd, peer)
      SpawnDetach(*owner, handler(*context, stream))
  })
```

`Release()` 只能在属主线程、且没有 pending read/write 时调用。若目标 loop 或
worker context 已经消失，`Post` 回调里 `close(fd)`，而不是把 fd 泄漏给下一个
进程。

`Socket::Release()` 是这条路径用的底层原语：交出描述符所有权，不再在析构时关闭。

## 4. WorkerGroup 启动顺序

`KqueueWorkerGroup::Start()`：

```text
workers_.resize(n)
for i in 1 .. n-1:          // I/O workers 先起来
    StartOne(i, accept=false, empty handler)
    把 workers_[i] 赋好再 Start()   // 轮询不能看到空槽
StartOne(0, accept=true, wrapping handler)
```

wrapping handler 在 `n > 1` 时调用上面的移交协程。`n == 1` 时连接直接在 worker 0
上跑原始 `OnConnection`，没有 `Post`。

`NextWorker()` 是 `fetch_add` 取模。acceptor 自己也可以成为 I/O 目标；移交仍走
`Post`，这样 Channel 注册发生在将要跑 session 的那个 loop 上，即使目标碰巧是
worker 0。

`reuse_port` 在 `StartOne` 里被强制为 `false`。kqueue 多 worker 不靠内核把 SYN
分到多个 listen socket。

停止时 `RequestStop()` 通知所有 worker；`Stop()` / 析构 `workers_.clear()` 会
join 每个 `jthread`。应先让 acceptor 不再 `Accept`/`Post`，再销毁 I/O loop。

## 5. 与 Reactor / luring 的对照

| | Reactor | luring | kqueue |
|---|---|---|---|
| 多 worker listen | 每 worker 一个 listener，`SO_REUSEPORT` | 每 worker 一个 listener / ring | **一个** listener |
| 连接如何到达 I/O 线程 | 内核把 accept 分到各 listener | 内核 / 该 ring 的 accept | 用户态 `Post(fd)` |
| 跨线程入口 | `RequestStop`（mailbox 是另一条 luring 路径） | mailbox + `MSG_RING` | `RequestStop` + `Post` |
| stream 跨 loop | 不移动 | 不移动 | 不移动；只移交 fd |

把 kqueue 做成 Reactor 的 `SO_REUSEPORT` 副本，会在 BSD 上得到一条该内核并不
作为默认多 listener 模型的路径，并绕过“stream 必须在属主线程构造”这条不变量。

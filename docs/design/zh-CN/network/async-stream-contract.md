# AsyncStream 与 AsyncListener 协程语义契约

> 草案。本文定义 `AsyncStream` / `AsyncListener` 的协程可观察语义。它约束
> reactor 和 luring 两类网络模块怎样解释同一套上层 I/O 语义，不规定具体文件组织。

## 定位

当前公共概念位于 `vexo::net` 命名空间：

```cpp
AsyncReadStream
AsyncWriteStream
AsyncClosableStream
AsyncStream
AsyncListener
```

这里的 `net` 是历史命名。语义上，这些概念属于公共协程 I/O 契约，而不是
reactor 实现细节。`ReactorStream`、`ReactorListener` 和将来的
`LUringStream`、`LUringListener` 都是这套契约的解释器。

本文只定义核心地板语义。io_uring 的 registered buffer、provided buffer、
multishot、zero-copy send、linked operation 等强能力不应污染核心接口。透明优化可以
沉入实现层；改变返回类型、完成基数或 buffer 生命周期的能力必须另起 concept。

## 可观察事件链

业务协程只能观察到异步操作的提交、完成和恢复：

```text
Submit(c, op)
  -> Suspend(c, op)
  -> Complete(op, result)
  -> Resume(c, result)
```

实现允许同步快路径。例如 reactor 可以在 `await_suspend` 中直接完成一次
nonblocking `read()`，luring 也可以在某些路径上立即得到失败结果。即使没有真实挂起，
逻辑上仍然必须保持：

```text
Submit(op) -> Complete(op, result) -> Resume(c, result)
```

后端内部事件，例如 epoll readiness、SQE 入队、CQE reap、batch submit、
timerfd 触发和 `io_uring_enter`，都属于内部机制事件。它们不能被业务层依赖。

## CoreStream 接口

核心 stream 至少提供：

```cpp
ReadSome(std::span<std::byte>)
    -> coro::Task<base::Result<std::size_t>>

WriteSome(std::span<const std::byte>)
    -> coro::Task<base::Result<std::size_t>>

Shutdown()
    -> coro::Task<base::Result<void>>

Close()
    -> coro::Task<base::Result<void>>
```

`ReadSome` 和 `WriteSome` 是 single-shot 操作：

```text
一次调用
  -> 至多一次 Complete
  -> 至多一次 Resume
```

multishot read、multishot accept 和 zero-copy send 不属于这个核心接口。

## Buffer 生命周期

调用方必须保证传给 `ReadSome` / `WriteSome` 的 buffer 存活到对应 `Task` 完成。

这个规则按 luring/io_uring 的更宽生命周期窗口定义：

```text
Submit SQE
  -> 内核可能持有 buffer
  -> CQE 返回
  -> Complete
  -> Resume
```

因此，以下写法是错误的：

```cpp
auto task = stream.ReadSome(std::span<std::byte>(local.data(), local.size()));
local.clear();
auto n = co_await task;
```

正确边界是：

```cpp
auto n = co_await stream.ReadSome(buffer);
// 从这里开始，buffer 才不再被该 read op 使用。
```

这个契约同样适用于 reactor。即使 reactor 当前可能在 loop 线程内同步访问 buffer，
业务代码也不能依赖这个实现细节。

## 并发提交规则

CoreStream 默认只保证每个 stream 同时最多有：

```text
一个 pending read
一个 pending write
```

同一个 stream 上并发提交多个 read，或者并发提交多个 write，不属于 CoreStream 保证。
实现可以选择断言、返回 `EBUSY` / `EALREADY`，或在调试构建中直接暴露契约错误。

read 和 write 可以同时 pending。`Close()` 必须能处理这两个槽位的收敛。

如果后续需要多 read 队列、写队列、scatter/gather pipeline 或 multishot 序列，必须通过
新的 concept 和 capability 明确声明。

## ReadSome 结果

`ReadSome(buffer)` 的结果含义如下：

```text
Result<N>, N > 0
  读取到 N 字节，N <= buffer.size()

Result<0>
  对端有序关闭，等价 EOF。它不是本地 Close，也不是取消。

unexpected(error)
  读取失败。error 保存 errno 风格错误码。
```

对非空 buffer，`0` 是传输层 EOF。业务层可以据此结束 session。

空 buffer 的行为不应被业务层依赖。实现可以立即返回 `0`，也可以拒绝。上层通用算法应避免
对空 buffer 提交实际 I/O。

## WriteSome 结果

`WriteSome(buffer)` 的结果含义如下：

```text
Result<N>, N > 0
  写出 N 字节，N <= buffer.size()

Result<0>
  未取得进展。通用 WriteAll 会把它解释为 EPIPE，避免死循环。

unexpected(error)
  写入失败。error 保存 errno 风格错误码。
```

`WriteSome` 不保证写完整个 buffer。调用方需要用 `WriteAll` 之类的算法处理短写。

zero-copy send 不属于 `WriteSome`。普通 `WriteSome` 完成后，调用方可复用或释放传入
buffer。zero-copy send 的“写完成”和“buffer 可复用”可能是两个不同 CQE 事件，因此必须
另起接口和 concept。

## Shutdown 语义

`Shutdown()` 表示本端不再继续写出数据，通常映射到 TCP half-close。

`Shutdown()` 不等价于 `Close()`：

```text
Shutdown
  关闭写方向；读方向仍可能继续收到数据。

Close
  关闭本地 stream 资源；pending op 必须收敛为完成或取消。
```

如果 stream 已经关闭，`Shutdown()` 可以返回 `EBADF` 或等价 closed error。

## Close 语义

`Close()` 是异步操作，允许真实挂起。

核心契约：

```text
Close(r)
  -> 资源进入 Closing / Closed
  -> pending read/write 被取消或以传输结果完成
  -> 后续 Submit 失败
  -> Close task 完成
```

`Close()` 不承诺同步取消。reactor 可以同步完成取消，这是实现的超额交付；luring 可能需要：

```text
mark closed
submit ASYNC_CANCEL
wait op CQE / cancel CQE
close fd
Complete Close
```

为了简化调用方生命周期，推荐实现让 `Close()` 在当前 stream 的 pending op 都已经完成或取消后
再返回。无论实现如何，buffer 的安全边界仍然是对应 op 的 `Task` 完成，而不是调用方主观上
认为已经执行过 `Close()`。

关闭后的新 `ReadSome` / `WriteSome` / `Shutdown` 应返回 `EBADF` 或等价 closed error。
重复 `Close()` 应该幂等成功，除非实现文档另有说明。

## CancelByClose

`CancelByClose` 的语义是 eventual：

```text
Close 会支配 pending op 的收敛；
pending op 最终完成为 ECANCELED、EOF、EPIPE 或具体传输错误；
不要求 Close 调用点同步完成所有取消。
```

取消不是静默丢弃。已经提交且尚未完成的 op 必须最终产生一个可观察结果，使等待该 op 的协程
能够恢复。

典型错误分类：

```text
ECANCELED
  本地 Close 导致 pending op 取消。

EBADF
  资源已经关闭后又提交新 op。

ETIMEDOUT
  带 timeout 的 op 超时。

EPIPE
  写方向观察到连接断开或无法继续写。
```

## EOF、EPIPE 和本地 Close 分离

不要把对端关闭、本地关闭和传输错误混成一个状态。

```text
对端有序关闭
  ReadSome -> Result<0>

本地主动 Close
  pending op -> ECANCELED 或已经发生的传输结果
  后续 submit -> EBADF / closed error

传输错误
  ReadSome / WriteSome -> unexpected(errno)

写方向断开
  WriteSome -> EPIPE 或等价错误
```

这个分离是 gateway、连接池、重试和健康检查判断行为的基础。

## AsyncListener 契约

listener 至少提供：

```cpp
Accept()
    -> coro::Task<base::Result<std::unique_ptr<Stream>>>

Close()
    -> coro::Task<base::Result<void>>
```

核心 listener 默认只保证一个 pending accept。并发多个 `Accept()` 不属于核心契约。

`Accept()` 成功时返回一个满足 `AsyncStream` 的 stream。失败时返回 errno 风格错误。

`Close()` 必须取消 pending accept，并让等待该 accept 的协程恢复。关闭后的新 `Accept()`
应返回 `EBADF` 或等价 closed error。重复 `Close()` 应该幂等成功。

multishot accept 不属于 `AsyncListener`。它是：

```text
一次 Submit
  -> 多次 Complete
  -> cancel / close 终止序列
```

因此必须使用新的 concept，例如 `AsyncMultishotListener`。

## Timeout 能力

核心 `AsyncStream` 当前没有强制 `ReadSomeFor`。如果业务声明需要 timeout，应该通过单独
concept 或 capability 绑定：

```cpp
ReadSomeFor(std::span<std::byte>, std::chrono::milliseconds)
    -> coro::Task<base::Result<std::size_t>>
```

timeout 的可观察结果是：

```text
读取成功 -> Result<N>
对端关闭 -> Result<0>
超时     -> ETIMEDOUT
其他错误 -> errno
```

实现方式属于内部机制：

```text
reactor: timerfd / TimerQueue 与 readiness race
luring : read + LINK_TIMEOUT 或 cancel
```

如果 timeout 被提升为 CoreStream 的必备能力，必须同步修改概念约束和 capability profile，
不能只在实现里“有就用”。

## 能力分层

capability bit 分三类。

A 类是核心语义，进入 active profile，所有被绑定的模块必须满足：

```text
kReadSome
kWriteSome
kClose
kCancelByClose
```

如果 timeout 被正式纳入核心，则加入：

```text
kTimeout
```

B 类是实现标签，不进入业务 active profile，允许透明 fallback：

```text
kReadinessPoll
kSubmitRead
kSubmitWrite
kRegisteredBuffer
kFixedFile
kSqPoll
kIoPoll
```

业务不应该声明“我要 readiness”或“我要 SQE/CQE”。它只声明所需语义。

C 类是不透明扩展，会改变返回类型、完成基数、生命周期或组合语义，必须新 concept + profile
gate：

```text
kProvidedBuffer
kMultishotRecv
kMultishotAccept
kLinkedOps
kSendZeroCopy
```

enum 只是门卫，不是契约本体。每个 C 类能力都必须有对应接口和不变量。

## 实现不变量

核心实现至少满足：

```text
I1: 每个 single-shot op 最多 Complete 一次。
I2: Complete 发生在 Submit 之后。
I3: Resume 恢复提交该 op 的正确 coroutine。
I4: Close 支配 pending op 收敛，后续 Submit 失败。
I5: buffer / fd / coroutine handle 活到 Complete。
I6: 每个 pending read/write/accept 槽位有唯一归属。
```

这些不变量是 reactor 和 luring 共同解释 `AsyncStream` / `AsyncListener` 的基础。后端可以改变
调度策略、submit batch、CQE reap 批量大小和内部数据结构，但不能改变这些可观察语义。


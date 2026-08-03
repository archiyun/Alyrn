# AsyncStream 与 AsyncListener 协程语义契约

> 状态：核心设计契约。本文定义业务协程可以依赖的 I/O 语义，以及 Reactor 和
> luring 实现必须共同满足的不变量。本文不描述 io_uring 的具体 SQE/CQE API，也不
> 把尚未落地的扩展能力写成核心能力。

## 1. 这份契约解决什么问题

业务层不应该知道一次网络 I/O 是通过 readiness 事件完成，还是通过 io_uring completion
完成。业务只应该依赖：

```text
调用一个异步操作
  -> 协程可能挂起
  -> 操作产生一个结果
  -> 原协程恢复并观察该结果
```

当前项目有两种网络机制：

```text
coropact::reactor   Reactor / epoll / nonblocking syscall
coropact::luring    io_uring / SQE / CQE
```

它们是两个独立的机制模块，不是同一个网络库的两个公开模式。两者通过公共协程 I/O
语义连接：

```text
业务层
  -> coropact::io::AsyncStream / AsyncListener
  -> ReactorStream / LUringStream
  -> Reactor 或 io_uring
```

公共 facade 位于 `coropact::io`；其 canonical concept 定义位于不依赖具体后端的
`coropact::backend`。`coropact::net` 提供共享的地址、socket 和网络工具，
`coropact::reactor` 承载 epoll/EventLoop 实现。

本文的核心边界是：

```text
核心层定义可观察语义；
后端层解释这些语义；
实现优化不能改变核心结果、生命周期和完成基数；
改变这些性质的能力必须另起 concept 和契约。
```

## 2. API 边界

### 2.1 CoreStream

当前公共概念约束的最小接口是：

```cpp
ReadSome(std::span<std::byte> buffer)
    -> 可 await，await_resume() 为 base::Result<std::size_t>

WriteSome(std::span<const std::byte> buffer)
    -> 可 await，await_resume() 为 base::Result<std::size_t>

Shutdown()
    -> coro::Task<base::Result<void>>

Close()
    -> coro::Task<base::Result<void>>
```

对应的公共概念是：

```cpp
coropact::io::AsyncReadStream
coropact::io::AsyncWriteStream
coropact::io::AsyncClosableStream
coropact::io::AsyncStream
```

`AsyncStream` 是四个方法的语义组合，不是某个具体类的基类，也不要求虚函数。当前
`ReactorStream` 和 `LUringStream` 都满足它。

### 2.2 TimedStream extension

timeout 不是 `AsyncStream` 的隐含开关。它把一次 read 解释为 read 与 timer/cancel 的
composite operation，因此使用独立 concept：

```cpp
ReadSomeFor(std::span<std::byte> buffer, std::chrono::milliseconds timeout)
    -> 可 await，await_resume() 为 base::Result<std::size_t>

coropact::io::AsyncTimedReadStream
coropact::io::AsyncTimedStream
```

`AsyncTimedStream` 表示完整的 `AsyncStream` 加上 timed read。当前 `ReactorStream` 与
`LUringStream` 都满足它；只满足 `AsyncStream` 的 adapter 不承诺 `ReadSomeFor`。
`CapabilitySet::TimedStream()` 是选择和验证此 extension 的 profile，不能替代上述编译期
interface。

### 2.3 Buffer ownership extension

`ReadSome(std::span<std::byte>)` 是核心的 borrowed-buffer 路径：调用者拥有 storage，
并且必须在 `await_resume()` 前保持对象存活、地址稳定，不能释放、扩容、移动或并发访问
同一段内存。这个约束包括 close 和 cancel 路径。

对 borrowed-buffer read，用户可观察完成不能早于后端放弃该地址：

```text
backend no longer accesses storage
  -> logical result becomes observable
  -> await_resume()
```

`AsyncOwnedReadStream` 是独立的可选 extension，不属于 `AsyncStream` 的最小接口：

```cpp
ReadInto(net::Buffer buffer, std::size_t reserve)
    -> 可 await，await_resume() 为 net::ReadIntoOutcome
```

它按值接收 move-only `net::Buffer`，awaiter 在整个 pending interval 内持有该 owner，并在
所有终态路径返回：

```cpp
struct net::ReadIntoOutcome {
  base::Result<std::size_t> result;
  net::Buffer buffer;
};
```

因此提交失败、关闭和取消也不会吞掉调用者转交的 buffer。`BufferLease` 则属于
`AsyncRecvSource`：它代表后端/池提供的存储及其归还协议，不能替代普通 `net::Buffer`。

### 2.3 CoreListener

listener 的最小接口是：

```cpp
using Stream = ...;

Accept()
    -> coro::Task<base::Result<Stream>>

Close()
    -> coro::Task<base::Result<void>>
```

`Stream` 必须满足 `coropact::io::AsyncStream`。当前 `ReactorListener` 和 `LUringListener`
都满足 `coropact::io::AsyncListener`。

`Connect()` 不属于 `AsyncStream` 或 `AsyncListener`。它是建立 outbound stream 的另一项
能力，由应用层需要的 connector concept 单独约束。

### 2.4 Awaitable 的使用规则

`ReadSome` 和 `WriteSome` 的返回值必须是可 await 对象，并产生约定的
`base::Result`。后端可以返回惰性的 `coro::Task<T>`，也可以返回直接承载操作状态的
底层 awaiter：

```cpp
auto result = co_await stream.ReadSome(buffer);
```

`ReactorStream` 和 `LUringStream` 的 span 读写入口都返回直接 awaiter，因此核心读写路径
不会为一次读写额外创建子协程帧。`io::Buffer` 等扩展重载以及 `Shutdown()`、`Close()`
仍可返回 `Task`。

如果具体接口返回 `Task`，它仍然是 move-only、single-consumer 对象，只能被 await 一次，
也可以交给 `coro::Spawn(scheduler, task)`。直接 awaitable 则在 `co_await` 时提交 I/O。

无论返回哪种 awaitable，丢弃未等待的操作都属于错误：

```cpp
stream.ReadSome(buffer);  // 错误：没有等待该 I/O operation
```

I/O 方法本身不抛出业务异常。结果通过 `coropact::base::Result<T>` 返回，它是
`std::expected<T, std::error_code>` 的别名。协程未处理异常会终止进程，不属于网络错误
传播机制。

## 3. 六元组状态机

本文使用的是**由后端、profile 和策略参数化**的六元组，而不是把这些固定解释条件误写成
运行时可变状态：

```text
M(B, P, π) = (X, x0, E_obs, δ_B, Inv, Live)
```

```text
B : 解释器（Reactor 或 luring），在实例生命周期内固定。
P : 已绑定且验证过的 active capability profile，在启动期固定。
π : 调度/批处理策略，在实例生命周期内固定或只按显式策略转换。
```

`H(trace)` 不是状态变量；它是从一次执行 trace 推导出的 happens-before 偏序。证明时需要
验证 `Submit(op) -> Complete(op) -> Resume(owner(op))` 等边存在，而不需要在每个状态复制一份
偏序图。

### 3.1 状态空间 X

```text
X = (C, R, O, Q, L)
```

其中：

```text
C : CoroutineId -> {Running, Waiting(opId), Ready, Done}

R : ResourceId -> {Open, Closing, Closed}

O : OpId -> OperationRecord
    OperationRecord = {
      family,              // SingleResult | Composite | EventSource | SplitRelease
      owner_coroutine,
      resource,
      scheduler_affinity,
      phase,
      physical_requests,
      logical_result_or_events
    }

Q : 后端 refinement 的队列投影。
    抽象层只关心“已投递但未 Resume”的逻辑 continuation；Reactor 的 ready list、
    epoll ready set 与 luring 的 SQ/CQ 不要求具有相同数据结构。

L : OpId -> LifetimeRecord。
    记录 awaiter、coroutine frame、fd、borrowed/owned buffer、BufferLease 与
    physical request 在哪个阶段仍被后端持有，以及何时获得 release authorization。
```

`x0` 表示资源已创建、没有 pending operation、协程尚未等待该资源操作的初始状态。
`OpId` 是逻辑归属的最小单位：一个 CQE、readiness、timer 或 cancel 只能改变其对应记录，
不能仅凭“当前有一个 read awaiter”恢复任意协程。

### 3.2 可观察事件 E_obs

语义事件分为两层：

```text
E_obs = {
  Submit(c, op),
  Suspend(c, op),
  Complete(op, result),
  Resume(c, result),
  Cancel(op),
  Close(r),
  Timeout(op)
}

E_int^B = {
  readiness,
  SQE queued,
  CQE reaped,
  timer fired,
  Schedule(work),
  ...
}
```

`E_obs` 是核心契约使用的语义事件，不代表业务可以直接调用这些事件。`E_int^B` 是
后端内部事件，不能被 HTTP、gateway 或通用 session 逻辑依赖。

对 read/write/accept 这类 single-shot 操作，核心链路是：

```text
Submit(c, op)
  -> [Suspend(c, op)]
  -> Complete(op, result)
  -> Resume(c, result)
```

方括号表示 `Suspend` 是可选的。Reactor 可能在 nonblocking syscall 立即得到结果，
io_uring 也可能在准备阶段立即拒绝操作。此时没有真实挂起，但仍然必须有唯一的逻辑
提交和完成结果。

`Submit` 表示一次语义上的提交尝试，不要求一定产生系统调用、SQE 或 readiness 注册。
例如资源已经关闭时，后端可以在提交点直接产生 `EBADF` 完成结果。

在立即完成路径中，`Resume` 表示结果回到当前协程的逻辑事件，不要求真的经过
`Schedule(ResumeWork)`；只有发生实际挂起时，恢复才需要经过后端的 ready queue 或等价
调度路径。

### 3.3 转移 δ_B、策略 π 与不变量

`δ_B` 是后端对物理事件的解释；它必须保持以下语义机制（也就是 `Inv` 的基础）：

```text
σ_submit   : 一次提交尝试创建唯一 operation 归属。
σ_complete : 一个 single-result operation 最多 Complete 一次。
σ_resume   : 协程只能由自己的 operation 结果或 source 的 Next() 结果恢复。
σ_cancel   : 取消必须产生该 operation family 定义的收敛状态，不能静默丢弃。
σ_close    : 资源关闭后不能产生新的成功 I/O。
σ_lifetime : backend 仍可能访问的对象必须保持存活。
σ_profile  : active profile 中的能力必须有真实语义解释。
```

`π` 负责选择调度和实现策略，例如：

```text
π_ready       : ready work 的处理顺序
π_batch       : SQE/CQE 的批量大小
π_poll        : epoll 或 io_uring 的等待策略
π_resume      : completion 到 coroutine resume 的投递策略
```

后端可以有不同的 `δ_B`，但必须在上述机制和不变量约束下解释同一组
核心事件：

```text
创建 Task
  -- co_await / Spawn --> Submit
  -- 需要等待 -------> Suspend
  -- 后端内部处理 ----> Complete(result)
  -- Schedule --------> Resume(result)
```

`π` 可以改变批量、调度顺序和延迟，但不能把一次完成变成两次恢复，也不能把一次
single-result 操作变成多次业务结果。

`Live` 单独描述最终性，而不是混入安全不变量：在公平的后端投递和 owner-loop 调度假设下，
pending single-result operation 最终 settled、已 settled 的等待协程最终 Ready、`Closing`
资源最终 `Closed`。`async_stream_core.tla` 已对这个最小模型检查这些性质；多 operation 与
source refinement 继续分别检查自己的终态条件。

### 3.4 按 operation family 解释取消

取消不是所有 family 共用的“立刻得到 ECANCELED”：

```text
SingleResult : cancel 的物理收敛产生一次 terminal logical result。
Composite   : 子 request 可先后完成；聚合结果只确定一次。
EventSource : Stop/error 才使 logical source terminal。高水位 pause 取消当前 multishot
              physical request 后可以进入 Paused，并在低水位 re-arm，不产生 source terminal。
SplitRelease: logical result、continuation resume 与 buffer/resource release 可以由不同
              physical completion 授权。
```

因此，`Cancel(op) -> Complete(op, ECANCELED)` 只适用于 single-result 的简化模型；它不能
作为 `AcceptSource`、`RecvSource` 或 send zero-copy 的通用规则。

## 4. 核心不变量

### I1：唯一完成

```text
任意 single-shot op 至多出现一次 Complete(op, result)。
```

取消路径、错误路径、正常 CQE/readiness 路径必须通过同一个 operation 归属收敛，不能
让两个路径分别恢复同一个协程。

### I2：因果完成

```text
若 Complete(op, result) 出现，则存在对应 Submit(c, op)，且
Submit(c, op) -> Complete(op, result)。
```

### I3：恢复授权

```text
若 Resume(c, result) 出现，则 c 正在等待该 op，且 result 来自该 op 的唯一完成事件。
```

协程句柄不能被无关的 CQE、timer 或其他 session 恢复。

### I4：关闭支配

关闭事件建立后：

```text
Close(r) -> 后续依赖 r 的 Submit
```

后续提交不得成功。Close 之前已经完成的 operation 不被追溯修改；Close 与 completion
发生竞争时，哪一个先在线程归属的事件序列中生效，决定该 operation 观察到成功、EOF、
取消或传输错误。

因此，“调用了 Close 就必然得到 ECANCELED”不是核心语义。核心语义是：pending operation
最终必须完成，且 Close 之后不能出现新的成功提交。

### I5：生命周期覆盖

每个 operation 使用的对象必须覆盖：

```text
Submit(op) -> Complete(op, result)
```

包括：

```text
ReadSome / WriteSome 传入的 buffer；
底层 fd 和 stream 对象；
operation awaiter；
用于恢复协程的 coroutine handle；
任何由后端保存的 sockaddr、iovec 或回调状态。
```

`Complete` 之后，后端不得继续访问该 operation 的 buffer。协程是否已经实际恢复，不
改变这个生命周期边界。

### I6：槽位唯一归属

核心 stream 默认只有两个 pending 槽位：

```text
一个 pending read；
一个 pending write。
```

read 和 write 可以同时 pending；同方向的两个 operation 不能同时 pending。listener
默认只有一个 pending accept。

### I7：线程和执行器归属

当前 ReactorStream、LUringStream、ReactorListener 和 LUringListener 都是 loop-bound：

```text
对象创建、operation 提交、Close、析构和后端状态修改
必须发生在对象所属的 loop 线程。
```

`EventLoop::RunOnOwner` 和 `Schedule` 都要求调用者位于所属 loop
线程；它们不提供跨线程投递能力。当前 `LUringLoop::Schedule` 也要求调用者位于 loop
线程。跨 loop 投递需要单独的 mailbox/message 机制；`eventfd` 和 `msg_ring` 都不属于
当前 CoreStream 契约。

### I8：能力 profile 固定

业务选择的 active profile 在后端绑定时检查，并在该运行实例的生命周期内固定：

```text
P_active ⊆ P_backend
```

能力 bit 只负责门卫，不能凭空创建 C++ 方法、返回类型或生命周期保证。内核 probe 报告
某个 opcode 存在，也不等于 coropact 已经提供了对应的业务 concept。

## 5. AsyncStream 语义

### 5.1 资源状态

stream 的核心资源状态为：

```text
Open -> Closing -> Closed
```

`Shutdown()` 只关闭本端写方向，不改变 stream 的资源状态；`Close()` 才负责本地资源
终止和 pending operation 收敛。

一个 stream 的生命周期由其拥有者管理。调用方必须保证 stream 对象至少存活到所有依赖
它的 operation 已经 Complete；通常应在 session 协程内统一负责 Close 和销毁。

### 5.2 ReadSome

```cpp
auto result = co_await stream.ReadSome(buffer);
```

语义为：一次调用最多产生一次读取结果，结果满足：

```text
Result<N>, 0 < N <= buffer.size()
  成功读取 N 字节。

Result<0>
  对端有序关闭，表示 EOF。

unexpected(error)
  读取失败，error 是 errno 风格的 std::error_code。
```

`ReadSome` 不保证填满 buffer。`Result<0>` 的 EOF 语义只适用于非空 buffer；空 buffer
不是通用业务算法应依赖的输入。当前实现可能直接返回 0，但这不应被用来判断对端关闭。

### 5.3 WriteSome

```cpp
auto result = co_await stream.WriteSome(buffer);
```

语义为一次短写操作：

```text
Result<N>, 0 < N <= buffer.size()
  成功写出 N 字节。

Result<0>
  本次没有取得进展。通用 WriteAll 将它转换为 EPIPE，避免空转。

unexpected(error)
  写入失败，error 是 errno 风格的 std::error_code。
```

调用方不能假设一次 `WriteSome` 写完整个 buffer。应使用 `coropact::io::WriteAll` 或等价
循环。buffer 在该 `WriteSome` 的 awaitable 完成前不能修改、移动或释放。

普通 `WriteSome` 的完成表示该调用使用的 buffer 已经不再被该 operation 访问。send
zero-copy 可能在“数据发送完成”和“buffer 可以复用”之间产生不同完成事件，因此不属于
`WriteSome`。

### 5.4 Shutdown

`Shutdown()` 表示本端不再继续发送数据，当前 TCP 实现映射到写方向 half-close：

```text
Shutdown()
  -> 后续本端写操作不再是正常写入
  -> 读方向仍可以继续观察数据或 EOF
```

`Shutdown()` 不会替代 `Close()`，也不负责取消 pending read/write。资源已 `Closed` 时，
调用 `Shutdown()` 应得到 closed error，当前实现使用 `EBADF`。

### 5.5 Close

`Close()` 是一个异步控制操作，允许立即完成，也允许真实挂起：

```text
Open
  -- Close --> Closing
  -- pending read/write 完成或取消 --> Closed
```

Close 必须满足：

```text
1. 资源进入 Closing 后，不再接受新的成功 I/O 提交；
2. 已经 pending 的 read/write 最终各自 Complete 一次；
3. 后端不再持有 fd 或 buffer 后，资源才进入 Closed；
4. Close 自己也必须产生一个可观察的 Result<void>；
5. Closed 后的 ReadSome / WriteSome / Shutdown 返回 closed error；
6. Closed 后重复 Close 可以成功返回。
```

`Close()` 完成不等价于其他等待中的协程已经恢复。它只保证相关 operation 已经离开后端
访问窗口；每个 pending operation 的调用方仍然必须等待自己的 Task 观察结果。

Reactor 可以通过 readiness 状态直接完成取消，luring 可能需要提交 cancel request、
等待 cancel CQE 和原 operation CQE。它们的内部路径不同，但不能改变上述状态转移。

并发调用 `Close()` 与同一个 stream 的其他操作不属于跨线程语义；同一 loop 内也不应
同时发起多个 Close。实现可以返回 `EBUSY` 或在调试构建中暴露调用错误。

### 5.6 关闭和完成的竞争

以下结果都可能是合法的，取决于事件在线程归属序列中的线性化顺序：

```text
read 先完成 -> ReadSome 得到 N、0 或传输错误；随后 Close 完成
Close 先取消 -> ReadSome 得到 ECANCELED；随后 Close 完成
底层连接先断开 -> ReadSome 得到 0，WriteSome 得到 EPIPE 或具体错误
```

实现不能同时为同一个 operation 报告两个结果，也不能因为 Close 已被调用就丢弃 pending
operation 的完成事件。

## 6. AsyncListener 语义

### 6.1 Accept

```cpp
auto result = co_await listener.Accept();
```

一次 `Accept()` 是 single-shot operation：

```text
成功 -> Stream 值
失败 -> errno 风格错误
```

成功返回的 stream 与 listener 属于同一个 loop/executor，调用方不能把它直接移动到另一个
ring 后继续 I/O。

核心 listener 只保证一个 pending accept。重复提交同一个 listener 的 accept 不属于契约，
实现可以返回 `EBUSY` 或在 debug 构建中断言。需要一个 submit 产生多个连接结果时，必须
另起 `AsyncMultishotListener` concept。

### 6.2 Listener Close

listener 的 `Close()` 与 stream 的 Close 遵循相同的 eventual 规则：

```text
Close(listener)
  -> 不再接受新连接
  -> pending Accept 最终完成或取消
  -> listener fd 关闭
  -> Close Task 完成
```

`Accept()` 已经 Complete 的连接不因 listener Close 而自动销毁。listener 只拥有监听
资源，不拥有已经交付给 session 的 stream。

## 7. Buffer、fd 和协程生命周期

### 7.1 span buffer

`std::span` 只携带地址和长度，不拥有内存。调用方必须保证内存覆盖：

```text
co_await stream.ReadSome(buffer)
或
co_await stream.WriteSome(buffer)
```

对应 I/O operation 完成的整个过程：

```text
创建 awaitable -> co_await -> Submit -> 可能 Suspend -> Complete -> await_resume
```

错误示例：

```cpp
coropact::coro::Task<void> Bad(coropact::io::AsyncStream auto& stream) {
  std::vector<std::byte> local(4096);
  auto task = stream.ReadSome(local);
  local = {};                    // 错误：底层 operation 仍可能使用这块内存
  auto result = co_await std::move(task);
  (void)result;
}
```

正确写法是让 buffer 由协程 frame、调用方对象或更长生命周期的 pool 持有：

```cpp
coropact::coro::Task<void> Good(coropact::io::AsyncStream auto& stream) {
  std::array<std::byte, 4096> buffer{};
  auto result = co_await stream.ReadSome(buffer);
  (void)result;
}
```

### 7.2 io::Buffer

`ReactorStream` 和 `stream_algorithms.h` 还提供 `coropact::io::Buffer` 重载。这是 buffer
管理层的扩展，不改变 CoreStream 的 span 契约。公开的 `io::Buffer` 是
`net::Buffer` 的零成本 alias：实现留在后端之下，调用者应使用 `io` spelling：

```text
PrepareWrite / ReadableIov 返回的内存必须覆盖 pending operation；
读成功后 CommitWrite，读失败后 AbortWrite；
写成功后 Drain 已写出的字节。
```

扩展 `RecvSource` 已明确提供 buffer 的所有权边界：luring 使用 provided buffer ring，
Reactor 使用固定 buffer pool；每个 `RecvEvent` 携带一个 `BufferLease`，consumer 必须
在 source 销毁前释放它。buffer id、归还时机和 RAII 所有权不能隐藏在普通 `std::span`
的成功结果里。luring 的 `LUringRecvSource` 在显式开启
`incremental_buffer_consumption` 后使用 `IOU_PBUF_RING_INC`，并根据 `F_BUF_MORE` 为同一个
buffer id 追踪连续 offset；每个 segment 仍有独立 `BufferLease`，只有终止 CQE 到达且全部
segment lease 释放后才把 buffer 归还 ring。registered fixed buffer 仍属于后续扩展。

### 7.3 fd、stream 和 operation owner

后端保存的 fd、sockaddr、iovec、awaiter 和 coroutine handle 都必须由 operation owner
保持有效。`Close()` 只能结束后端访问；它不能让调用方提前释放仍被 pending Task 引用的
协程 frame 或 buffer。

## 8. Timeout 语义

timeout 的目标业务语义是：

```cpp
ReadSomeFor(std::span<std::byte> buffer, std::chrono::milliseconds timeout)
    -> 可 await，await_resume() 为 base::Result<std::size_t>
```

结果应为：

```text
读取到数据 -> Result<N>
对端关闭   -> Result<0>
超时       -> unexpected(ETIMEDOUT)
其他失败   -> unexpected(errno)
```

实现可以使用 Reactor 的 TimerQueue，或 io_uring 的 linked timeout/cancel，但业务不应
观察这些内部机制。

它由 `AsyncTimedReadStream` / `AsyncTimedStream` 明确表达。当前代码状态为：

```text
ReactorStream 已提供 ReadSomeFor；
LUringStream 也已提供 ReadSomeFor；
两者满足 io::AsyncTimedStream；
kTimeout 只在 TimedStream/TimedNetwork profile 中选择这一已存在的 extension，
不扩张 AsyncStream 的最小 interface。
```

因此，通用算法必须按需要约束正确的 concept：

```text
AsyncStream       不要求 ReadSomeFor；
AsyncTimedStream  才要求 ReadSomeFor；
profile bind      验证选择的 backend 已实现 timed extension；
native probe      仍不等于 CoroPact 已经提供可观察 timed 语义。
```

超时由后端作为 composite operation 实现：Reactor 使用 TimerQueue，luring 使用 linked
timeout/cancel；业务不观察这些内部机制。profile 不满足时应在 bind 阶段拒绝，而不是让
`AsyncStream` 调用在运行时静默降级。

## 9. Capability 分层

Capability 描述的是语义 profile 或实现标签，不是 API 的替代品。

### A 类：核心语义能力

A 类进入 active profile，后端必须提供可观察且可测试的统一语义：

```text
kReadSome
kWriteSome
kShutdown
kClose
kCancelByClose
kAccept       // CoreNetwork
kConnect      // CoreNetwork / Connector
kTimeout      // TimedStream / TimedNetwork 的 AsyncTimedStream extension
```

### B 类：实现标签

B 类描述后端怎样实现核心语义，业务不声明，允许透明 fallback：

```text
kReadinessPoll
kSubmitRead
kSubmitWrite
kRegisteredBuffer
kFixedFile
kSqPoll
kIoPoll
```

业务请求的是 `CoreStream` 或 `CoreNetwork`，不是“我要 readiness”或“我要 SQE”。

### C 类：不透明扩展

C 类会改变返回类型、完成基数、生命周期、所有权或组合语义，必须使用新的 concept、
方法或 profile gate：

```text
kProvidedBuffer
kMultishotRecv
kMultishotAccept
kLinkedOps
kSendZeroCopy
```

例如：

```text
普通 ReadSome：一次 Submit -> 一次 Complete -> 一次 Resume
multishot recv：一次 Submit -> 多次 Complete -> cancel/close 终止
```

因此 multishot 不能塞进 `ReadSome`，provided buffer 不能伪装成普通 span，send zero-copy
不能复用普通 `WriteSome` 的 buffer 完成边界。

当前 luring 还提供显式扩展：

```cpp
auto result = co_await stream.SendZeroCopy(buffer);
```

该 awaiter 将 send CQE 与可选的 `F_NOTIF` notification 作为同一个 split-release operation
处理；只有 terminal CQE 到达后才恢复调用方，因此 `buffer` 在 `co_await` 返回前必须保持有效。
Reactor 没有对应的两阶段内核通知，不为普通 send 伪造该扩展语义。

## 10. 两个后端如何解释同一契约

### Reactor

Reactor 的典型内部路径是：

```text
TryRead/TryWrite
  -> 立即成功或得到 EAGAIN
  -> EAGAIN 时注册 Channel readiness
  -> readiness callback 再次尝试 syscall
  -> Complete
  -> Scheduler::Schedule
  -> Resume
```

TimerQueue、Channel 和 owner-local ready/work queue 都是 Reactor 内部机制。它们不能泄漏
到 CoreStream 的业务接口。

### luring

luring 的典型内部路径是：

```text
await_suspend
  -> 准备 SQE
  -> loop flush / submit
  -> CQE reap
  -> op->Complete(cqe->res)
  -> Schedule(ResumeWork)
  -> Resume
```

`io_uring_enter`、SQE 批量提交、CQE 批量回收、`ASYNC_CANCEL` 和每个 ring 的线程归属都
属于 luring 内部机制。

两条路径必须对业务保留同一个核心投影：

```text
ReadSome      -> Result<N> / Result<0> / error
WriteSome     -> Result<N> / Result<0> / error
Close         -> eventual resource closure
Accept        -> one stream or error
Resume        -> the coroutine waiting for that operation
```

当前 luring 没有跨 ring 的公共消息层，也没有 runtime hot-swap。后端在启动期绑定后固定，
不是运行中任意迁移 pending operation。

## 11. 错误和传输结束

核心层不把所有异常情况压成一个 `closed` 状态：

```text
ReadSome -> Result<0>
  对端有序关闭，EOF。

Close 取消 pending operation -> ECANCELED
  本地关闭导致的取消结果；仍然要通过原 Task 观察。

Close 之后新提交 -> EBADF 或等价 closed error
  不是对端 EOF。

WriteSome -> EPIPE 或具体传输错误
  写方向无法继续。

Timed operation -> ETIMEDOUT
  只有在 TimedStream profile 真正绑定后才是核心可用结果。
```

底层系统调用返回的其他 errno 应保留，不应在后端层无理由改写。应用可以据此区分
EOF、本地取消、连接失败、上游失败和超时。

`WriteAll` 是上层算法，不是 stream 的单次操作。它会重复提交短写，并把成功但零进展的
结果转换成 `EPIPE`，防止无限循环。

## 12. 实现和测试义务

任何新的 `AsyncStream` 解释器至少需要验证：

```text
1. 立即读成功和 pending 读成功都只恢复一次；
2. 短写由 WriteAll 正确推进；
3. 对端关闭产生 ReadSome -> Result<0>；
4. pending read 在 Close 后最终完成，且不会悬挂；
5. pending write 在 Close 后最终完成，且不会悬挂；
6. Close 后的新 read/write/shutdown 失败；
7. 同方向第二个 pending operation 被拒绝或明确暴露为契约错误；
8. read 和 write 可以同时 pending；
9. buffer 在 Complete 前被修改或释放时不属于合法用法；
10. listener 的 pending accept 可被 Close 收敛；
11. Reactor 和 luring 对同一测试场景的核心结果投影一致；
12. timeout 在 AsyncTimedStream、对应 profile 与后端实现同时存在时，验证 read/timeout
    竞争只产生一个逻辑结果和一次恢复；
13. EventSource 的 high-water pause 只终止当前 physical request，不把 logical source
    误报为 terminal；
14. SplitRelease 的业务结果、恢复和 buffer/resource release 按各自授权边界发生。
```

测试应覆盖成功、EOF、ECANCELED、EBADF、EPIPE、资源关闭竞争和 loop 归属，而不是只
验证“最终收到了一段数据”。

## 13. 当前明确不属于 CoreStream

以下能力不能通过修改 `ReadSome` 或 `WriteSome` 的隐含行为加入核心层：

```text
provided buffer
multishot recv / accept
send zero-copy
暴露给业务的 linked operation
跨 ring msg_ring 通信
runtime hot-swap
per-ring upstream keep-alive pool
```

其中 registered buffer、fixed file、SQPOLL 等如果只改变提交方式、而不改变业务返回值
和生命周期，可以作为 B 类透明优化；一旦改变所有权或完成边界，就必须升级为 C 类扩展。

## 14. 契约结论

这套抽象的最小可替换单元不是 epoll API，也不是 io_uring API，而是：

```text
一个 single-result operation
  -> 一个语义提交
  -> 至多一个完成
  -> 至多一次正确协程恢复
```

并且：

```text
operation identity 把 completion、continuation 与 resource owner 绑定；
buffer 活到后端 release authorization，而 borrowed buffer 至少活到 Complete；
Close 支配后续提交，但不抹掉已经完成的结果；
EOF、取消、closed error 和传输错误保持可区分；
read/write/accept 的槽位归属唯一；
后端线程归属和 capability profile 在启动期固定；
后端内部事件不进入业务接口。
```

因此，Reactor 和 luring 可以在同一语义地板上分别实现：

```text
业务只依赖 AsyncStream / AsyncListener；
Reactor 和 luring 是不同解释器；
公共层约束可观察语义；
扩展层保留 io_uring 的能力；
不支持的能力在 bind 阶段拒绝，而不是运行时静默妥协。
```

六元组状态机及其 refinement 讨论见：[Lamport 视角下的运行时语义](lamport-hot-swap-runtime.md)。
`async_stream_core.tla` 验证最小 single-result 模型；`async_stream_multiop.tla` 验证
read/write 并行归属；`accept_source_refinement.tla` 与 `recv_source_lease.tla` 验证
EventSource 的 pause/re-arm 与 lease 生命周期。它们是同一抽象模型的不同有界检查，
不是对无限执行空间的全自动定理证明。

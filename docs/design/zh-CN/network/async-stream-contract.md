# AsyncStream 与 AsyncListener 协程语义契约

> 状态：已冻结的 Core contract。本文定义业务协程可以依赖的 I/O 语义，以及 Epoll 和
> luring 实现必须共同满足的不变量。本文不描述 io_uring 的具体 SQE/CQE API，也不
> 把尚未落地的扩展能力写成核心能力。

Core contract 的冻结范围是 `AsyncStream`、`AsyncListener`、`AsyncConnector` 及其
可观察的结果、生命周期、buffer 和线程归属语义。后端可以继续更换内部队列、提交
批处理、completion dispatch 和内存池实现，但不得改变这些语义。`AsyncRecvStream`、`AsyncRecvCopyStream`、`AsyncRecvSource`、`BufferLease` 和 zero-copy 属于独立的
extension，不得通过扩大 Core contract 的隐含行为加入。

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
alyrn::epoll   Linux epoll / nonblocking syscall
alyrn::uring    io_uring / SQE / CQE
```

它们是两个独立的机制模块，不是同一个网络库的两个公开模式。两者通过公共协程 I/O
语义连接：

```text
业务层
  -> alyrn::io::AsyncStream / AsyncListener
  -> Stream / Stream
  -> Epoll 或 io_uring
```

公共 facade 位于 `alyrn::io`；其 canonical concept 定义位于不依赖具体后端的
`alyrn::backend`。`alyrn::net` 提供共享的地址、socket 和网络工具，
`alyrn::epoll` 承载 epoll/Loop 实现。

本文的核心边界是：

```text
核心层定义可观察语义；
后端层解释这些语义；
实现优化不能改变核心结果、生命周期和完成基数；
改变这些性质的能力必须另起 concept 和契约。
```

## 2. API 边界

### 2.0 ManagedLoop lifecycle extension

loop 的停止控制与 `AsyncStream::Close()` 是不同的语义层。公共 contract 使用：

```cpp
alyrn::io::LoopState
alyrn::io::ManagedLoop

Run(stop_token)
RequestStop()
State()
```

`RequestStop()` 可跨线程调用、幂等，并唤醒正在等待后端事件的 dispatcher；`Run()` 只能在
owner thread 调用。进入 `Stopping` 后，后端必须取消/结算已登记的 physical operation 并 drain
其 continuation；因此 `LoopState::kStopped` 表示 loop 自己不再持有 pending backend operation，
但**不**单独证明 listener、stream、borrowed buffer、BufferLease 或 coroutine frame 已经释放。

完整 runtime shutdown 必须在 `RequestStop()` 后显式执行资源 close/cancel 与 completion drain：

```text
RequestStop
  -> dispatcher enters Stopping
  -> resource Close / operation cancel
  -> backend completion drain
  -> dispatcher enters Stopped
  -> resource release authorization
  -> destroy loop-owned resources
```

这条区分避免把“退出 `epoll_wait` / ring wait”错误地当成所有 operation 都已经结束。

### 2.1 CoreStream

当前公共概念约束的最小接口是：

```cpp
Read(std::span<std::byte> buffer)
    -> 可 await，await_resume() 为 Result<std::size_t>

Write(std::span<const std::byte> buffer)
    -> 可 await，await_resume() 为 Result<void>

Shutdown()
    -> alyrn::Task<Result<void>>

CloseRead()
    -> alyrn::Task<Result<void>>

CloseWrite()
    -> alyrn::Task<Result<void>>

Close()
    -> alyrn::Task<Result<void>>

LocalAddr()
    -> Result<net::Endpoint>

RemoteAddr()
    -> const net::Endpoint&
```

对应的公共概念是：

```cpp
alyrn::io::AsyncReadStream
alyrn::io::AsyncWriteStream
alyrn::io::AsyncClosableStream
alyrn::io::AsyncStream
```

`AsyncStream` 是上述八个方法的语义组合，不是某个具体类的基类，也不要求虚函数。
`CloseWrite()` 是 `Shutdown()` 的明确别名；地址查询属于 stream 的连接语义：UDP 不满足
这个字节流 contract，Unix datagram 也不属于此 contract。

`LocalAddr()` 必须在 owner loop 上执行，并通过 `Result` 报告 `getsockname` 失败；
`RemoteAddr()` 返回连接建立时保存的 peer endpoint，不重复发起 `getpeername`。
当前 `epoll::Stream` 和 `uring::Stream` 都满足它。

### 2.2 Timeout 不是 Stream 方法

per-call `ReadFor(buffer, timeout)` 已从公开契约撤回。超时不是 `AsyncStream` 的隐含开关，
也不再以一次性覆盖 API 出现；连接级 sticky 每操超时是后续工作，不在本契约范围内。

loop 级 `SleepFor` / `RunAfter` 仍然存在，它们挂的是 loop 的 timer tree，不是 stream I/O。

### 2.3 Buffer ownership extension

`Read(std::span<std::byte>)` 是核心的 borrowed-buffer 路径：调用者拥有 storage，
并且必须在 `await_resume()` 前保持对象存活、地址稳定，不能释放、扩容、移动或并发访问
同一段内存。这个约束包括 close 和 cancel 路径。

对 borrowed-buffer read，用户可观察完成不能早于后端放弃该地址：

```text
backend no longer accesses storage
  -> logical result becomes observable
  -> await_resume()
```

`AsyncRecvStream` 是独立的可选 extension，不属于 `AsyncStream` 的最小接口：

```cpp
Recv(net::Buffer buffer, std::size_t reserve)
    -> 可 await，await_resume() 为 net::RecvOutcome
```

它按值接收 move-only `net::Buffer`，awaiter 在整个 pending interval 内持有该 owner，并在
所有终态路径返回：

```cpp
struct net::RecvOutcome {
  Result<std::size_t> result;
  net::Buffer buffer;
};
```

因此提交失败、关闭和取消也不会吞掉调用者转交的 buffer。`BufferLease` 则属于
`AsyncRecvSource`：它代表后端/池提供的存储及其归还协议，不能替代普通 `net::Buffer`。

`AsyncRecvCopyStream` 是另一条独立的可选 extension，也不属于 `AsyncStream` 的最小接口：

```cpp
Recv()
    -> 可 await，await_resume() 为 Result<net::Buffer>
```

调用者不提供存储。成功（含 0 字节 EOF）返回一块新的 `net::Buffer`；失败返回
`unexpected`，没有 caller-owned buffer 需要归还。io_uring 使用 loop 的 provided-buffer
ring 作为内核写入目标，再 copy-out 并在 resume 前归还 slot；epoll 接收进内部
`Buffer`。它不暴露 `BufferLease`，也不把 `Recv()` 并入 `AsyncRecvStream`：后者要求调用者
移交一块 `Buffer`。

`Buffer::PrepareWrite()` 与 `CommitWrite()`/`AbortWrite()` 构成一个短暂的 reservation
transaction。该区间内不得再次 `PrepareWrite()`、`Append()`、`Drain()` 或 move buffer；这些不是
可恢复的网络错误，而是会破坏 storage ownership 的程序错误，因此在所有构建中被拒绝。后端 awaiter
必须在任何终态路径结束 reservation，随后才把 buffer 交还给调用者。

### 2.3 CoreListener

listener 的最小接口是：

```cpp
using StreamType = ...;

Accept()
    -> alyrn::Task<Result<StreamType>>

Close()
    -> alyrn::Task<Result<void>>
```

`StreamType` 必须满足 `alyrn::io::AsyncStream`。当前 `epoll::Listener` 和 `uring::Listener`
都满足 `alyrn::io::AsyncListener`。

### Close preparation 与 committed Close

`Close()` 在 owner loop 上先进入一个不可重入的 **Close preparation**，临时拒绝新的
read/write/accept。若后端在任何 cancel SQE 成为 submission protocol 的一部分之前就发生
本地提交失败，preparation 必须完全撤销：当前 `Close()` 返回该错误，stream/listener 仍为
Open，已有 logical source 不得被误报 terminal，调用者可以稍后显式重试 `Close()`。

当 cancel SQE 已被 owner loop 接受（或确认没有 backend-visible operation 需要取消）时，
Close 才成为 **committed Close**。从这一刻起它是 resource-level drain barrier：新的操作失败，
cancel acknowledgement、target terminal、buffer/lease release 全部收敛后才允许释放 fd 并恢复
Close continuation。提交 SQE 不等于内核已经产生 CQE，但已经足以使其关联的 operation state
不能安全回滚。

`PrepareShutdown()`、`PrepareCloseRead()` 与 `PrepareClose()` 的失败是可观察的 `Result`；但 adapter
在成功准备后调用 `CommitShutdown()` 或 `CommitCloseRead()`，在 shutdown syscall 的本地失败后调用
对应的 `Abort*Preparation()`，或在尚未提交物理取消前调用 `AbortClosePreparation()`，属于内部状态机
的后续 transition。若这些调用不满足前置状态，说明后端违反生命周期协议，必须在所有构建中终止，
而不能把状态悄悄改写成新的业务错误。

`Connect()` 不属于 `AsyncStream` 或 `AsyncListener`。它是建立 outbound stream 的另一项
能力，由 `AsyncConnector` 单独约束：

```cpp
auto result = co_await connector.Connect(host, port);
```

公共 connector 语义如下：

- connector 绑定 owner loop；创建和 `Connect()` 都必须发生在 owner loop 线程；
- 每次 `Connect()` 拥有独立的 socket、physical request、result 和 continuation，因此同一
  connector 可以同时存在多个 pending connect；
- 成功返回的 stream 绑定同一个 backend/owner loop；
- 当前 `host` 路径解析数值 IP，非法地址返回 `EINVAL`，DNS 解析不是 CoreConnector 的隐式
  行为；
- `Connect(host, port)` 在返回惰性 Task 前同步解析并快照地址；返回后修改或销毁原始
  `string_view` 的底层字符不会影响该 operation；
- transport errno（例如 `ECONNREFUSED`）原样保留；
- loop 进入 `Stopping` 后不再创建 socket 或提交 request，新 `Connect()` 返回
  `ECANCELED`；已经 pending 的 connect 必须经后端取消路径收敛且只恢复一次。

connector 本身没有 `Close()`：它不长期拥有连接资源。Epoll 的每次调用在 awaiter 内持有
临时 `Channel`；io_uring 的每次调用持有独立 operation。成功后 socket 所有权转移给返回的
Stream，失败或取消时由该次 operation 回收。

### 2.4 Awaitable 的使用规则

`Read` 和 `Write` 的返回值必须是可 await 对象，并产生约定的
`Result`。后端可以返回惰性的 `alyrn::Task<T>`，也可以返回直接承载操作状态的
底层 awaiter：

```cpp
auto result = co_await stream.Read(buffer);
```

`Stream` 的 `Read` 和 `Write` 都返回直接 awaiter；luring 的 `Read` 返回
直接 awaiter，而 `Write` 以一个后端内建 `Task` 驱动多个内部 send request，以保留普通
send 与 send-zc 的不同 completion/release 语义。`Shutdown()`、`Close()` 也可以返回 `Task`。

如果具体接口返回 `Task`，它仍然是 move-only、single-consumer 对象，只能被 await 一次，
也可以交给 `alyrn::Spawn(scheduler, task)`。直接 awaitable 则在 `co_await` 时提交 I/O。

无论返回哪种 awaitable，丢弃未等待的操作都属于错误：

```cpp
stream.Read(buffer);  // 错误：没有等待该 I/O operation
```

I/O 方法本身不抛出业务异常。结果通过 `alyrn::Result<T>` 返回，它是
`std::expected<T, std::error_code>` 的别名。协程未处理异常会终止进程，不属于网络错误
传播机制。

## 3. 逻辑状态与可观察事件

后端是固定的解释器，不是运行时可变状态。具体方法和 extension 由 `io::*` concept
在编译期约束；调度与批处理策略在实例生命周期内固定，或只按显式策略转换。

`H(trace)` 不是状态变量；它是从一次执行 trace 推导出的 happens-before 偏序。证明时需要
验证 `Submit(op) -> Complete(op) -> Resume(owner(op))` 等边存在，而不需要在每个状态复制一份
偏序图。逻辑操作的三条一次授权边界见
[生命周期精化协程 I/O](lifecycle-refined-coroutine-io.md)。

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
      result_cardinality,  // Single | Multiple
      convergence,         // Single | Composite
      release_coupling,    // Coupled | Split
      owner_coroutine,
      resource,
      scheduler_affinity,
      phase,
      physical_requests,
      logical_result_or_events
    }

Q : 后端 refinement 的队列投影。
    抽象层只关心“已投递但未 Resume”的逻辑 continuation；Epoll 的 ready list、
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

方括号表示 `Suspend` 是可选的。Epoll 可能在 nonblocking syscall 立即得到结果，
io_uring 也可能在准备阶段立即拒绝操作。此时没有真实挂起，但仍然必须有唯一的逻辑
提交和完成结果。

`Submit` 表示一次语义上的提交尝试，不要求一定产生系统调用、SQE 或 readiness 注册。
例如资源已经关闭时，后端可以在提交点直接产生 `EBADF` 完成结果。

在立即完成路径中，结果通过 `await_suspend() == false` 直接进入当前协程的
`await_resume()`；它不是一次 scheduler continuation resume，也不需要
`Schedule(ResumeWork)`。只有发生实际挂起时，结果、释放授权与恢复授权才需要经过后端的
ready queue 或等价调度路径。形式模型将两者分别记为 `InlineContinue` 与 `Resume`。

### 3.3 后端解释、调度策略与不变量

后端对物理事件的解释必须保持以下语义机制：

```text
σ_submit   : 一次提交尝试创建唯一 operation 归属。
σ_complete : 一个 single-result operation 最多 Complete 一次。
σ_resume   : 协程只能由自己的 operation 结果或 source 的 Next() 结果恢复。
σ_cancel   : 取消必须产生该 operation lifecycle 定义的收敛状态，不能静默丢弃。
σ_close    : 资源关闭后不能产生新的成功 I/O。
σ_lifetime : backend 仍可能访问的对象必须保持存活。
σ_contract : 对外暴露的每个 concept 都必须有真实且可测试的语义解释。
```

调度与批处理策略负责选择实现细节，例如：

```text
ready 顺序     : ready work 的处理顺序
批处理大小     : SQE/CQE 的批量大小
等待策略       : epoll 或 io_uring 的等待策略
恢复投递       : completion 到 coroutine resume 的投递策略
```

后端可以有不同的物理解释，但必须在上述机制和不变量约束下解释同一组
核心事件：

```text
创建 Task
  -- co_await / Spawn --> Submit
  -- 需要等待 -------> Suspend
  -- 后端内部处理 ----> Complete(result)
  -- Schedule --------> Resume(result)
```

调度策略可以改变批量、调度顺序和延迟，但不能把一次完成变成两次恢复，也不能把一次
single-result 操作变成多次业务结果。

最终性单独描述，而不是混入安全不变量：在公平的后端投递和 owner-loop 调度假设下，
pending single-result operation 最终 settled、已 settled 的等待协程最终 Ready、`Closing`
资源最终 `Closed`。`async_stream_core.tla` 已对这个最小模型检查这些性质；多 operation 与
source refinement 继续分别检查自己的终态条件。

### 3.4 按 operation lifecycle 解释取消

取消不是所有 lifecycle shape 共用的“立刻得到 ECANCELED”：

```text
Single result       : cancel 的物理收敛产生一次 terminal logical result。
Composite convergence: 子 request 可先后完成；聚合结果只确定一次。
Multiple results    : Stop/error 才使 logical source terminal。高水位 pause 取消当前
                      multishot physical request 后可以进入 Paused，并在低水位 re-arm，
                      不产生 source terminal。
Split release       : result readiness、continuation resume 与 buffer/resource release
                      可以由不同 physical completion 授权。
```

因此，`Cancel(op) -> Complete(op, ECANCELED)` 只适用于 single-result 的简化模型；它不能
作为 `AcceptSource`、`RecvSource` 或 send zero-copy 的通用规则。

`Close()` 与 event source 的 `Stop()` 也故意不共享“本地 cancel preparation 失败”的语义。
resource `Close()` 在尚未向 backend 提交 cancel request 前必须能回滚到 Open；它保护的是原 fd
和后续新 I/O 的可用性。source `Stop()` 首先撤销新的 event admission，因此 luring 的 cancel SQE
preparation 若本地失败，`Stop()` 可以返回该 error，但 source 保持 `Stopping`，不会重新变成
Active。调用方必须保留 source 并重试 `Stop()`，或由已 committed 的 owner resource `Close()` 收敛它；
在 Stop 成功前销毁 source 仍违反其 physical request 生命周期。Epoll 没有对应 SQE preparation
阶段，但实现同一“Stop 后不再接纳新事件”的可观察语义。

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
Read / Write 传入的 buffer；
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
默认只有一个 pending accept。当前 CoreStream Adapter 对第二个同方向 operation 稳定返回
`EBUSY`；空 buffer operation 也必须先经过槽位检查，不能绕过该规则。

### I7：线程和执行器归属

当前 Stream、Stream、Listener 和 Listener 都是 loop-bound：

```text
对象创建、operation 提交、Close、析构和后端状态修改
必须发生在对象所属的 loop 线程。
```

`Loop::RunOnOwner` 和 `Schedule` 都要求调用者位于所属 loop
线程；它们不提供跨线程投递能力。当前 `Loop::Schedule` 也要求调用者位于 loop
线程。跨 loop 投递需要单独的 mailbox/message 机制；`eventfd` 和 `msg_ring` 都不属于
当前 CoreStream 契约。

### I8：语义 contract 固定

业务通过 `io::*` concept 选择所需的语义 interface；具体 backend 不得在运行实例中改变
这些方法的含义。concept 只负责编译期 interface 约束，不能凭空保证内核资源。内核或 ring
不支持某条具体路径时，由对应 operation 的 `Result` 返回错误。

## 5. AsyncStream 语义

### 5.1 资源状态

stream 的核心资源状态为：

```text
Open -> Closing -> Closed
```

读写方向是与 fd 生命周期独立的两个状态：

```text
Readable -- CloseRead --> ReadShutdown
Writable -- Shutdown --> WriteShutdown
```

`Shutdown()` 只改变写方向，不改变 stream 的资源状态；`Close()` 才负责本地资源终止和
pending operation 收敛。因此 `Open + WriteShutdown` 仍然允许 `Read()`，但不再接受新的
`Write()`。空 span 不产生物理 write request，但仍是一次逻辑 operation，必须经过 loop、
资源状态和写槽位验证。

`CloseRead()` 只改变读方向，不关闭 fd，也不影响 `Write()`。它要求没有 pending read；否则
返回 `EBUSY`。成功后新的 `Read()` 和 `Recv(Buffer)` 立即得到项目约定的 `Result<0>` EOF，
无参 `Recv()` 立即得到一块空的 `Buffer`；都不再提交新的 receive operation。读侧关闭和写侧
关闭可以独立发生，二者都不等价于 `Close()`。

一个 stream 的生命周期由其拥有者管理。调用方必须保证 stream 对象至少存活到所有依赖
它的 operation 已经 Complete；通常应在 session 协程内统一负责 Close 和销毁。

### 5.2 Read

```cpp
auto result = co_await stream.Read(buffer);
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

`Read` 不保证填满 buffer。`Result<0>` 的 EOF 语义只适用于非空 buffer；空 buffer
不是通用业务算法应依赖的输入。当前实现可能直接返回 0，但这不应被用来判断对端关闭。

### 5.3 Write

```cpp
auto result = co_await stream.Write(buffer);
```

`Write` 是 Core contract 的完整写入操作：它重复提交后端的物理 write request，直到
输入 span 被完全接受，或任一 request 到达终态错误。它产生的业务结果只有：

```text
Result<void>
  整个 span 已写入。

unexpected(EPIPE)
  某次成功完成却返回 0，无法继续推进，避免无限循环。

unexpected(error)
  某个物理 write request 失败。
```

调用方必须把 span 的 storage 保持有效、地址稳定，直到 `Write` 的 `await_resume()`。
后端可以把短写循环做成直接 awaiter 或内部 coroutine；短写 request 及其 bytes-progress
只属于 backend 私有状态，不再作为公共 API 暴露。send zero-copy 启用时，luring 的
`Write` 在每轮 send-zc 的 kernel resource-release 边界之后，才继续推进或降级为普通
send：primary 无 `F_MORE` 时该边界就是 primary CQE，带 `F_MORE` 时则是 notification CQE。

空 span 立即产生 `Result<void>`，但不会绕过逻辑验证：已有 pending write 时返回 `EBUSY`，
loop 停止时返回 `ECANCELED`，stream 已关闭时返回 `EBADF`，写方向已 Shutdown 时返回
`EPIPE`。这保证空操作不能绕过 I6 的槽位和资源状态规则。

多块 `Buffer` 的写出由业务显式迭代 `ContiguousView()`，并在每个 `Write` 成功后调用
`Drain()`；Core contract 不提供 `Buffer&` 或 scatter-write convenience overload。

### 5.4 Shutdown

`Shutdown()` 表示本端不再继续发送数据，当前 TCP 实现映射到写方向 half-close：

```text
Shutdown()
  -> WriteState: Writable -> WriteShutdown
  -> 后续本端 Write() 在提交到后端前返回 EPIPE
  -> 读方向仍可以继续观察数据或 EOF
```

`Shutdown()` 是幂等的：已经处于 `WriteShutdown` 时成功返回。它不会替代 `Close()`，也不
负责取消 pending read/write；若已经存在 pending write，当前 core contract 返回 `EBUSY`，而
不是让 half-close 与写入竞争。资源 `Closing` 时返回 `ECANCELED`，资源已 `Closed` 时返回
`EBADF`。

`CloseWrite()` 是写方向 half-close 的 canonical spelling；`Shutdown()` 保留为兼容旧调用方的
明确别名，二者都只执行 `SHUT_WR`，不表示双向 shutdown。

### 5.5 CloseRead

`CloseRead()` 表示本端不再接收数据，当前 TCP 实现映射到读方向 half-close：

```text
CloseRead()
  -> ReadState: Readable -> ReadShutdown
  -> 后续本端 Read() / Recv(Buffer) 直接返回 Result<0>
  -> 后续本端 Recv() 直接返回空 Buffer
  -> 写方向仍可继续使用
```

`CloseRead()` 是幂等的；如果存在 pending read，返回 `EBUSY`，不会取消或强行结算该 read。
资源 `Closing` 时返回 `ECANCELED`，资源已 `Closed` 时返回 `EBADF`。它不会向对端表达应用层
“读取完成”，也不替代 `Close()`。

### 5.6 Close

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
5. Closed 后的 Read / Write / Shutdown / CloseRead / CloseWrite 返回 closed error；
6. Closed 后重复 Close 可以成功返回。
```

`Close()` 完成不等价于其他等待中的协程已经恢复。它只保证相关 operation 已经离开后端
访问窗口；每个 pending operation 的调用方仍然必须等待自己的 Task 观察结果。

### 5.6.1 析构 / drop

owner loop 上销毁 Stream 是合法的会话结束，不要求先 `co_await Close()`。

```text
1. 空闲析构（没有 pending read/write/close）关闭 fd；对端观察到连接结束；
2. 错线程析构仍是编程错误，所有构建中都会终止；
3. Epoll 的 pending drop 等价于一次 CloseNow：已挂起的 read/write 各完成一次
   ECANCELED，continuation 在之后的 loop turn 恢复，而不是在析构里同步 resume；
4. io_uring 在仍有 in-flight SQE 时析构仍是契约违反。Op 嵌在 awaiter 帧上，
   析构无法等待 CQE。取消 pending 仍走 `co_await Close()`。
```

`Close()` 留下来观察关闭结果，或在 uring 上取消仍在飞的 I/O。它不是空闲会话结束的
正确性前置条件。

Epoll 可以通过 readiness 状态直接完成取消，luring 可能需要提交 cancel request、
等待 cancel CQE 和原 operation CQE。它们的内部路径不同，但不能改变上述状态转移。

并发调用 `Close()` 与同一个 stream 的其他操作不属于跨线程语义；同一 loop 内也不应
同时发起多个 Close。实现可以返回 `EBUSY` 或在调试构建中暴露调用错误。

### 5.7 关闭和完成的竞争

以下结果都可能是合法的，取决于事件在线程归属序列中的线性化顺序：

```text
read 先完成 -> Read 得到 N、0 或传输错误；随后 Close 完成
Close 先取消 -> Read 得到 ECANCELED；随后 Close 完成
底层连接先断开 -> Read 得到 0，Write 得到 EPIPE 或具体错误
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

可移植的核心 listener 用法只依赖一个 pending accept。Epoll 对第二个 pending
`Accept()` 在所有构建模式下稳定返回 `EBUSY`；io_uring 后端可以在内部或显式扩展路径中
维持多个 one-shot accept，但业务代码不能把该能力当作公共 `AsyncListener` 契约。需要持续
产生连接结果时，应使用 `AcceptSource`，而不是并发创建一组普通 `Accept()` awaiter。

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
co_await stream.Read(buffer)
或
co_await stream.Write(buffer)
```

对应 I/O operation 完成的整个过程：

```text
创建 awaitable -> co_await -> Submit -> 可能 Suspend -> Complete -> await_resume
```

错误示例：

```cpp
alyrn::Task<void> Bad(alyrn::io::AsyncStream auto& stream) {
  std::vector<std::byte> local(4096);
  auto task = stream.Read(local);
  local = {};                    // 错误：底层 operation 仍可能使用这块内存
  auto result = co_await std::move(task);
  (void)result;
}
```

正确写法是让 buffer 由协程 frame、调用方对象或更长生命周期的 pool 持有：

```cpp
alyrn::Task<void> Good(alyrn::io::AsyncStream auto& stream) {
  std::array<std::byte, 4096> buffer{};
  auto result = co_await stream.Read(buffer);
  (void)result;
}
```

### 7.2 io::Buffer

`alyrn::io::Buffer` 是 `net::Buffer` 的零成本公开 spelling。它不是
`Read` 的第二种 borrowed overload：可增长 buffer 的异步读取必须使用
`Recv(std::move(buffer))`，以便 pending operation 独占 storage 并在每条终态路径归还
owner。实现留在 `net` 以保持后端位于 `io` facade 之下，调用者应使用 `io` spelling：

```text
Recv 在读成功后 CommitWrite，在读失败后 AbortWrite；写成功后由调用者在
Write 后 Drain 已写出的字节。
```

扩展 `RecvSource` 已明确提供 buffer 的所有权边界：luring 使用每 worker 共享的 provided
buffer ring，Epoll 使用固定 buffer pool；每个 `RecvEvent` 携带一个 `BufferLease`，consumer
必须在 source 销毁前释放它。buffer id、归还时机和 RAII 所有权不能隐藏在普通 `std::span`
的成功结果里。`Options::shared_buffer_capacity` 配置该 worker 的聚合上限，buffer slot
会随着 RecvSource 创建惰性发布；CQE 返回的
buffer id 在这个共享 pool 内解释。`F_BUF_MORE` 增量 source 尚未接入当前 `RecvSource` 路径，
registered fixed buffer 仍属于后续扩展。

两个后端的 `RecvSource::Next()` 都是直接 awaiter：已有事件或 terminal result 时，它在调用
协程内 inline 完成；否则 source 保存该 continuation，待逻辑事件 ready 后按所属 scheduler 恢复。
这条 API 不创建每事件一个中间 `Task` frame；事件队列、backpressure 和 `BufferLease` 的生命周期
仍完全由 source state machine 管理。

### 7.3 fd、stream 和 operation owner

后端保存的 fd、sockaddr、iovec、awaiter 和 coroutine handle 都必须由 operation owner
保持有效。`Close()` 只能结束后端访问；它不能让调用方提前释放仍被 pending Task 引用的
协程 frame 或 buffer。

## 8. Timeout 语义

Stream 不提供 per-call `ReadFor`。把超时做成单次操作覆盖会引入第三套状态机
（无超时 / 本次覆盖 / 连接级），调用方必须参加。Stream 的 deadline 是连接级 sticky
绝对单调时刻：`SetReadDeadline(t)` 与 `SetWriteDeadline(t)` 同时作用于当前 pending
operation 和之后的同向 operation，直到传入 `std::nullopt` 清除。

deadline 已过期时，后续 operation 不提交后端 I/O，确定返回 `ETIMEDOUT`。当 owner loop
先观察到在途 operation 的 deadline 到期，该 operation 同样确定返回 `ETIMEDOUT`；与其竞争
的 I/O completion 不改变该结果。deadline 是 `AsyncDeadlineStream` 的可选 capability，
不属于 `AsyncStream` 核心 contract。

loop 级延时仍然存在：

```text
SleepFor / RunAfter
  -> owner loop 的 timer tree
  -> 到期后在同一 loop 上恢复一次
```

`AsyncStream` 不因此变成 timed contract。

## 9. Capability 分层

Capability 描述的是语义 profile 或实现标签，不是 API 的替代品。

### A 类：核心语义能力

A 类进入 active profile，后端必须提供可观察且可测试的统一语义：

```text
kRead
kWrite
kShutdown
kClose
kCancelByClose
kAccept       // CoreNetwork
kConnect      // CoreNetwork / Connector
```

### B 类：实现标签

B 类描述后端怎样实现核心语义，业务不声明，允许透明 fallback：

```text
kReadinessPoll
kSubmitRead
kSubmitWrite
kSqPoll
kIoPoll
```

业务请求的是 `CoreStream` 或 `CoreNetwork`，不是“我要 readiness”或“我要 SQE”。

### C 类：不透明扩展

C 类会改变返回类型、完成基数、生命周期、所有权或组合语义，必须使用新的 concept、
方法或 profile gate：

```text
kMultishotRecv
kSendZeroCopy
```

例如：

```text
普通 Read：一次 Submit -> 一次 Complete -> 一次 Resume
multishot recv：一次 Submit -> 多次 Complete -> cancel/close 终止
```

因此 multishot 不能塞进 `Read`，provided buffer 不能伪装成普通 span，send zero-copy
不能复用普通 `Write` 的 buffer 完成边界。

当前 luring 还提供显式扩展：

```cpp
auto result = co_await stream.SendZeroCopy(buffer);
```

该 awaiter 将 send CQE 与可选的 `F_NOTIF` notification 作为同一个 split-release operation
处理；只有 terminal CQE 到达后才恢复调用方，因此 `buffer` 在 `co_await` 返回前必须保持有效。
Epoll 没有对应的两阶段内核通知，不为普通 send 伪造该扩展语义。

## 10. 两个后端如何解释同一契约

### Epoll

Epoll 的典型内部路径是：

```text
TryRead/TryWrite
  -> 立即成功或得到 EAGAIN
  -> EAGAIN 时注册 Channel readiness
  -> readiness callback 再次尝试 syscall
  -> Complete
  -> Scheduler::Schedule
  -> Resume
```

TimerQueue、Channel 和 owner-local ready/work queue 都是 Epoll 内部机制。它们不能泄漏
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
Read      -> Result<N> / Result<0> / error
Write      -> Result<void> / error
Close         -> eventual resource closure
Accept        -> one stream or error
Resume        -> the coroutine waiting for that operation
```

当前 luring 没有跨 ring 的公共消息层。后端在启动期绑定后固定，
不是运行中任意迁移 pending operation。

## 11. 错误和传输结束

核心层不把所有异常情况压成一个 `closed` 状态：

```text
Read -> Result<0>
  对端有序关闭，EOF。

Close 取消 pending operation -> ECANCELED
  本地关闭导致的取消结果；仍然要通过原 Task 观察。

Close 之后新提交 -> EBADF 或等价 closed error
  不是对端 EOF。

CloseRead 之后新读 -> Result<0>
  本地读方向已关闭，但 stream 资源和写方向仍然存在。

Write -> EPIPE 或具体传输错误
  写方向无法继续。
```

底层系统调用返回的其他 errno 应保留，不应在后端层无理由改写。应用可以据此区分
EOF、本地取消、连接失败、上游失败和超时。

`Write` 不是单次物理 write；它是后端实现的完整写入 Core operation。它会重复提交短写，
并把成功但零进展的结果转换成 `EPIPE`，防止无限循环。

## 12. 实现和测试义务

任何新的 `AsyncStream` 解释器至少需要验证：

```text
1. 立即读成功和 pending 读成功都只恢复一次；
2. 短写由 Write 正确推进；
3. 对端关闭产生 Read -> Result<0>；
4. pending read 在 Close 后最终完成，且不会悬挂；
5. pending write 在 Close 后最终完成，且不会悬挂；
6. CloseRead 后的新 read 立即得到 EOF，且 write 仍然可用；
7. Close 后的新 read/write/shutdown/CloseRead/CloseWrite 失败；
8. pending read 上调用 CloseRead 返回 `EBUSY`，读完成后可重试成功；
9. 同方向第二个 pending operation 稳定返回 `EBUSY`，包括空 buffer operation；
10. read 和 write 可以同时 pending；
11. buffer 在 Complete 前被修改或释放时不属于合法用法；
12. listener 的 pending accept 可被 Close 收敛；
13. Epoll 和 luring 对同一测试场景的核心结果投影一致；
14. loop 级 SleepFor / RunAfter 到期后恢复一次；Stream 不提供 per-call ReadFor；
15. EventSource 的 high-water pause 只终止当前 physical request，不把 logical source
    误报为 terminal；
16. SplitRelease 的业务结果、恢复和 buffer/resource release 按各自授权边界发生。
17. listener/source 的 `Stop()` 与 `Close()` 保持幂等，terminal `Next()` 保持 sticky；source Stop 的
    local cancel preparation failure 后仍可重试并最终收敛；
18. loop 进入 `Stopping` 后，新的 `Accept()` 与 `CreateAcceptSource()` 返回 `ECANCELED`。
19. connector 保留成功、`EINVAL`、`ECONNREFUSED` 与 `ECANCELED` 的区别；
20. 同一 connector 的并发 `Connect()` 具有独立结果、恢复授权和资源回收。
21. `Connect()` 的 continuation 只在 `Result<Stream>` 已固定、fd 已转移给 stream 或关闭后才运行；
    恢复后可以立刻对该 stream 调用 `Close()`。
22. `Accept()` 的 continuation 只在 `Result<Stream>` 已固定、listener 的 pending-accept
    reservation 已释放后才运行；恢复后可以立刻关闭该 stream 或发起下一次 `Accept()`。
23. 空闲析构关闭 fd，无需先 Close；Epoll 上 pending drop 使挂起的 I/O 以 ECANCELED
    完成一次。io_uring 的 pending drop 仍是契约违反。
```

测试应覆盖成功、EOF、ECANCELED、EBADF、EPIPE、资源关闭竞争和 loop 归属，而不是只
验证“最终收到了一段数据”。

## 13. 当前明确不属于 CoreStream

以下能力不能通过修改 `Read` 或 `Write` 的隐含行为加入核心层：

```text
provided buffer
multishot recv / accept
send zero-copy
暴露给业务的 linked operation
跨 ring msg_ring 通信
运行中切换后端
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
后端线程归属在启动期固定；扩展通过独立 contract 暴露；
后端内部事件不进入业务接口。
```

因此，Epoll 和 luring 可以在同一语义地板上分别实现：

```text
业务只依赖 AsyncStream / AsyncListener；
Epoll 和 luring 是不同解释器；
公共层约束可观察语义；
扩展层保留 io_uring 的能力；
不支持的能力在 bind 阶段拒绝，而不是运行时静默妥协。
```

逻辑生命周期与后端 refinement 见 [LRCI](lifecycle-refined-coroutine-io.md)
和 [TLA+ 模型](formal/index.md)。
`async_stream_core.tla` 验证最小 single-result 模型；`async_stream_multiop.tla` 验证
read/write 并行归属；`accept_source_refinement.tla` 与 `recv_source_lease.tla` 验证
EventSource 的 pause/re-arm 与 lease 生命周期。它们是同一抽象模型的不同有界检查，
不是对无限执行空间的全自动定理证明。

`resource_close_cancel.tla` 则单独验证 Close 作为 resource-level drain barrier：cancel CQE
不等于 target completion，fd release 必须等待 active physical use、cancel command 与
backend-held storage 一并收敛。

`stream_shutdown_transaction.tla` 补充验证同步写半关闭的内部 transaction：准备期间不能接受新的
write 或开始 Close；`shutdown(2)` 成功后 write direction 终态为 Shutdown，本地失败则完整回滚为
Writable，后续调用可以显式重试。

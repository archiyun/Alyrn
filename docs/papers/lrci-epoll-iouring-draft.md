# 生命周期精化协程 I/O：epoll 与 io_uring 的可观察语义统一

**草稿。** 本文不是投稿终稿。主张已按现有设计与 TLA+ 模型对齐；§9 的 TLC 数字已填。负例表和跨库对照仍待补。形式化部分目前是有界模型检查，不是对 C++ 实现或内核公平性的机器证明。

- 状态：内部草稿 / 2026-08-31；TLC 2.19（rev 5a47802），12 workers，无错
- 对应实现：Alyrn（C++23 协程网络运行时）
- 规范来源：`docs/design/zh-CN/network/lifecycle-refined-coroutine-io.md`、`async-stream-contract.md`、`accept-source-contract.md`、`docs/design/zh-CN/network/formal/`

---

**Lifecycle-Refined Coroutine I/O: Unifying epoll Readiness and io_uring Completion**

## Abstract

Linux exposes two dominant asynchronous network contracts: epoll’s readiness model and io_uring’s submission/completion model. Coroutine runtimes usually hide the difference behind a common `await`, either by collapsing both backends to the lowest common denominator or by treating kernel events as coroutine completions. Both choices fail once operations stop being “one request, one result, one buffer release”: multishot accept and receive, provided-buffer leases, zero-copy send notifications, and composite close all separate *result readiness*, *continuation authorization*, and *resource release*.

This paper proposes Lifecycle-Refined Coroutine I/O (LRCI). A backend event is evidence, not a logical completion. Each logical operation is refined through three once-only authorization boundaries. Epoll and io_uring keep distinct physical control flow; observation functions project their traces onto one logical specification. Capabilities that change result cardinality or release coupling are extensions, not implicit Core `Read`/`Write` behavior.

We instantiate LRCI in Alyrn, a C++23 runtime with parallel epoll and io_uring adapters over a shared POSIX socket layer. Bounded TLA+ models check the refinement for single-result reads and the lifecycle shapes of sources and split-release sends. The same application-observable conformance suite runs on both backends. We deliberately do not treat Windows IOCP as a third scientific object: it is also a proactor, but it does not share the POSIX resource domain that makes the Linux pairwise refinement well-defined.

**CCS Concepts:** • Software and its engineering → Concurrent programming structures; Formal software verification; • Networks → Programming interfaces.

**Keywords:** coroutines, epoll, io_uring, refinement, proactor, reactor, cancellation, buffer ownership

## 摘要

Linux 上有两套主导的异步网络合同：epoll 的就绪模型，以及 io_uring 的提交/完成模型。协程运行时通常用同一种 `await` 把差异藏起来，要么把两端压成最低公分母，要么把内核事件直接当成协程完成。一旦操作不再是「一个请求、一个结果、一次释放」，两条路都会失败：multishot accept/recv、provided-buffer lease、zero-copy 的 notification，以及由 cancel 与原 I/O 共同收敛的 close，都会把**结果何时固定**、**continuation 何时可入队**、**资源何时可释放**拆开。

本文提出生命周期精化协程 I/O（LRCI）。后端事件是证据，不是逻辑完成。每个逻辑操作经过三条一次授权的边界。epoll 与 io_uring 保留各自的物理控制流；观察函数把它们的 trace 投影到同一条逻辑规范。会改变完成基数或释放耦合的能力是扩展，不能回流成 Core `Read`/`Write` 的隐含行为。

我们在 Alyrn 中实现了 LRCI：平行的 epoll / io_uring adapter，共享 POSIX socket 层。有界 TLA+ 模型检查单结果 read 的精化，以及 source 与分离式释放的生命周期形状。同一套应用可观察的 conformance 测试在两个后端上运行。我们不把 Windows IOCP 当作第三种科学对象：它同属 Proactor，但不共享使这条 Linux 成对精化得以成立的 POSIX 资源域。

---

## 1 引言

业务协程看见的是：

```cpp
auto n = co_await stream.Read(buf);
co_await stream.Write(data);
co_await stream.Close();
```

它看不见 `epoll_wait`，也看不见 SQE/CQE。因此一个自然的工程问题是：

> 在不改变协程可观察语义的前提下，能否替换网络后端？

现有答案多半是能力列表。epoll 提供 LT/ET/oneshot；io_uring 提供 registered buffer、fixed file、linked SQE、multishot、send zerocopy。每多一个 opcode，列表就加长一截。这不是可检查的规范，只是越来越长的相容表。

更常见的两种实现策略都有明确失败模式。

**最低公分母。** 把 io_uring 当成更快的 `epoll_wait`：用 `IORING_OP_POLL_ADD` 等就绪，再走非阻塞 `recv`/`send`。协程 API 变简单了，multishot、provided buffer 和 zerocopy 的生命周期从统一层消失。统一成立，能力被抹掉。

**事件即完成。** 把 readiness、CQE、cancel acknowledgement 直接 `resume()`。这对「一次 read、一次结果、buffer 与 continuation 同时结束」的操作够用。对下面这些操作不够用：

- close：cancel CQE 到达后，原 read/write 的 CQE 仍可能到来，业务 continuation 只能恢复一次；
- multishot recv：带 `IORING_CQE_F_MORE` 的 CQE 已经可以产生业务事件，物理 request 仍然存活；
- send zerocopy：primary CQE 可以固定发送结果；若带 `F_MORE`，要等 notification 才能复用发送 buffer。

本文的对象不是「实现两个 Linux 后端」，而是这两套内核合同之间的缝：

```text
同一条逻辑 Read / Accept / Close
  epoll:   非阻塞 syscall + 就绪 + 再试     （事件常常是 stutter）
  io_uring: SQE → CQE / 多 CQE / cancel+target （事件看起来像完成，常常不是）
```

**贡献。**

1. **LRCI。** 把逻辑操作从物理请求里分开，并给出三条一次授权边界：Result Ready、Continuation Authorized、Release Authorized。
2. **成对精化。** 为 epoll 与 io_uring 各定义观察函数。物理步骤要么投影为逻辑步骤，要么是不改变逻辑观察的 stutter。Core 操作必须 refine 同一规范；改变基数或释放边界的能力留在扩展里。
3. **非平凡成对例。** `AcceptSource` 用同一套 `Next()` 序列覆盖 epoll accept-drain 与 io_uring native multishot（以及 one-shot 回退）。这不是最简单的 `Read`。
4. **实现与有界检查。** Alyrn 用平行 adapter 而不是共享 poller。TLA+ 检查精化与生命周期形状；conformance 测试在同一主机、同一组 socket 上跑两个后端。

我们不主张任意时刻热替换任意后端，也不主张 IOCP 已被同一套观察函数覆盖。精化的是事件模型，不是操作系统的 socket ABI。

---

## 2 背景：两套 Linux 合同

### 2.1 epoll：就绪

应用程序登记对 fd 的兴趣，调用 `epoll_wait`，在内核报告可读/可写后再做非阻塞 syscall。一次就绪不是一次 I/O 结果：`EAGAIN`、短读、边缘触发丢失、以及「就绪后 close 抢先」都是一等公民。没有长期驻留内核的 `read` 请求；pending read 是用户态执行：syscall 失败后登记兴趣，事件到了再试。

因此，不要把 epoll interest 描述成 io_uring 风格的 Physical Request。它是 Backend Execution 里的一段，不是提交给内核、拥有独立物理终态的那条请求。

### 2.2 io_uring：提交与完成

应用程序把操作写成 SQE，内核把结果写成 CQE。准备 SQE 与消费已有 CQE 本身可以不进内核；提交与等待通常仍经过 `io_uring_enter`，除非启用 SQPOLL。完成可以乱序，靠 `user_data` 找回操作。一条 SQE 可以对应多条 CQE（`F_MORE`）；一条逻辑 close 可以对应 cancel 与原 I/O 两组 CQE。

io_uring 不是「带完成队列的 epoll」。它把执行委托给内核，并引入 epoll 路径上不存在的所有权问题：provided buffer 由 ring 选出，zerocopy 发送在 notification 之前不能复用用户 buffer。

### 2.3 协程看见什么

C++20 协程把一次异步调用拆成 `await_suspend` / `await_resume`。`await_suspend` 返回 `false` 时，当前协程内联进入 `await_resume`，**不会**入调度队列。逻辑规范必须区分「内联继续」和「被授权的 continuation 恢复」。把两者都叫 resume，会把立即完成的 epoll 路径和 CQE 后的调度恢复焊成同一种观察。

应用可依赖的只有：提交、可选挂起、结果、恢复、取消、关闭。物理队列、兴趣集合和 ring 偏移不在观察里。

---

## 3 问题：事件不等于完成

一次原始后端事件携带的是证据，例如：

```text
CompletionEvent = { result, flags, user_data }
```

或一次 `epoll_wait` 返回的就绪位。下列问题可能在不同时刻得到答案：

```text
这条物理请求还会不会再产生事件？
业务结果是否已经固定？
等待中的 continuation 是否可以入队？
buffer / fd / lease 是否可以复用或销毁？
loop 的 inflight 计数是否应当减少？
```

「事件 = 完成 = 恢复 = 释放」把五条问题收成一个布尔值。表 1 给出若干反例。它们不是边角；它们是现代 Linux 网络路径上的常规协议。

**表 1.** 把内核事件直接当成协程完成时的失败。

| 物理事件 | 若当作逻辑完成 | 实际边界 |
| --- | --- | --- |
| epoll `EPOLLIN` | 恢复 `Read` | 只说明值得再 `recv`；结果尚未固定 |
| close 的 cancel CQE | 恢复并销毁 stream | 原 I/O 的 CQE 仍可能访问 borrowed 状态 |
| `F_MORE` 的 recv CQE | 结束 operation 并 resume 一次 | 事件可交付；request 仍 active |
| zerocopy 的 primary CQE | 复用发送 buffer | 结果可就绪；若有 `F_MORE`，release 更晚 |
| `F_BUF_MORE` | 归还 provided buffer | kernel 仍拥有该 slot |
| source 高水位后的 terminal CQE | 向 `Next()` 交付逻辑结束 | 这是可恢复的 pause，不是 source terminal |

问题因而可以收成一句：

> 给出一个逻辑 I/O 规范，使 readiness 执行与 completion 执行 refine 它，而不要求它们共享控制流，也不把扩展操作的基数与释放规则偷运进 Core `await`。

---

## 4 LRCI

### 4.1 物理域与逻辑域

```text
            Physical domain
 syscall / readiness / CQE / cancel ack
                    │
            Backend Refinement
                    │
 ════════════════════════════════════
                    │
            Logical domain
                    ▼
     Result Ready, Resume Authorized, Release Authorized
```

物理域描述后端做了什么。逻辑域只描述应用能依赖的结果、恢复和所有权。多个物理步骤可以投影为一个逻辑步骤，也可以是逻辑上不可见的 stutter。观察一个就绪或一条 CQE，本身不移动三条逻辑边界。

**Logical Operation** 是调用者发起的一次异步动作：一次 `Read`、一次 `Close`、一次 source 上的 `Next()`。它不等于 syscall、SQE 或 CQE。

**Physical Request** 是提交给内核、具有独立物理终态的请求。对应关系不是一一的：

```text
close:              1 个逻辑操作, N 个物理请求（cancel + 原 I/O）
multishot accept:   1 个逻辑 source, 1 个物理请求, N 个后端事件
zerocopy send:      1 个逻辑操作, 1 个物理请求, primary CQE [+ 可选 notification]
epoll pending read: 1 个逻辑操作, 0 个长期驻留的 read 请求
```

**Backend Event** 是一份证据。Adapter 必须按当前生命周期解释它；事件名字不能决定逻辑含义。

### 4.2 三条一次授权边界

**Result Ready.** 应用可观察结果已经固定：成功、EOF、错误、取消，或一个 source 事件。固定之后不得改写。

**Continuation Authorized.** 允许对应协程进入调度队列的一次边界。Result Ready 不蕴含可以立即恢复：composite 操作可能还在等其他物理成员。立即完成的路径结果已就绪且已授权释放，但 *不* 授权调度恢复；当前协程内联继续。

**Release Authorized.** 允许复用或销毁 operation 状态、buffer、fd 或 lease 的一次边界。它可以与 Result Ready 重合，也可以明显更晚。

三条不变量：

- **LRCI-1（事件分离）。** 观察到后端事件，本身不蕴含结果就绪、恢复授权或释放授权。
- **LRCI-2（独立寿命）。** 恢复与释放按逻辑收敛和物理收敛分别授权。
- **LRCI-3（后端精化）。** 后端的物理执行可以不同，可观察 trace 必须 refine 同一逻辑规范。仅把 `OnCqe()` 转发给公共 helper，不足以证明精化。

### 4.3 三个正交维度

LRCI 不把操作推进四个互斥家族。它用三个维度描述协议形状：

```text
结果基数     Single | Multiple
物理收敛     Single | Composite
释放耦合     Coupled | Split
```

它们构成常见模式，但不是互斥类型。`Multiple + Single + Split`（multishot recv + `BufferLease`）与 `Single + Composite + Coupled`（cancel+close）都合法。未来的扩展不必再增加全局枚举。

**表 2.** 典型操作的生命周期形状。

| 操作 | 基数 | 收敛 | 释放 |
| --- | --- | --- | --- |
| 普通 read/write | Single | Single | Coupled |
| connect / accept | Single | Single | Coupled |
| cancel + close | Single | Composite | Coupled |
| multishot accept | Multiple | Single | Coupled |
| zerocopy send | Single | Single | Split |
| multishot recv + lease | Multiple | Single | Split |

Core 契约冻结 Single+Single+Coupled 的 `Read`/`Write`/`Accept`/`Connect`/`Close`，以及它们的可观察结果、borrowed buffer 寿命和 owner-thread 规则。其余形状是扩展。扩展可以增加能力，不能改变 Core `await` 的完成基数或释放点。

---

## 5 成对精化

### 5.1 观察函数

为每个后端定义观察函数，投影到同一逻辑记录：

```text
LogicalState = {
  coroutine, resource, operation, result,
  resultReady, continuationAuthorized, releaseAuthorized
}

ObsEpoll  : PhysicalState → LogicalState
ObsUring  : PhysicalState → LogicalState
```

两端故意投影同一组 adapter 持有的逻辑字段，隐藏的是不同的执行状态：epoll 的 `{Idle, ChannelWaiting, Ready}`，io_uring 的 `{Idle, SQEQueued, Submitted, CQEReady}`。精化要求每条物理转移要么是 `LogicalNext`，要么不改变 `LogicalState`（stutter）。

当前模型固定后端，不允许执行中途切换。这与「热插拔」口号不同：我们检查的是*可替换性*——同一逻辑规范的两条独立精化——不是运行中改 `B`。

### 5.2 逻辑 Read

投影后，两端都呈现：

```text
Submit → (ImmediateResult | Suspend → Result) → (InlineContinue | Resume)
```

物理故事不同。

**epoll。**

```text
Logical Read
  → 立即 recv
       ├─ 字节 / EOF / 错误 → Result Ready ∧ Release
       │                      continuation 不授权（InlineContinue）
       └─ EAGAIN
            → 登记 EPOLLIN                         [stutter]
            → Suspend
            → readiness                            [stutter]
            → 再 recv
            → Result Ready ∧ Release ∧ Resume
```

就绪只改变 `epollState`。在 `ObsEpoll` 下它是 stutter：协程仍在等待，结果仍未固定。

**io_uring。**

```text
Logical Read
  → 准备 recv SQE                                  [stutter]
  → 提交 Physical Request                          [stutter]
  → Suspend
  → CQE 到达                                       [stutter]
  → adapter 解释结果
  → Physical Terminal ∧ Result Ready ∧ Release ∧ Resume
```

CQE *可用* 仍是 stutter。Adapter 解释之前，逻辑结果未固定。这避免「CQE 名字 = 业务结果」：成功的 connect CQE 只说明内核连接完成；adapter 仍须设置非阻塞、把 fd 交给 `Stream`（失败则关闭），固定 `Result<Stream>`，然后才能授权释放与恢复。

### 5.3 立即完成不是调度恢复

epoll 上的立即成功是一等逻辑步骤，不是实现小手段。`await_suspend() == false` 时没有 `ResumeWork`。规范把它写成 `InlineContinue`：

```text
resultReady ∧ releaseAuthorized ∧ ¬continuationAuthorized
```

io_uring 上的普通完成走另一条：结果、释放与 continuation 一齐授权，调度器稍后恢复协程。两种路径的可观察结果相同；恢复机制不同。把「continuation 入队」焊进所有成功路径，会误伤立即完成，或反过来把每个 CQE 都变成同步 `resume`。

### 5.4 Close 是 composite

`Close` 是一个逻辑操作、多条物理请求。cancel acknowledgement 不证明目标请求已经 Physical Terminal。逻辑结果（被取消的 pending I/O 各完成一次，随后 close 完成）在 *全部* 相关物理成员收敛后才就绪；continuation 只授权一次。更细的 cancel/target 交错在单独模型里，再投影回单结果 Read 规范。

---

## 6 AcceptSource：非平凡成对例

单结果 `Accept()` 仍保留。`AcceptSource` 是独立扩展：持续产生连接的逻辑事件源，不是对 `io_uring` multishot accept 的直接封装。

可观察接口是：

```text
Next() → Result<optional<Stream>>
Stop() → Result<void>
```

`Result<Stream>` 交付一个连接；`Result<nullopt>` 是正常结束；`Result<Error>` 是终止错误。`Next()` 必须先交付已入队连接，再交付终止结果。终止结果是 sticky 的。`Stop()` 停止 admission、收敛物理请求、保留已入队连接；排空后 `Next()` 得到 `nullopt`。高水位暂停不是逻辑 terminal。

三条物理路径 refine 同一序列：

```text
epoll:              就绪 → accept drain → 可选 pause / re-arm
io_uring one-shot:  每次 terminal 后重新提交
io_uring multishot: 一个 request，F_MORE 产生事件，无 F_MORE 为 request terminal
```

业务只看见 `Next()`。native multishot 的 CQE 不直接映射成反复 `co_await` resume。内核拒绝 multishot flag 时，同一逻辑 source 回退到 one-shot；事件序列不变。

这是 pairwise 主张里比 `Read` 更硬的例：物理请求数可以不同，逻辑事件序列必须相同。bounded admission 的 conformance 只观察 `Next()`、`Stop()` 和 terminal，不窥探就绪位或 CQE。

---

## 7 不许回流 Core 的扩展

io_uring 还提供会改变表 2 中某一维的操作。LRCI 允许它们存在，禁止它们改写 Core `await` 的含义。

**RecvSource + BufferLease。** 一条 provided-buffer multishot recv 产生多个 `RecvEvent`。`F_MORE` 属于 request 寿命；lease 归还属于释放寿命；source 的 pause/stop 属于逻辑寿命。`Stop()` 完成要求 request terminal、队列排空、outstanding lease 为零。没有 `BufferLease` 的 `Read` 不得假装提供同一所有权。epoll 可以用 readiness 与自管 buffer 解释同一逻辑 source；它不能伪造 kernel buffer id。

**Send zerocopy。** 发送结果可以在 primary CQE 上就绪。若 primary 带 `F_MORE`，buffer 释放等到 notification；否则 primary 自身就是释放边界。`Write` 仍表示「完整写入或终态失败」。短写推进和 notification 是后端实现，不是业务要拼接的通用 fallback。

分层是：

```text
1. Core     两端 refine 同一可观察 trace
2. 同源扩展 AcceptSource；有对应物的 RecvSource
3. 单边扩展 send-zc、纯 uring 的 ring 细节
```

第三层预测新 opcode 落在哪一维，而不是扩大 `AsyncStream`。

---

## 8 实现：平行 adapter，共享 POSIX 资源

Alyrn 把 epoll 与 io_uring 做成平行 adapter，不是带预处理分支的一份 poller。每个 worker 拥有自己的线程、loop、连接和 operation。continuation 在拥有该操作的执行上下文中恢复；协程帧不在 loop 之间移动。

共享的是 POSIX 资源层：`int` fd、`errno`、`sockaddr`、`SOCK_NONBLOCK | SOCK_CLOEXEC`、`SO_REUSEADDR` / `SO_REUSEPORT`。观察函数因此只投影事件与生命周期，不必同时抽象句柄宽度、错误域和套接字选项。这不是疏忽。§10 说明为什么 IOCP 不能靠宏贴进这一层。

逻辑生命周期是小型、不拥有资源的状态机：`SingleResultLifecycle`、`CompletionGate`、`CompositeLifecycle`、`SplitReleaseLifecycle`，以及 source 状态机。它们不拥有 fd、buffer、SQE 或 CQE。Adapter 持有物理资源；`backend` 层只保存已经固定的 `Result<T>`。Loop 消费原始 CQE 或就绪事件，并把 disposition 交回 adapter：是否物理终态、是否减少 inflight、是否恢复 *operation 自带的* continuation。Event-stream 操作（AcceptSource、RecvSource）明确返回「此次 CQE 不结束内核请求」，使得 `F_MORE` 不会减少 inflight，也不会销毁 operation。

C++ 协程是语法，不是规范。`Task` / `DetachedTask` 提供结构化启动与 join；公开 `Next()` 在热路径上用直接 awaiter，避免每个逻辑事件一个中间帧。这个选择不改变可观察结果。

---

## 9 验证

验证有两层，互相不能替代。

**有界模型检查。** TLA+ 模型检查提交、完成、取消、恢复和释放在何处线性化。它们不是运行时的逐行翻译。`async_stream_backend_refinement.tla` 给出 `ObsEpoll` / `ObsLUring` 以及 `TraceRefinement`：每步要么是逻辑转移，要么是 stutter。单独的模型覆盖多操作身份、close/cancel、loop stop、AcceptSource、RecvSource lease、zerocopy 分离释放，以及三个正交维度的组合。TLC 检查有限实例。这不证明 C++ 内存安全，也不证明真实内核公平性。

表 3 是 2026-08-31 在本仓库 `docs/design/zh-CN/network/formal/` 上、TLC 2.19（rev 5a47802）、12 workers 的一次完整复现。15 个配置全部 `Model checking completed. No error has been found.` 未发现指纹碰撞报警以外的风险（optimistic collision 估计均在 \(10^{-16}\) 量级）。最大实例是 `resource_close_cancel`：41 709 生成状态、16 439 不同状态、直径 27。成对精化模型本身很小（单结果 126 不同状态 / 直径 9；多操作 1 750 / 13），说明当前检查的是协议形状，不是实现状态空间。

**表 3.** 有界 TLC 结果。生成 / 不同指 reachable states；直径是完整 BFS 深度。

| 模型 | 界限 | 生成 | 不同 | 直径 | 时间 |
| --- | --- | ---: | ---: | ---: | ---: |
| `async_stream_core` | 单 stream、单操作 | 75 | 48 | 7 | 1s |
| `async_stream_backend_refinement` | Epoll \| LUring，单 Read | 189 | 126 | 9 | 1s |
| `async_stream_multiop` | read/write 并行 | 350 | 190 | 9 | 1s |
| `async_stream_multiop_backend_refinement` | 两端多操作 | 2 454 | 1 750 | 13 | 1s |
| `async_operation_lifecycle_shapes` | `MaxEvents = 3`，8 个 shape | 441 | 286 | 12 | 1s |
| `accept_source_refinement` | `MaxEvents = 2`，`MaxRequests = 3` | 775 | 334 | 15 | 1s |
| `recv_source_lease` | `Buffer=2`，`Event=2`，`MaxEvents=4` | 2 075 | 714 | 29 | 1s |
| `recv_source_incremental_lease` | `Buffer=1`，`Size=2`，`MaxSegments=2` | 617 | 240 | 15 | 1s |
| `send_zc_split_release_refinement` | 单次 ZC send | 49 | 46 | 9 | 1s |
| `resource_close_cancel` | close + cancel + target | 41 709 | 16 439 | 27 | 6s |
| `stream_shutdown_transaction` | 写半关闭 | 138 | 129 | 10 | 1s |
| `loop_stop_control` | `RequestStop` vs close | 115 | 52 | 9 | 1s |
| `luring_loop_stop_retry` | 全局 cancel 准备失败 | 88 | 43 | 14 | 1s |
| `timer_preparation_failure` | 4 个初始 timer 形状 | 86 | 76 | 11 | 1s |
| `scheduler_completion_liveness` | 单次 completion 投递 | 11 | 6 | 4 | 1s |

`async_stream_core`、`resource_close_cancel`、`luring_loop_stop_retry`、`scheduler_completion_liveness` 另查了对应 `.cfg` 里的 temporal properties，均通过。`async_stream_backend_refinement` 查了 `TraceRefinement`。

**跨后端 conformance。** 同一组应用可观察场景在 epoll 与 io_uring 上运行：EOF、半关闭、pending I/O 上的 close、一次恢复、buffer 归还、完成后立即同方向 follow-up read，以及对刚连接的 stream 立刻 `Close()`。AcceptSource 用同一公开序列检查 burst 入队、低水位再 admission、以及 `Stop()` / listener `Close()` 的交错。测试不读取后端的就绪或 CQE 状态。

第一层覆盖交错；第二层覆盖具体内核与所有权。声称「两个 adapter 调用同一 helper」不属于验证。

**本稿仍缺、投稿前应补的证据。**

1. 表 1 的扩充：Tokio、Asio、手写 uring 路径上的公开缺陷或 API 陷阱，并指出它们违反哪一条 LRCI 边界。
2. 可选：不走最低公分母时，AcceptSource / RecvSource 相对朴素 `Accept`/`Read` 的代价。吞吐不是主证据。

---

## 10 范围：POSIX 资源域

IOCP 也是 Proactor：提交请求、等待完成、重叠操作结束前不能乱动 buffer。把它加进来，多半是 completion 家族里的另一个实现（`CancelIoEx`、`AcceptEx`、`ConnectEx`），不是第三条观察函数。

真正的障碍是资源身份。Alyrn 的 `net` 层是 POSIX socket 工具箱，不是操作系统中立的 fd 抽象。Windows 要换句柄类型、错误域、accept/connect 原语，以及完成端口与 socket 的关联。用宏假装 `SOCKET` 是 `int`、`WSARecv` 是 `recv`、`OVERLAPPED` 是 `Op*`，会把 ABI 相容伪装成语义精化。

正确的分层是：

```text
逻辑 I/O 规范     OS 无关（本文对象）
C++ 协程契约     语言相关，backend 中立
POSIX net        fd / sockaddr / sockopt（Unix 后端的底座）
epoll | io_uring
```

kqueue 换 dispatcher，仍使用 POSIX socket，因此若重新出现也是第三条 adapter，不是第三个科学对象；当前不在树内。IOCP 连资源身份都换了。Core 契约保持 OS 中立；`net` 承认自己是 POSIX。Windows 若出现，应是平行的资源层加平行的 Proactor adapter，而不是 `socket.h` 里的宏。

这条边界强化而非削弱成对主张：epoll 与 io_uring 共享资源域，所以精化可以只讨论事件与生命周期。

---

## 11 相关工作

**Reactor 与 Proactor。** Schmidt 等人把就绪多路复用与完成分发写成稳定模式。LRCI 不否认这个区分；它给出两者在协程观察下必须 refine 的规范。io_uring 文献强调完成模型相对 epoll 降低了系统调用与统一了存储/网络提交。本文关心的是完成事件 *之后*、协程可依赖结果 *之前* 的那段生命周期。

**可移植运行时。** libuv、Boost.Asio、Seastar、Tokio/mio 都在一个 await 或 callback API 下暴露多种机制。它们的统一多半是工程适配：执行器、reactor、或把 completion 降成 readiness。P2300 sender/receiver 把完成做成可组合的公开代数；它不规定 readiness 重试或 CQE 标志如何授权释放。LRCI 更窄：三个授权，加上一条明确的精化义务。

**结构化并发与取消。** Trio 与后续工作要求取消有明确寿命。C++ 协程把帧寿命和 borrowed buffer 变成实现负担。LRCI 把取消放进 composite 收敛，并禁止 cancel acknowledgement 单独授权释放。这是规范选择，不是新的取消演算。

**精化与 TLA+。** Abadi 与 Lamport 的 refinement mapping、以及 TLA+ 的 stuttering，是本模型的方法，不是贡献。贡献是观察函数的选择：隐藏就绪与 SQ/CQ 步骤，保留三条授权。有界 TLC 是检查，不是全称证明。

**io_uring 系统研究。** 近期工作（例如 DBMS 如何配置 uring）评估吞吐、轮询和队列深度。它们把 uring 当作更快的提交路径。本文把 uring 当作必须与就绪模型对拍的合同，并拒绝靠丢掉 multishot 与 zerocopy 来统一。

---

## 12 限制

- **有界、不是证明。** 模型固定小的操作数与事件数。没有对实现做 refinement mapping 的机器检查。
- **单一资源域。** 只声称 POSIX/Linux。kqueue 不在树内，也不是第三个科学对象；IOCP 未实现，也不在精化定理里。
- **无动态切换。** 后端在初始化时固定。可替换 ≠ 热插拔。
- **扩展覆盖不对称。** 最尖锐的分裂（lease、`F_NOTIF`、`F_MORE`）出现在 io_uring。epoll 对这些扩展的精化，或是类似物（accept drain），或是明确的非目标。Core 主张不依赖单边扩展。
- **尚无负例外评。** 表 1 来自内核合同与本实现。对 Tokio/Asio 的系统对照仍待补。
- **性能不是主结果。** 平行 adapter 的目的是避免最低公分母，不是赢得 epoll 对 uring 的基准。

---

## 13 结论

epoll 与 io_uring 不必共享控制流，才能对协程呈现同一套 I/O 语义。它们必须 refine 同一逻辑生命周期。使精化可检查的最小结构是三条一次授权边界：结果、恢复、释放。会改变完成基数或释放耦合的内核能力是扩展；把它们焊进 Core `Read`，不是统一，是泄漏。

Linux 上的成对精化已经足够成为对象。两端共享 POSIX 资源域，并在就绪与完成之间给出真实的合同缝。IOCP 同属 Proactor，但会换掉资源身份；用宏把它贴上，会把 ABI 问题扮成语义结果。

有界 TLC 数字见表 3。下一步是用跨运行时负例压表 1，并保持 Core 契约冻结。第三种操作系统后端是预测，不是入场券。

---

## 致谢

本稿压缩自 Alyrn 的设计契约与 TLA+ 模型。若与实现冲突，以 `docs/design/zh-CN/network/` 下的冻结契约和 `docs/design/zh-CN/network/formal/` 为准。

---

## 参考文献

1. M. Abadi and L. Lamport. The existence of refinement mappings. *Theoretical Computer Science*, 82(2):253–284, 1991.
2. J. Axboe. Efficient I/O with io_uring. Linux kernel documentation and liburing, 2019–.
3. D. Charousset, R. Hiesgen, and T. C. Schmidt. CAF — the C++ Actor Framework. *JPDC*, 2016. （actor 运行时，对照：完成与寿命绑定在 actor 上，而不是独立的 I/O 授权。）
4. M. Jasny, M. Weisgut, T. Ziegler, D. Ritter, and T. Rabl. High-performance DBMSs with io_uring: When and how to use it. *PVLDB*, 19(8), 2026.
5. C. Kohlhoff. Boost.Asio. https://www.boost.org/doc/libs/release/doc/html/boost_asio.html
6. L. Lamport. Time, clocks, and the ordering of events in a distributed system. *CACM*, 21(7):558–565, 1978.
7. L. Lamport. *Specifying Systems*. Addison-Wesley, 2002.
8. G. Nishanov. C++ coroutines. ISO/IEC 14882, 2020.
9. D. C. Schmidt, M. Stal, H. Rohnert, and F. Buschmann. *Pattern-Oriented Software Architecture, Vol. 2: Patterns for Concurrent and Networked Objects*. Wiley, 2000. Reactor / Proactor.
10. N. J. Smith. Notes on structured concurrency, or: Go statement considered harmful. 2018. https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/
11. Tokio Contributors. Tokio and mio. https://tokio.rs/
12. libuv contributors. libuv. https://libuv.org/
13. Seastar contributors. Seastar. https://seastar.io/
14. ISO C++. P2300R10: `std::execution`. 2024.
15. Microsoft. I/O Completion Ports. Windows documentation.

---

## 附录 A 逻辑 Read 的精化草图

`LogicalNext` 的构造性步骤来自 `async_stream_backend_refinement.tla`：

```text
LogicalSubmit
LogicalImmediateResult     结果就绪 ∧ 释放；¬continuation
LogicalSuspend
LogicalResult              等待中的操作完成：三条授权一齐为真
LogicalCancelResult
LogicalClose / LogicalFinalizeClose
LogicalInlineContinue      立即路径；无 ResumeWork
LogicalResume              仅当 continuationAuthorized
LogicalFinish
```

epoll `Ready` 与 uring `UringSubmit` / `UringCQE` 不在其中。它们必须是 stutter。`UniqueCompletion` 与 `ResumeSafety` 是对应的安全性质：至多一个逻辑完成；进入 Ready/Done 之前必须已经授权释放且结果非空。

## 附录 B 投稿前清单

- [x] 跑通 formal 目录下全部 TLC 配置，把数字写入 §9（2026-08-31，TLC 2.19，15/15 无错）
- [ ] 为表 1 增加至少三个外部负例（附出处）
- [ ] 决定轨道：研讨会 / 经验 / 完整研究；按此删减 §11–§12
- [ ] 英译；把「精化」固定译为 *refinement*，不要写成 *unification* 当主动词
- [x] 从正文去掉 Lamport 六元组；热插拔只作为「可替换 ≠ 运行中切换」的反例保留
- [ ] 不要把 RecvSource 热路径性能写成贡献，除非有对照实验

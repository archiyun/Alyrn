# 生命周期精化协程 I/O

Lifecycle-Refined Coroutine I/O（LRCI）是 CoroPact 用来统一 Reactor 与 io_uring
业务语义的设计模型。它不要求两个后端共享物理控制流，而是要求它们的可观察 trace
精化同一套逻辑 I/O specification。

一句话定义：

> 后端事件不会直接等同于协程完成；后端先把物理执行精化为逻辑 operation 生命周期，再分别
> 确定结果何时 ready、continuation 何时可以恢复、资源何时可以释放。

## 1. 物理域与逻辑域

```text
                         Physical domain

 syscall result       readiness         CQE / cancel acknowledgement
       │                  │                         │
       └──────────────────┼─────────────────────────┘
                          ▼
                  Backend Refinement
                          │
══════════════════════════╪══════════════════════════════════
                          │
                         Logical domain
                          ▼
                Logical Operation State
                          │
             ┌────────────┼────────────┐
             ▼            ▼            ▼
        Result Ready    Resume       Release
                       Authorized   Authorized
```

物理域描述后端实际上做了什么：非阻塞 syscall、Channel 注册、SQE 提交、CQE 消费、
cancel acknowledgement。逻辑域只描述应用能够依赖的结果、恢复和所有权语义。

物理域中的多个 transition 可以投影为逻辑域中的一个 transition，也可以是逻辑上不可见的
stuttering step。一个 readiness 或 CQE 到达本身不自动改变三个逻辑边界。

## 2. 领域词汇

### Logical Operation

调用者认为自己发起的一次异步动作，例如一次 `ReadSome`、timed read、`Close` 或 source
消费。它不等于 syscall、SQE 或 CQE。

### Backend Execution

具体 Adapter 为实现一个 Logical Operation 所维护的执行过程。Reactor execution 可以包含多次
syscall 与 readiness re-arm；io_uring execution 可以包含一个或多个 Physical Request。

### Physical Request

提交给内核并具有独立物理终态的请求。一个 Logical Operation 与 Physical Request 不是一一
对应：

```text
timed read:
  1 Logical Operation
  2 Physical Requests: read + timeout

multishot accept:
  1 Logical Operation source
  1 Physical Request
  N Backend Events

zero-copy send:
  1 Logical Operation
  1 Physical Request
  primary CQE [+ notification CQE when primary has F_MORE]
```

Reactor 的 pending read 通常没有长期驻留内核的 read request。它是一个 Backend Execution：
立即 syscall 返回 `EAGAIN` 后登记 readiness，事件到达后再重试 syscall。因此不要把 epoll
interest 强行描述成 io_uring 风格的 Physical Request。

### Backend Event

后端获得的一项证据：syscall result、readiness、CQE 或 cancel acknowledgement。Adapter 必须按
当前 lifecycle 解释它；事件名称不能决定逻辑含义。

### Result Readiness

应用可观察结果已经固定的边界。结果可能是成功、EOF、错误、取消或一个 source event。

### Continuation Authorization

允许对应协程进入调度队列的 once-only 边界。Result Ready 不必意味着可以立即恢复：
composite operation 可能仍需等待其他物理成员收敛。

### Physical Terminal

一个 Physical Request 不会再产生 Backend Event，也不会再访问其 borrowed resource 的边界。
cancel acknowledgement 本身不证明 target request 已经 Physical Terminal。

### Release Authorization

允许复用或销毁 operation state、buffer、fd 或 lease 的 once-only 边界。它可能和 Result
Readiness 相同，也可能明显更晚。

## 3. 三条不变量

### LRCI-1 — Event Separation

```text
Observing a backend event does not by itself imply
result readiness, continuation authorization,
or release authorization.
```

例如：

- epoll readiness 只说明 syscall 值得重试；
- timed read 的任意一个 CQE 不一定使所有物理成员终态；
- zero-copy primary CQE 不授权复用发送 buffer；
- cancel CQE 不证明原 target request 已终态。

### LRCI-2 — Independent Lifetime Boundaries

```text
Continuation resumption and resource release are
authorized independently according to logical and
physical convergence.
```

普通 single-result 操作可以让两个边界相邻；zero-copy、provided buffer 和异步 close 不能依赖
这种巧合。

### LRCI-3 — Backend Refinement

```text
Backend-specific physical executions may differ,
but their observable traces must refine the same
logical I/O specification.
```

`refinement` 在这里采用形式化含义。需要为每个后端定义 observation function：

```text
ObsReactor : ReactorState -> LogicalState
ObsLUring  : LUringState  -> LogicalState
```

具体执行 trace 经相应 observation function 投影后，必须满足相同 LogicalSpec。仅仅在 C++ 中把
`OnCqe()` 转发给一个公共 helper，不足以证明 Backend Refinement。

## 4. 正交生命周期维度

LRCI 不把 operation 强制分进四个互斥 family。它使用三个正交维度描述常见协议：

### Result cardinality

```text
Single    一个逻辑结果
Multiple  多个逻辑事件，随后有一个 source terminal
```

### Physical convergence

```text
Single     一个主要物理执行路径即可收敛
Composite  多个物理成员共同决定终态
```

### Release coupling

```text
Coupled  结果与资源释放共享同一个安全边界
Split    结果 ready 后，资源仍需等待更晚的物理终态或 lease 归还
```

这些维度形成典型模式，但不是互斥类型：

| 操作 | Result cardinality | Physical convergence | Release coupling |
| --- | --- | --- | --- |
| 普通 read/write | Single | Single | Coupled |
| timed read | Single | Composite | Coupled |
| cancel + close | Single | Composite | Coupled |
| multishot accept | Multiple | Single | Coupled |
| zero-copy send | Single | Single | Split |
| multishot recv + BufferLease | Multiple | Single | Split |

composite operation 也可以包含 split-release member；未来扩展不需要增加另一个全局枚举值。

## 5. 两个后端如何精化同一逻辑 read

### Reactor

```text
Logical ReadSome
  -> immediate recv
       ├─ bytes / EOF / error -> Result Ready
       └─ EAGAIN
            -> register EPOLLIN
            -> Suspend
            -> readiness                      [stuttering]
            -> retry recv
            -> Result Ready
            -> Continuation Authorized
            -> Release Authorized
```

### io_uring

```text
Logical ReadSome
  -> prepare recv SQE                         [stuttering]
  -> submit Physical Request                  [stuttering]
  -> Suspend
  -> CQE                                      [stuttering]
  -> interpret result
  -> Physical Terminal
  -> Result Ready
  -> Continuation Authorized
  -> Release Authorized
```

投影后两者都呈现：

```text
Submit -> optional Suspend -> Result Ready -> Resume
```

但 LRCI 不要求它们共享 Channel、SQE、队列或 completion handler。

## 6. 代码、模型和测试如何对应

```text
领域词汇
    ↕
AsyncStream / source contract
    ↕
backend lifecycle implementation
    ↕
TLA+ observation + trace refinement
    ↕
cross-backend conformance tests
```

代码中的 `CompletionGate`、`CompositeLifecycle`、`SplitReleaseLifecycle` 和 source lifecycle
分别实现局部授权规则。它们不是四个互斥 family，也不拥有 fd、buffer、Channel、SQE 或 CQE；
具体 Adapter 仍负责物理资源和结果存储。

TLA+ 检查有界 interleaving 和 observation projection。conformance tests 则对 Reactor 和
io_uring 运行同一组应用可观察场景，例如 EOF、半关闭、pending I/O close、一次恢复和 buffer
归还。两者不能互相替代。

## 7. 不属于 LRCI 的主张

LRCI 不表示：

- Reactor 与 io_uring 使用相同物理执行机制；
- 所有后端能力都能通过同一个返回类型表达；
- CoroPact 已经验证 kqueue 或 IOCP；
- 收到一个事件就必须恢复协程；
- Result Ready 后资源一定可以立即复用；
- 每个 C++ implementation transition 都有一个可见 logical transition。

它只承诺：后端保留原生机制，同时把应用可观察行为精化到共同的生命周期 specification。

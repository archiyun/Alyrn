# 用 Lamport 视角讨论协程网络运行时的语义统一与热插拔

> 草稿。本文尝试说明：网络库热插拔不应该建立在后端功能穷举上，而应该建立在协程可观察语义、不变量和 happens-before 偏序上。

## 问题不在于 epoll 和 io_uring 谁更高级

讨论高性能网络运行时时，很容易落入一种比较表：

- `epoll` 是 readiness 模型；
- `io_uring` 是 submission/completion 模型；
- `io_uring` 支持 registered buffer、fixed file、linked SQE、multishot accept；
- `epoll` 支持 level-triggered、edge-triggered、oneshot；
- 某些库跨平台，某些库只面向 Linux。

这些比较有工程价值，但它们不能回答一个更基础的问题：

> 如果业务代码写成协程，运行时能否在不改变协程语义的前提下替换网络后端？

如果回答方式只是穷举后端能力，我们很快会失去边界。每加入一个后端，就要重新检查一遍所有组合；每出现一个新内核特性，就要重新讨论抽象是否仍然成立。这不是一个可证明的系统，只是一个越来越长的兼容性列表。

本文的思路是换一个问题：

> 协程真正能观察到什么？

协程看不到 `epoll_wait`，也看不到 submission queue entry。协程看到的是：

```cpp
auto n = co_await stream.Read(buf);
co_await stream.Write(data);
co_await timer.SleepFor(delay);
```

也就是说，协程可观察到的是异步操作的提交、挂起、完成、恢复、取消和资源关闭。只要这些事件之间的因果顺序不变，后端内部怎样实现，不应该影响协程语义。

这正是 Lamport 的 happens-before 关系适合发挥作用的地方。

## 为什么不能只依赖直觉

“协程似乎可以把业务逻辑和真实网络 I/O 解耦”是一个有用的工程直觉，但它不足以直接支撑运行时设计。直觉至少有三个问题。

第一，无法穷举。

`epoll`、`io_uring`、`kqueue`、IOCP 以及未来的内核接口都有不同的事件模型和能力集合。如果只靠逐项比较特性，后端数量、能力组合和状态交错会迅速膨胀。每新增一种机制，都要重新讨论它和现有机制的所有组合。

第二，无法验证。

如果抽象只停留在“看起来等价”，就很难写出有效测试。测试只能覆盖有限执行路径，而不能说明没有遗漏某条取消、关闭、超时、迁移或并发恢复路径。没有明确不变量，测试失败时也很难判断是实现错误，还是抽象边界本身不成立。

第三，无法划界。

如果没有形式化边界，统一层很容易在两个方向上失控：要么把所有后端压成最低公分母，牺牲 `io_uring` 这类后端的能力；要么把后端特性直接泄漏给业务协程，使所谓“统一语义”变成空话。

因此，这套理论不是为了给已经写好的代码补一个解释，而是为了在写代码之前回答三个问题：

```text
哪些行为必须被穷尽描述？
哪些性质可以被测试或证明？
哪些能力属于核心语义，哪些能力属于后端扩展？
```

只有先回答这些问题，协程网络运行时的抽象才不是经验性的包装，而是一套可验证、可演化、可指导实现的方法。

本文不证明“任意网络库可以在任意时刻被任意替换”。这个命题太强，也没有工程意义。本文要证明的是一个更有边界的命题：

```text
在固定机制 Σ、不变量 Inv 和当前能力 profile P 下，
若后端切换保持协程可观察 happens-before 偏序，
则该切换不改变协程语义。
```

## 一个运行时特权机六元组

把协程网络运行时抽象成一个状态机：

```text
M = (S, s0, E, Σ, π, δ)
```

其中：

```text
S   : 状态空间
s0  : 初始状态
E   : 事件集合
Σ   : 固定机制，即运行时必须满足的语义规则
π   : 策略，用于选择下一步执行哪个事件
δ   : 状态转移函数，δ : S × E -> S
```

不变量不直接放进六元组，而是作为约束集合：

```text
Inv = {I1, I2, ..., In}
```

这里也可以把 `M` 称为运行时特权机。原因是 `Σ` 描述的是运行时有权执行、但用户协程不能绕开的状态转移规则。例如恢复协程、完成 I/O、关闭资源和切换后端，都属于运行时掌握的特权动作。

这里最重要的划分是 `Σ` 和 `π`。

`Σ` 是机制，表示系统不能破坏的语义边界。例如：一个 I/O 操作不能在提交之前完成；一个协程不能被无关事件恢复；关闭 fd 之后不能再产生成功 I/O。

`π` 是策略，表示系统可以自由选择的优化空间。例如：FIFO 调度、优先级调度、批处理 completion、选择 `epoll` 还是 `io_uring`、一次处理多少个 ready task。

一个运行时如果把机制和策略混在一起，就很难证明后端替换不会改变语义。因为你不知道改变的是性能路径，还是语义本身。

## 状态空间 S

状态空间可以拆成：

```text
S = (C, R, O, Q, B, H, P)
```

含义如下：

```text
C : 协程集合及其状态
R : 资源集合，例如 fd、socket、buffer、timer
O : 未完成异步操作集合
Q : ready queue、completion queue、timer queue
B : 当前网络后端
H : happens-before 偏序或 Lamport logical clock
P : 当前启用的能力 profile
```

协程状态可以写成：

```text
CState = Running | Suspended | Ready | Done
```

资源状态可以写成：

```text
RState = Open | Closing | Closed
```

异步操作状态可以写成：

```text
OpState = Init | Submitted | Completed | Cancelled
```

`B` 不是一个特殊中心。它只是后端集合中的一个元素：

```text
B ∈ Backend
Backend = {epoll, io_uring, ...}
```

在这个模型里，`epoll` 和 `io_uring` 是平等的。它们不是谁模拟谁，也不是谁降级成谁。它们只需要解释同一个语义系统。

## 事件集合 E

协程网络运行时的核心事件可以定义为：

```text
E = {
  Submit(c, op),
  Suspend(c, op),
  Complete(op, result),
  Resume(c, result),
  Cancel(op),
  Close(r),
  Timeout(timer),
  SwitchBackend(b1, b2)
}
```

其中 `c` 是协程，`op` 是异步操作，`r` 是资源，`b1` 和 `b2` 是网络后端。

对协程来说，最核心的链路是：

```text
Submit(c, op)
  -> Suspend(c, op)
  -> Complete(op, result)
  -> Resume(c, result)
```

这条链路不是实现细节，而是协程异步 I/O 的可观察语义。无论后端是 readiness 模型还是 completion 模型，最终都必须给协程提供这条因果链。

## Lamport happens-before

Lamport 的 happens-before 关系记作 `->`。它是一种严格偏序，用于描述事件之间的潜在因果关系，而不是物理时间。

在协程网络运行时中，可以定义以下基本规则。

同一协程内，程序顺序建立 happens-before：

```text
e1 和 e2 属于同一协程，且 e1 在程序顺序上先于 e2
=> e1 -> e2
```

提交和完成之间建立 happens-before：

```text
Submit(c, op) -> Complete(op, result)
```

完成和恢复之间建立 happens-before：

```text
Complete(op, result) -> Resume(c, result)
```

取消和取消结果之间建立 happens-before：

```text
Cancel(op) -> Complete(op, cancelled)
```

关闭资源和后续失败结果之间建立 happens-before：

```text
Close(r) -> Complete(op, closed_error)
```

事件循环出队顺序也建立 happens-before：

```text
Dequeue(Q, e1) 先于 Dequeue(Q, e2)
=> e1 -> e2
```

跨线程唤醒、mutex、atomic、channel 等同步操作也可以加入 `->`。它们不是额外魔法，只是更多的因果边。

如果使用 Lamport logical clock，则每个事件 `e` 有一个逻辑时间戳 `L(e)`，满足：

```text
e1 -> e2 => L(e1) < L(e2)
```

注意反过来不成立：

```text
L(e1) < L(e2) 不推出 e1 -> e2
```

这点很重要。Lamport clock 用来维护因果一致性，不应该被误用成真实时间或完整因果证明。

## 固定机制 Σ

`Σ` 是运行时的固定语义机制。它不是后端选择策略，而是所有后端都必须满足的规则。

可以把 `Σ` 写成：

```text
Σ = {
  σ_submit,
  σ_complete,
  σ_resume,
  σ_cancel,
  σ_close,
  σ_switch,
  σ_cap
}
```

其中：

```text
σ_submit  : 提交操作必须创建唯一 pending op
σ_complete: pending op 最多完成一次
σ_resume  : 协程只能由与其等待条件匹配的完成事件恢复
σ_cancel  : 取消必须最终表现为取消完成或已完成
σ_close   : 关闭资源后不得产生新的成功 I/O
σ_switch  : 后端切换不得破坏已有 happens-before
σ_cap     : 已启用能力在新后端上必须有语义解释
```

这些规则组成协程运行时的“特权机制”。策略可以替换，机制不能破坏。

## 策略 π

`π` 只负责选择下一步事件，例如：

```text
π_fifo      : 按 FIFO 处理 ready queue
π_priority  : 按优先级处理 ready queue
π_batch     : 批量处理 completion
π_backend   : 选择 epoll 或 io_uring 后端
π_migration : 选择何时进行后端切换
```

策略可以影响吞吐、延迟、公平性和 cache locality，但不应该改变协程语义。

因此我们希望证明的是：

```text
在同一 Σ 和 Inv 下，改变 π 不改变协程可观察语义。
```

这里的“语义不变”不是说事件物理顺序完全相同，而是说协程可观察事件的 happens-before 偏序保持一致，或者至少保持同一组不变量。

## 不变量 Inv

不变量是整个论证的边界。没有不变量，所谓热插拔只是工程直觉。

可以定义如下不变量。

```text
I1: 唯一完成
任意 op 至多出现一次 Complete(op, result)。
```

```text
I2: 因果完成
若 Complete(op, result) 出现，则必然存在 Submit(c, op)，且
Submit(c, op) -> Complete(op, result)。
```

```text
I3: 恢复授权
若 Resume(c, result) 出现，则 c 必须处于 Suspended，
且存在与 c 等待条件匹配的 Complete/Cancel/Timeout 事件。
```

```text
I4: 关闭支配
若 Close(r) -> Submit(c, op)，且 op 依赖 r，
则 op 不得以 success 完成。
```

```text
I5: 生命周期覆盖
op 使用的 buffer、fd、callback、coroutine handle 的生命周期
必须覆盖 Submit(op) 到 Complete(op, result) 的区间。
```

```text
I6: 后端唯一所有权
任意 pending op 在任意时刻最多由一个后端拥有。
```

```text
I7: 切换保持偏序
SwitchBackend(b1, b2) 不得删除、反转或伪造已有 happens-before 边。
```

```text
I8: 能力 profile 保持
若当前活跃能力集合为 P，则切换到 b2 前必须满足：
∀ cap ∈ P, [[cap]]_b2 ≠ ⊥。
```

这些不变量比“支持哪些系统调用”更关键。它们描述的是运行时语义的安全边界。

## 能力扩展层不是最低公分母

统一抽象最常见的问题，是把所有后端压成最低公分母。

如果为了让 `epoll` 和 `io_uring` 使用同一个接口，就禁止 `io_uring` 使用 registered buffer、fixed file、linked SQE 或 multishot accept，那么统一层确实成立，但代价是抹掉了后端能力。

更合理的做法是引入公共能力集合：

```text
Cap = {
  readiness_poll,
  edge_trigger,
  oneshot,
  submit_read,
  submit_write,
  registered_buffer,
  fixed_file,
  linked_ops,
  multishot_accept,
  sqpoll
}
```

每个后端都解释同一个能力命名空间，但解释函数允许部分定义：

```text
[[cap]]_B : Cap -> Semantics ∪ {⊥}
```

含义是：

```text
[[cap]]_B = 某个语义解释  表示后端 B 支持该能力
[[cap]]_B = ⊥             表示后端 B 不支持该能力
```

例如：

```text
[[edge_trigger]]_epoll       ≠ ⊥
[[registered_buffer]]_epoll  = ⊥
[[linked_ops]]_epoll         = ⊥

[[submit_read]]_io_uring     ≠ ⊥
[[registered_buffer]]_io_uring ≠ ⊥
[[linked_ops]]_io_uring      ≠ ⊥
```

这里的 `⊥` 不是 no-op。它表示不支持。把不支持静默实现成 no-op 会破坏语义，因为调用者可能依赖该能力提供的生命周期、所有权或性能语义。

因此，能力扩展层可以是公共的，但不是每个后端都必须给出非空实现。

## 热插拔条件

现在可以定义后端热插拔。

设当前后端为 `b1`，目标后端为 `b2`，当前活跃能力 profile 为 `P`。

后端切换可行，当且仅当：

```text
Switchable(b1, b2, S, P)
iff
  Inv(S)
  ∧ ∀ cap ∈ P, [[cap]]_b2 ≠ ⊥
  ∧ Pending(S) 可被 drain、cancel 或 transfer
  ∧ H 在切换后保持单调扩展
```

其中 `H` 是当前 happens-before 偏序。

最保守的切换点是 quiescent point：

```text
Pending(S) = ∅
ReadyQueue(S) = ∅ 或可完整迁移
CompletionQueue(S) = ∅ 或可完整迁移
```

在这个点上，旧后端没有未完成操作，新后端只需要从一个干净状态启动。证明最简单，但工程灵活性较差。

更强的方案是迁移 pending op：

```text
Freeze(b1)
Drain 或 Transfer pending ops
Install(b2)
L_new >= L_old + 1
Resume dispatch
```

这要求每个 pending op 的所有权、buffer 生命周期、取消语义和完成事件都能被形式化迁移。证明难度明显更高，但允许更低停顿。

## 语义等价定理

现在可以给出一个核心定理。

```text
Theorem: Coroutine Observational Equivalence

给定两个后端 b1 和 b2。
若：
1. b1 和 b2 均满足固定机制 Σ；
2. b1 和 b2 的所有状态转移 δ 都保持 Inv；
3. 对当前活跃能力 profile P，∀ cap ∈ P, [[cap]]_b1 ≠ ⊥ 且 [[cap]]_b2 ≠ ⊥；
4. b1 与 b2 对协程可观察事件产生相同的 happens-before 偏序，
   或产生保持 Inv 的偏序 refinement；

则：
b1 和 b2 在 profile P 下对协程层语义等价。
```

这个定理没有声称两个后端性能相同，也没有声称内部事件相同。它只声称：从协程层观察，语义边界相同。

进一步得到热插拔推论：

```text
Corollary: Backend Hot Swappability

若 SwitchBackend(b1, b2) 的状态转移保持 Inv，
且切换前后的 happens-before 偏序单调扩展，
且目标后端 b2 支持当前活跃 profile P，
则该切换不会改变协程可观察语义。
```

这就是热插拔的形式化边界。

## 为什么这比功能穷举更可靠

功能穷举会问：

```text
epoll 有什么？
io_uring 有什么？
这个特性怎么模拟那个特性？
```

形式化系统会问：

```text
协程观察到哪些事件？
哪些因果边必须保持？
哪些状态转移必须维持不变量？
哪些能力当前真的被启用？
目标后端是否解释这些能力？
```

前者容易陷入无边界的兼容性讨论；后者给出了明确的证明义务。

更重要的是，这种模型不会要求 `io_uring` 向 `epoll` 妥协，也不会要求 `epoll` 伪装成 `io_uring`。两者都是后端解释器：

```text
Backend B interprets Core + supported Cap.
```

核心层要求全定义：

```text
Core_B(op) ≠ ⊥
```

能力层允许部分定义：

```text
Cap_B(cap) ∈ Semantics ∪ {⊥}
```

程序选择了哪些能力，就形成当前 profile。热插拔只需要在该 profile 下成立，不要求所有后端支持所有能力。

## 对工程实现的启发

这个模型落到 C++ 协程运行时，大致可以形成三层。

第一层是协程核心：

```text
Coro<T>
Scheduler
Awaiter
Resume queue
Cancellation token
```

它不依赖 `epoll` 或 `io_uring`。

第二层是核心异步语义：

```text
AsyncStream
AsyncListener
AsyncTimer
Submit/Complete/Cancel/Close
```

这层定义所有后端必须满足的 `Σ` 和 `Inv`。

第三层是后端能力解释：

```text
EpollBackend
IoUringBackend
CapabilitySet
BackendProfile
```

它允许不同后端保留自己的能力，而不是强行压平。

热插拔 API 不应该只是：

```cpp
runtime.SwitchBackend(new_backend);
```

它至少应该表达切换条件：

```cpp
runtime.SwitchBackend(new_backend, SwitchMode::Quiescent);
runtime.SwitchBackend(new_backend, SwitchMode::DrainAndTransfer);
```

并且运行时需要能回答：

```cpp
backend.Supports(profile);
runtime.PendingOps();
runtime.CurrentLogicalClock();
```

否则所谓热插拔只是一个危险的状态突变。

## 结论

协程网络运行时的统一，不应该建立在“哪个后端更强”上，而应该建立在协程可观察语义上。

用 Lamport 的 happens-before 关系看，后端热插拔的关键不是让 `epoll` 和 `io_uring` 内部行为一致，而是保证：

```text
Submit -> Complete -> Resume
```

这类因果链不被破坏。

用六元组状态机看，问题可以写成：

```text
M = (S, s0, E, Σ, π, δ)
```

其中 `Σ` 固定语义机制，`π` 选择调度和后端策略，`δ` 执行状态转移，`Inv` 给出不可破坏的不变量集合。

因此，热插拔不是一个“支持多少后端”的问题，而是一个“状态转移是否保持不变量和 happens-before 偏序”的问题。

这给了我们一个更清晰的边界：

```text
后端可以不同；
能力可以不同；
性能可以不同；
但协程可观察因果语义必须一致。
```

这也是一个可被证明、可被测试、可被工程化的边界。

## 参考资料

- Leslie Lamport, "Time, Clocks, and the Ordering of Events in a Distributed System", Communications of the ACM, 1978.
- libuv design overview: <https://docs.libuv.org/en/v1.x/design.html>
- libevent event_base documentation: <https://libevent.org/libevent-book/Ref2_eventbase.html>
- Boost.Asio basic anatomy: <https://www.boost.org/doc/libs/latest/doc/html/boost_asio/overview/basics.html>
- mio crate documentation: <https://docs.rs/mio/latest/mio/>
- tokio-uring crate documentation: <https://docs.rs/tokio-uring/latest/tokio_uring/>

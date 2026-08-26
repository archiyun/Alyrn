# io_uring Operation Matrix

本文档记录 CoroPact 当前以及计划支持的 io_uring operation 语义。
它描述的是 luring 适配层的完成协议，不是把 io_uring 原始 SQE/CQE 暴露给业务代码。

当前公共网络接口仍然是 `AsyncStream`、`AsyncListener` 和一次性
`Task<Result<T>>` 操作。multishot、provided buffer、send zerocopy 等能力会改变完成次数、
结果类型或 buffer 生命周期，因此必须通过独立的扩展接口表达，不能伪装成普通
`ReadSome()` 或单次内部 send。

持续 accept 的具体语义见 [AcceptSource 语义契约](accept-source-contract.md)。

## 1. 完成协议的基本区分

一个原始 CQE 只携带后端事件：

```text
CompletionEvent = { cqe_res, cqe_flags, user_data }
```

operation-specific lifecycle 再解释这个事件。不要把 `terminal` 直接当成 CQE 的属性，因为下面几件
事可能发生在不同时间：

```text
kernel operation 是否还会产生后续 CQE
业务结果是否已经确定
等待中的 continuation 是否应该恢复
buffer 是否可以复用
operation frame 是否可以销毁
inflight 是否应该减少
```

例如：

- timed read 的 read CQE 和 timeout CQE 都可能到达，但业务 continuation 只能恢复一次；
- multishot recv 的 `F_MORE` CQE 已经可以产生业务事件，但 operation 仍然存活；
- send zerocopy 的 send CQE 可以确定发送结果；若带 `F_MORE`，notification CQE 才确定 buffer
  可以复用，否则 primary CQE 本身就是该边界；
- close 的 cancel CQE 到达后，原有 read/write CQE 仍然可能在后面到达。

因此第一阶段的 loop 只应接收原始 CQE，并交给 operation-specific handler；handler 再决定这
次事件是否产生业务事件、是否保持 operation、以及何时提交恢复工作。

## 2. 编译期接口与运行期路径选择

`io::*` concepts 描述应用可使用的语义接口；它们不尝试把内核探测结果编码成 luring 的
第二套 profile。具体 backend 的实现可用性分为以下几层：

| 维度 | 要回答的问题 | 例子 |
| --- | --- | --- |
| 构建期 | 当前构建是否启用了 io_uring，以及 liburing 头文件是否提供接口或 flag？ | `COROPACT_ENABLE_URING`、`IORING_CQE_F_MORE` |
| 内核期 | 当前 kernel/ring 是否接受该 opcode 或注册操作？ | ring setup 成功，operation CQE 不返回 unsupported |
| 配置期 | 当前 ring setup 是否满足使用条件？ | SQPOLL、CQ 大小、buffer ring 注册 |
| 语义期 | 当前适配器是否实现了结果、取消和资源生命周期？ | multishot 的重复事件与终止 CQE |
| 运行期 | 当前 operation 的对象、资源和状态是否允许走该路径？ | buffer ring 有可用 buffer、send zerocopy 满足 buffer 条件 |

构建期条件决定具体 backend 是否参与编译；其余条件由 `Loop::Init()`、source 创建
或 operation 提交返回 `Result`。luring 不再维护独立的 native capability/profile 层：

```cpp
enum class OperationPath {
  kSingleShot,
  kMultiShot,
  kZeroCopy,
  kFallback,
};
```

最终路径选择可以表达为：

```text
Compiled
  ∧ KernelSupported
  ∧ RingConfigured
  ∧ AdapterImplemented
  ∧ RuntimeEligible
  = OperationPathSelected
```

例如，provided buffer ring 注册失败时，`RecvSource::Create()` 直接返回对应错误；send
zerocopy 的内核错误则从实际 CQE 返回。具体 operation 仍需处理 buffer 耗尽、socket 条件
和提交失败。

特别是：

```text
IORING_OP_RECV 存在
    不等于
IORING_RECV_MULTISHOT 可用
```

当前 RecvSource 使用 provided buffer ring；legacy `provide_buffers` 不属于本模块的运行时路径。

## 3. 当前核心 operation

下表中的“完成”指物理 operation 的 CQE；“业务恢复”指等待该 operation 的协程恢复。

| Operation | 提交方式 | 物理完成 | 业务结果 | 资源释放边界 | 当前状态 |
| --- | --- | --- | --- | --- | --- |
| `recv` / `ReadSome` | 1 个 SQE | 1 个 CQE | CQE 到达后确定 | CQE dispatch 后，await 结束时释放 awaiter frame；buffer 至少存活到 CQE | 已实现，single-shot |
| `send` / `WriteAll` 的内部 request | 1 个 SQE | 1 个 CQE | CQE 到达后确定 | CQE 到达后 buffer 可按普通 send 语义释放 | 已实现，single-shot |
| `accept` / `Accept` | 1 个 SQE；listener 可以同时保持多个 pending accept | 每个 SQE 1 个 CQE | adapter 将 CQE 转换为 `Result<Stream>` 后确定 | 新 stream 转移 fd 所有权；listener pending-accept reservation 在 continuation 前释放 | 已实现，single-shot coupled |
| `connect` / `Connect` | 1 个 SQE | 1 个 CQE | adapter 将 CQE 转换为 `Result<Stream>` 后确定 | stream 接管 fd，或 error 路径关闭 fd；两者均先于 continuation | 已实现，single-shot coupled |
| `link_timeout` / timed read | read SQE + timeout SQE | 最多 2 个 CQE | read 和 timeout 两个物理完成都观察到后确定 | 两个 operation 都结束后 awaiter 才能安全释放；只恢复一次 | 已实现，linked composite |
| fd cancel / `Close` | cancel SQE 加上原有 pending I/O | cancel 和原 I/O 各自产生 CQE | cancel 完成且 pending I/O 全部收敛后确定 | fd 关闭前必须等待相关 operation 收敛；不能只看到 cancel CQE 就销毁 stream | 已实现，composite close |
| timer driver/control | timeout 或 timeout update SQE | 每次提交通常 1 个 CQE | timer queue 内部状态更新 | timer operation 自身完成后复用 | 已实现，内部 operation |
| MSG_RING / wake | notification 或 poll SQE | 特殊 CQE | 不产生普通业务结果 | mailbox/wake 状态按专用协议清理，不走普通网络 operation dispatch | 已实现，内部 operation |

当前 `Loop::HandleCqe()` 对普通 single-shot operation 的行为是：每个 CQE 先通过
`Op::TryRecordCqeCompletion()` 固定物理结果，再交给 adapter 解释为逻辑结果与 release，最后
最多调度一次 `ResumeWork` 并减少一次 `inflight_`。这个行为适合上表中的 single-shot operation；
它不能直接承载 multishot operation。

当前公共 API 不提供 `WritePart` 或 scatter-write 路径。需要保证完整发送
时使用 `stream.WriteAll` 的连续 span；多块 `Buffer` 由业务按 `ContiguousView()` 和 `Drain()`
显式推进，不属于当前 `AsyncStream` 契约。

## 4. 扩展 operation

| 扩展 | 提交与完成 | 业务结果 | buffer / operation 生命周期 | Reactor 解释 | 当前状态 |
| --- | --- | --- | --- | --- | --- |
| multishot accept | 1 个 SQE，多个 CQE；`F_MORE` 表示 operation 继续 | 每个 CQE 产生一个新 stream；无 `F_MORE` 的 CQE 结束 source | source 在终止 CQE、错误或取消收敛后释放；不能每个 CQE 都销毁 operation | readiness 后反复 `accept()`，每次成功 emit 一个 stream | luring 原生实现；不支持时降级 single-shot |
| multishot recv | 1 个 SQE，多个 CQE；每个 CQE 可能产生数据事件 | 每个 CQE 是一个 `RecvEvent`，最终 CQE/错误结束 source | source 持续存活；每个数据 buffer 必须有独立 `BufferLease` | readiness + 非阻塞 `recv()` 循环 emit 事件 | `RecvSource` 使用 provided buffer ring；`RecvSource` 使用 readiness drain + 固定 buffer pool；两者共享 `AsyncRecvSource` |
| legacy provided buffers | 当前 backend 不提交 legacy `provide_buffers` | 不属于当前公共结果契约 | 无 legacy buffer group 所有权规则 | 无对应的 Reactor 语义 | 未实现；当前路径不支持 |
| provided buffer ring | 注册 buffer ring，CQE flags 返回 buffer id；可带 `F_BUF_MORE` | 结果包含字节数、buffer id 和继续消费信息；增量 CQE 使用同一 id 的连续 offset | `BufferLease` 归还 ring 后才能复用；`F_BUF_MORE` 结束前以及所有 segment lease 释放前都不能归还 | 应用层 buffer pool；没有内核选择 id 的等价语义 | `RecvSource` 的非增量路径共享每 worker 一个 ring；逐 source ring 已删除；`F_BUF_MORE` source 路径待后续单独实现 |
| send zerocopy | 一个 send CQE；primary 带 `F_MORE` 时另有 `F_NOTIF` | send CQE 确定发送结果；primary 无 `F_MORE` 或 notification 确定 memory 可复用 | send result 与 buffer release 可分离；`F_MORE` 后不能在 notification 前释放 buffer | 普通 write 完成后释放发送 buffer；没有同等的两阶段 zc 协议 | `Stream::SendZeroCopy` 已实现；按 primary `F_MORE` 选择 terminal 边界 |
| registered fixed buffer | 当前 backend 未提交 registered buffer SQE | 结果仍可为 single-shot 或其它 lifecycle shape | registration 的 owner 必须覆盖所有 in-flight operation | 普通用户 buffer；没有固定 buffer 的相同语义 | 未实现；当前没有公共接口 |
| fixed file | 当前 backend 未提交 fixed-file SQE | 结果语义由具体 operation 决定 | file table slot 释放前不能有引用 | 普通 fd 所有权 | 未实现；当前没有公共接口 |
| linked operations | 多个 SQE 组成一个逻辑操作 | 可能有多个物理 CQE，但业务结果通常只确定一次 | 所有影响结果或资源的 link member 都必须收敛 | Reactor 通过组合 awaiter/状态机模拟 | timed read 已内部使用；通用公共 API 未实现 |

### 4.1 multishot 的恢复规则

multishot 不能把同一个 continuation 对每个 CQE 直接 `resume()`。推荐使用持续 source：

```text
CQE(F_MORE)
  -> 解析一个业务事件
  -> 放入有界 event queue
  -> 如果 consumer 已等待，调度一次 consumer
  -> operation 保持 active

CQE(无 F_MORE)
  -> 记录 source terminal
  -> 不再 re-arm
  -> 等待已产生事件被消费
  -> 最终释放 source
```

业务接口应类似：

```cpp
auto source = listener.CreateAcceptSource();
while (auto event = co_await source.Next()) {
  // consume one accepted stream
}
```

IOCP 可以通过重复投递 one-shot `OVERLAPPED` 操作实现同一业务语义，但不能因此声称它具备
io_uring 原生 multishot 能力。

### 4.2 send zerocopy 的恢复规则

send zerocopy 必须区分 primary result 与 physical terminal；是否存在第二个事件由 primary
`F_MORE` 决定：

```text
send CQE, no F_MORE
  -> 业务发送结果确定
  -> primary 同时是 physical terminal
  -> release buffer，恢复协程

send CQE, F_MORE
  -> 业务发送结果确定
  -> 等待 split-release operation 的 terminal notification

notification CQE(F_NOTIF)
  -> 内核不再使用发送内存
  -> operation 完成，恢复协程；调用方此时可以复用发送 buffer
```

因此不能复用普通内部 send 的“收到一个 CQE 就释放所有状态”规则。
当前显式扩展接口为：

```cpp
auto result = co_await stream.SendZeroCopy(buffer);
// await 返回后，buffer 已离开 io_uring 的发送使用窗口。
```

如果 notification 提供 usage report，接口通过 `ZeroCopySendResult::usage` 标记实际的 copy 或
zerocopy 路径；若 primary 已经 terminal，`usage` 为 `kUnknown`，不能据此推断实际路径。
`notification_received` 记录本次是否确实观察到了独立的 `F_NOTIF` CQE。

## 5. 与 TLA+ 模型的对应关系

现有 `async_stream_core.tla` 明确是 single-shot stream 模型，它的不变量仍然成立：

```text
一个 single-shot physical request 只能提交一次
一个 single-shot physical request 只能完成一次
一个等待者只能恢复一次
```

`async_operation_lifecycle_shapes.tla` 进一步把 physical request 与 logical operation 分开，
并以三个正交维度描述 lifecycle：

```text
Result cardinality : Single | Multiple
Physical convergence: Single | Composite
Release coupling   : Coupled | Split
```

因此 multishot recv + provided buffer 可以同时是 `Multiple + Single + Split`，不需要被塞进
某个互斥 family。每个 Physical Request 仍有自己的终态；Logical Operation 则按其 convergence
和 release coupling 决定何时产生 terminal、释放资源及恢复 continuation。

timed read、close 和 send zerocopy 分别记录：

```text
physical completion count
logical result completion
continuation resume claim
buffer release
operation destruction
```

对应模型文件：

- `formal/async_stream_core.tla`：单个 single-shot stream operation；
- `formal/resource_close_cancel.tla`：resource-level Close barrier、cancel command 与 target
  request 的分离、borrowed storage release，以及 fd release 的 quiescence 条件；
- `formal/stream_shutdown_transaction.tla`：同步 `Shutdown()` 的 write-direction preparation、
  syscall success commit、local-error rollback，以及与 Close preparation 的排斥；
- `formal/async_stream_multiop.tla`：并发 read/write 的 operation identity、owner coroutine
  与 Close 收敛；
- `formal/linked_timeout_submission_failure.tla`：linked read 已提交但 timeout SQE 提交失败时，
  synthetic timeout completion 与 read CQE 的一次性收敛；
- `formal/scheduler_completion_liveness.tla`：在 worker 持续执行 turn 的公平性假设下，
  completion-ready continuation 最终被调度，且 normal ready backlog 不会令其饥饿；
- `formal/async_stream_multiop_backend_refinement.tla`：Reactor readiness 与 io_uring
  SQE/CQE 的内部 stuttering 映射到同一条并发 read/write 可观察 trace；
- `formal/send_zc_split_release_refinement.tla`：`SendZeroCopy` 的 primary CQE、`F_NOTIF`、
  buffer release 与 completion-ready resume 的具体 io_uring refinement；
- `formal/async_stream_backend_refinement.tla`：Reactor 与 io_uring 的 single-shot refinement；
- `formal/async_operation_lifecycle_shapes.tla`：result cardinality、physical convergence
  与 release coupling 的正交组合和 release/resume 授权。
- `formal/accept_source_refinement.tla`：Reactor readiness、io_uring one-shot re-arm 和
  native multishot 三条 AcceptSource 路径的有界业务语义 refinement；
- `formal/recv_source_lease.tla`：provided-buffer multishot recv 的 queue、BufferLease、
  cancel 和 Stop 收敛不变量。
- `formal/recv_source_incremental_lease.tla`：未来 `F_BUF_MORE` source 路径的设计模型；当前
  runtime 未启用该路径。它描述
  同一 provided buffer 的连续 segment、offset 不重叠、最后 segment 与全部 lease 释放后才归还
  buffer ring。

这些 TLA+ 模型主要检查协议级 safety，不是 C++ 实现的自动内存安全证明。
`async_stream_core.tla` 另外在显式的 backend/owner-loop 公平假设下检查 pending settlement、
settled waiter resume 和 Closing 收敛三条活性；这不等于证明真实内核或任意调度策略天然公平。
当前配置使用：

```text
accept_source_refinement.cfg:
  MaxEvents = 2, MaxRequests = 3

recv_source_lease.cfg:
  BufferCapacity = 2, EventCapacity = 2, MaxEvents = 4
```

可用 TLC 复现有界检查：

```bash
tlc docs/design/zh-CN/network/formal/async_stream_core.tla \
  -config docs/design/zh-CN/network/formal/async_stream_core.cfg
tlc docs/design/zh-CN/network/formal/resource_close_cancel.tla \
  -config docs/design/zh-CN/network/formal/resource_close_cancel.cfg
tlc docs/design/zh-CN/network/formal/stream_shutdown_transaction.tla \
  -config docs/design/zh-CN/network/formal/stream_shutdown_transaction.cfg
tlc docs/design/zh-CN/network/formal/async_stream_multiop.tla \
  -config docs/design/zh-CN/network/formal/async_stream_multiop.cfg
tlc docs/design/zh-CN/network/formal/async_operation_lifecycle_shapes.tla \
  -config docs/design/zh-CN/network/formal/async_operation_lifecycle_shapes.cfg
tlc docs/design/zh-CN/network/formal/linked_timeout_submission_failure.tla \
  -config docs/design/zh-CN/network/formal/linked_timeout_submission_failure.cfg
tlc docs/design/zh-CN/network/formal/scheduler_completion_liveness.tla \
  -config docs/design/zh-CN/network/formal/scheduler_completion_liveness.cfg
tlc docs/design/zh-CN/network/formal/async_stream_multiop_backend_refinement.tla \
  -config docs/design/zh-CN/network/formal/async_stream_multiop_backend_refinement.cfg
tlc docs/design/zh-CN/network/formal/send_zc_split_release_refinement.tla \
  -config docs/design/zh-CN/network/formal/send_zc_split_release_refinement.cfg
tlc docs/design/zh-CN/network/formal/accept_source_refinement.tla \
  -config docs/design/zh-CN/network/formal/accept_source_refinement.cfg
tlc docs/design/zh-CN/network/formal/recv_source_lease.tla \
  -config docs/design/zh-CN/network/formal/recv_source_lease.cfg
tlc docs/design/zh-CN/network/formal/recv_source_incremental_lease.tla \
  -config docs/design/zh-CN/network/formal/recv_source_incremental_lease.cfg
```

当前模型覆盖的核心 safety 条件是：

```text
AcceptSource:
  event 至多交付一次；terminal 至多观察一次；Stop 后不再 admission；
  Reactor / UringSingle / UringMulti 都只能通过 Terminal/Draining 收敛。

RecvSource:
  available、queued、leased buffer 两两不重叠且覆盖 buffer pool；
  queue 和 lease 受容量限制；Stop 完成前没有 outstanding lease；
 cancel CQE 不替代 recv request 自己的 terminal CQE。

Incremental RecvSource (`F_BUF_MORE`):
  同一 provided buffer 的 segment 区间不重叠；terminal CQE 前必须结束其
  incremental buffer；只有 final segment 已观察且该 buffer 的全部 segment
  lease 已释放，buffer 才能归还 ring。
```

`scheduler_completion_liveness.tla` 的 liveness 是条件性的：它假设 worker 未退出且持续执行
`RunReady()` turn。它证明 completion-ready 队列的选择规则不会被 normal ready backlog 饿死；
它不替代内核、线程或进程调度的系统级公平性保证。

## 6. 实施顺序

1. 为 `CompletionEvent` 和 operation-specific handler 建立不改变现有 ABI 的内部测试模型。（已完成。）
2. 覆盖 immediate、普通 CQE、重复 CQE、cancel-before-complete、complete-before-cancel、
   close 与 pending I/O 交错。（已完成核心路径和提交失败注入。）
3. 让具体 operation 直接处理 runtime unsupported、资源耗尽和提交失败，不再引入第二套
   native capability/profile 层。（已完成。）
4. 单独实现 `AcceptSource`，保留现有一次性 `Accept()`。（已完成。）
5. 实现带 `BufferLease` 的 multishot recv。（luring 使用每 worker 共享的 provided-buffer ring，
   Reactor 使用 readiness source；当前两条路径都完成了 lease safety，`F_BUF_MORE` 增量消费仍是
   后续独立设计。）
6. 实现 send zerocopy 的结果与条件式 release 生命周期。（`Stream::SendZeroCopy`
   已完成；primary `F_MORE` 时等待 notification，否则 primary 本身终态；Reactor 保持普通 send
   语义，不伪造 zerocopy notification。）

## 参考

- [io_uring multishot operations](https://man7.org/linux/man-pages/man7/io_uring_multishot.7.html)
- [io_uring provided buffers](https://man7.org/linux/man-pages/man7/io_uring_provided_buffers.7.html)
- [io_uring multishot recv](https://man7.org/linux/man-pages/man3/io_uring_prep_recv.3.html)
- [io_uring send zerocopy](https://man7.org/linux/man-pages/man3/io_uring_prep_send_zc.3.html)

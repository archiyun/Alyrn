# io_uring Operation Matrix

本文档记录 CoroPact 当前以及计划支持的 io_uring operation 语义。
它描述的是 luring 适配层的完成协议，不是把 io_uring 原始 SQE/CQE 暴露给业务代码。

当前公共网络接口仍然是 `AsyncStream`、`AsyncListener` 和一次性
`Task<Result<T>>` 操作。multishot、provided buffer、send zerocopy 等能力会改变完成次数、
结果类型或 buffer 生命周期，因此必须通过独立的扩展接口表达，不能伪装成普通
`ReadSome()` 或 `WriteSome()`。

持续 accept 的具体语义见 [AcceptSource 语义契约](accept-source-contract.md)。

## 1. 完成协议的基本区分

一个原始 CQE 只携带后端事件：

```text
CompletionEvent = { cqe_res, cqe_flags, user_data }
```

operation family 再解释这个事件。不要把 `terminal` 直接当成 CQE 的属性，因为下面几件
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
- send zerocopy 的 send CQE 可以确定发送结果，但 notification CQE 才确定 buffer 可以复用；
- close 的 cancel CQE 到达后，原有 read/write CQE 仍然可能在后面到达。

因此第一阶段的 loop 只应接收原始 CQE，并交给 operation family handler；handler 再决定这
次事件是否产生业务事件、是否保持 operation、以及何时提交恢复工作。

## 2. 能力矩阵与运行期路径选择

`IoCapability` 的一个标记不能同时代表所有可用性。每项能力至少要经过以下五层：

| 维度 | 要回答的问题 | 例子 |
| --- | --- | --- |
| 编译期 | 当前 liburing 头文件是否提供接口或 flag？ | `IORING_OP_SEND_ZC`、`IORING_CQE_F_MORE` |
| 内核期 | 当前 kernel/ring 是否接受该 opcode 或注册操作？ | probe 中存在 `IORING_OP_RECV` |
| 配置期 | 当前 ring setup 是否满足使用条件？ | SQPOLL、CQ 大小、buffer ring 注册 |
| 语义期 | 当前适配器是否实现了结果、取消和资源生命周期？ | multishot 的重复事件与终止 CQE |
| 运行期 | 当前 operation 的对象、资源和状态是否允许走该路径？ | buffer ring 有可用 buffer、send zerocopy 满足 buffer 条件 |

前四层决定能力是否可以进入 active profile；运行期条件不应写入静态
`CapabilitySet`，而应由 operation path selector 在每次提交前判断：

```cpp
enum class OperationPath {
  kSingleShot,
  kMultiShot,
  kProvidedBuffer,
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

例如，capability 可能支持 provided buffer ring，但当前 ring 暂时没有可用 buffer；这时
本次操作应选择 `kFallback` 或返回明确的资源耗尽结果，而不是修改全局 capability。

特别是：

```text
IORING_OP_RECV 存在
    不等于
IORING_RECV_MULTISHOT 可用
```

同样，legacy `provide_buffers` 与 registered provided buffer ring 不是同一项能力；应用层
复用 buffer 也不等于内核选择 buffer。

## 3. 当前核心 operation

下表中的“完成”指物理 operation 的 CQE；“业务恢复”指等待该 operation 的协程恢复。

| Operation | 提交方式 | 物理完成 | 业务结果 | 资源释放边界 | 当前状态 |
| --- | --- | --- | --- | --- | --- |
| `recv` / `ReadSome` | 1 个 SQE | 1 个 CQE | CQE 到达后确定 | CQE dispatch 后，await 结束时释放 awaiter frame；buffer 至少存活到 CQE | 已实现，single-shot |
| `send` / `WriteSome` | 1 个 SQE | 1 个 CQE | CQE 到达后确定 | CQE 到达后 buffer 可按普通 send 语义释放 | 已实现，single-shot |
| `sendmsg` / `WriteSome(parts)` | 1 个 SQE | 1 个 CQE | CQE 到达后确定 | `msghdr`、iovecs 和发送片段必须存活到 CQE | 已实现，single-shot |
| `accept` / `Accept` | 1 个 SQE；listener 可以同时保持多个 pending accept | 每个 SQE 1 个 CQE | CQE 返回新 fd 后确定 | accept operation 和临时地址存储在 CQE dispatch 后释放；新 stream 转移 fd 所有权 | 已实现，single-shot |
| `connect` / `Connect` | 1 个 SQE | 1 个 CQE | CQE 到达后确定 | connect operation 完成后释放 | 已实现，single-shot |
| `link_timeout` / timed read | read SQE + timeout SQE | 最多 2 个 CQE | read 和 timeout 两个物理完成都观察到后确定 | 两个 operation 都结束后 awaiter 才能安全释放；只恢复一次 | 已实现，linked composite |
| fd cancel / `Close` | cancel SQE 加上原有 pending I/O | cancel 和原 I/O 各自产生 CQE | cancel 完成且 pending I/O 全部收敛后确定 | fd 关闭前必须等待相关 operation 收敛；不能只看到 cancel CQE 就销毁 stream | 已实现，composite close |
| timer driver/control | timeout 或 timeout update SQE | 每次提交通常 1 个 CQE | timer queue 内部状态更新 | timer operation 自身完成后复用 | 已实现，内部 operation |
| MSG_RING / wake | notification 或 poll SQE | 特殊 CQE | 不产生普通业务结果 | mailbox/wake 状态按专用协议清理，不走普通网络 operation dispatch | 已实现，内部 operation |

当前 `LUringLoop::HandleCqe()` 对普通 single-shot operation 的行为是：每个 CQE 减少一次
`inflight_`，调用一次 `LUringOp::Complete()`，然后最多调度一次 `ResumeWork`。这个行为适合
上表中的 single-shot operation；它不能直接承载 multishot operation。

## 4. 计划中的扩展 operation

| 扩展 | 提交与完成 | 业务结果 | buffer / operation 生命周期 | Reactor 解释 | 当前状态 |
| --- | --- | --- | --- | --- | --- |
| multishot accept | 1 个 SQE，多个 CQE；`F_MORE` 表示 operation 继续 | 每个 CQE 产生一个新 stream；无 `F_MORE` 的 CQE 结束 source | source 在终止 CQE、错误或取消收敛后释放；不能每个 CQE 都销毁 operation | readiness 后反复 `accept()`，每次成功 emit 一个 stream | 未实现；当前 opcode probe 不能证明该语义可用 |
| multishot recv | 1 个 SQE，多个 CQE；每个 CQE 可能产生数据事件 | 每个 CQE 是一个 `ReadEvent`，最终 CQE/错误结束 source | source 持续存活；每个数据 buffer 必须有独立 lease | readiness + `recv()` 循环 emit 事件 | 未实现；不得塞进 `ReadSome` |
| legacy provided buffers | 注册 buffer group，CQE 返回 buffer id | 结果包含字节数和 buffer id | consumer 归还 buffer 前不得重新提供 | buffer pool 由应用选择和管理 | 仅有 opcode 探测；无公共 API |
| provided buffer ring | 注册 buffer ring，CQE flags 返回 buffer id；可带 `F_BUF_MORE` | 结果包含字节数、buffer id 和继续消费信息 | `BufferLease` 归还 ring 后才能复用 | 应用层 buffer pool；没有内核选择 id 的等价语义 | 未实现；应与 legacy provided buffers 分开 |
| send zerocopy | 一个 send CQE，可能另有 `F_NOTIF` notification CQE | send CQE 确定发送结果；notification 确定 memory 可复用 | send result 和 buffer release 是两个边界；notification 之前不能释放 buffer | 普通 write 完成后释放发送 buffer；没有同等的两阶段 zc 协议 | 仅有 opcode 探测；无公共 API |
| registered fixed buffer | 使用注册 buffer 的固定索引/切片 | 结果仍可为 single-shot 或其它 family | registration 的 owner 必须覆盖所有 in-flight operation | 普通用户 buffer；没有固定 buffer 的相同语义 | capability 枚举存在，未实现 |
| fixed file | SQE 使用注册 file slot | 结果语义由具体 operation 决定 | file table slot 释放前不能有引用 | 普通 fd 所有权 | capability 枚举存在，未实现 |
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
auto source = listener.AcceptSource();
while (auto event = co_await source.Next()) {
  // consume one accepted stream
}
```

IOCP 可以通过重复投递 one-shot `OVERLAPPED` 操作实现同一业务语义，但不能因此声称它具备
io_uring 原生 multishot 能力。

### 4.2 send zerocopy 的恢复规则

send zerocopy 至少要区分两个事件：

```text
send CQE
  -> 业务发送结果确定
  -> 可选择恢复等待发送结果的协程

notification CQE(F_NOTIF)
  -> 内核不再使用发送内存
  -> 释放 BufferLease
```

因此不能复用普通 `WriteSome()` 的“收到一个 CQE 就释放所有状态”规则。

## 5. 当前 capability 探测的注意事项

当前实现位置：`src/luring/capabilities.cc`。

| 当前标记 | 当前做法 | 问题 |
| --- | --- | --- |
| `kMultishotAccept` | 仅探测 `IORING_OP_ACCEPT` | opcode 存在不代表 accept multishot flag 和终止语义可用 |
| `kMultishotRecv` | 仅探测 `IORING_OP_RECV` | opcode 存在不代表 `IORING_RECV_MULTISHOT` 可用 |
| `kProvidedBuffer` | 探测 `PROVIDE_BUFFERS` / `REMOVE_BUFFERS` | 只覆盖 legacy 机制，未覆盖 provided buffer ring |
| `kSendZeroCopy` | 探测 `IORING_OP_SEND_ZC` | 没有 send CQE 与 notification CQE 的生命周期实现 |
| `kLinkedOps` | 当前无条件启用 | timed read 有内部使用，但通用 linked-operation API 尚未定义 |
| `kRegisteredBuffer` | capability 枚举存在 | 当前 probe 和公共接口都未完成 |
| `kFixedFile` | capability 枚举存在 | 当前 probe 和公共接口都未完成 |

在这些问题修正前，extension capability 只能作为探测信息，不能作为业务 active profile 的
充分条件。尤其不能让业务看到 `kMultishotRecv` 后就假设 `ReadSource` 已经可以使用。

## 6. 与 TLA+ 模型的对应关系

现有 `async_stream_core.tla` 明确是 single-shot stream 模型，它的不变量仍然成立：

```text
一个 single-shot physical request 只能提交一次
一个 single-shot physical request 只能完成一次
一个等待者只能恢复一次
```

`async_operation_families.tla` 进一步把 physical request 与 logical operation 分开：

```text
SingleShotRequest:
    submitCount <= 1
    cqeCount <= 1

MultiShotRequest:
    submitCount <= 1
    cqeCount >= 0
    terminalCqeCount <= 1

CompositeOperation:
    physicalRequestCount = 2
    每个 member 各自只 submit 一次
    logicalTerminalCount <= 1

LogicalEventSource:
    eventCount >= 0
    terminalCount <= 1
    terminal 后不能产生新事件
```

因此，“一次 physical request 只能完成一次”只适用于 single-shot request；multishot 的
physical request 可以产生多个 CQE，但只能有一个终止 CQE。Composite 则是多个
single-shot physical request 的聚合，不能把聚合计数误当成一个 request 的 CQE 计数。

timed read、close 和 send zerocopy 还需要分别记录：

```text
physical completion count
logical result completion
continuation resume claim
buffer release
operation destruction
```

对应模型文件：

- `formal/async_stream_core.tla`：单个 single-shot stream operation；
- `formal/async_stream_backend_refinement.tla`：Reactor 与 io_uring 的 single-shot refinement；
- `formal/async_operation_families.tla`：single-shot、multishot、composite 的完成基数。

## 7. 实施顺序

1. 为 `CompletionEvent` 和 operation family handler 建立不改变现有 ABI 的内部测试模型。
2. 覆盖 immediate、普通 CQE、重复 CQE、cancel-before-complete、complete-before-cancel、
   close 与 pending I/O 交错。
3. 修正 capability 探测，拆分 multishot、legacy provided buffers 与 provided buffer ring；
   再为实际提交增加运行期 path selector。
4. 单独实现 `AcceptSource`，保留现有一次性 `Accept()`。
5. 实现带 `BufferLease` 的 multishot recv。
6. 最后实现 send zerocopy 的结果与 notification 双阶段生命周期。

## 参考

- [io_uring multishot operations](https://man7.org/linux/man-pages/man7/io_uring_multishot.7.html)
- [io_uring provided buffers](https://man7.org/linux/man-pages/man7/io_uring_provided_buffers.7.html)
- [io_uring multishot recv](https://man7.org/linux/man-pages/man3/io_uring_prep_recv.3.html)
- [io_uring send zerocopy](https://man7.org/linux/man-pages/man3/io_uring_prep_send_zc.3.html)

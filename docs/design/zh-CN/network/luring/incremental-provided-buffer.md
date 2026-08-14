# 增量 provided-buffer 的 LRCI 精化

## 状态

已实现为 capability-gated 的 luring refinement：支持它的 host 为每个
`LUringRecvSource` 创建私有 incremental buffer group；不支持时回退到既有的 loop-shared
one-CQE/one-slot pool。本文件不改变 `net::RecvEvent`、`BufferLease` 或 backend-neutral
`RecvSource` contract，也不承诺所有 io_uring host 都具备该能力。

## 目标与非目标

Linux 的 incremental provided-buffer ring（`IOU_PBUF_RING_INC`）可以让一个内核 slot 在
多次 CQE 中返回不重叠的 byte range。它的价值是减少重挂 buffer 和 ring 写入；它不是把一个
CQE 解释为多个业务事件，也不是通用的 zero-copy buffer API。

本设计只为 `LUringRecvSource` 增加一个 capability-gated backend refinement：

```text
one provided-buffer slot
  -> CQE(segment 0, F_BUF_MORE)
  -> CQE(segment 1, F_BUF_MORE)
  -> CQE(segment N, no F_BUF_MORE)
  -> slot returns to the ring exactly once
```

不做以下事情：

- 不扩大 `AsyncRecvSource`，不要求 Reactor 或 kqueue 模拟 incremental buffer；
- 不把 `IOU_PBUF_RING_INC`、bundle 开关或每轮预算加入 `LUringOptions`；
- 不允许一个 `LUringRecvSource` 直接操作另一个 loop 的 slot；
- 不把 `F_BUF_MORE` 当作 multishot request 的 `F_MORE`。前者描述 buffer 的物理借用，后者
  描述 recv request 是否仍然 active，两者独立。

## 逻辑模型

`RecvSource` 仍然是一个 Multiple-result、Single-physical-request、Split-release 的 Logical
Operation。区别在于一项 Physical Request 可对同一个 slot 产生多项 Backend Event。

```text
CQE(segment)
  -> validate slot and byte range                         [Backend Event]
  -> create one RecvEvent{BufferLease(segment)}           [Result Readiness]
  -> authorize the waiting Next continuation once         [Continuation Authorization]
  -> wait for both kernel-final and all segment leases
  -> return slot to provided-buffer ring                   [Release Authorization]
```

一个 slot 的状态由 luring adapter 持有，不进入 `operation/detail`：

```text
Available
  -> KernelBorrowed(offset = 0, application_leases = 0)
  -> KernelBorrowed(offset += CQE.res, application_leases += 1)
       -- each F_BUF_MORE CQE
  -> KernelDone(offset += CQE.res, application_leases += 1)
       -- final CQE, no F_BUF_MORE
  -> Reclaimable(application_leases = 0)
  -> Available
```

`KernelDone` 与 `application_leases == 0` 缺一不可。前者说明内核不会再访问该 slot；后者
说明应用不再能通过 `BufferLease::Bytes()` 观察它。只有两者同时满足，adapter 才能调用
`io_uring_buf_ring_add()`。因此 `BufferLease` 的析构不是 slot 立即归还的充分条件。

## 所有权与计数

现有 `RecvSourceStateMachine::outstanding_leases_` 继续按**业务 segment lease**计数，而不是
按 slot 计数：每个 `RecvEvent` 产生一个 lease，交给 `Next()` 的 continuation 前就必须完成
一次 increment。这样既保留既有 stop/drain 语义，也让一个 slot 的多个 segment 都阻止 source
提前终态。

新增的 loop-affine `IncrementalSlotState` 至少记录：

```text
buffer_id
next_offset
application_lease_count
kernel_done
```

一个私有 pool 的 slot 只会在所有 segment lease 都释放、且 kernel 已经结束对它的使用后才重新
发布；因此没有陈旧 lease 可以跨越同一 id 的两代所有权。`BufferLease` 的现有 callback 只做
owner-thread 的 decrement，最后一个 lease 才尝试 `ReturnIfReclaimable()`。公开
`BufferLease` 不需要暴露 offset、generation 或 ring 指针。

## buffer group 的归属

增量 slot 不能放进现有的 loop-shared provided-buffer ring。用两个并发
`recv_multishot` 对同一个 `IOU_PBUF_RING_INC` group 的裸 io_uring probe 已验证：两个请求都可能
获得相同的 `bid`，各自从 offset 0 开始。这符合 incremental ring 的“内核与应用共同维护 read
index”模型，却不能提供 `LUringRecvSource` 所需的独占 byte ownership。

因此增量模式必须使用：

```text
one LUringRecvSource
  -> one private buffer group
  -> one private ProvidedBufferPool
  -> one owner-local slot-state table
```

现有 `LUringLoop` shared pool 继续仅服务 non-incremental source。它的 pool-global buffer id 在
普通 selected-buffer 语义下安全；不能混入 `IOU_PBUF_RING_INC`。这使增量模式的内存成本按 source
计，而不是按 worker 计；现有 `buffer_capacity` 仍是每个 source 的明确上限，不增加新的调优开关。

事件队列从 `{buffer_id, size}` 扩展为 `{buffer_id, offset, size}`。direct handoff 和 queued
handoff 必须走相同的 segment lease 构造路径，不能让 direct path 绕过 lease 计数。

## CQE 解释规则

对正 `res` 的 CQE：

1. 验证 `IORING_CQE_F_BUFFER`、buffer group、slot owner 与 `res <= remaining slot bytes`；
2. 若是该 slot 的首个 CQE，取得 pool ownership；若非首个 CQE，必须匹配当前 source 与 generation；
3. 以当前 `next_offset` 创建 segment，随后递增 offset 和 source 的 outstanding lease；
4. `F_BUF_MORE` 存在时 slot 保持 `KernelBorrowed`；不存在时先标记 `kernel_done`，再尝试归还；
5. request 的 `F_MORE` 独立决定 recv SQE 是否 terminal、inflight 是否递减以及是否需要重挂请求。

下列情况是 protocol error，必须停止 source；若 slot 已被取得，仍要按上述双边界收敛：

- `F_BUF_MORE` 但没有有效 selected buffer；
- 同一 active slot 的 buffer id 或 generation 改变；
- segment 越过 slot 容量，或 final 前累计 offset 不单调；
- 一个已 `kernel_done` 的 slot 再收到 segment；
- cancel/terminal CQE 到来时，仍存在未解释的 buffer continuation。

`res == 0` 和负 `res` 不产生业务 segment。只有 target recv CQE 的 `F_MORE` 消失才证明该
request terminal；cancel acknowledgement 只证明 cancel request 自身 terminal。target request
terminal 时，adapter 必须先让所有 `KernelBorrowed` slot 进入 `KernelDone`（或报告 protocol
error），再走现有 stop/drain 路径。

## 停止、暂停与释放

`RequestStop()` 仍只停止 admission 并提交 cancel；`Stop()` 的完成条件扩展为：

```text
recv request terminal
AND cancel request terminal (if submitted)
AND queued events consumed/discarded
AND every incremental slot is KernelDone
AND outstanding segment leases == 0
```

暂停只阻止新 recv admission；它不得通过归还仍处于 `KernelBorrowed` 的 slot 来制造容量。若
应用长期持有早期 segment，后续 CQE 仍可产生 segment，但 `event_capacity` 与
`buffer_capacity` 的既有背压会限制 source。capacity 在这个 extension 中仍表示最大未释放的
**业务 lease 数**，不是内核 slot 数。

## 能力与降级

初始化时以 private buffer-group 的 `IOU_PBUF_RING_INC` allocation 作为 capability probe。
该 allocation 被内核拒绝时，`LUringRecvSource` 保持现有 shared one-CQE/one-slot 路径；它不是
错误，也不改变公开返回值。能力选择属于 `luring/detail` 的固定 backend policy，不能泄漏为
Runtime Builder 开关。

## 实施顺序与验证

1. 保留 loop-shared pool 的 non-incremental 语义；为 source-private group 建立独立 allocation /
   destruction protocol，并限制其按 source 计的内存成本。
2. 让 `LUringRecvSource` 的 queued/direct 两条路径统一构造 segment lease；补齐 stop drain。
3. capability-gated 地在 private group 接受 `F_BUF_MORE`，保留 shared-pool fallback。
4. 在支持该特性的 Linux host 上验证：同 slot 两段、lease 乱序释放、final CQE 先于 lease 释放、
   cancel 与 active slot、source stop 与 queued segment。
5. 保留既有 non-incremental RecvSource 测试作为回归；不为内部字段新增 test-hook 或公开调优开关。

这些测试只观察 `RecvEvent`、`Stop()`、slot 归还和一次性 continuation，不直接断言内部 CQE
dispatch 顺序。

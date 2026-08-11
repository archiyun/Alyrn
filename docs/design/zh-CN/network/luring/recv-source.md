# multishot recv 与 provided buffer

`LUringRecvSource` 是 luring 的持续接收扩展。它用一个 io_uring multishot recv request
和 provided-buffer ring 产生多个 `RecvEvent`，将“持续收包”与“消费一个 buffer”分开。

## 创建与配置

source 通过 `LUringRecvSource::Create(loop, fd, options)` 创建，socket fd 由调用方借用，
source 不负责关闭它。

当前公共路径的关键约束是：

- `pending_depth` 必须为 1；
- `buffer_capacity` 至少覆盖 `event_capacity`，且当前实现要求为不超过 32K 的 2 的幂；
- `buffer_size` 必须大于 0，并且和 loop 的 shared buffer ring 配置一致；
- loop 的 `shared_buffer_capacity` 是该 worker 上所有 recv source 的聚合上限。

这些是当前实现约束，不是 io_uring 原始 API 的全部能力。

## 一个事件的生命周期

```text
kernel recv
  -> CQE.res = payload size
  -> CQE flags 提供 buffer id
  -> source 创建 RecvEvent
  -> BufferLease 借出一个 slot
  -> Next() 把事件交给 consumer
  -> lease 析构或 Release()
  -> slot 归还 provided-buffer ring
```

`BufferLease` 是 move-only 的。它的 `Bytes()` 是只读视图，`BufferId()` 只用于诊断或测试，
业务不应自行按 id 归还。正确的归还动作是释放 lease。

## 背压与事件队列

source 既有 event queue，也有 lease ownership 计数：

```text
事件队列达到 event_capacity
  -> 暂停后端 admission / 取消当前物理 request
  -> 继续消费已经产生的事件
  -> 队列降到 resume_threshold 以下
  -> 等待旧 request terminal 后重新 arm
```

高水位暂停是可恢复状态，不是 source terminal。已交给业务的 lease 仍然算作 outstanding，
所以 source 的资源上下文必须活到所有 lease 释放。

## `F_MORE` 与 `F_BUF_MORE`

- `F_MORE` 表示 multishot recv request 仍然 active；没有它的 CQE 是 request terminal。
- `F_BUF_MORE` 表示一个 provided buffer 还有增量 segment。当前 `LUringRecvSource` 对外
  只承诺完整 slot 事件；逐 segment 的增量 source 尚未作为公共 API 发布。

因此当前测试不能把每个 `F_BUF_MORE` CQE 当成可以立即归还同一个 slot 的独立完整 buffer。
未来如果开放增量语义，必须等最后 segment 且全部 segment lease 释放后才归还 slot。

## Stop

`RequestStop()` 只停止新事件 admission，可以立即返回。`Stop()` 才等待：

```text
terminal request
  + queued event drain
  + outstanding BufferLease == 0
  -> source stop 完成
```

这条边界确保 provided buffer ring 的 payload 内存不会在 consumer 仍读取时被重新分配给
kernel。

对于 luring，`RequestStop()`/`Stop()` 在撤销 admission 后准备 cancel SQE。若这一步本地失败，
调用返回 error，但 source 保持 `Stopping` 且仍拥有旧 recv request/lease 上下文；它不会回滚为
Active，也不能立刻析构。owner 必须保留 source 并重试 `Stop()`，直到 terminal request、queued event
和 outstanding lease 都收敛。

## 这是不是“零拷贝”

provided buffer 让 kernel 直接写入 luring 管理的接收 slot，避免了“先读到临时 buffer、再
复制到业务 buffer”的额外路径。但它不保证 NIC 到最终业务对象的端到端零拷贝，也不等于
`SendZeroCopy`。它首先解决的是 buffer 复用和 ownership，而不是宣传一个绝对的性能结论。

## 测试观察点

- 收到的数据与 `BufferLease::Size/Bytes` 一致；
- lease move、Release、析构都只归还一次 slot；
- event queue 满时暂停，消费到低水位后恢复；
- terminal 不丢弃已经产生的事件；
- `Stop()` 在最后一个 outstanding lease 释放前不会完成；
- shared pool 的容量是 per-worker aggregate，而不是每个 source 独占一份。

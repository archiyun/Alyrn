# luring 功能说明

`luring` 是 Alyrn 基于 Linux `io_uring` 的网络 backend。它向上提供协程友好的
网络对象，业务代码不需要直接操作 SQE、CQE 或 `io_uring_enter()`。

本文档组描述的是外部可观察的行为：什么时候协程恢复、什么时候结果确定、什么时候
buffer 可以复用，以及关闭和取消如何收敛。内部类、CQE 分发器和 ring 布局不属于稳定
业务接口。

## 功能状态

| 功能 | 对外入口 | 状态 | 首要测试目标 |
| --- | --- | --- | --- |
| stream I/O | `ReadSome`、`ReadInto`、`WriteAll` | 稳定 | 单次结果、完整写入、错误和 buffer 生命周期 |
| 超时读 | `ReadSomeFor` | 稳定扩展 | read/timeout 两个 CQE 只恢复一次 |
| listener / 单次 accept | `Accept` | 稳定 | 新连接所有权与关闭 |
| 持续 accept | `AcceptSource`、`AcceptMode::kMultishot` | 已实现扩展 | 多事件、终止 CQE、背压、降级 |
| 持续 recv | `RecvSource` | 已实现扩展 | provided buffer、事件队列、`BufferLease` |
| provided buffer ring | `RecvSource` 的内部资源 | 已实现扩展 | loop-shared slot 借出、归还与 source 停止 |
| 发送 zerocopy | `SendZeroCopy` | 已实现扩展 | primary CQE 与可选 `F_NOTIF` 的分离式 release |
| timer | `SleepFor`、`RunAfter` | 已实现 | 到期恢复、错误和 loop 归属 |
| 多 worker / CPU 绑定 | `WorkerGroup` | 已实现 | 每 worker 一个 loop、启动和停止 |
| 跨 worker 通知 | `PostMessage` / `MSG_RING` | runtime 内部能力 | 有界 mailbox、通知合并、目标 loop 投递 |
| fixed registered buffer | 暂无公共入口 | 设计占位 | 不应误标为已支持 |
| fixed file / 通用 linked API | 暂无公共入口 | 设计占位 | 不应从内部实现泄露为 API |

## 从提交到恢复

所有 luring operation 都可以先用下面的通用模型理解：

```text
业务协程
  -> 创建逻辑 operation
  -> 准备 SQE
  -> 批量提交到 ring
  -> kernel 执行
  -> 收到一个或多个 CQE
  -> operation-specific lifecycle 解释 CQE
  -> 结果确定 / 资源释放
  -> 最多恢复一次等待中的协程
```

single-shot 通常是一条 SQE 对一条 CQE；multishot、超时和 zerocopy 会产生多个物理
事件，因此必须经过自己的状态机。业务接口看到的是逻辑结果，不是 CQE 数量。

对于普通的 single-shot awaiter，loop 的内部顺序也被固定为：

```text
CQE
  -> Op 记录原始 CQE result
  -> operation adapter 解释结果，并释放 pending stream slot / buffer reservation
  -> loop 将 ResumeWork 放入 completion queue
  -> coroutine await_resume()
```

因此 coroutine 恢复后可以立刻开始同方向的下一次 stream operation；它不应因为上一项
operation 的 slot 尚未释放而得到 `EBUSY`。这个规则只适用于 coupled single-shot 路径。
timed read、close、multishot source 和 zerocopy 各自根据其 composite、event-source 或
split-release 生命周期决定何时进入最后两步。

## 生命周期上的硬规则

- `submit` 返回不等于 kernel 已经停止访问用户 buffer。
- 同一个等待者最多恢复一次；同一个逻辑 operation 也只能释放一次。
- `Cancel` 或 `Close` 只是开始收敛，不能单独作为资源释放依据。
- `Stream`、`Listener` 和 source 都是 loop-affine，只能在所属 loop 线程上
  操作。
- `BufferLease`、zerocopy 发送 buffer 和 operation frame 的存活边界彼此独立，不能用
  “某一个 CQE 已经回来”替代全部边界。

## 文档入口

- [operation 生命周期与完成分层](operation-lifecycle.md)
- [loop、ring 与 worker](loop-and-workers.md)
- [stream 操作](stream.md)
- [listener 与 AcceptSource](listener-and-accept.md)
- [multishot recv 与 provided buffer](recv-source.md)
- [增量 provided-buffer 的设计占位](incremental-provided-buffer.md)
- [SendZeroCopy](zero-copy-send.md)
- [timer、超时与停止](timers-and-timeouts.md)
- [注册资源的边界](registered-resources.md)
- [跨 worker mailbox](cross-worker-mailbox.md)
- [io_uring operation 语义矩阵（内部设计对照）](../luring-operation-matrix.md)

## 测试框架的切片方式

测试应优先从这些 public seam 开始，而不是直接构造内部 `Op`：

1. loop 初始化成功、环境不支持时可识别地 skip、错误时不泄漏。
2. single-shot operation 的结果、错误、关闭和一次性恢复。
3. source 的事件顺序、终止、背压和资源归还。
4. zerocopy 的发送结果与 buffer release 分离。
5. worker group 的启动、停止、loop 线程归属和可选 CPU affinity。

每个集成测试都应区分“内核/容器不支持导致的 skip”和“Alyrn 语义失败”。

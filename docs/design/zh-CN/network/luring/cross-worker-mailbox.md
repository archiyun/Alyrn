# 跨 worker mailbox

跨 worker 通知是 luring runtime 的内部能力，用于把工作投递到目标 `LUringLoop`，不用于
跨线程直接操作 `LUringStream` 或转移其 fd 所有权。

## 路径

```text
source worker
  -> target.PostMessage(message)
  -> 有界 MPSC mailbox
  -> 首个消息申请一次 MSG_RING notification
  -> target loop 收到 CQE
  -> Drain mailbox
  -> 在 target loop 上 Schedule/ScheduleCompletion
```

多个 producer 的消息会合并通知，避免每个消息都提交一个唤醒请求。mailbox 满时返回
`kFull`，调用方必须决定丢弃、重试或施加背压；runtime 不会无限增长队列。

## loop 归属

message 携带的是可由目标 loop 解释的数据，不是可以在任意线程调用的对象句柄。需要访问
stream、listener 或 source 的工作，必须在目标 loop 上执行，并遵守对象自身的 lifetime。

kernel 不支持 `IORING_OP_MSG_RING` 时，测试应报告环境 skip 或使用已有 wake/fallback
路径；不能把容器 seccomp 拒绝误认为 mailbox 逻辑失败。

## 测试观察点

- 第一个消息返回“需要通知”，后续消息合并为“已排队”；
- mailbox 满时有界失败；
- 通知提交失败后可以重试而不会永久卡住 `notification_pending`；
- 目标 loop 在 owner thread 执行 work；
- drain 过程中并发 producer 不丢消息、不重复通知。

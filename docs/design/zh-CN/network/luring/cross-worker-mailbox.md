# 跨线程投递（Loop::Post）

`epoll::Loop` 与 `uring::Loop` 都提供线程安全的 `Post(callback)`：任意线程都可以把一个回调交给
目标 Loop，回调在 owner 线程上、以该 Loop 为当前 scheduler 的上下文执行。它用于把工作投递到
目标 Loop，不用于跨线程直接操作 `Stream`，也不转移 fd 的所有权。

## 语义

- 同一线程的多次投递按投递顺序执行；不同线程之间不保证相对顺序。
- 成功的 `Post` 恰好执行一次。Loop 关停时，在最终排空结束前接受的投递仍会执行。
- 排空结束后 `Post` 返回 `operation_canceled`，回调不会执行，并在调用线程上析构。
- 队列无界。生产者需要背压时，由应用自己限制在途投递的数量。
- Loop 对象必须比所有 `Post` 调用活得久，这是调用方的同步责任（见下文的注册表示例）。
  从未运行就被销毁的 Loop 会析构队列中的回调而不执行。
- 回调可以操作 owner-affine 的 Channel，也可以 `Spawn` / `SpawnDetach` 根任务；关停排空阶段
  执行的回调不能发起新的 I/O。

## 路径

```text
producer thread
  -> target.Post(callback)
  -> 互斥锁保护的队列；队列由空变非空的那次投递写一次 wakeup eventfd，其余投递合并
  -> target loop 观察到 eventfd 可读（epoll：readiness；uring：常驻 POLL_ADD 的 CQE）
  -> 在 owner 线程、Loop 的调度上下文中批量执行
```

`Run()` 返回前反复排空队列直到为空，并在同一个临界区内关闭队列，保证每次投递"要么执行，
要么返回 `ECANCELED`"，不会滞留。uring 在 `Init()` 之前无法写 wakeup eventfd，所以 `Run()`
开始时会先执行一次积压的投递。

## 与 Runtime 配合

`Runtime` 的 worker Loop 通过 `OnWorkerStart` / `OnWorkerStop` 暴露。跨 worker 投递需要一个由
应用维护、与 worker 生命周期同步的注册表：

```cpp
std::shared_mutex registry_mutex;
std::vector<alyrn::epoll::Loop*> loops;  // OnWorkerStart 中登记，OnWorkerStop 中移除

void Broadcast(Message message) {
  std::shared_lock lock{registry_mutex};
  for (auto* loop : loops) {
    (void)loop->Post([loop, message] { DeliverLocally(*loop, message); });
  }
}
```

持共享锁调用 `Post` 保证目标 Loop 在调用期间存活；`OnWorkerStop` 持独占锁移除自己的 Loop。
worker 停止后到达的投递会返回 `ECANCELED`。

## 后续优化

uring 可以用 `IORING_OP_MSG_RING` 直接向目标 ring 投递通知，省去 eventfd 与 poll 请求。提交
`735cd86` 之前的 `detail::Mailbox`（有界 MPSC 与通知合并）可作参考；内核不支持
`IORING_OP_MSG_RING` 时应退回 eventfd 路径。

## 测试观察点

- 多个生产者线程并发投递时，每个回调恰好执行一次、在 owner 线程、同一生产者内保持顺序；
- `Run()` 之前的投递在 `Run()` 开始后执行；
- 关停过程中（排空结束前）的投递仍会执行；
- `Run()` 返回后的投递返回 `ECANCELED`，回调未执行并已在调用线程析构。

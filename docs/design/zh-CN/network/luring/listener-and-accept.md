# listener 与 AcceptSource

## Listener

`Listener::Create()` 创建 loop-affine listener。它提供两种接受连接的方式：

```text
Accept()             一次等待一个连接
CreateAcceptSource() 持续产生连接事件
```

`Accept()` 是 backend-neutral 的单次操作：一个 accepted fd 被包装成 `Stream` 后，
fd 所有权转移给该 stream。listener 关闭后不能再接受连接。

## AcceptSource

`AcceptSource` 将“持续接受连接”建模成一个有界异步事件源：

```cpp
auto source_result = listener.CreateAcceptSource();
if (!source_result.has_value()) co_return;
auto source = std::move(*source_result);
for (;;) {
  auto result = co_await source.Next();
  if (!result.has_value()) break;       // source error
  if (!result->has_value()) break;      // source terminal
  Stream stream = std::move(**result);
  // consume one connection
}
co_await source.Stop();
```

上面的代码只展示语义，实际业务还应记录和处理 `source.Stop()` 的返回错误；它不要求业务
直接接触 CQE。

每个成功 CQE 产生一个 stream 事件。source 的 terminal 只表示不会再有新事件，已经排队
的 stream 仍需交付或按停止策略收敛。

## multishot 与降级

worker 可以通过 `AcceptMode::kMultishot` 请求原生 multishot accept。source 会优先提交
一个 multishot accept：

```text
一个 accept SQE
  -> 多个成功 CQE（F_MORE）
  -> 一个无 F_MORE 的 terminal CQE
```

如果 kernel 不接受 multishot opcode，luring 会把同一个逻辑 source 切换到 single-shot
re-arm 路径。调用方仍看到相同的 `Next()` 事件协议；不能把“成功走了 fallback”视为业务
错误。

source 的 event capacity 和 listener 的 `accept_depth` 共同控制突发连接压力。队列达到
高水位时，backend 会停止继续接纳并在消费降到低水位后恢复；这不是 terminal。

## Stop 与 Close

`Stop()` 的终止条件包含：

- 不再提交新的 accept；
- cancel 和原 accept request 的 CQE 都已处理；
- 已产生的 stream 事件已交付或按 source 协议结束；
- listener reservation 已释放。

`Stop()` 先停止新的 event admission，再准备 cancel SQE。因此 cancel SQE 的本地 preparation
失败会使本次 `Stop()` 返回 error，但不会把 source 回滚为 Active：旧 accept request 仍可能存在，
source 也不能在用户已请求停止后继续交付新连接。调用方必须保留 source 并重试 `Stop()`；重试成功后
才允许销毁 source。这个规则不同于 listener `Close()` 的可回滚 preparation，后者在 cancel SQE
尚未进入 submission protocol 时必须保持 listener/source Open/Active。

listener `Close()` 与 source 并发时，source 必须收到明确的 terminal，而不是悬挂在旧 fd
上。调用方不应在 `Stop()` 完成前销毁仍持有 source 的对象。

这里的 listener terminal 只在 **committed Close** 后发生：若 cancel SQE 尚未进入 loop 的
submission protocol 就本地失败，`Close()` 立即返回该错误，listener 与 active source 都保持
Open/Active，调用方可以显式重试。不能在可回滚的 Close preparation 阶段先调用
`OnListenerClosed()`，否则一次本地提交错误会错误地把仍在运行的 source 置为 Stopping。

## 测试观察点

- single-shot 和 native multishot 产生相同的业务事件序列；
- multishot 的每个事件最多交付一次，terminal 最多出现一次；
- kernel 不支持 multishot 时正确 fallback；
- event capacity 达到高水位后 source 暂停，消费到低水位后重新接纳；
- stop、cancel、listener close 交错时不丢失 ownership、不重复恢复。

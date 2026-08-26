# 协程 Channel

`coro::Channel<T>` 是一个 scheduler-affine 的 FIFO 值交接模块。它拥有有界 value
buffer 和等待 sender/receiver 的队列；不拥有线程、event loop、fd 或 backend mailbox。

接口为 `TrySend`、`TryReceive`、`Send`、`Receive` 与 `Close`。容量为零时，`Send` 和
`Receive` 只会直接交接 value；容量大于零时，buffer 满会使 `Send` 等待，buffer 空会使
`Receive` 等待。

`Close()` 禁止新的发送，等待 sender 得到 `EPIPE`；已缓冲 value 仍可被接收。buffer 被
drain 后，等待及后续 receiver 得到成功的空 `optional<T>`。

Channel 的所有操作必须在构造它的 `Scheduler` 上执行。唤醒只通过该 scheduler 排队，不会
直接恢复协程。跨线程或跨 worker 交接必须先用 backend 的 `Post`/mailbox 回投到 channel
owner；当前 `Scheduler` 没有通用 thread-safe post seam，Channel 不能伪造这一保证。

等待 awaiter 在 coroutine frame 被销毁前会从 channel 队列移除。和所有已调度的
`coro::Work` 一样，已被 scheduler 接管的 continuation 必须保持有效，直到 scheduler 执行它。
因此 owner 销毁 Channel 前必须先 `Close()` 并 drain scheduler；仍有等待 sender 或 receiver 时
销毁会触发 fail-fast，而不会调度指向已销毁 Channel 的 continuation。

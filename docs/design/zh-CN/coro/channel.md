# 协程 Channel

`coro::Channel<T>` 是一个 scheduler-affine 的 FIFO 值交接模块。它拥有有界 value
buffer 和等待 sender/receiver 的队列；不拥有线程、event loop、fd 或 backend mailbox。

接口为 Go 风格的阻塞运算符 `<<` / `>>` 与 `Close`。
发送和接收分别写成：

```cpp
co_await (channel << value);

std::optional<T> received;
co_await (channel >> received);
```

## Select

`Select` 接受任意数量的 send/receive case，并返回胜出 case 在参数列表中的下标：

```cpp
std::optional<Message> primary_message;
std::optional<Message> fallback_message;

const std::size_t selected = co_await Select(
    primary >> primary_message,
    fallback >> fallback_message);

if (selected == 0) {
  Handle(*primary_message);
} else {
  Handle(*fallback_message);
}
```

send 和 receive 可以混合，也可以来自不同 value type 的 Channel：

```cpp
const std::size_t selected = co_await Select(
    output << response,
    shutdown >> stop_reason);
```

当至少一个 case 已就绪时，`Select` 不挂起；多个 case 同时就绪时会做伪随机等概率选择，
避免固定偏向第一个 case。没有 case 就绪时，`SelectDefault()` 可作为非阻塞 fallback：

```cpp
const std::size_t selected = co_await Select(
    input >> message,
    SelectDefault());
```

没有 default 时，所有 case 会注册到各自 Channel，直到一个 case 胜出。winner 会在恢复协程前
移除所有 loser 注册；挂起中的 coroutine frame 被销毁时也会自动取消这些注册。一个 `Select`
可以同时包含同一 Channel 的 send 和 receive case，它们不会彼此自匹配。

所有 Channel case 必须属于当前正在执行的同一个 Scheduler。receive closed Channel 会立即胜出并
清空对应 `optional`；send closed Channel 被选中时触发 panic。完整可运行示例见
`examples/coro_select.cc`。

构造函数的 `capacity` 必须为 `0` 或 `2` 的幂（例如 `1`、`2`、`4`、`8`）。`0` 表示无缓冲
channel；传入其他容量会在构造时触发 fail-fast。

容量为零时，`<<` 和 `>>` 只会直接交接 value；容量大于零时，buffer 满会使 `<<` 等待，
buffer 空会使 `>>` 等待。接收成功后，`received` 保存 value；channel 已关闭且 buffer
已耗尽时，`received` 为空。`Result<void>` 仍用于报告操作错误。

`Close()` 禁止新的发送；向已关闭 channel 发送，以及重复 `Close()`，都会触发 panic。
关闭时仍在等待的 sender 也会触发 panic，符合 Go 对阻塞发送的处理。
已缓冲 value 仍可被接收。buffer 被 drain 后，等待及后续 `>>` 操作成功并清空 output
optional。

Channel 的所有操作必须在构造它的 `Scheduler` 上执行。唤醒只通过该 scheduler 排队，不会
直接恢复协程。跨线程或跨 worker 交接必须先用 backend 的 `Post`/mailbox 回投到 channel
owner；当前 `Scheduler` 没有通用 thread-safe post seam，Channel 不能伪造这一保证。

等待 awaiter 在 coroutine frame 被销毁前会从 channel 队列移除。和所有已调度的
`coro::Work` 一样，已被 scheduler 接管的 continuation 必须保持有效，直到 scheduler 执行它。
因此 owner 销毁 Channel 前必须先 `Close()` 并 drain scheduler；仍有等待 sender 或 receiver 时
销毁会触发 fail-fast，而不会调度指向已销毁 Channel 的 continuation。

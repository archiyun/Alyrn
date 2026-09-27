# 取消与超时

Alyrn 按资源取消：每个会挂起的操作都属于某个资源（stream、connector、loop 定时器、channel、
loop 本身），取消与超时都通过这个资源表达。库不提供"取消任意任务"的接口。本页汇总各类需求
对应的写法，两个后端语义一致。

## 按需求选择

| 需求 | 做法 | 等待者看到的结果 |
|---|---|---|
| 读 / 写超时、keep-alive 空闲超时 | `stream.SetReadDeadline(t)` / `SetWriteDeadline(t)` / `SetDeadline(t)`：连接级、sticky 的单调绝对时刻，作用于当前与之后的同向操作 | `ETIMEDOUT` |
| 连接超时 | `ConnectorOptions::connect_timeout`：到期即关闭 socket | `ETIMEDOUT` |
| 延时 | `SleepFor(loop, delay)` | 成功；loop 停止时 `operation_canceled` |
| 等待 channel 时加超时、周期性事件 | `Timer` / `Ticker` 作为 `Select` 的 case | 触发时刻；loop 停止时空 `optional` |
| 立即放弃某个连接上挂起的 I/O | `co_await stream.Close()` | 挂起的 read / write 得到 `operation_canceled` |
| 从另一个线程取消 | `loop.Post(...)` 投递一个回调，在 owner 线程上关闭资源或设置 deadline | 同上 |
| 停止整个 loop | `loop.RequestStop()` | 所有挂起的 I/O、connect、sleep 得到 `operation_canceled`；`Timer`/`Ticker` 关闭 channel |
| 服务端优雅关停 | `Runtime::Builder::ShutdownGrace(grace)` + `OnWorkerDrain(hook)` | 在途请求完成；宽限期后剩余的得到 `operation_canceled` |
| 不再关心一个 spawn 出去的任务 | `JoinHandle::Detach()` | 任务照常运行到结束；要让它提前结束，就取消它等待的资源 |

## 常见写法

**给一段 I/O 设总时限。** 在开始前设置 deadline，之后每次 `Read`/`Write` 都受它约束；结束后用
`std::nullopt` 清除：

```cpp
ALYRN_CO_TRY(stream.SetDeadline(alyrn::time::SteadyNow() + alyrn::time::Seconds(5)));
// ... 若干次 Read / Write，任何一次超时都返回 ETIMEDOUT ...
ALYRN_CO_TRY(stream.SetDeadline(std::nullopt));
```

**空闲超时。** deadline 是绝对时刻，每次等待下一个请求前重新设置：

```cpp
ALYRN_CO_TRY(stream.SetReadDeadline(alyrn::time::SteadyNow() + alyrn::time::Seconds(30)));
auto n = co_await stream.Read(buffer);
```

**等待 channel 时加超时。**

```cpp
alyrn::epoll::Timer timeout(loop, alyrn::time::Milliseconds(200));
std::optional<Job> job;
std::optional<alyrn::time::Deadline> fired;
if (co_await Select(jobs >> job, timeout >> fired) == 1) {
  // 超时
}
```

**一个方向出错时取消另一个方向。** 代理这类全双工会话里，两个方向各自挂着一个 `Read`。出错的一方
`co_await` 关闭两端的 stream，另一方挂起的操作随即以 `operation_canceled` 结束；同一个 stream
只能有一个 `Close()` 在进行，用一个会话内的标志保证只关闭一次。

## 为什么没有任务级取消

一个挂起操作的取消必须经过它自己的后端生命周期收敛：epoll 要注销 readiness 注册，io_uring
要提交取消请求并等到每个 CQE 都回来，之后才能释放缓冲区、fd 和协程帧。按资源取消让这件事始终
由资源的拥有者完成，所有权清楚；任务级取消则要求每一种 awaitable 都响应同一套取消协议，并在
任意挂起点上都能安全地放弃。库没有采用后一种模型，缺少的超时与唤醒手段以资源能力的形式补上
（`connect_timeout`、`Timer`/`Ticker`、`ShutdownGrace`）。

## loop 停止时的保证

`RequestStop()` 之后，每个挂起的等待者都会恢复一次，不会被静默丢弃：

- stream / listener / connector 的挂起操作：`operation_canceled`；
- `SleepFor`：`operation_canceled`（到期已确定为成功的除外）；
- `Timer` / `Ticker`：关闭内部 channel，receive 以空 `optional` 完成；
- 应用自己的 `Channel`：由应用在 `OnWorkerStop` 或自己的停止路径中 `Close()`，等待者随之恢复。

停止之后，loop 会排空这些恢复的协程，然后才进入 `Stopped`。

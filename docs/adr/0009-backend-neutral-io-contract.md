# ADR-0009: 冻结 backend-neutral I/O Core contract

## 状态

Accepted

## 背景

Alyrn 同时提供 Epoll、io_uring 与 kqueue 三个平行网络后端。业务协程应该依赖共同的异步
语义，而不是依赖 epoll、SQE/CQE、`kevent` 或某个具体 loop 的实现细节。

当前项目已经有 `alyrn::io` facade 和对应的 concepts，但如果没有明确的稳定范围，
后端特性容易逐步渗透到 `AsyncStream`，最终让业务代码同时理解普通 I/O、timeout、
provided buffer 和 zero-copy 的不同生命周期。

## 决策

冻结以下 Core contract：

- `AsyncStream`：`ReadSome`、`WriteAll`、`Shutdown`、`CloseRead`、`CloseWrite`、`Close`、
  `LocalAddr`、`RemoteAddr`；
- `AsyncListener`：`Accept`、`Close`；
- `AsyncConnector`：建立满足 `AsyncStream` 的 outbound stream；
- `Task`/`DetachedTask` 的 await、ownership 和 scheduler-bound resume 语义。

冻结的不只是方法形状，还包括：

- awaitable 的结果类型和一次逻辑完成/一次 continuation resume；
- borrowed buffer 在 `await_resume` 前必须保持有效和地址稳定；
- address query、read half-close 和 write half-close 的可观察语义；
- pending I/O 在 Close、取消和错误路径上的收敛；
- Close 后的新提交如何失败；
- stream、loop、fd 和 coroutine work 的 owner-thread 规则。

以下能力保持为独立 extension：

- `AsyncReadIntoStream`；
- `AsyncRecvSource`、`BufferLease` 和 provided buffer；
- send zero-copy、multishot 以及其他 backend capability。

extension 可以增加能力，但不能改变 Core contract 的含义，也不能把可选能力变成
`AsyncStream` 的隐含要求。

`WriteAll` 虽然可能在一个后端内提交多个物理 write request，但它仍属于 Core contract：
它只把“完整写入或终态失败”这一业务语义暴露给调用方。短写推进、send zero-copy 的
notification 边界以及 coroutine frame 布局是后端 implementation，不是业务侧需要拼接的
通用 fallback。

## 结果

业务模块可以使用 concepts 和模板依赖 Core contract，不需要包含具体 backend 头文件。
Epoll 和 io_uring 继续保留独立实现，并通过集中式 compile-time contract 检查和各自
的生命周期测试证明符合 Core contract。

内部可以继续优化 SQE/CQE 批处理、ready queue、completion dispatch、frame allocator
和 buffer pool。只有 Core contract 的可观察语义变化，才需要更新本 ADR、契约文档、两
个后端 adapter 和共享测试。

## 验证

契约测试至少覆盖成功、pending、EOF、短写、Close 竞争、重复完成、buffer 生命周期和
owner-thread 规则。现有 Epoll/io_uring stream、listener、recv-source 与 completion
生命周期测试继续负责行为验证；集中式测试负责确保所有公开 adapter 满足相同的 concepts。

## 修订（2026-08）

owner-thread 上销毁 Stream 是 Core 可观察的资源释放：空闲 drop 关闭 fd，不必先
`Close()`。Epoll 将 pending drop refine 为 `CloseNow()`（pending I/O 各完成一次
`ECANCELED`，continuation 延后 resume）。io_uring 在 in-flight SQE 仍占用协程帧上的
`Op` 时，pending drop 仍是契约违反；取消那些 operation 继续走 `Close()`。

## 修订（2026-08，timeout）

per-call `ReadSomeFor` 与 `AsyncTimedStream` 已从公开契约撤回。超时不再是 Stream 的
一次性覆盖 API；连接级 sticky 每操超时是后续工作。loop 级 `SleepFor` / `RunAfter` 不受影响。

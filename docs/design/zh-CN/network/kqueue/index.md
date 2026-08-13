# kqueue

kqueue 是 CoroPact 的 BSD/Darwin readiness 后端。它以非阻塞 socket syscall 与
`kqueue(2)` / `kevent(2)` 为基础，不提交长期存在的 kernel I/O request。

它是第三条**平行 adapter**，不是 `reactor` 里的 `#ifdef`。Linux epoll Reactor 与
kqueue 可以共享真正后端无关的契约（`io`、`net`、`coro`），但不能共享 poller、
Channel 或 worker bootstrap 实现。

## 功能状态

| 功能 | 对外入口 | 状态 | 说明 |
| --- | --- | --- | --- |
| loop / dispatcher | `KqueueLoop` | 已实现 | owner-thread `kevent` + 用户态 timer tree |
| 跨线程投递 | `KqueueLoop::Post` | 已实现 | 线程安全回调队列；唤醒属主 loop |
| stream I/O | `ReadSome`、`ReadInto`、`WriteAll` | 已实现 | 当前仅 one-shot readiness |
| 超时读 | `ReadSomeFor` | 已实现 | 逻辑 timer 走 loop 的用户态树，不是每操作一个 `EVFILT_TIMER` |
| listener / accept | `KqueueListener` | 已实现 | 多 worker 时只有 acceptor 持有 listener |
| connector | `KqueueConnector` | 已实现 | |
| 持续 recv | `KqueueRecvSource` | 已实现 | readiness 驱动的多事件 source |
| Runtime 组合根 | `Runtime::Builder<runtime::Kqueue>` | 已实现 | 与 Reactor/luring 同一套启停 interface |
| 多 worker | `KqueueWorkerGroup`（`kqueue/detail`） | 已实现 | 主从：单 listener + fd 移交 |
| LT / ET stream | `TriggerMode` | 枚举已公开 | stream 当前只支持 `kOneShot` |

## 公开 seam

业务代码只应持有这些类型：

```text
kqueue::KqueueLoop
kqueue::KqueueStream
kqueue::KqueueListener
kqueue::KqueueConnector
kqueue::KqueueRecvSource
kqueue::TriggerMode / KqueueStreamOptions
Runtime::Builder<runtime::Kqueue>
```

`kqueue/detail`（Channel、KqueuePoller、TimerQueue、Worker、WorkerGroup）不是应用接口。

源码入口：

- `include/coropact/kqueue.h`（BSD/Darwin 伞头文件）
- `include/coropact/kqueue/loop.h`
- `include/coropact/kqueue/stream.h`
- `include/coropact/kqueue/runtime.h`
- `src/kqueue/`

## 多 worker 拓扑

`Workers(n)` 表示 n 条线程，**不是** n 个 `SO_REUSEPORT` listener。

- `n == 1`：该 worker 自己 bind、accept、处理连接，没有移交。
- `n > 1`：只有 worker 0 接受连接。I/O worker `1..n-1` **先**启动，保证它们的
  `KqueueLoop` 已存在；acceptor 再 `Release()` 描述符，经 `Post` 轮询交给目标
  loop，在**属主线程**上重建 `KqueueStream`。

这与 Reactor（独立 listener + `SO_REUSEPORT`）和 luring（每个 worker 一个 ring）
都不同。kqueue 默认强制 `reuse_port = false`。细节见
[KqueueLoop、Post 与主从移交](loop-and-handoff.md)。

## 构建与验证

| 构建 | CMake | 覆盖范围 |
| --- | --- | --- |
| 原生 kqueue | `-DCOROPACT_ENABLE_KQUEUE=ON` | 仅 FreeBSD / NetBSD / OpenBSD / Darwin；含 worker-group 与 Runtime smoke |
| Linux shim | `-DCOROPACT_ENABLE_KQUEUE_SHIM_TESTS=ON` | 假 `kevent`：oneshot 注册状态机、`Post`；不监视真实 fd |

`coropact/kqueue.h` 在非 BSD 宿主上 `#error`。个别实现头文件可以在 Linux 上借助
`tests/support/kqueue_shim` 编译，那不是对外安装路径。

安装目标是 `CoroPact::coropact_kqueue`（仅当 `COROPACT_ENABLE_KQUEUE` 打开时导出）。

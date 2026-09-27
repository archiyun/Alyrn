# 第二轮用户试用修复计划（2026-09-27）

第二轮以用户身份把 `main`（88feb74）安装到临时前缀，用 `find_package(Alyrn)` 写了 TCP 代理、
HTTP keep-alive 服务（含优雅关停）、channel/select 批处理流水线、带主机名解析的客户端和迷你
netcat，并在两个后端、ASan/UBSan/LSan 下运行。本文记录发现的问题、根因、修复方案和验收清单。

范围决定（已与维护者确认）：

- 取消沿用现有的“按资源取消”模型（deadline、`Close()`、loop 停止），不引入任务级取消；
  缺失的工具（connect 超时、select 可用的定时器、Runtime 优雅关停）补齐，并写清取消模式。
- UDP 与 Unix domain socket 本轮不做，只在文档中记录为当前不支持。

## G1 uring `SleepFor` 在 loop 停止时永不恢复（缺陷，高）

现象：loop 停止时正在 `uring::SleepFor` 的协程不会恢复，帧与其持有的资源（例如会话的
`uring::Stream`）静默泄漏；epoll 按文档以 `operation_canceled` 恢复。优雅关停宽限期短于慢请求时，
uring 会话泄漏；代理的 connect 超时竞速在 uring + LSan 下报告 719 字节泄漏。

根因：uring `SleepAwaiter` 只注册 `RunAfter` 定时器；`Loop::Run()` 停止时 `timers_->DiscardAll()`
销毁未到期回调而不运行，continuation 从未被调度。uring Loop 也没有 epoll 那样的关停参与者机制。
此外 uring 版在“提前销毁睡眠中的帧”时不撤销定时器（epoll 支持），两后端语义不一致。

修复：

- [x] 把 `LoopShutdownParticipant` / `LoopShutdownRegistry` 移到共享的 `alyrn/detail/loop_shutdown.h`，
      epoll 继续以原名使用。
- [x] uring Loop 增加关停参与者注册表：停止时先通知参与者，再取消并排空 ring 操作；
      析构时检查注册表已空。
- [x] uring `SleepFor` 与 epoll 契约一致：停止时以 `ECANCELED` 恢复；loop 已停止时立即返回
      `ECANCELED`；提前销毁时撤销定时器。
- [x] 测试：睡眠中的 detached 协程在停止时恢复并销毁帧；停止后 `SleepFor` 立即返回
      `ECANCELED`；提前销毁不触发回调（ASan）。

## G2 `Connect` 没有超时，挂起的 connect 无法取消（缺陷，中）

现象：上游不响应时客户端挂约 2 分钟（SYN 重试）。用户自拼超时需要约 40 行，且超时后 connect
仍占着 socket，20 个客户端超时后代理还持有 20 个 SYN-SENT socket，可能耗尽 fd。

修复：

- [x] `ConnectorOptions::connect_timeout`（`time::Duration`，零表示不限）适用于每次 `Connect()`。
      到期以 `ETIMEDOUT` 完成并立即关闭 socket；loop 停止仍报告 `ECANCELED`。
- [x] epoll：EINPROGRESS 后挂 loop 定时器，完成时撤销。
- [x] uring：沿用 stream deadline 的模式——定时器到期提交按 user_data 取消的请求，
      connect 与取消两个 CQE 都收到后才恢复。
- [x] 测试（两后端 conformance）：用积满 accept 队列的监听端制造确定的 SYN 丢弃；超时时间与
      fd 释放；超时前成功时定时器被撤销；停止时为 `ECANCELED`。

## G3 `Select` 只能等 channel，定时器要自己拼（缺陷，中）

现象：批处理“满 100 条或每 50 ms”需要另起 ticker 任务往 channel 发信号，且 `TrySend` 前必须检查
`Closed()`，否则 panic；loop 停止时等在自制 ticker 上的协程也会永远挂起。

修复：

- [x] 两个后端提供 `Timer`（一次性）与 `Ticker`（周期性，接收方跟不上时丢弃多余 tick）。
      二者都支持 `co_await (timer >> fired)` 和 `Select(..., ticker >> tick)`；`Stop()`、`Reset()`
      丢弃未接收的值；loop 停止时关闭内部 channel，使等待者收到空值而不是永远挂起。
- [x] 测试：select 超时、ticker 丢弃、Stop/Reset、loop 停止唤醒等待者，两后端各一份。

## G4 Runtime 没有优雅关停（缺陷，中）

现象：默认关停立即取消所有 I/O。自行实现“停止接受、让在途请求完成、宽限期后强停”需要约 60 行
（worker 钩子 + `Post` + deadline + 自建会话表），且关停期间 listener 仍接受新连接。

修复：

- [x] `Builder::ShutdownGrace(time::Duration)`：停止请求到达后，各 worker 先关闭 listener 停止接受，
      在 loop 线程上调用 `OnWorkerDrain` 钩子，然后等待该 worker 的连接 handler 全部结束或宽限期
      到期，再按原流程停止。宽限期为零时行为不变；排空期间再次 `RequestStop()` 立即强停。
- [x] `Builder::OnWorkerDrain(hook)`：应用在这里唤醒空闲会话（例如设置 read deadline）。
- [x] `DetachedTask` 增加内部完成通知，Runtime 用它统计存活的 handler。
- [x] 测试（两后端）：在途请求完成、钩子关闭空闲连接、排空后不再接受新连接、宽限期到期强停。

## G5 小问题

- [x] **handler 取不到 Loop**：更正——两个后端的 `Stream::OwnerLoop()` 早已公开（第一轮的
      `07_recv_source_echo.cc` 就用过），但注释只说它给 native 扩展用，试用时没找到。改为在注释、
      两份 README 与 runtime builder 文档中写明 handler 用它向外连接、`Spawn` 或开定时器。
- [x] **`Channel::Close()` 注释与行为不符**：注释改为“等待中的发送方会 panic”，panic 文案指明是
      “关闭时仍有发送方在等待”。
- [x] **`Stream` 与非 socket fd**：epoll 读路径改用 `recv`/`recvmsg`，两后端对非 socket 一律返回
      `ENOTSOCK`；构造函数写明只接受已连接的流式 socket。
- [x] **两后端定时器接口不一致**：uring Loop 增加 `RunAt`、`RunEvery`、`Cancel`（`CancelTimer`
      保留为别名）；uring 注册定时器需要提交 SQE，因此仍返回 `Result`，文档说明差异。
- [x] **`Endpoint` 缺少改端口的方法**：增加 `WithPort()`；`net::ParseIpAddress(ip, port)` 早已公开，
      试用时没找到，在 `Endpoint` 注释中给出指引。另外补上 uring 缺少的 `Connect(net::Endpoint)`，
      并把它加入 `AsyncConnector` 契约。
- [x] **`net::Buffer` 无法跨块查找**：增加 `Find()` 与 `Linearize(n)`。
- [x] **UDP / Unix domain socket**：文档记录为当前不支持。
- [x] **取消与超时模式**：新增文档，说明各资源的取消方式与推荐写法，以及不提供任务级取消的原因。

## 验收

- [x] 每个修复先写失败的测试，修复后通过。
- [x] 每个提交在干净副本中单独通过 epoll 与 uring 全量测试；最终版本跑 CI 全部配置
      （clang/GCC 严格警告、Release、ASan/UBSan、TSan）与 `mkdocs build --strict`。
- [x] 用修复后的库重跑第二轮试用程序，改用新 API 后删掉对应的自制代码。

## 结果

分支 `agent/user-trial-round2-fixes`，在 `main`（88feb74）之上：

| 提交 | 内容 |
|---|---|
| f1e95fe | 本计划 |
| 175499a | G1 uring `SleepFor` 在停止时恢复；共享关停参与者注册表 |
| 7ad4e63 | `Channel::Close()` 注释与 panic 文案 |
| 1d4ff11 | 非 socket fd 在所有读路径上一致返回 `ENOTSOCK` |
| 935b8e0 | uring 定时器接口与 epoll 对齐 |
| 23ec969 | G3 `Timer` / `Ticker` |
| afae2c5 | G2 `connect_timeout` 与 uring `Connect(Endpoint)` |
| 569b1df | `Endpoint::WithPort()`，`OwnerLoop()` 文档 |
| 1247e9a | `DetachedTask::OnComplete` |
| e95740c | G4 `ShutdownGrace` / `OnWorkerDrain` |
| 0283aa1 | `net::Buffer::Find()` / `Linearize()` |
| 99ddf1f | 取消与超时文档、传输层范围说明 |

用修复后的库重跑第二轮试用程序：代理去掉自制的 connect 竞速（233 → 181 行），20 个客户端在
300 ms 超时后全部释放且不再残留 SYN-SENT socket（修复前 20 个）；uring 代理在 ASan/LSan 下
不再报告泄漏（修复前 719 字节）；uring 睡眠中的协程在停止时以 `operation_canceled` 恢复；优雅关停
改用 `ShutdownGrace` + `OnWorkerDrain`，结果与手写版一致，关停期间的新连接在连接阶段即被拒绝；
流水线改用 `Ticker`；解析客户端改用 `WithPort()` + `Connect(Endpoint)`。

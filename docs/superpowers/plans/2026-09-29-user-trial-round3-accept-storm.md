# 第三轮用户试用修复计划：accept 资源耗尽（2026-09-29）

第三轮继续以用户身份用 `find_package(Alyrn)` 写对抗性程序：普通 `Runtime` echo 服务器 + 把 fd
顶爆的客户端、带宽限期的优雅关停 + 空闲连接。发现一个可被任意客户端远程触发、且不会自愈的
拒绝服务漏洞，两后端症状不同但同源。本文记录问题、根因、修复方案与验收。

## G6 accept 遇到 fd 耗尽时 epoll 服务器 100% CPU 永久卡死（缺陷，严重）

现象：普通 `Runtime` echo 服务器，进程 fd 上限为 Linux 默认的 1024。一个客户端把连接开到超过
上限即可（实测 `prlimit --nofile=64` + 多开 84 条连接）：

| 阶段 | epoll | uring |
|---|---|---|
| 耗尽前 | echo ok，CPU 0% | echo ok，CPU 0% |
| 耗尽时 | **CPU 100%**，老连接 echo **超时** | CPU 0%，老连接 echo ok |
| 放开后（关掉多余连接） | **CPU 100%**，新/老连接全 **超时** | CPU 0%，老连接 ok，新连接 **超时** |

根因（`src/epoll/worker.cc` 的 `AcceptLoop` + `src/epoll/listener.cc` 的 `AcceptAwaiter`）：

- `AcceptLoop` 对非 `ECANCELED/EBADF` 的错误一律立即 `continue` 重试；
- `AcceptAwaiter::await_suspend` 中，`accept4` 返回 `EMFILE`（不是 `EAGAIN`）时走 `CompleteInline`
  后 **`return false`——协程不挂起**，`co_await` 同步返回错误；
- 于是 `AcceptLoop` 成了一个**从不让出事件循环的同步死循环**：出错的连接仍在 accept 队列里，
  监听 fd 一直可读，立刻再 `EMFILE`，再转。

致命之处是**永不恢复**：worker 线程被死转独占，事件循环再不运行，已接受连接的 EOF 永远得不到
处理、服务端 fd 永远关不掉，fd 压力自我维持。`ENFILE` / `ENOBUFS` / `ENOMEM` 走同一路径，症状相同
（`net::detail::IsAcceptedConnectionError` 不含这几个 errno，因此既不算"可跳过的单连接错误"，也
不是 `EAGAIN`）。

## G7 uring 多发 accept 遇到 fd 耗尽后永久停止接受（缺陷，高）

同源的另一面。uring `Runtime` 默认多发 accept（`src/uring/runtime.cc` 设 `AcceptMode::kMultishot`）。
`EMFILE` 的 CQE 是终态（无 `IORING_CQE_F_MORE`），因不属于单连接错误而走
`AcceptSource::RequestBackendStop`（`src/uring/listener.cc`）→ **accept source 永久停摆**。不烧 CPU、
老连接照常，但此后所有新连接永远被拒，直到进程重启。比 epoll 温和，但一次 `EMFILE` 就打死监听。

## 修复方案

把 `EMFILE` / `ENFILE` / `ENOBUFS` / `ENOMEM` 归为**可重试的瞬时资源错误**：不是致命错误，也不能
立即空转重试，而应退避后重试，让事件循环在退避窗口里服务已有连接、腾出 fd。

- [x] 在 `net::detail` 增加 `IsTransientAcceptResourceError(int)`，与既有的
      `IsAcceptedConnectionError` 并列，集中定义这组 errno。
- [x] epoll `AcceptLoop`（`src/epoll/worker.cc`）：遇到瞬时资源错误时先 `co_await SleepFor` 退避
      再重试；`SleepFor` 在 loop 停止时返回 `ECANCELED`，据此退出循环。这既打破同步死转，又让
      loop 在退避期间处理已有连接。
- [x] uring 单发 `AcceptLoop`（`src/uring/worker.cc`，非默认路径）：同样退避后重试。
- [x] uring 多发 `MultishotAcceptLoop`（默认路径）：源在瞬时错误时本就把 errno 通过 `Next()` 抛出
      （`RequestBackendStop(NegErrno)` 已设 `terminal_error_`）。消费循环识别该 errno →
      `co_await source.Stop()` 收敛旧源 → 销毁旧源 → 退避 → `CreateAcceptSource` 重建，继续接受。
      **完全复用已测的源生命周期，不改 LRCI 收敛状态机。**
- [x] 退避固定为 `kAcceptBackoff = 10ms`（打破空转、可预测）；持续压力下每个退避周期只尝试一次
      accept，CPU 可忽略，能服务的连接仍在 backlog 等待 fd 释放。

不做（记录）：Marc Lehmann/libev 的"预留一个备用 fd，`EMFILE` 时关掉它、accept 后立即 close 摘掉
队首坏连接、再补回备用 fd"是更主动的削峰手法，可作为后续优化；本轮以退避为核心，已能消除 DoS
并自愈。

## 验收

- [x] `IsTransientAcceptResourceError` 有单元测试覆盖分类（`test_net_utils.cc`）。
- [x] 行为测试（两后端，`test_accept_resource_exhaustion_smoke.cc`）：fork 子进程内降
      `RLIMIT_NOFILE` 隔离（服务器在子进程，客户端在父进程用自己的 fd 预算把服务端顶爆），断言
      ①老连接在压力下仍能 echo；②放开 fd 后新连接能被服务（自愈）；③请求停止后子进程在墙钟期限内
      干净退出。父进程用墙钟超时兜底，卡死即杀并判失败。已做正反验证：还原任一后端的旧逻辑，测试
      都能在期限内失败（epoll 报饿死+未恢复+未停止；uring 报未恢复）。uring 不可用时 SKIP。
- [x] 每个提交在干净副本中单独通过 epoll 与 uring 全量测试；最终版本跑 CI 全部配置
      （clang/GCC 严格警告、Release、ASan/UBSan、TSan）与 `mkdocs build --strict`。
- [x] 用修复后的库重跑第三轮试用程序，确认两后端在 fd 耗尽后都不再卡死、能自愈。

## 结果

分支 `agent/user-trial-round3-fixes`，在 `main`（fc83dcf）之上：

| 提交 | 内容 |
|---|---|
| fe486a8 | 本计划 |
| 23b79f4 | `net::detail::IsTransientAcceptResourceError` 分类 + 单元测试 |
| b61c909 | epoll accept 退避（G6）+ fork 行为测试 |
| a9e5dda | uring 单发/多发 accept 退避与源重建（G7） |

用修复后的库（干净副本，隔离用户 WIP）重跑第三轮试用程序，`prlimit --nofile=64` + 多开 84 条连接：

| 阶段 | 修复前 epoll | 修复后 epoll | 修复前 uring | 修复后 uring |
|---|---|---|---|---|
| 耗尽时 | CPU 100%，老连接超时 | CPU 0%，老连接 ok | CPU 0%，老连接 ok | CPU 0%，老连接 ok |
| 放开后 | CPU 100%，新/老全超时 | CPU 0%，新/老 ok | 新连接超时 | 新/老 ok |

两后端现在都：不空转、耗尽期间照常服务已有连接、fd 释放后完全自愈。

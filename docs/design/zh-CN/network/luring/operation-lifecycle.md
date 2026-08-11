# operation 生命周期与完成分层

这篇说明 luring 所有功能共同遵守的 operation 生命周期。它是写测试时最重要的外部
契约：测试不需要知道某个 awaiter 内部有几个 hook，只需要验证可观察的结果和所有权。

## 三个层次

```text
物理请求（physical request）
  一个提交到 ring 的 SQE，以及它可能产生的 CQE

逻辑操作（logical operation）
  业务上要完成的一次 read、accept、timeout 或 send

业务等待者（continuation）
  等待逻辑操作结果的协程，只能恢复一次
```

它们不是一一对应：

| 形态 | 物理请求 | CQE | 逻辑结果 | 释放边界 |
| --- | ---: | ---: | ---: | --- |
| single-shot read/write | 1 | 通常 1 | 1 | CQE dispatch 后 |
| timed read | 2（read + timeout） | 最多 2 | 1 | 两个 member 收敛后 |
| multishot | 1 | 多个，最后一个 terminal | 多个事件 + 1 个 terminal | terminal 和已产生事件都收敛后 |
| send zerocopy | 1 | primary，primary `F_MORE` 时另有 notification | 1 | primary 无 `F_MORE` 后，或 notification 到达后 |
| close/cancel | cancel + 原 pending 请求 | 多个 | 1 个 close 结果 | 所有关联请求收敛后 |

## 通用状态序列

```text
Idle
  -> Submitted       已准备并排队，可能尚未进入 kernel
  -> InFlight        已提交，等待 CQE
  -> Resolving       CQE 正在被 operation-specific lifecycle 解释
  -> Completed       逻辑结果已确定
  -> Released        buffer、fd、frame 等 ownership 已交回
```

并非每个状态都会以公共 API 的名字出现，但测试可以从以下现象观察状态是否正确：

- operation 未完成时，调用方提供的 buffer 仍可安全持有但不能复用；
- operation 完成后，等待协程只恢复一次；
- `Close()` 返回前，仍可能影响该 fd 的 pending operation 已经收敛；
- source 的 terminal 不会抹掉已经排队的业务事件；
- release 之后不再出现指向同一资源的 CQE 处理。

## CQE 不是业务结果

普通 CQE 只提供 `res`、flags 和 user data。luring 根据 operation-specific lifecycle 解释它：

- `F_MORE`：multishot request 仍会继续；对 send-zc primary 则承诺后续 `F_NOTIF`，不能提前释放 buffer；
- 没有 `F_MORE`：multishot request 的物理终止边界；对 send-zc primary 也表示不会再有 notification；
- `F_NOTIF`：zerocopy 发送 buffer 的 kernel 使用边界，不是发送字节数；
- `F_BUF_MORE`：同一 provided buffer 还有后续 segment，当前 `LUringRecvSource` 公共
  路径尚未承诺该增量语义。

CQE dispatch 必须先把结果写入 operation，再授权 awaiter 恢复。因而 awaiter 在
`await_resume()` 中观察到缺失的 CQE 结果，表示内部生命周期协议被破坏；这不是可向业务
伪造为 `EIO` 的普通 I/O 错误，而是运行时不变量失败。

因此测试不应只断言“收到了一个 CQE”，而应断言最终业务 trace，例如：

```text
send result -> terminal primary/notification -> buffer reusable -> coroutine resumed
```

## 取消与关闭

取消请求本身也是异步操作。正确顺序是：

```text
RequestStop / Close
  -> 停止接收新的业务事件
  -> 提交或等待 cancel
  -> 收割 cancel CQE 和原 operation CQE
  -> 清空已产生事件或等待 lease
  -> 释放资源
```

看到 cancel CQE 不代表原 operation 的 CQE 不会再到达。测试应专门覆盖“正常完成和取消
同时到达”的顺序变化，并检查不会 double resume、double release 或使用已关闭 fd。

更严格地说，`Close()` 是 resource-level drain barrier，而 cancel 只是促使某个 physical
request 尽快终态的协议。下列边界必须分开：

```text
Close preparation（临时拒绝新操作）
  -> 本地 SQE preparation 失败：abort preparation，Close 返回 error，资源仍 Open
  -> cancel SQE 进入 owner loop submission protocol：committed Close
  -> cancel request terminal CQE
  -> original target CQE terminal
  -> buffer / awaiter release authorized
  -> fd/channel/ring registration released
  -> Close continuation resumed
```

cancel CQE 只说明 cancel command 已终态；它不是 original request 已终态的证据。
因此 fd 的释放条件是**所有 active physical use**均已 drain，而不是“每个 read/write 槽位
都有一个 terminal operation”。未使用槽位不会阻塞关闭；已提交的 target、已提交的 cancel
command，以及 backend 仍持有的 borrowed/owned storage 都会阻塞关闭。

对 source 的 `cancel64` 路径，`-ENOENT` 表示 target 已在 cancel 查找前完成，`-EALREADY`
表示 target 已进入无法取消但很快会产生自己的 CQE 的阶段；两者都不是 source 的 terminal
error。更高层 loop shutdown 取消 cancel request 自身时的 `-ECANCELED` 也不归因于 source。
source 仍必须等待 target CQE，再决定 pause、re-arm 或 stop 的逻辑结果。其他 cancel CQE
error 才会被记录为 source terminal error；无论结果如何，cancel CQE 都不能单独授权 target
storage 或 source state 的释放。

该规则由 [`resource_close_cancel.tla`](../formal/resource_close_cancel.tla) 独立建模。模型
覆盖 read/write、target CQE 与 cancel CQE 的任意顺序、local cancel-SQE preparation failure
的完整回滚、borrowed storage release、fd release 和 Close continuation。失败的 preparation
不是已提交 Close 的内部后台重试：它同步返回 error，后续重试是调用方发起的新 `Close()`。
模型的活性结论只在“owner loop 继续运行、committed request 最终得到 CQE”的前提下成立。

## 测试观察点

- 成功、负 errno、提交失败三条路径都能结束；
- 多个 physical completion 只产生一个 logical continuation；
- operation 完成前资源地址和 ownership 不被复用；
- loop drain 后 `InflightCount()` 和可观察业务资源都收敛；
- 失败注入不会把 pending operation 永久留在 ring 中。

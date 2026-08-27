# AcceptSource 语义契约

本文档定义持续接受连接的逻辑事件源。它不是对 io_uring multishot accept 的直接封装，
而是一个可以由 Epoll、io_uring one-shot re-arm、io_uring native multishot 共同实现的
后端中立语义。

现有的：

```cpp
alyrn::Task<Result<Stream>> Accept();
```

仍然保留，表示一次 single-shot accept。`AcceptSource` 是独立的扩展接口，不改变
`AsyncListener` 的现有契约。

## 1. 公共语义

建议的最小接口是：

```cpp
template <class Source>
concept AsyncAcceptSource = requires(Source& source) {
  typename Source::Stream;

  requires coro::Awaitable<decltype(source.Next())>;
  requires std::same_as<
      coro::AwaitResult<decltype(source.Next())>,
      Result<std::optional<typename Source::Stream>>>;

  {
    source.Stop()
  } -> std::same_as<alyrn::Task<Result<void>>>;
};
```

语义结果：

```text
Result<Stream>       接收到一个连接，Stream 所有权转移给调用者
Result<nullopt>      source 正常结束，后续不会再产生连接
Result<Error>        source 发生终止性错误
```

`Next()` 必须先交付已经进入队列的连接，再交付终止结果。这样某个 accept completion
和后续错误交错时，已经成功接收的连接不会因为错误被静默丢弃。

两个后端的 `Next()` 都是直接 awaiter，而不是每个 logical event 一个中间 `Task`：已有连接或
terminal result 时在调用协程中 inline 完成；否则 source 保存 caller continuation，待事件 ready
后按 source 所属 scheduler 恢复。这个实现选择不改变上述可观察结果或单 consumer 限制。

终止结果是 sticky 的：

- 正常结束后，后续 `Next()` 都返回 `Result<nullopt>`；
- 终止错误在队列排空后返回，后续 `Next()` 继续返回同一个错误；
- source 终止后不允许重新开始；需要新 source 才能重新进入 accept 模式。

`Stop()` 是幂等的 graceful stop：

1. 停止新的 accept admission；
2. 取消或等待已有的 physical accept request 收敛；
3. 保留已经入队的连接；
4. 队列排空后，`Next()` 返回 `Result<nullopt>`。

第一版不提供“立即丢弃队列”的第二种停止模式。调用者如果不再需要已入队的连接，应继续
消费并主动关闭这些 `Stream`。

## 2. 所有权与并发约束

`AcceptSource` 是 move-only、loop-affine 的 source：

```text
Listener owns listening fd
AcceptSource borrows Listener
AcceptSource owns pending accept state and queued Stream values
```

约束如下：

- source 不能超过 listener 的生命周期；
- listener 进入 committed `Close()` 前必须停止 source；listener 的 committed `Close()` 也必须
  隐式终止 source。仅在本地 cancel SQE preparation 失败的 `Close()` 不改变 source 的 logical
  state；
- 同一个 listener 同时只能处于一种 accept 模式；`Accept()` 与 `AcceptSource` 互斥；
- `Next()` 暂时只允许一个并发等待者，重复等待返回 `EBUSY`；
- source 的创建、`Next()`、`Stop()` 和移动都发生在 owner loop 线程；
- source 停止后，queued stream 仍由 source 持有，直到被 `Next()` 转移或 source 被明确销毁；
- source 析构时必须处于 `Idle`、`Draining` 或 `Terminal`，且没有 pending `Next()`、
  `Stop()` 或 physical request。该约束在 Release 构建中同样检查，违反它属于所有权错误。

listener 已关闭时，`CreateAcceptSource()` 返回 `EBADF`；owner loop 已进入 `Stopping` 或
`Stopped` 时返回 `ECANCELED`。因此创建 source 成功意味着当前仍允许建立新的 logical
accept lifecycle，而不只是 listener fd 数值上仍然有效。

这里的“borrow”只针对 listener 的 accept fd。已经成功接收并由 `Next()` 返回的 stream 不再
依赖 listener，可以独立转移到业务协程。

## 3. pending depth 与 event queue

配置至少包含：

```cpp
struct AcceptSourceOptions {
  std::size_t pending_depth{4};
  std::size_t event_capacity{64};
  std::size_t resume_threshold{0}; // 0 表示 event_capacity / 2
};
```

约束：

```text
pending_depth > 0
event_capacity >= pending_depth
resume_threshold == 0 || resume_threshold < event_capacity
```

`pending_depth` 表示最多有多少个 physical accept request 同时交给后端；
`event_capacity` 表示 source 最多为业务保留多少个尚未消费的 accepted stream。

source 始终维护以下不变量：

```text
queued_events + armed_accept_requests <= event_capacity
```

其中：

- `queued_events` 是已经完成 accept、但还没有被 `Next()` 转移的 stream 数量；
- `armed_accept_requests` 是已经提交但还没有完成的 one-shot accept 数量；
- native multishot 的一个 request 也必须按其可能产生的事件占用 admission budget，不能
  把它简单计为一个永远免费的 pending slot。

这个不变量保证 consumer 停顿时不会无限堆积 accepted socket。

### Admission 规则

```text
有空闲 budget
  -> 可以提交/重新提交 accept request

达到 high-water mark
  -> source 进入 Pausing（不是终止）
  -> Epoll 暂停 accept readiness
  -> io_uring one-shot 不再 re-arm；已有 request 自然收敛
  -> native multishot 提交 cancel，并等待目标 request 的 terminal CQE
  -> 目标 request 的 terminal CQE 到达后，source 进入 Paused
  -> cancel CQE 也收敛后才允许重新提交 request

consumer 成功 Next()，且 queued_events <= resume_threshold
  -> source 从 Paused 回到 Active
  -> 恢复 readiness 或提交新的 request
```

`pending_depth` 是吞吐和 burst 吸收能力；`event_capacity` 是内存和连接资源的硬上限，
两者不能混为一个参数。

## 4. 生命周期状态

逻辑 source 状态：

```text
Idle
  -> Active
  -> Pausing
  -> Paused
  -> Active
  -> Stopping
  -> Draining
  -> Terminal
```

含义：

| 状态 | 含义 |
| --- | --- |
| `Idle` | source 已创建，尚未提交 accept request |
| `Active` | 允许 admission，source 可以产生新连接 |
| `Pausing` | 已达到 high-water mark，正在等待既有 physical request 收敛 |
| `Paused` | target request 已终态；保留 queue，等待 low-water 与在途 cancel CQE 收敛后恢复 |
| `Stopping` | 已请求停止，不再提交新的 accept |
| `Draining` | pending request 已收敛，只交付已有 queue 内容 |
| `Terminal` | queue 已排空，后续只返回 sticky terminal result |

物理 request 的状态与逻辑 source 分开：

```text
PhysicalRequest:
  Submitted -> CQE/Event -> Released

Single-shot:
  一个 request 对应一个 CQE

Native multishot:
  一个 request 对应多个 CQE，最后一个 CQE 才结束 request
```

source 进入 `Pausing` 或 `Stopping` 不等于所有 physical request 已经释放；前者必须等到
目标 request 的终态后才进入 `Paused`，后者则进入 `Draining` 或 `Terminal`。取消 request
自己的 CQE 可以稍后到达；在它到达前 source 不得 re-arm。两条路径的区别在于：pause 不
产生业务可见 terminal result，也不设置 stop 语义。

## 5. 错误与终止策略

第一版采用保守策略：

| 后端结果 | source 行为 |
| --- | --- |
| accept 成功 | 构造 Stream，入队，继续 admission |
| Epoll `EAGAIN` | 本轮没有事件，重新等待 readiness |
| `ECONNABORTED` | 丢弃该连接尝试，继续 re-arm；不终止 source |
| `EMFILE` / `ENFILE` | source 进入终止错误；避免 busy retry |
| source queue 达到容量 | pause admission；不是 `ENOBUFS` 终止错误 |
| backend allocation `ENOMEM` | source 进入终止错误；由上层决定重建 source |
| listener close | source 停止 admission，排空已有 queue 后正常结束 |
| explicit `Stop()` | 取消 pending request，排空已有 queue 后正常结束 |
| 后端未知错误 | source 进入 sticky 终止错误 |

未来可以加入带 timer/backoff 的 resource-exhaustion retry，但不放入第一版契约；否则
`Next()` 的终止语义会与 retry policy 耦合。

## 6. Epoll 实现映射

Epoll 没有 native multishot accept。它通过 readiness 和 accept-drain 实现相同的 source
语义：

```text
source Active
  -> 注册 listen fd readable
  -> callback 中循环 accept() 直到 EAGAIN 或 admission budget 用尽
  -> 每个成功 fd 入队一个 Stream
  -> queue 达到 high-water 时移除/暂停 readable interest
  -> Next() 使 queue 降到 low-water 后重新注册 readiness
```

Epoll 的 readiness 本身不是业务事件；业务事件只由成功的 `accept()` 产生。
由于同一个监听 fd 的 readiness 不需要并行投递多个请求，当前 Epoll path 实际最多保留
一个 armed accept；`pending_depth` 在该后端仍作为共享 admission 上限保留，但不会制造多份
并行 poll request。`event_capacity` 才是 Epoll path 中真正限制已接收连接积压的参数。

## 7. io_uring 实现映射

io_uring 可以使用 one-shot accept re-arm：

```text
source Active
  -> 按 pending_depth 提交多个 accept SQE
  -> 每个 CQE 产生一个 accepted Stream
  -> CQE dispatch 后根据 admission budget 决定是否重新提交
  -> queue 满时停止 re-arm
  -> Stop/Close 时提交 cancel，并等待所有 accept CQE 收敛
```

当前实现优先选择 native multishot accept，并在内核拒绝 multishot flag 时自动退回
one-shot re-arm：

```text
一个 multishot request
  -> CQE(F_MORE)：入队一个 Stream，request 保持 active
  -> queue 达到 high-water：source 进入 Pausing，取消 request
  -> 目标 request 的最终 CQE：source 进入 Paused（不交付 terminal）
  -> Next() 使 queue 降到 low-water：重新提交 request 并回到 Active
  -> Stop/Close 才进入 Stopping/Draining/Terminal
```

两种 path 对业务都只暴露 `Next()`；区别保留在 luring 内部的 operation-specific lifecycle 和
capability/path selector 中。

## 8. 第一版不做的事情

- 不修改现有 `AsyncListener::Accept()` 的返回类型；
- 不让 `AcceptSource` 和普通 `Accept()` 并发共享一个 listener；
- 不在 source 内部无限缓存 accepted stream；
- 不把 native multishot 的 CQE 直接映射成重复 `co_await` resume；
- 不在第一版引入跨 worker 的 accept source；
- 不把 RIO、provided buffer 或 send zerocopy 混入 accept source。

## 9. 最小实现顺序

当前进度：

- [x] 增加 `AcceptSourceOptions`、`AsyncAcceptSource` 和 source 状态机的单元测试；
- [x] 在 Epoll 实现 bounded accept-drain 与 readiness pause/resume；
- [x] 覆盖 source `Stop()`、listener `Close()`、队列消费和 listener 生命周期回归。
- [x] 覆盖 io_uring native multishot 的 high-water pause、terminal CQE 收敛和 low-water
  re-arm；测试中只有第三个、在低水位后新建的连接能够证明重挂成功。
- [x] 以同一套 Epoll/io_uring conformance 场景覆盖 pending `Accept()` + `Close()`、
  pending `Next()` + `Stop()`、listener `Close()` + source，以及 loop stop 后拒绝新操作。
- [x] 以同一条公开可观察序列覆盖两个 backend 的 bounded admission：三条 burst 连接填满
  queue、consumer 到达 low-water、随后新建的第四条连接必须被重新 admission；该测试不窥探
  backend 的 readiness/CQE 状态，只验证 `AcceptSource::Next()`、`Stop()` 和 terminal result。

后续顺序：

1. 扩展 io_uring one-shot fallback 的多 request admission，复用现有 `accept_depth` 作为
   初始 `pending_depth`；

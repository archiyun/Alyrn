# AcceptSource 语义契约

本文档定义持续接受连接的逻辑事件源。它不是对 io_uring multishot accept 的直接封装，
而是一个可以由 Reactor、io_uring one-shot re-arm、io_uring native multishot 共同实现的
后端中立语义。

现有的：

```cpp
coro::Task<base::Result<Stream>> Accept();
```

仍然保留，表示一次 single-shot accept。`AcceptSource` 是独立的扩展接口，不改变
`AsyncListener` 的现有契约。

## 1. 公共语义

建议的最小接口是：

```cpp
template <class Source>
concept AsyncAcceptSource = requires(Source& source) {
  typename Source::Stream;

  {
    source.Next()
  } -> std::same_as<
      coro::Task<base::Result<std::optional<typename Source::Stream>>>>;

  {
    source.Stop()
  } -> std::same_as<coro::Task<base::Result<void>>>;
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
- listener 进入 `Close()` 前必须停止 source；listener 的 `Close()` 也必须隐式终止 source；
- 同一个 listener 同时只能处于一种 accept 模式；`Accept()` 与 `AcceptSource` 互斥；
- `Next()` 暂时只允许一个并发等待者，重复等待返回 `EBUSY`；
- source 的创建、`Next()`、`Stop()` 和移动都发生在 owner loop 线程；
- source 停止后，queued stream 仍由 source 持有，直到被 `Next()` 转移或 source 被明确销毁；
- source 析构时必须已经停止且没有 pending physical request。

这里的“borrow”只针对 listener 的 accept fd。已经成功接收并由 `Next()` 返回的 stream 不再
依赖 listener，可以独立转移到业务协程。

## 3. pending depth 与 event queue

配置至少包含：

```cpp
struct AcceptSourceOptions {
  std::size_t pending_depth{4};
  std::size_t event_capacity{64};
};
```

约束：

```text
pending_depth > 0
event_capacity >= pending_depth
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

没有空闲 budget
  -> 不再 re-arm 新 request
  -> Reactor 暂停 accept readiness
  -> io_uring one-shot 等待现有 request 收敛
  -> native multishot 请求进入 cancel/terminal 收敛路径

consumer 成功 Next()
  -> queued_events 减一
  -> 释放一个 admission slot
  -> source 恢复 re-arm
```

`pending_depth` 是吞吐和 burst 吸收能力；`event_capacity` 是内存和连接资源的硬上限，
两者不能混为一个参数。

## 4. 生命周期状态

逻辑 source 状态：

```text
Idle
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

source 进入 `Stopping` 不等于所有 physical request 已经释放；只有所有 pending request
都观察到终止结果后，source 才能进入 `Draining`。

## 5. 错误与终止策略

第一版采用保守策略：

| 后端结果 | source 行为 |
| --- | --- |
| accept 成功 | 构造 Stream，入队，继续 admission |
| Reactor `EAGAIN` | 本轮没有事件，重新等待 readiness |
| `ECONNABORTED` | 丢弃该连接尝试，继续 re-arm；不终止 source |
| `EMFILE` / `ENFILE` | source 进入终止错误；避免 busy retry |
| `ENOBUFS` / `ENOMEM` | source 进入终止错误；由上层决定重建 source |
| listener close | source 停止 admission，排空已有 queue 后正常结束 |
| explicit `Stop()` | 取消 pending request，排空已有 queue 后正常结束 |
| 后端未知错误 | source 进入 sticky 终止错误 |

未来可以加入带 timer/backoff 的 resource-exhaustion retry，但不放入第一版契约；否则
`Next()` 的终止语义会与 retry policy 耦合。

## 6. Reactor 实现映射

Reactor 没有 native multishot accept。它通过 readiness 和 accept-drain 实现相同的 source
语义：

```text
source Active
  -> 注册 listen fd readable
  -> callback 中循环 accept() 直到 EAGAIN 或 admission budget 用尽
  -> 每个成功 fd 入队一个 Stream
  -> queue 满时移除/暂停 readable interest
  -> Next() 消费后重新注册 readiness
```

Reactor 的 readiness 本身不是业务事件；业务事件只由成功的 `accept()` 产生。
由于同一个监听 fd 的 readiness 不需要并行投递多个请求，当前 Reactor path 实际最多保留
一个 armed accept；`pending_depth` 在该后端仍作为共享 admission 上限保留，但不会制造多份
并行 poll request。`event_capacity` 才是 Reactor path 中真正限制已接收连接积压的参数。

## 7. io_uring 实现映射

第一版使用 one-shot accept re-arm：

```text
source Active
  -> 按 pending_depth 提交多个 accept SQE
  -> 每个 CQE 产生一个 accepted Stream
  -> CQE dispatch 后根据 admission budget 决定是否重新提交
  -> queue 满时停止 re-arm
  -> Stop/Close 时提交 cancel，并等待所有 accept CQE 收敛
```

native multishot accept 作为第二种内部 path：

```text
一个 multishot request
  -> CQE(F_MORE)：入队一个 Stream，request 保持 active
  -> queue 达到 watermark：取消 request
  -> 最终 CQE：记录 source terminal
  -> 等待 queue 排空后释放 source
```

两种 path 对业务都只暴露 `Next()`；区别保留在 luring 内部的 operation family 和
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
- [x] 在 Reactor 实现 bounded accept-drain 与 readiness pause/resume；
- [x] 覆盖 source `Stop()`、listener `Close()`、队列消费和 listener 生命周期回归。

后续顺序：

1. 在 io_uring 实现 one-shot accept re-arm，复用现有 `accept_depth` 作为初始
   `pending_depth`；
2. 增加 Reactor/io_uring source 与 listener close/cancel 的交错测试；
3. 确认 one-shot source 的生命周期和背压后，再接 native multishot accept。

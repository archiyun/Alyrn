# loop、ring 与 worker

## Loop

`Loop` 是一个单线程、一个 `io_uring` ring 的调度单元：

```text
一个 worker thread
  └── 一个 Loop
        ├── 一个 Ring
        ├── completion dispatch
        ├── coroutine ready queues
        ├── timers
        └── shared provided-buffer ring
```

基本使用顺序是：

```cpp
alyrn::uring::Loop loop;
auto initialized = loop.Init(options);
if (!initialized.HasValue()) {
  // 根据 error 判断环境不支持或配置错误
}
loop.Run(stop_token);
```

`Init()`、`RunAfter()`、`CancelTimer()`、`Schedule()` 与所有提交/消费 CQE 的 loop 内部操作都
必须发生在 loop 所属线程；这不是仅供 debug 的约定，错误线程会在所有构建配置中被拒绝。
`RequestStop()` 是唯一可由其他线程调用的 loop API：它唤醒 ring wait，并由 owner loop 提交全局
取消、消费目标 CQE、drain ready work，然后才进入 `Stopped`。这仍不等价于销毁 listener/stream
或归还 BufferLease；这些对象的最终 close/release 仍属于 worker/runtime 的 owner-thread 协议。

`Loop` 析构不是隐式 shutdown，且始终必须在创建它的 owner thread 执行。若已初始化的 loop
仍持有 user operation、pending SQE、inflight CQE 或 ready continuation，析构会触发不变量失败，
而不会把 `io_uring_queue_exit()` 当作取消 awaiter-owned storage 的替代品。正常 owner 路径应先让
`Run()` 完成 stop drain；手动驱动的测试也必须先把 loop 排空。

`Run()` 没有 shutdown error return，因此全局 cancel SQE 的一次本地 preparation 失败、或 drain
期间一次本地 submit/poll 失败，都不能让 loop 假装已经 `Stopped`。owner loop 会继续 reaping 已有
CQE，并在后续 turn 重试全局 cancel 和 pending submit；只有 pending submit、inflight request 和 ready
continuation 都已排空，才发布 `Stopped`。这与 source/stream 的显式 `Stop()` 或 `Close()` 不同：后两者
可以把 preparation error 返回给调用方，由调用方决定是否重试。

这个 io_uring 专属的 retry 约束由
[`luring_loop_stop_retry.tla`](../formal/luring_loop_stop_retry.tla) 建模：global cancel CQE 只让
cancel request 自身 terminal，target request 仍必须各自收到 terminal CQE；一次本地 preparation 或
flush 失败后，`Stopped` 只能在后续 retry 成功并完全 drain 后出现。

## loop 配置

`Options` 中最影响外部行为的选项是：

| 选项 | 作用 | 说明 |
| --- | --- | --- |
| `entries` | SQ 容量 | 影响突发期间可排队的物理请求 |
| `setup_sqpoll` | 启用 kernel submission thread | opt-in，可能需要权限并长期占用资源 |
| `shared_buffer_capacity/size` | worker 共享的 provided-buffer ring 上限 | 为零时不能创建 `RecvSource`，也不能使用无参 `Recv()` |

CQ 深度、submission flag 和每轮 CQE/continuation 公平性预算是 `Loop` 的内部策略，
不属于调用方配置。这样每个 loop 都使用相同的提交和调度模型；需要改变资源成本时，调用方
只选择 SQ 深度、SQPOLL 与 provided-buffer 容量。

`setup_sqpoll` 不是“永远更快”的开关。它以一个专用 kernel 线程和 CPU/权限成本换取
更低的提交 syscall 频率，只有在持续高压且经过基准验证时才应启用。

## Worker 与 WorkerGroup

`WorkerGroup` 为每个 worker 创建独立的 loop、ring、listener 和 connector。连接回调
在对应 worker 上执行，并通过 `WorkerContext` 取得当前 loop 绑定的对象。

`cpu_affinity` 或 `cpu_affinity_factory` 是可选的线程亲和性设置：

```text
创建 worker thread
  -> 设置目标 CPU（如果配置）
  -> 初始化 ring
  -> 发布启动成功
  -> 进入 loop
```

它不是协程迁移机制，也不改变 operation 的 loop affinity。跨 worker 发送工作要经过
mailbox/`MSG_RING`，不能把一个 worker 的 stream 直接交给另一个 worker 使用。

## 测试观察点

- ring 初始化成功、权限不足和内核不支持能被区分；
- 所有 stream/listener 操作都在 owner loop 上执行；
- `Start()` 的部分失败会停止并回收已经启动的 worker；
- `Stop()` 返回后 worker thread、ring 和 pending operation 都已收敛；
- CPU affinity 是可选行为，测试不应依赖机器恰好拥有某个 CPU；
- 配置 `max_*_per_turn` 后，completion work 和普通 ready work 都最终获得服务。

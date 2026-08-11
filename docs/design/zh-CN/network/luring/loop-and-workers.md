# loop、ring 与 worker

## LUringLoop

`LUringLoop` 是一个单线程、一个 `io_uring` ring 的调度单元：

```text
一个 worker thread
  └── 一个 LUringLoop
        ├── 一个 LUringRing
        ├── completion dispatch
        ├── coroutine ready queues
        ├── timers
        └── shared provided-buffer ring
```

基本使用顺序是：

```cpp
coropact::luring::LUringLoop loop;
auto initialized = loop.Init(options);
if (!initialized.has_value()) {
  // 根据 error 判断环境不支持或配置错误
}
loop.Run(stop_token);
```

`Init()` 和 loop-affine 资源的创建必须发生在 loop 所属线程。`RequestStop()` 可以由其他
线程调用：它唤醒 ring wait，并由 owner loop 提交全局取消、消费目标 CQE、drain ready work，
然后才进入 `Stopped`。这仍不等价于销毁 listener/stream 或归还 BufferLease；这些对象的最终
close/release 仍属于 worker/runtime 的 owner-thread 协议。

`Run()` 没有 shutdown error return，因此全局 cancel SQE 的一次本地 preparation 失败不能让
loop 假装已经 `Stopped`。owner loop 会继续 reaping 已有 CQE，并在后续 turn 重试全局 cancel；
只有 pending submit、inflight request 和 ready continuation 都已排空，才发布 `Stopped`。这与
source/stream 的显式 `Stop()` 或 `Close()` 不同：后两者可以把 preparation error 返回给调用方，
由调用方决定是否重试。

## loop 配置

`LUringOptions` 中最影响外部行为的选项是：

| 选项 | 作用 | 说明 |
| --- | --- | --- |
| `entries` / `cq_entries` | SQ/CQ 容量 | 影响突发期间可排队的物理请求 |
| `submit_batch` | 达到多少准备好的 SQE 后倾向批量提交 | 影响 syscall 次数与提交延迟 |
| `setup_sqpoll` | 启用 kernel submission thread | opt-in，可能需要权限并长期占用资源 |
| `setup_defer_taskrun` | 延迟 kernel task work 到 enter 转换 | 依赖 single issuer，环境不支持时初始化失败 |
| `max_cqe_per_turn` | 一轮最多处理的 CQE | 防止 completion burst 独占 loop |
| `max_ready_work_per_turn` | 一轮最多恢复的 ready work | 防止普通协程独占 loop |
| `shared_buffer_capacity/size` | worker 共享的 provided-buffer ring 上限 | 为零时不能创建 `RecvSource` |

`setup_sqpoll` 不是“永远更快”的开关。它以一个专用 kernel 线程和 CPU/权限成本换取
更低的提交 syscall 频率，只有在持续高压且经过基准验证时才应启用。

## LUringWorker 与 WorkerGroup

`LUringWorkerGroup` 为每个 worker 创建独立的 loop、ring、listener 和 connector。连接回调
在对应 worker 上执行，并通过 `LUringWorkerContext` 取得当前 loop 绑定的对象。

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

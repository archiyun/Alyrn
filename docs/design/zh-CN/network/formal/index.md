# TLA+ 生命周期模型

这些模型不是运行时代码的逐行翻译，而是对 CoroPact 可观察语义的有限状态验证：
提交、完成、取消、协程恢复与资源释放分别在何处线性化。

推荐阅读顺序：

1. `async_stream_core.tla`：单个 stream operation 的抽象控制流。
2. `async_stream_multiop.tla`：read/write 并行与 operation identity。
3. `resource_close_cancel.tla`：Close、Cancel、target completion 与 storage release。
4. `loop_stop_control.tla`：`RequestStop()` 与资源 close 的边界。
5. `async_stream_backend_refinement.tla`：Reactor 与 luring 对抽象模型的映射。
6. `accept_source_refinement.tla`、`recv_source_*.tla`：多事件 source 与 lease。
7. `send_zc_split_release_refinement.tla`：zero-copy send 的业务完成与 buffer release 分离。
8. `async_operation_lifecycle_shapes.tla`：三个正交 lifecycle 维度的组合约束。

## 模型索引

| 模型 | 关注的问题 |
| --- | --- |
| `async_stream_core.tla` | 单次操作的 suspend、complete、resume 与资源状态 |
| `async_stream_multiop.tla` | operation identity，read/write 同时 pending 时的归属 |
| `async_stream_backend_refinement.tla` | `ObsReactor` / `ObsLUring` 如何隐藏 readiness、SQE/CQE step 并映射到同一逻辑 trace |
| `async_stream_multiop_backend_refinement.tla` | 多 operation 的后端 refinement |
| `resource_close_cancel.tla` | Close preparation、committed drain、cancel request terminal CQE 与 target/storage release 的边界 |
| `loop_stop_control.tla` | dispatcher stop 不等于资源 close；Stopped 需要 pending operation 收敛 |
| `scheduler_completion_liveness.tla` | completion 后 continuation 最终获得调度服务 |
| `linked_timeout_submission_failure.tla` | timed operation 的子请求提交失败与收敛 |
| `accept_source_refinement.tla` | Reactor accept drain 与 luring multishot accept 的逻辑 source 语义 |
| `recv_source_lease.tla` | `BufferLease` 归还与 source 停止 |
| `recv_source_incremental_lease.tla` | 多个 lease 的逐步归还与背压 |
| `send_zc_split_release_refinement.tla` | primary completion 与可选 `F_NOTIF` 的分离式释放 |
| `async_operation_lifecycle_shapes.tla` | result cardinality、physical convergence 与 release coupling 的正交组合 |

每个模型的 `.cfg` 约束了 TLC 的有限实例。执行方式：

```bash
tlc -config docs/design/zh-CN/network/formal/loop_stop_control.cfg \
  docs/design/zh-CN/network/formal/loop_stop_control.tla
```

模型验证的是协议不变量，不替代真实 socket/ring 的集成测试。两者共同构成验证：模型覆盖
交错状态，测试覆盖具体后端、内核能力和资源所有权。

# luring 协程帧优化记录（已撤回）

> 状态：帧池在 luring 网络路径中的接入已撤回。历史压测显示它在当前固定 HTTP 负载下没有稳定收益；`CoroFramePoolResource` 仍保留为通用能力，供后续独立实验使用。

## 目标

侵入式队列中的 `Work`、`ResumeWork` 和 luring awaiter 会直接进入协程帧。优化目标是减少每个挂起操作的固定字段，同时保持 io_uring CQE 的完成、取消和跨 loop 调度语义不变。

## 优化过程

1. 压缩调度工作状态。

   `Work`/`ResumeWork` 使用紧凑的指针标记和句柄存储，`ResumeWork` 稳定在 16B，减少 ready queue 节点和 awaiter 中的固定开销。

2. 压缩 `LUringOp`。

   - `LUringCqeResult` 用一个 32 位整数表达 pending、成功值和负 errno，`INT_MIN` 作为空值。
   - `LUringOpKind` 的最高位复用为 completed 标记，`kCount` 保留为非提交占位 sentinel，并通过静态断言保证 kind 不越过 7-bit dispatch 范围。
   - 删除 `owner` 和 `on_complete` 指针，使用 CRTP `LUringOpHook<Owner, Tag>` 从匹配的基类子对象恢复 awaiter，再用集中 dispatch 将 CQE 路由到类型化回调。

   结果：`LUringOp` 从 48B 降到 24B。

3. 复用 `op.result` 保存立即结果。

   Read/Write awaiter 的立即成功或立即 errno 不再各自持有 `optional<Result<...>>`，统一写入 `LUringOp::result`。异步 CQE 和立即路径共用同一个结果读取逻辑。

4. 压缩具体 awaiter。

   - `ReadSome` / `WriteSome`：64B → 48B。
   - `WriteSomeParts`：248B → 232B。
   - 带超时读取：120B → 104B。
   - `SleepAwaiter`：`expected<void>` 换成 4B result state，56B → 40B。
   - `ConnectAwaiter` 删除 `optional<Result<LUringStream>>`，立即错误直接复用 `op.result`。
   - listener close 复用 op 内置的 `ResumeWork` 和紧凑 `LUringCloseState`，移除独立 cancel op、resume work、optional result 和完成 bool。

5. 收口完成路径。

   close 的两个完成来源可能是 cancel CQE 和最后一个 accept/read/write CQE。当前路径用 `current` 判断避免在 `HandleCqe` 自动调度之外再次把同一个 continuation 入队。

## frame pool 对比

测试使用同一台机器、同一构建和同一 benchmark 参数，仅切换 luring 的 `FRAME_POOL`：

- Linux 6.x/7.x Arch，12th Gen Intel(R) Core(TM) i5-12450H，12 CPU。
- Release 构建，4 个 Reactor worker 和 4 个 io_uring worker/ring，io_uring entries=8192。
- 4 个本地 Nginx upstream，round-robin，`MAX_IDLE_PER_PEER=0`，`MAX_IDLE_TOTAL=64`。
- wrk 4 threads；并发 100、500、1000、5000、10000；每档 warmup 3s，正式 15s × 3 轮。
- Nginx 1.30.4；所有样本均为 0 non-2xx、0 socket error、0 timeout。

| 并发 | luring off RPS | luring on RPS | 吞吐变化 | off p99 | on p99 | off RSS | on RSS |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 224.2k | 241.0k | +7.5% | 3.227ms | 3.277ms | 19.4MB | 20.6MB |
| 500 | 173.4k | 192.5k | +11.1% | 8.453ms | 7.713ms | 29.9MB | 34.0MB |
| 1000 | 155.0k | 168.7k | +8.8% | 16.013ms | 15.717ms | 42.4MB | 49.7MB |
| 5000 | 138.4k | 148.0k | +7.0% | 63.597ms | 60.707ms | 137.1MB | 167.5MB |
| 10000 | 122.4k | 125.9k | +2.8% | 127.963ms | 127.047ms | 253.8MB | 314.0MB |

frame pool 开启后吞吐提升约 3%～11%，高并发收益趋于减弱；代价是池保留的帧内存使 10000 并发 RSS 增加约 60MB。由于 Nginx 在两轮独立测试中的吞吐也有波动，这组结果用于工程方向判断，不作为微小收益的严格显著性证明。

完整均值数据见同目录的 `results-luring-frame-pool-20260726.csv`。

## 验证

- `cmake --build build -j2`
- 全量 `ctest --test-dir build --output-on-failure`：75/75 通过。
- luring/Reactor/Nginx 三方 benchmark：off/on 各 45 个 wrk 样本，全部无错误。
- `git diff --check`，以及 `[[nodiscard]]` 独立换行检查通过。

仓库中还包含跨模块 `[[nodiscard]]` 排版统一；这只是格式整理，不影响上述布局或调度设计。

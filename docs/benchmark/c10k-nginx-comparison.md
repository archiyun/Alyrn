# C10k 压测报告：运行时网关 vs Nginx

> 对应 issue #11。目标：在**同一台机器、同一组上游、同一压测工具**下，把本项目的反向代理网关
> (`runtime_gateway`) 和 nginx 反向代理放在完全对等的条件里压测，覆盖 100 → 10000 并发，
> 评估 RPS、延迟分布、错误率、CPU/内存占用与高并发下的连接稳定性。
>
> 所有配置与脚本都在本目录下，可一键复现（见[复现步骤](#复现步骤)）。原始数据见
> `results/`（每个并发档 3 轮的逐次结果 + 聚合均值）。

## 1. 测试环境

| 项 | 值 |
|---|---|
| 机器 | 单机，回环（loopback）压测 |
| CPU | 12th Gen Intel Core i5-12450H — 混合架构 **4 P-core + 4 E-core = 8 核 / 12 线程** |
| 内存 | 16 GiB |
| OS / 内核 | Arch Linux / Linux 7.0.10-arch1-1 |
| CPU 调速器 | `powersave`（见[注意事项](#7-注意事项与公平性)） |
| 编译器 | gcc 16.1.1，`-DCMAKE_BUILD_TYPE=Release` |
| Buffer 实现 | `muduo`（默认，`RUNTIME_BUFFER_IMPL=muduo`） |
| 被测版本 | `bench/c10k-nginx-comparison` @ `ef08a89` |
| nginx | 1.30.2（reuseport + epoll + multi_accept） |
| 压测工具 | wrk f8eb608 \[epoll] |
| `ulimit -n` | 524288 |
| `net.core.somaxconn` | 4096 |

**拓扑**——两个网关代理到**完全相同**的一组上游：

```
                    ┌─────────────────────────────────────┐
   wrk  ──────────► │  被测网关                            │ ──► 上游 nginx :9001
 (-t4 -c{N})        │   A) runtime_gateway  :8080 (4 线程) │ ──► 上游 nginx :9002
                    │   B) nginx 反代       :8088 (4 worker)│ ──► 上游 nginx :9003
                    └─────────────────────────────────────┘ ──► 上游 nginx :9004
                                                                  (每个返回固定 512B JSON)
```

上游是 4 个 nginx 虚拟服务（端口 9001–9004），每个对任意请求 `return 200` 一段**精确 512 字节**
的 JSON。两个网关都以 round-robin 把请求转发到这 4 个上游，使用 keepalive 长连接回源。

## 2. 对等性设置

issue 要求两边「同机器 / 同 OS / 同压测工具 / 同线程数 / 同连接数 / 同测试时长 / 同响应体」。
逐项对齐如下：

| 维度 | runtime_gateway | nginx 网关 | 是否对等 |
|---|---|---|---|
| 监听 | `:8080` | `:8088` | ✅ |
| 工作线程 | `IO_THREADS=4` | `worker_processes 4` | ✅ |
| 回源连接池 | `max_idle_per_peer=64` | `keepalive 64` | ✅ |
| 负载均衡 | `round_robin` | upstream 默认 round-robin | ✅ |
| 上游 | 9001–9004（共享同一组） | 9001–9004（同一组） | ✅ |
| 响应体 | 512B JSON（同一上游产出） | 512B JSON（同一上游产出） | ✅ |
| 压测工具/参数 | `wrk -t4 -c{N} -d15s --latency` | 同左 | ✅ |
| 测试时长 / 轮数 | 15s × 3 轮取均值，前置 3s 预热 | 同左 | ✅ |

CPU/内存只统计**被测网关自身**的进程：runtime_gateway 按进程名采样；nginx 网关按其 master
pidfile + 子 worker 采样，从而**排除共享上游 nginx 的开销**（采样实现见 `run_bench.sh` 的
`sample_cpu`，基于 `/proc/[pid]/stat` 的 utime+stime 增量算真实区间 CPU）。

## 3. 测试命令

```bash
# 1) 构建（Release）
cmake -B build-release -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j"$(nproc)" --target demo_bench_gateway_multi

# 2) 一键跑完整套（起上游 → 压 8080 → 压 8088 → 收尾）
DURATION=15s ROUNDS=3 LEVELS="100 500 1000 5000 10000" \
  docs/benchmark/bench_all.sh
```

单档压测等价于：

```bash
wrk -t4 -c100   -d15s --latency http://127.0.0.1:8080/   # 网关
wrk -t4 -c100   -d15s --latency http://127.0.0.1:8088/   # nginx
# … c=500 / 1000 / 5000 / 10000，每档 3 轮
```

## 4. 结果（3 轮均值）

### RPS / QPS

| 并发 | runtime_gateway | nginx | gateway / nginx |
|---:|---:|---:|---:|
| 100   | **278,962** | 309,131 | 90.2% |
| 500   | **221,499** | 241,723 | 91.6% |
| 1000  | **201,745** | 212,023 | 95.2% |
| 5000  | **120,954** | 191,115 | 63.3% |
| 10000 | **98,300**  | 143,638 | 68.4% |

### 延迟（ms）

| 并发 | 服务 | avg | p50 | p90 | p99 |
|---:|---|---:|---:|---:|---:|
| 100   | gateway | 0.37  | 0.30  | 0.49  | 1.10  |
|       | nginx   | 0.33  | 0.29  | 0.52  | 0.82  |
| 500   | gateway | 2.25  | 1.96  | 2.93  | 3.27  |
|       | nginx   | 2.06  | 1.79  | 2.92  | 3.37  |
| 1000  | gateway | 4.95  | 4.54  | 6.44  | 6.77  |
|       | nginx   | 4.71  | 4.19  | 6.31  | 7.28  |
| 5000  | gateway | 41.03 | 37.12 | 60.47 | 83.94 |
|       | nginx   | 25.95 | 23.62 | 34.01 | 40.01 |
| 10000 | gateway | 100.36| 103.47| 143.98| 174.44|
|       | nginx   | 69.05 | 63.01 | 91.94 | 122.89|

### 错误率 / 连接稳定性

| 并发 | gateway 错误 | nginx 错误 |
|---:|---:|---:|
| 100 / 500 / 1000 / 5000 / 10000 | **0** | **0** |

两边在所有并发档（含 10k）的 3 轮里，non-2xx、socket error（connect/read/write）、timeout
**全部为 0**。即在本环境下，两个网关在 C10k 都能保持连接稳定、无丢请求。

### CPU / 内存（被测网关进程自身）

| 并发 | gateway CPU% | gateway RSS(MB) | nginx CPU% | nginx RSS(MB) |
|---:|---:|---:|---:|---:|
| 100   | 305 | 36.3  | 303 | 128.1 |
| 500   | 303 | 40.9  | 301 | 131.5 |
| 1000  | 307 | 46.1  | 301 | 135.3 |
| 5000  | 300 | 87.0  | 300 | 169.0 |
| 10000 | 302 | 140.2 | 286 | 219.4 |

> CPU% 为区间真实占用（100% = 1 个逻辑核）；RSS 取测试期间峰值。

## 5. 对比小结

- **低并发（100–1000）**：runtime_gateway 跟得很紧，RPS 约为 nginx 的 **90–95%**；
  延迟分布几乎重合，在 500/1000 档 p99 甚至略优于 nginx（6.77 vs 7.28ms @1000）。
- **高并发（5000–10000）**：差距拉开，RPS 降到 nginx 的 **63–68%**，p99 延迟在 5000 档约为
  nginx 的 **2 倍**（83.9 vs 40.0ms）。这是当前主要短板。
- **内存**：runtime_gateway 全程显著更省——36→140MB，nginx 128→219MB。即便在 10k 档，
  网关 RSS 也只有 nginx 的 **~64%**，空载差距更大(~1/6，见 [§7 PSS 复测](#7-内存占用归因pss-复测))。
  归因:优势主要在**固定基线**(单进程 + 不按 `worker_connections` 预分配)，每连接边际内存两边相当(~10KB)。
- **CPU**：两者在所有档位都顶在 **~300%（≈3 核）** 的同一上限——说明在本机这套同机压测里，
  瓶颈是**整机 CPU 被三方（被测网关 + 上游 nginx + wrk）瓜分到饱和**，而非网关单点。
  在「同样吃满 3 核」的前提下 nginx 产出更高吞吐 = **单位 CPU 效率更高**。
- **错误率**：两者均为 0，连接稳定性都达标。

## 6. 瓶颈分析

1. **整机 CPU 饱和是首要约束。** 8 核 /12 线程的机器上，被测网关(4) + 上游 nginx(4) +
   wrk(4) 恰好把核占满；三方任一升压都在抢同一批核。两个被测网关都稳定停在 ~300% CPU，
   说明测的其实是「给定 ~3 核能榨出多少吞吐」。**这套数字反映的是相对效率，不是各自的吞吐上限**
   ——独立机器（压力机 / 网关 / 上游分离）下两者绝对值都会更高。

2. **高并发下网关扩展性弱于 nginx。** 从 1000 → 10000，nginx RPS 衰减 212k→144k（-32%），
   网关衰减 202k→98k（-51%），且 p99 增长更陡。连接数从千级涨到万级时，网关在
   每连接状态管理 / epoll 事件处理 / 回源连接池调度上的开销增长比 nginx 快。可疑点：
   - 回源连接池在高并发下的命中/调度路径（`UpstreamConnPool`，每 IO 线程一份）；
   - 单次请求的 buffer 分配/拷贝（当前 `muduo` Buffer 实现，可对比 `ringbuf`/`nginx` 实现）；
   - `ProxySession` 在 5k+ 活跃会话时的对象/定时器开销。

3. **混合架构（P-core/E-core）放大尾延迟。** i5-12450H 的 4 个 E-core 单核较弱，调度器把
   网关线程或 wrk 线程落到 E-core 时会抬高 p90/p99。这对两边都成立，但吞吐更高、调度更激进的
   一方受影响更明显，可能解释 5k 档网关尾延迟的跳变。

4. **`powersave` 调速器** 压低了两边的绝对频率；换 `performance` 后绝对 RPS 会整体上移，
   但因为两边同等受影响，**相对对比结论不变**。

### 后续可深入的方向

- 把压力机 / 网关 / 上游拆到不同机器（或至少 CPU 亲和性隔离 `taskset`），测各自真实吞吐上限。
- 在 `performance` 调速器 + CPU 绑核下复测，去掉频率与混合调度的噪声。
- 切 `RUNTIME_BUFFER_IMPL=ringbuf` / `nginx` 复测，定位 buffer 路径是否为高并发瓶颈。
- 用 `perf` 火焰图定位 5k+ 并发下网关的热点（连接池 vs 解析 vs 系统调用）。

## 7. 内存占用归因（PSS 复测）

第 4 节的内存用的是 `ps`/RSS。但**跨进程把 RSS 相加会把进程间共享的页（代码段、只读数据）
重复计数**，对 5 进程的 nginx 不公平。为此用 `/proc/[pid]/smaps_rollup` 的 **PSS**（按比例摊分
共享页）复测了空载与 10k 负载两种状态：

| 状态 | 指标 | runtime_gateway | nginx_gateway | 网关 / nginx |
|---|---|---:|---:|:--:|
| 空载    | RSS | 22.1 MB  | 124.4 MB | 18% |
| 空载    | **PSS** | **18.4 MB**  | **112.7 MB** | **16%** |
| 10k 负载 | RSS | 123.3 MB | 216.9 MB | 57% |
| 10k 负载 | **PSS** | **119.6 MB** | **202.9 MB** | **59%** |

**结论:省内存是真的，不是测量假象。**

1. **双重计数影响很小。** nginx 的 PSS 仅比 RSS 低约 14MB（202.9 vs 216.9），说明其 worker 内存
   绝大部分是**私有页**而非共享页——换 PSS 后差距基本不变。runtime_gateway 是单进程，PSS≈RSS。

2. **优势主要在固定基线。** 空载时网关 PSS 18.4MB，仅为 nginx 112.7MB 的 **1/6**。nginx 这 113MB
   基线绝大多数是**按 `worker_connections=65535 × 4 worker` 预分配的连接/事件结构池**——启动即占。
   网关不做这种「按上限预分配」，连接结构随实际连接增长。

3. **每连接边际内存两边相当（~10KB）。** 用 PSS 算 100→10000 连接的增量：
   网关 (119.6−18.4)/9900 ≈ **10.5 KB/连接**，nginx (202.9−112.7)/9900 ≈ **9.3 KB/连接**。
   即「省内存」省的是**固定开销**（单进程模型 + 不预分配），不是单连接更省。

> 复测命令见 `run_bench.sh` 思路；PSS 取自 `awk '/^Pss:/' /proc/<pid>/smaps_rollup`，
> nginx 对 master + 4 worker 求和，10k 负载下取 15s 内的峰值。

## 8. 注意事项与公平性

- **同机回环**：wrk、上游 nginx、被测网关共享 12 线程，存在三方互相挤占——这是本报告最大的
  系统性偏差来源，绝对数值偏保守。但两个被测网关在**完全相同**的挤占条件下轮流测试，
  故横向对比是公平的。
- CPU/内存仅计被测网关自身，已排除共享上游 nginx。
- 每档 3 轮取均值，并有 3s 预热消除冷启动 / 连接建立抖动。
- `powersave` 调速器、未做 CPU 绑核——刻意保持「开箱即测」的默认环境，便于他人复现。

## 复现步骤

```bash
# 前置：nginx、wrk 已安装
cmake -B build-release -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j"$(nproc)" --target demo_bench_gateway_multi

# 一键复现全部结果（约 9–12 分钟）
DURATION=15s ROUNDS=3 LEVELS="100 500 1000 5000 10000" \
  docs/benchmark/bench_all.sh

# 产物：
#   docs/benchmark/results/gateway.csv   逐轮原始数据
#   docs/benchmark/results/nginx.csv     逐轮原始数据
#   docs/benchmark/results/raw/*.txt     每次 wrk 完整输出
```

本目录文件清单：

| 文件 | 用途 |
|---|---|
| `nginx_upstream.conf` | 共享上游（9001–9004，512B JSON） |
| `nginx_gateway.conf` | 对照用 nginx 反代网关（:8088） |
| `run_bench.sh` | 单目标并发扫描 + 结果解析 + CPU/RSS 采样 |
| `bench_all.sh` | 一键编排：起上游 → 压两个网关 → 收尾 |
| `results/` | 原始与聚合结果 |

# 网络库统一 HTTP 压测

本报告记录当前 CoroPact checkout 中 Reactor、CoroPact luring、raw liburing、Asio、Monoio、Compio、libaio、libuv、libevent 和 libev 的统一压测结果。

Reactor 与 luring 的独立 ET/LT 对照见 [CoroPact luring 与 Reactor 独立对比](luring-reactor-comparison-20260802.md)。注意：LT/ET 只属于 Reactor 的 epoll readiness 路径，luring 使用 io_uring CQE，不存在 luring LT/ET。

## 结论

- CoroPact luring 与 raw liburing 位于吞吐第一梯队。两者在 10000 并发的吞吐几乎相同：luring `475.1k RPS`，raw liburing `474.3k RPS`；luring P99 为 `30.82 ms`，raw liburing 受少量长尾样本影响为 `494.08 ms`，但本轮没有 wrk timeout。
- luring 在 200/500/1000 并发领先 raw liburing，raw liburing 在 100/2000/5000 略高；这组差异属于同一量级，不能当作稳定的绝对排名。
- Reactor 没有错误或 timeout，10000 并发为 `350.1k RPS / 37.16 ms P99`，但 CPU 快照最高之一（约 `293%`）。
- Asio、Compio、libaio、libev 在本轮均无正式错误；10000 并发分别为 `458.1k / 124.81 ms`、`388.2k / 43.45 ms`、`407.5k / 37.61 ms`、`344.4k / 44.75 ms`。
- Monoio 在 10000 并发出现 `566` 个 timeout，平均 P99 `699.35 ms`；libuv 出现 `1839` 个 timeout，平均 P99 `2913.33 ms`。两者的高并发结果应视为本轮稳定性异常，不是正常稳态尾延迟。
- libevent 没有 timeout，但 10000 并发吞吐为 `219.8k RPS`，明显低于其他适配器。
- libaio 使用 `IO_CMD_POLL + 非阻塞 socket` 兼容路径，completion 到达后再执行 `accept4`/`recv`/`send`，因此只作兼容性参考，不能与原生异步 socket read/write 结果完全等价比较。

这不是网络框架的综合排名。工作负载只覆盖 loopback 上的固定 HTTP accept/read/write 路径，不包含 TLS、上游代理、HTTP 路由、业务逻辑或真实网卡。

## 测试环境

- CPU：12th Gen Intel(R) Core(TM) i5-12450H，12 个逻辑 CPU
- Compiler：GCC 16.1.1；Rust `rustc 1.97.1`
- Kernel：`7.1.5-arch1-2`
- Client：`wrk f8eb608 [epoll]`
- 系统库：libuv `1.52.1`、libevent `2.1.13`、libev `4.33`、libaio `0.3.113`、liburing `2.15`
- 测试日期：2026-07-31

## 对比目标

- **Reactor**：CoroPact 的 epoll `ReactorWorkerGroup` 后端。
- **CoroPact luring**：CoroPact 的协程 io_uring 后端。
- **raw liburing**：原生 liburing 状态机，不使用 CoroPact 协程封装。
- **Asio**：standalone Asio 1.38.2。
- **Monoio**：Monoio 0.2.4，Rust thread-per-core io_uring runtime。
- **Compio**：Compio 0.19.1，Rust completion runtime。
- **libaio**：Linux AIO poll-only 兼容适配器。
- **libuv**：libuv 1.52.1。
- **libevent**：libevent 2.1.13，使用 bufferevent。
- **libev**：libev 4.33，使用非阻塞 socket + ev_io。

## 测试口径

- 每个目标是一个固定 HTTP keep-alive 服务：请求头检测到 `\r\n\r\n` 后返回 HTTP 200 和 512-byte body。
- 所有响应使用相同的 `Server: unified-http-bench`、`Content-Length: 512` 和 keep-alive 头。
- 每个服务使用 4 个 worker thread；每个 worker 拥有自己的 loop/runtime/ring/listener，并使用 `SO_REUSEPORT`。
- raw liburing、luring、Monoio 和 Compio 使用 io_uring 路径；libaio 使用 poll completion 兼容路径。
- 客户端：`wrk -t8`。
- 并发档位：`100 200 500 1000 2000 5000 10000`。
- 每档：预热 5 秒，正式 3 轮，每轮 10 秒，socket timeout 5 秒。
- 表中 RPS、P99 是三轮算术平均；P99 是三轮 wrk 报告值的平均，不是合并样本后的全局 percentile。
- CPU/RSS 是每轮结束时的进程快照；4 个 worker 的 CPU 会累计，因此可能超过 100%。

## 三轮平均结果

单元为 `RPS / P99(ms)`；timeout 另见错误表。

| 并发 | Reactor | luring | raw liburing | Asio | Monoio | Compio | libaio | libuv | libevent | libev |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 618,509 / 1.03 | 711,153 / 2.28 | 736,643 / 2.53 | 667,868 / 2.28 | 683,299 / 2.46 | 609,839 / 2.73 | 571,251 / 1.75 | 612,731 / 2.18 | 380,352 / 1.16 | 630,789 / 2.45 |
| 200 | 549,768 / 0.91 | 777,894 / 2.80 | 759,918 / 2.99 | 679,233 / 2.54 | 655,562 / 3.33 | 646,335 / 3.07 | 591,260 / 2.30 | 615,357 / 2.89 | 388,402 / 1.51 | 626,218 / 2.77 |
| 500 | 491,186 / 1.75 | 752,050 / 3.69 | 728,497 / 3.68 | 701,554 / 3.48 | 587,888 / 4.21 | 563,998 / 3.89 | 543,199 / 3.32 | 578,668 / 3.35 | 380,650 / 2.62 | 580,510 / 3.79 |
| 1000 | 423,971 / 3.48 | 691,530 / 4.42 | 637,535 / 4.61 | 618,217 / 4.33 | 554,357 / 5.12 | 467,535 / 5.63 | 567,087 / 4.26 | 499,518 / 5.07 | 305,686 / 5.32 | 554,259 / 5.00 |
| 2000 | 382,813 / 8.03 | 541,005 / 6.50 | 564,016 / 6.58 | 493,303 / 7.63 | 480,174 / 8.90 | 459,801 / 7.77 | 459,271 / 7.92 | 460,497 / 10.00 | 259,548 / 10.03 | 465,398 / 8.49 |
| 5000 | 348,043 / 19.66 | 500,613 / 15.54 | 510,084 / 15.28 | 462,954 / 22.29 | 437,028 / 21.19 | 422,423 / 18.23 | 437,416 / 19.27 | 387,715 / 228.94 | 245,845 / 25.35 | 370,213 / 22.81 |
| 10000 | 350,073 / 37.16 | 475,104 / 30.82 | 474,344 / 494.08 | 458,078 / 124.81 | 404,608 / 699.35 | 388,214 / 43.45 | 407,503 / 37.61 | 360,719 / 2913.33 | 219,797 / 62.21 | 344,378 / 44.75 |

## 10000 并发资源快照

这是 10000 并发三轮快照的平均值，RSS 单位为 MiB。

| 目标 | CPU | RSS |
| --- | ---: | ---: |
| Reactor | 293.0% | 181.8 |
| luring | 253.7% | 146.3 |
| raw liburing | 241.0% | 163.6 |
| Asio | 261.0% | 166.6 |
| Monoio | 237.7% | 249.2 |
| Compio | 257.7% | 234.1 |
| libaio | 261.0% | 158.4 |
| libuv | 270.0% | 5.5 |
| libevent | 312.0% | 26.8 |
| libev | 258.7% | 163.4 |

RSS 是每轮结束时的一次进程快照；各库的连接对象、缓冲区和事件对象生命周期不同，因此这里只作相对参考，不作为 allocator 对比结论。

## 错误与异常

| 目标 | 正常 HTTP 响应错误 | socket error | timeout 总数 |
| --- | ---: | ---: | ---: |
| Reactor | 0 | 0 | 0 |
| luring | 0 | 0 | 0 |
| raw liburing | 0 | 0 | 0 |
| Asio | 0 | 0 | 0 |
| Monoio | 0 | 0 | 566 |
| Compio | 0 | 0 | 0 |
| libaio | 0 | 0 | 0 |
| libuv | 0 | 0 | 1839 |
| libevent | 0 | 0 | 0 |
| libev | 0 | 0 | 0 |

10000 并发的异常轮次：

- Monoio：三轮 timeout 为 `0 / 504 / 62`，P99 为 `43.28 / 1710 / 344.77 ms`。
- libuv：三轮 timeout 为 `861 / 610 / 368`，P99 为 `2860 / 3350 / 2530 ms`。
- raw liburing 没有 timeout，但三轮 P99 为 `723.07 / 469.71 / 289.45 ms`，说明仍存在严重长尾样本。

## Reproduce

C++ 目标需要先配置 io_uring Release build；Asio checkout 路径可通过 `ASIO_ROOT` 指定：

```bash
cmake -S . -B build-uring \
  -DASIO_ROOT=/tmp/asio-1-38-2 \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_EXAMPLES=ON \
  -DBUILD_TESTS=OFF \
  -DBUILD_BENCHMARKS=OFF \
  -DCOROPACT_ENABLE_URING=ON
cmake --build build-uring -j2
```

Rust 目标：

```bash
CARGO_NET_OFFLINE=true cargo build --release --bins \
  --manifest-path benchmarks/network-libraries-rust/Cargo.toml
```

正式压测和汇总：

```bash
OUTDIR=/tmp/coropact-network-libraries-$(date +%Y%m%d-%H%M%S) \
  ./docs/benchmark/run_network_libraries.sh
./docs/benchmark/summarize_network_libraries.sh "$OUTDIR"
```

可用环境变量覆盖 `LEVELS`、`WARMUP`、`DURATION`、`ROUNDS`、`THREADS`、`WORKERS`、`REACTOR_TRIGGER_MODE` 和 `TARGETS`。`REACTOR_TRIGGER_MODE` 支持 `et`（默认）和 `lt`，用于对比流 socket 的 edge-triggered 与 level-triggered 路径。例如短试跑：

```bash
WARMUP=1s DURATION=2s ROUNDS=1 LEVELS="100 1000" \
  TARGETS="luring raw-liburing libuv libevent libev" \
  ./docs/benchmark/run_network_libraries.sh
```

只压测 Reactor 的 LT 路径：

```bash
REACTOR_TRIGGER_MODE=lt WARMUP=1s DURATION=2s ROUNDS=1 LEVELS="100 1000" \
  TARGETS="reactor" ./docs/benchmark/run_network_libraries.sh
```

本轮原始结果保存在 `/tmp/coropact-network-libraries-20260731/`，其中 `raw/` 是每轮 wrk 输出，`runs.csv` 是运行索引，`resources.csv` 是资源快照，`averages.csv` 是三轮均值。临时结果不纳入仓库。

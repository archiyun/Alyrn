# 2026-08-10 网络库基线

这是一轮用于回归观察的 loopback HTTP 基线，不是网络框架综合排名。它比较 Alyrn 的 Epoll、当前源码构建的 luring、raw liburing、standalone Asio、libuv、libevent 和 libev；工作负载不覆盖 TLS、真实网卡、HTTP 解析、路由或业务逻辑。

## 结论

- `C=100` 时 raw liburing 吞吐最高（`890.3k RPS`），luring 为 `846.1k RPS`；luring P99 更低（`1.11 ms` 对 `2.35 ms`）。
- `C=1000` 时 raw liburing 为 `759.7k RPS`，luring 为 `747.9k RPS`；luring P99 更低（`3.34 ms` 对 `4.09 ms`）。两项吞吐差异均接近本机轮次波动范围，不能作绝对排名。
- `C=5000` 时 luring 为 `558.9k RPS / 10.91 ms P99`，raw liburing 为 `553.6k RPS / 13.56 ms P99`。约 1% 的吞吐差异不具显著性；luring 在本轮的 P99 更低。相对 Epoll，luring 吞吐高 `13.6%`，P99 低 `1.68 ms`。
- Epoll 在 `C=100` 与 `C=1000` 的 P99 分别为 `0.63 ms` 和 `2.34 ms`，低于 luring；这是 epoll readiness 路径在这项小型固定响应 workload 下的低延迟表现，不应据此推导一般业务结论。
- raw liburing 曾在 `C=5000` 的 SQE 耗尽时直接关闭连接，产生 read error；该 reference adapter 现已改为 deferred submission queue 和 accept depth 4，并用零错误的三轮结果替换旧数据。

## 测试口径

- 日期：2026-08-10
- 主机：12th Gen Intel(R) Core(TM) i5-12450H，12 个逻辑 CPU；Linux `7.1.6-arch1-1`
- 编译器：GCC `16.1.1`；客户端：`wrk f8eb608 [epoll]`
- 构建：Release，`ALYRN_ENABLE_URING=ON`
- 服务：固定 HTTP/1.1 keep-alive；收到完整请求头后返回 HTTP 200 和 512-byte body。
- 每个目标：4 worker，`SO_REUSEPORT`；luring 和 raw liburing 使用 `URING_ENTRIES=1024`。
- luring：当前 checkout 构建的 `examples/uring/demo_bench_http_luring`，single-shot accept，普通借用 buffer，协程帧池关闭。
- raw liburing：当前 checkout 构建的 `examples/uring/demo_bench_http_liburing`；每 worker 维持 accept depth 4，并在 SQE 暂时耗尽时 FIFO 重试，而不是关闭连接。
- 客户端：`wrk -t8`；每档预热 3 秒，正式 5 秒，3 轮；socket timeout 5 秒。
- RPS 与 P99 为三轮算术平均；P99 是每轮 wrk P99 的平均值，而不是合并样本的全局 percentile。汇总脚本同时输出每项的最小值、最大值、样本标准差和 CV，避免把桌面环境中的小幅差异误读为优化收益。

本机初始软 `nofile` 为 1024。`C=5000` 首轮中所有目标均恰好出现 3,987 个 connect error，因此整档作废；随后将客户端软限制设为 `65535` 后完整重跑。`C=1000` 也在同一高限制下完整复测，避免靠近文件描述符边界。脚本现会在限制不足时失败，而不是输出误导性结果。

## 三轮平均结果

单位为 `RPS / P99(ms)`。所有未标注为无效的单元格均为零 connect/read/write error、零 timeout、零 non-2xx。

| 并发 | Epoll | luring | raw liburing | Asio | libuv | libevent | libev |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 790,069 / 0.63 | 846,132 / 1.11 | 890,350 / 2.35 | 745,541 / 0.63 | 687,627 / 0.50 | 413,023 / 0.55 | 634,065 / 0.67 |
| 1000 | 672,202 / 2.34 | 747,908 / 3.34 | 759,657 / 4.09 | 706,484 / 2.93 | 608,484 / 2.94 | 353,862 / 4.20 | 658,715 / 3.05 |
| 5000 | 492,078 / 12.59 | 558,934 / 10.91 | 553,572 / 13.56 | 509,706 / 12.08 | 407,070 / 14.79 | 298,458 / 22.82 | 435,641 / 13.74 |

Epoll、luring、Asio、libuv、libevent 与 libev 的 `C=100` 原始目录为 `/tmp/alyrn-network-libraries-current-20260810/`；`C=1000` 的高 `nofile` 复测为 `/tmp/alyrn-network-libraries-current-20260810-c1000-highfd/`；`C=5000` 的高 `nofile` 复测为 `/tmp/alyrn-network-libraries-current-20260810-highfd/`。修复后 raw liburing 的全档复测为 `/tmp/alyrn-raw-liburing-fixed-full-20260810/`。它与其它目标是同一配置下的独立批次，临时原始日志不纳入仓库。

### 波动范围

这里的 `±` 是三轮样本标准差。luring 的 RPS 为：`C=100` `846,132 ± 20,532`（CV `2.43%`）、`C=1000` `747,908 ± 18,479`（CV `2.47%`）、`C=5000` `558,934 ± 9,479`（CV `1.70%`）；修复后的 raw liburing 在 `C=5000` 为 `553,572 ± 12,952`（CV `2.34%`）。因此共享桌面环境下约 2% 的差异不应被解释为性能优化；应在同一高 `nofile` 配置、更多轮次和隔离主机上复验。

## 5000 并发资源快照

CPU 与 RSS 是每个正式轮次结束后由 `ps -p <pid> -o %cpu=,rss=` 读取的三次样本均值。procps 的 `%CPU` 是从进程启动至采样时刻的累计 CPU 时间与已运行时间之比，不是瞬时或固定窗口利用率；它包含该进程的 worker 线程，因此可以超过 100%。RSS 是采样时驻留内存，不代表峰值。它们只说明本轮运行成本，不能代替 allocator 或内存上界比较。

| 目标 | CPU | RSS |
| --- | ---: | ---: |
| Epoll | 271.7% | 100.4 |
| luring | 259.3% | 87.1 |
| raw liburing | 247.4% | 36.4 |
| Asio | 268.7% | 84.8 |
| libuv | 293.0% | 70.9 |
| libevent | 314.7% | 14.2 |
| libev | 288.7% | 83.8 |

raw liburing 的资源快照来自修复后的独立复测批次。

## 复现

先配置带 io_uring 的 Release build：

```bash
cmake -S . -B build-uring \\
  -DASIO_ROOT=/tmp/asio-1-38-2 \\
  -DCMAKE_BUILD_TYPE=Release \\
  -DBUILD_EXAMPLES=ON \\
  -DBUILD_TESTS=OFF \\
  -DBUILD_BENCHMARKS=OFF \\
  -DALYRN_ENABLE_URING=ON
cmake --build build-uring -j2
```

提高客户端文件描述符上限后运行：

```bash
ulimit -n 65535
OUTDIR=/tmp/alyrn-network-libraries-$(date +%Y%m%d-%H%M%S) \\
  TARGETS='epoll luring raw-liburing asio libuv libevent libev' \\
  ENTRIES=1024 WORKERS=4 THREADS=8 \\
  LEVELS='100 1000 5000' WARMUP=3s DURATION=5s ROUNDS=3 TIMEOUT=5s \\
  ./docs/benchmark/run_network_libraries.sh

./docs/benchmark/summarize_network_libraries.sh "$OUTDIR"
```

该脚本会检查当前软 `nofile`。对于这里的最大并发 5000，要求至少为 `5136`；`65535` 是保守、可读的运行值。

## 解释边界

- 本机是共享桌面环境，结果用于开发回归而非对外发布；需要可发表数据时，应在空闲、频率稳定的机器上延长到 20–30 秒并重复更多轮次。
- raw liburing、Asio、libuv、libevent 与 libev 是参考适配器，不等于其完整框架的最佳配置。
- 本轮 luring 使用 single-shot accept、普通 buffer 和关闭的协程帧池；它不是 multishot、provided buffer 或 zero-copy receive 的评估。

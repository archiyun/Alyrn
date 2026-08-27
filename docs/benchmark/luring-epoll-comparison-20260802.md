# Alyrn luring 与 Epoll 独立对比

本报告记录 2026-08-02 对 Alyrn `src/uring` 和 `src/epoll` 两个网络后端的统一 HTTP 压测。本文专门修正一个容易误解的地方：`LT/ET` 是 Epoll 的 epoll readiness 触发模式，luring 不存在 LT/ET 两条路径。

## 两个模块的测试对象

### Epoll

Epoll 使用 epoll 监听 socket readiness，再通过非阻塞 `accept`、`recv` 和 `send` drain 出逻辑事件。`EPOLL_TRIGGER_MODE` 只对 Epoll 生效：

- `et`：edge-triggered readiness；
- `lt`：level-triggered readiness。

因此本次分别测试了 Epoll ET 和 Epoll LT，用于观察 readiness 触发策略对吞吐与尾延迟的影响。

### luring

luring 使用 io_uring 的 SQE/CQE 完成路径，由每个 worker 独占自己的 ring、loop 和 listener。它等待 CQE，而不是等待 epoll readiness，所以没有 LT/ET 的概念。两次测试命令中虽然分别设置了 `EPOLL_TRIGGER_MODE=et` 和 `EPOLL_TRIGGER_MODE=lt`，但该变量在 luring 分支中不会被读取；两次 luring 数据只是两次独立重复运行，不能解释为“luring ET”和“luring LT”。

本次 luring benchmark 使用 demo 的默认 single-shot accept，accept depth 为 4。

## 测试口径

- 构建：`build-uring`，Release
- worker：4；客户端：`wrk -t8`
- luring ring entries：`URING_ENTRIES=1024`
- 并发：`100 200 500 1000 2000 5000 10000`
- 每档：预热 5 秒，正式测试 10 秒，3 轮
- socket timeout：5 秒
- 响应：固定 HTTP 200，512-byte body，keep-alive
- 表中 RPS 和 P99 是三轮算术平均；P99 单位为毫秒

`URING_ENTRIES=8192` 在当前环境的 4 worker 配置下启动阶段返回 `ENOMEM`，因此本次对比统一使用 1024。这个启动限制应单独修复，不能把 1024 当作 luring 的默认容量结论。

## 结果一：Epoll ET 对 luring

此批次先运行 luring，再运行 Epoll ET。luring 列是 luring 的一次独立参考运行，不是 luring 的 ET 模式。

| 并发 | luring RPS / P99 | Epoll ET RPS / P99 |
| ---: | ---: | ---: |
| 100 | 712,061 / 2.92 ms | 616,270 / 2.93 ms |
| 200 | 642,146 / 3.50 ms | 639,227 / 3.21 ms |
| 500 | 676,101 / 3.93 ms | 640,096 / 3.55 ms |
| 1000 | 585,380 / 5.41 ms | 587,511 / 4.81 ms |
| 2000 | 519,359 / 7.66 ms | 456,307 / 8.42 ms |
| 5000 | 471,665 / 19.78 ms | 424,015 / 20.97 ms |
| 10000 | 449,368 / 40.05 ms | 394,350 / 47.60 ms |

## 结果二：Epoll LT 对 luring

此批次先运行 luring，再运行 Epoll LT。这里的 luring 列仍然只是 luring 的另一次独立参考运行，不能称为 luring LT。

| 并发 | luring RPS / P99 | Epoll LT RPS / P99 |
| ---: | ---: | ---: |
| 100 | 693,625 / 2.90 ms | 620,960 / 2.54 ms |
| 200 | 688,388 / 3.28 ms | 579,477 / 3.42 ms |
| 500 | 691,130 / 3.97 ms | 572,650 / 4.31 ms |
| 1000 | 563,218 / 5.35 ms | 591,969 / 4.67 ms |
| 2000 | 505,940 / 8.21 ms | 470,925 / 9.70 ms |
| 5000 | 436,502 / 20.95 ms | 425,773 / 23.89 ms |
| 10000 | 416,375 / 43.23 ms | 403,734 / 49.94 ms |

## 资源快照

资源数据是每轮结束时的进程快照，CPU 为 4 个 worker 的累计进程 CPU，RSS 单位为 MiB。这里只作运行成本参考，不替代 profiler 或完整时间序列采样。

| 批次 / 目标 | C=10000 平均 CPU | C=10000 平均 RSS |
| --- | ---: | ---: |
| ET / luring | 239.7% | 171.2 MiB |
| ET / Epoll | 259.3% | 187.3 MiB |
| LT / luring | 235.0% | 163.6 MiB |
| LT / Epoll | 250.3% | 184.9 MiB |

两批次所有档位均为 0 connect/read/write error、0 timeout、0 non-2xx response。

## 结论边界

- luring 在 C≥2000 的两批次中通常比 Epoll ET/LT 有更高吞吐；C=1000 附近两者接近，Epoll P99 略有优势。
- C≥2000 时 luring 的 P99 通常也低于 Epoll；这反映的是本次固定 HTTP workload 下的完成路径差异，不代表所有业务负载都成立。
- luring 两次结果之间存在正常运行波动，不能从两次数据推导“luring ET/LT 差异”。正确的对比关系只有：`luring vs Epoll ET` 和 `luring vs Epoll LT`。
- `URING_ENTRIES=8192` 的 `ENOMEM` 启动问题仍是 luring 运行配置问题，后续应定位 ring 内存需求、worker 数量和启动阶段额外 ring 的关系。

## 原始结果

- ET 批次：`/tmp/alyrn-luring-epoll-20260802-et-entries1024/`
- LT 批次：`/tmp/alyrn-luring-epoll-20260802-lt-entries1024/`

临时目录不纳入仓库；本文件保留汇总数据和可复核的测试口径。

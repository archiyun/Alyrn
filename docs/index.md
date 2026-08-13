# CoroPact 文档

设计说明目前以中文为主。根目录 [README](../README.md) / [README.zh-CN.md](../README.zh-CN.md)
是使用入口；[`CONTEXT.md`](../CONTEXT.md) 是领域词汇；[`SUBSYSTEMS.md`](SUBSYSTEMS.md)
是模块依赖的规范。

## 先读

- [CONTEXT.md](../CONTEXT.md) — 术语、公开 seam、线程亲和、协程帧
- [Runtime 子系统边界](SUBSYSTEMS.md) — 分层与硬依赖规则
- [ADR](adr/) — 已接受的架构决策

## 网络

- [网络库总览](design/zh-CN/network/index.md)
- [生命周期精化协程 I/O（LRCI）](design/zh-CN/network/lifecycle-refined-coroutine-io.md)
- [AsyncStream 语义契约](design/zh-CN/network/async-stream-contract.md)
- [AcceptSource 语义契约](design/zh-CN/network/accept-source-contract.md)
- [Runtime Builder](design/zh-CN/network/runtime-builder.md)
- [Reactor / epoll](design/zh-CN/network/reactor/index.md)
- [luring / io_uring](design/zh-CN/network/luring/index.md)
- [kqueue](design/zh-CN/network/kqueue/index.md)
- [KqueueLoop、Post 与主从移交](design/zh-CN/network/kqueue/loop-and-handoff.md)

## 数据结构

- [侵入式结构总览](design/zh-CN/datastructure/index.md)
- [侵入式模型](design/zh-CN/datastructure/intrusive-model.md)

## 构建、打包与发布

- [打包与安装](packaging.md) — Linux 源码安装、`.deb`、Arch `PKGBUILD`、Docker
- [Release notes: CoroPact 0.1.0](releases/v0.1.0.md)

kqueue 后端不进入 Linux 发行包。BSD/Darwin 上用
`-DCOROPACT_ENABLE_KQUEUE=ON` 从源码构建，并链接 `CoroPact::coropact_kqueue`。

## 基准报告

- [2026-08-10 C++ 网络库基线](benchmark/network-libraries-20260810.md)
- [网络库统一 HTTP 压测](benchmark/network-libraries.md)
- [CoroPact luring 与 Reactor 独立对比](benchmark/luring-reactor-comparison-20260802.md)

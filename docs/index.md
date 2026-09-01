# Alyrn 文档

设计说明目前以中文为主。根目录 [README](https://github.com/archiyun/Alyrn/blob/main/README.md) /
[README.zh-CN.md](https://github.com/archiyun/Alyrn/blob/main/README.zh-CN.md) 是使用入口；
`CONTEXT.md` 是仅供本地开发使用的领域词汇，不属于仓库发布内容；
[`SUBSYSTEMS.md`](SUBSYSTEMS.md)
是模块依赖的规范；[C++ 书写规范](coding-style.md)
覆盖 `auto` 等语义拼写规则。

## 先读

- [Runtime 子系统边界](SUBSYSTEMS.md) — 分层与硬依赖规则
- [C++ 书写规范](coding-style.md) — `auto` / `auto*` 与长限定名
- [ADR](adr/) — 已接受的架构决策

## 论文草稿

- [LRCI：epoll 与 io_uring 的可观察语义统一](papers/lrci-epoll-iouring-draft.md)

## 网络

- [网络库总览](design/zh-CN/network/index.md)
- [生命周期精化协程 I/O（LRCI）](design/zh-CN/network/lifecycle-refined-coroutine-io.md)
- [AsyncStream 语义契约](design/zh-CN/network/async-stream-contract.md)
- [AcceptSource 语义契约](design/zh-CN/network/accept-source-contract.md)
- [Runtime Builder](design/zh-CN/network/runtime-builder.md)
- [Epoll](design/zh-CN/network/epoll/index.md)
- [luring / io_uring](design/zh-CN/network/luring/index.md)

## 数据结构

- [侵入式结构总览](design/zh-CN/datastructure/index.md)
- [侵入式模型](design/zh-CN/datastructure/intrusive-model.md)

## 构建、打包与发布

- [打包与安装](packaging.md) — Linux 源码安装、`.deb`、Arch `PKGBUILD`、Docker
- [Release notes: Alyrn 0.1.0](releases/v0.1.0.md)

## 基准报告

- [2026-08-10 C++ 网络库基线](benchmark/network-libraries-20260810.md)
- [网络库统一 HTTP 压测](benchmark/network-libraries.md)
- [Alyrn luring 与 Epoll 独立对比](benchmark/luring-epoll-comparison-20260802.md)

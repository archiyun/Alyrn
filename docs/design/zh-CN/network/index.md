# 网络库

两个网络后端是平行 adapter：Linux epoll 与 Linux `io_uring` luring。
它们 refine 同一套 `alyrn::backend` 契约（应用通过 `alyrn::io`
别名使用），不共享 event loop 或 poller 实现。

- [生命周期精化协程 I/O（LRCI）](lifecycle-refined-coroutine-io.md)
- [AsyncStream 与 AsyncListener 协程语义契约](async-stream-contract.md)
- [AcceptSource 语义契约](accept-source-contract.md)
- [Runtime Builder](runtime-builder.md)
- [Epoll](epoll/index.md)
- [luring 功能说明](luring/index.md)
- [io_uring Operation Matrix](luring-operation-matrix.md)
- [TLA+ 生命周期模型](formal/index.md)

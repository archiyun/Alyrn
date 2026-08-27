# 网络库

三个网络后端是平行 adapter：Linux epoll、Linux `io_uring` luring、
BSD/Darwin kqueue。它们 refine 同一套 `io` 契约，不共享 event loop 或 poller 实现。

- [生命周期精化协程 I/O（LRCI）](lifecycle-refined-coroutine-io.md)
- [AsyncStream 与 AsyncListener 协程语义契约](async-stream-contract.md)
- [AcceptSource 语义契约](accept-source-contract.md)
- [Runtime Builder](runtime-builder.md)
- [Epoll](epoll/index.md)
- [luring 功能说明](luring/index.md)
- [io_uring Operation Matrix](luring-operation-matrix.md)
- [kqueue](kqueue/index.md)
- [Loop、Post 与主从移交](kqueue/loop-and-handoff.md)
- [用 Lamport 视角讨论协程网络运行时的语义统一与热插拔](lamport-hot-swap-runtime.md)

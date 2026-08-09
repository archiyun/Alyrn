# Reactor / epoll

- [EventLoop、readiness 与取消/关闭](loop-and-cancellation.md)

Reactor 是 CoroPact 的 Linux readiness 后端。它以非阻塞 socket syscall 与
`epoll` 为基础；它不提交长期存在的 kernel I/O request。这个区别决定了它的
取消、buffer 生命周期与 luring 不同。

# Epoll

- [Loop、readiness 与取消/关闭](loop-and-cancellation.md)

Epoll 是 Alyrn 的 Linux readiness backend，以非阻塞 socket syscall 与
`epoll` 为基础；它不提交长期存在的 kernel I/O request。这个区别决定了它的
取消、buffer 生命周期与 luring 不同。BSD/Darwin 的平行 readiness backend 是
[kqueue](../kqueue/index.md)，不会通过条件编译复用 Epoll 的实现。

“Reactor pattern”仍然是对 readiness 驱动事件处理模型的通用称呼；Epoll
这里只表示 Alyrn 的 Linux 具体 backend，kqueue 同样可以采用这一架构模式。

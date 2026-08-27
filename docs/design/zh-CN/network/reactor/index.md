# Reactor / epoll

- [Loop、readiness 与取消/关闭](loop-and-cancellation.md)

Reactor 是 Alyrn 的 Linux readiness 后端。它以非阻塞 socket syscall 与
`epoll` 为基础；它不提交长期存在的 kernel I/O request。这个区别决定了它的
取消、buffer 生命周期与 luring 不同。BSD/Darwin 的平行 readiness 后端是
[kqueue](../kqueue/index.md)，不要把 kqueue 写成 Reactor 的条件编译。

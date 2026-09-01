# 注册资源的边界

io_uring 的“注册资源”不是一个单一功能。luring 当前已经使用一种 provided-buffer ring，
但这不等于已经公开了 `io_uring_register_buffers()` 或 fixed file API。

## 当前已实现：provided buffer ring

`RecvSource` 和无参 `Recv()` 共用 loop 级共享 provided-buffer ring：

```text
loop init / source create
  -> 分配并发布固定大小的接收 slot
  -> recv CQE 返回 slot id
  -> BufferLease 借出 slot
  -> lease 释放
  -> slot 重新可供 kernel 选择
```

`RecvSource` 的外部契约见 [multishot recv 与 provided buffer](recv-source.md)。这里的关键是
`BufferLease` 的 ownership，而不是让业务保存或修改内核 buffer id。无参 `Recv()` 只把
ring 当作内核写入目标：CQE 后 copy-out 到 `net::Buffer`，并在 resume 前归还 slot，不把
lease 交给调用者。

## 尚未公开：fixed registered buffer

`io_uring_register_buffers()` + `IORING_OP_READ_FIXED` 的模型是：应用预先注册一组 buffer，
之后 SQE 通过固定索引引用它们。当前 Alyrn 没有对应的公共 luring API，也没有稳定的
“注册表 owner、slot 借用、关闭时等待”契约，因此文档和测试都不能把它标为已支持。

未来若开放，至少需要明确：

- 注册表由谁拥有，以及是否 loop-affine；
- 一个 slot 能否同时被多个 in-flight operation 使用；
- operation 完成、取消和 close 哪一个是归还 slot 的边界；
- 注册失败、内核页锁限制和 ring 销毁如何报告；
- 是否有 backend-neutral 的上层抽象，还是保持为 luring extension。

## 尚未公开：fixed file

fixed file 缓存的是 kernel file reference，不改变业务层 fd ownership。它同样没有当前公共
入口。不要因为内部 `Ring` 能准备 SQE，就把 fixed file 当成 Alyrn 的功能。

## 与零拷贝的关系

注册 buffer 可能减少重复 pin/page lookup 或 buffer 分配，但它本身不保证业务处理链路
零拷贝。当前 provided-buffer ring 重点解决的是：

```text
kernel 访问窗口
  ↔ BufferLease ownership
  ↔ slot reuse boundary
```

发送方向的内存 release 则由 [SendZeroCopy](zero-copy-send.md) 的 notification 协议负责，
两者不能共用一个“收到 CQE 即释放”的测试假设。

## 测试观察点

- 当前测试应覆盖 provided buffer 的创建、slot 归还和 source stop；
- 应有测试或文档检查明确表明 fixed registered buffer 没有公共入口；
- 不把 mmap 地址稳定误报成 end-to-end zero-copy；
- 未来新增 fixed buffer API 时，先补 ownership/lifetime 契约再补性能测试。

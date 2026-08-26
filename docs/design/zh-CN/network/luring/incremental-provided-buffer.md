# 增量 provided-buffer 的设计占位

`LUringRecvSource` 当前只使用所属 `LUringLoop` 的 shared provided-buffer pool：一条正
`recv` CQE 对应一个 `BufferLease`，该 lease 释放且内核不再持有该 slot 后，slot 才会归还 ring。

此前的 source-private `IOU_PBUF_RING_INC` 路径已移除。它会在高并发 source 创建时为每个
source 分配独立 buffer group；内核资源耗尽后，该路径无法可靠地收敛 recv request 与 source
析构边界。该问题不能通过静默 fallback 修复：部分 source 成功创建私有 group 时仍会进入不同的
lifecycle。

future incremental 实现必须先满足以下边界，才可以重新引入：

- 私有 group 的创建、失败与销毁不影响其他 source；
- 同一 slot 的每个 segment 都有独立 lease，最后一个 lease 与 kernel-final 共同授权归还；
- `F_BUF_MORE` 与 multishot request 的 `F_MORE` 分开解释；
- 高并发 source 创建、取消、stop 和析构在真实 Linux host 上通过压力验证。

相关的 TLA+ 文件 `formal/recv_source_incremental_lease.tla` 保留为未来路径的协议模型；它
不代表当前 runtime 已支持 `IOU_PBUF_RING_INC`。

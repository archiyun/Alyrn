# SendZeroCopy

`Stream::SendZeroCopy()` 是显式的 luring 扩展。它使用 `send_zc` 请求，并把“发送
结果已经知道”和“kernel 不再使用调用方 buffer”建模成两个不同的完成边界。

## 使用方式

```cpp
auto result = co_await stream.SendZeroCopy(payload);
if (!result.has_value()) {
  // 发送失败
}

// await 返回后，payload 已经越过 luring 承诺的 kernel 使用边界。
const auto bytes = result->bytes;
```

`payload` 必须在 await 返回前保持有效，不能在 `SendZeroCopy()` 返回 awaiter 后立即复用。
空 buffer 是立即成功的特殊情况。

## 物理完成分支

`send_zc` 总会先产生 primary send CQE，但并不总会产生第二个 CQE。primary 的
`F_MORE` 决定后续是否还存在 notification 边界：

```text
primary send CQE, no F_MORE
  -> 保存发送字节数或原始 errno
  -> primary 本身是 physical terminal
  -> buffer 可以 release，恢复 awaiter

primary send CQE, F_MORE
  -> 保存发送字节数或原始 errno
  -> 继续等待 notification

F_NOTIF CQE
  -> 读取 usage bits
  -> 是 kernel 不再访问 buffer 的 physical terminal
  -> buffer 可以 release，恢复 awaiter
```

`IORING_SEND_ZC_REPORT_USAGE` 只规定 notification 出现时 `cqe_res` 如何报告 usage；它不替代
`F_MORE` 成为“必有 notification”的承诺。`F_NOTIF` 不是远端 TCP ACK，也不表示对端应用已经
读取数据；它只表示本地 kernel 不再需要调用方提供的发送内存。

## 返回值

`ZeroCopySendResult` 包含：

| 字段 | 含义 |
| --- | --- |
| `bytes` | primary send CQE 报告的发送字节数 |
| `usage` | `kZeroCopy`、`kCopied` 或 `kUnknown`；只有 notification 提供 usage report 时才可确定 |
| `notification_received` | 是否观察到了独立的 `F_NOTIF` release CQE |

`usage == kCopied` 不是业务错误。`usage == kUnknown` 表示 primary CQE 已经物理终态、但没有
独立 usage report，不能据此推断 copy 或 zerocopy。应用如果只关心数据是否发送，应检查
`bytes`；如果关心内存复用边界，应等待整个 await 完成。

## 和普通 send 的区别

```text
internal short send
  -> 一个普通发送完成 CQE
  -> 按普通 send 生命周期结束

SendZeroCopy
  -> primary CQE
  -> 无 F_MORE：直接 release
  -> 有 F_MORE：等待 release notification CQE
  -> buffer 才可以复用
```

listener 的 `zero_copy_writes` 会让 `Stream::WriteAll()` 在每个完整写入轮次优先选择
这条路径；它不会改变内部普通 send 的语义，也不会让所有写操作自动变成 zerocopy。

## 测试观察点

- primary CQE 带 `F_MORE` 时，协程和 buffer 都不能过早完成/复用；
- primary CQE 不带 `F_MORE` 时，必须直接成为 release boundary，不能等待永远不会到的通知；
- notification 先到时，仍要等待发送结果；
- notification usage 的 copied bit 被正确解释为状态，而不是负 errno；
- 正常发送、copy fallback、发送错误和 close 交错都只恢复一次；
- `WriteAll` 开启该扩展后仍保持完整发送语义。

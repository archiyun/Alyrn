# SendZeroCopy

`LUringStream::SendZeroCopy()` 是显式的 luring 扩展。它使用 `send_zc` 请求，并把“发送
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

## 两个物理事件

当前提交使用 `IORING_SEND_ZC_REPORT_USAGE`，所以逻辑上至少有：

```text
primary send CQE
  -> 保存发送字节数或原始 errno

F_NOTIF CQE
  -> 读取 usage bits
  -> 确认 kernel 已越过 buffer release boundary
```

只有两个事件都被 operation 观察到后，await 才恢复。`F_NOTIF` 不是远端 TCP ACK，也不
表示对端应用已经读取数据；它只表示本地 kernel 不再需要调用方提供的发送内存。

## 返回值

`ZeroCopySendResult` 包含：

| 字段 | 含义 |
| --- | --- |
| `bytes` | primary send CQE 报告的发送字节数 |
| `copied` | kernel 对该次发送选择了 copy path，而非真正 zerocopy |
| `notification_received` | 是否观察到了独立的 `F_NOTIF` release CQE |

`copied == true` 不是业务错误。应用如果只关心数据是否发送，应检查 `bytes`；如果关心
内存复用边界，应等待整个 await 完成。

## 和普通 WriteSome 的区别

```text
WriteSome
  -> 一个普通发送完成 CQE
  -> 按普通 send 生命周期结束

SendZeroCopy
  -> 发送结果 CQE
  -> release notification CQE
  -> buffer 才可以复用
```

listener 的 `zero_copy_writes` 只会让上层写算法选择这条显式路径，不会改变
`WriteSome()` 的语义，也不会让所有写操作自动变成 zerocopy。

## 测试观察点

- primary CQE 先到时，协程和 buffer 都不能过早完成/复用；
- notification 先到时，仍要等待发送结果；
- notification usage 的 copied bit 被正确解释为状态，而不是负 errno；
- 正常发送、copy fallback、发送错误和 close 交错都只恢复一次；
- `WriteAll` 开启该扩展后仍保持完整发送语义。

# stream 操作

`LUringStream` 是 loop-affine 的 TCP stream。它保持 backend-neutral 的
`AsyncStream` 语义，但内部使用 io_uring 的 single-shot request；应用不需要处理 CQE。

## 可用操作

| API | 结果 | 生命周期重点 |
| --- | --- | --- |
| `ReadSome(span<byte>)` | `Result<size_t>` | 写入 buffer 的 request 完成前不能复用 buffer |
| `ReadInto(Buffer, reserve)` | `ReadIntoOutcome` | `Buffer` 的 ownership 交给 awaiter，完成后返回给调用方 |
| `ReadSomeFor(span<byte>, timeout)` | `Result<size_t>` | read 和 timeout 两个物理结果只产生一次业务恢复 |
| `WriteSome(span<const byte>)` | `Result<size_t>` | CQE 到达前发送视图必须保持有效 |
| `Shutdown()` | `Task<Result<void>>` | 半关闭，不等价于销毁 stream 对象 |
| `Close()` | `Task<Result<void>>` | 等待关联 pending I/O 收敛后关闭 fd |

所有 operation 都要求在 stream 所属 `LUringLoop` 线程上进入 `await_suspend()`。把 stream
跨线程传给另一个 loop，或在 foreign thread 上调用操作，是 runtime contract violation。

## 单次读写

典型生命周期是：

```text
ReadSome / WriteSome
  -> 一个 SQE
  -> 一个 CQE
  -> 保存 CQE.res
  -> 调度等待协程
  -> await_resume 返回 Result
```

`res >= 0` 表示字节数，负值映射为错误。`WriteSome()` 不承诺完整发送；需要完整发送
应使用 `coropact::io::WriteAll` 之类的上层算法。当前没有公共 scatter-write 或
`WritePart` 接口。

## ReadInto

`ReadInto()` 把可增长的 `net::Buffer` 移入 operation。它适合不希望业务层手动管理一块
固定 scratch span 的场景：

```text
Buffer ownership -> awaiter
  -> kernel writes into reserved writable area
  -> CQE
  -> outcome returns Buffer ownership
```

测试需要验证成功、EOF、错误和 close 期间 ownership 都只转移一次。

## Close 的可观察语义

`Close()` 不是立即 `close(fd)`：如果 stream 还有 pending read/write，必须先让这些物理
operation 经过正常或取消完成路径，再关闭 fd。这样可以防止 fd 被复用后，迟到 CQE 错误地
影响新对象。

## 测试观察点

- 一次 read/write 只恢复一次，即使 loop 一轮处理了多个 CQE；
- buffer 在 CQE 前保持有效，`ReadInto` 的 ownership 在成功和错误路径都可回收；
- `ReadSomeFor` 的 read-first、timeout-first 和 submission failure 顺序都收敛；
- `Close()` 与 pending read/write 交错时不 double close、不泄漏 fd；
- `WriteSome` 的部分发送不会被误报成完整发送。

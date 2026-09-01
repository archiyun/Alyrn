# stream 操作

`Stream` 是 loop-affine 的 TCP stream。它保持 backend-neutral 的
`AsyncStream` 语义，但内部使用 io_uring 的 single-shot request；应用不需要处理 CQE。

## 可用操作

| API | 结果 | 生命周期重点 |
| --- | --- | --- |
| `Read(span<byte>)` | `Result<size_t>` | 写入 buffer 的 request 完成前不能复用 buffer |
| `Recv(Buffer, reserve)` | `RecvOutcome` | `Buffer` 的 ownership 交给 awaiter，完成后返回给调用方 |
| `Recv()` | `Result<Buffer>` | 内核写入 shared provided-buffer ring，copy-out 后立即归还 slot |
| `Write(span<const byte>)` | `Task<Result<void>>` | 内建短写循环；每轮 send-zc 越过 kernel release boundary 后才可继续使用同一视图 |
| `Shutdown()` | `Task<Result<void>>` | 写方向 half-close；幂等，保留读方向，拒绝新写入 |
| `CloseRead()` | `Task<Result<void>>` | 读方向 half-close；幂等，后续读立即返回 EOF，保留写方向 |
| `CloseWrite()` | `Task<Result<void>>` | 写方向 half-close 的 canonical spelling |
| `Close()` | `Task<Result<void>>` | 等待关联 pending I/O 收敛后关闭 fd |
| `LocalAddr()` | `Result<Endpoint>` | 在 owner loop 上查询本地 IPv4/IPv6 endpoint |
| `RemoteAddr()` | `const Endpoint&` | 返回 accept/connect 时保存的 peer endpoint |

所有 operation 都要求在 stream 所属 `Loop` 线程上进入 `await_suspend()`。把 stream
跨线程传给另一个 loop，或在 foreign thread 上调用操作，是 runtime contract violation。

## 单次读写

典型生命周期是：

```text
Read / internal short send
  -> 一个 SQE
  -> 一个 CQE
  -> 保存 CQE.res
  -> 调度等待协程
  -> await_resume 返回 Result
```

`res >= 0` 表示字节数，负值映射为错误。内部 short send 不承诺完整发送；`Write()` 是
后端内建的完整发送操作，按短写推进输入 span 并把零进展转换为 `EPIPE`。短写 awaiter 是
实现细节，不属于公共 `AsyncStream` 契约。当前没有公共 scatter-write 或 `WritePart` 接口。

## Recv

`Recv(Buffer)` 把可增长的 `net::Buffer` 移入 operation。它适合不希望业务层手动管理一块
固定 scratch span 的场景：

```text
Buffer ownership -> awaiter
  -> kernel writes into reserved writable area
  -> CQE
  -> outcome returns Buffer ownership
```

测试需要验证成功、EOF、错误和 close 期间 ownership 都只转移一次。

无参 `Recv()` 不接收调用者 buffer。它提交带 `IOSQE_BUFFER_SELECT` 的 single-shot recv，
把内核写入 loop 共享 ring 的一个 slot，再 memcpy 进新的 `net::Buffer` 并在 resume 前
归还该 slot。`RecvSource` 仍独占 `BufferLease` 路径；两条 API 共用 ring，但不能共用
lease 语义。epoll 没有 ring，用内部 `Buffer` 接收后返回同一 `Result<Buffer>`。

## Close 的可观察语义

`Close()` 不是立即 `close(fd)`：如果 stream 还有 pending read/write，必须先让这些物理
operation 经过正常或取消完成路径，再关闭 fd。这样可以防止 fd 被复用后，迟到 CQE 错误地
影响新对象。

`Shutdown()` 与 `Close()` 是正交状态：`Shutdown()` 只从 `Writable` 转到
`WriteShutdown`，因此后续 `Read()` 仍然有效；新的 `Write()` 在提交 SQE 前返回
`EPIPE`。空 span 不提交 SQE，但仍检查 loop、stream 生命周期和写槽位。若已有 write request
仍在 pending，`Shutdown()` 返回 `EBUSY`，避免把 half-close 与内核仍可能访问的发送 operation
交错。

`CloseWrite()` 是 `Shutdown()` 的兼容别名。`CloseRead()` 从 `Readable` 转到
`ReadShutdown`；它要求没有 pending read，成功后新的 `Read()` 和 `Recv(Buffer)`
不再提交 recv SQE，而是立即返回 `Result<0>`，无参 `Recv()` 立即返回空 `Buffer`，写侧仍可
继续使用。

## 测试观察点

- 一次 read/write 只恢复一次，即使 loop 一轮处理了多个 CQE；
- buffer 在 CQE 前保持有效，`Recv` 的 ownership 在成功和错误路径都可回收；
- `Close()` 与 pending read/write 交错时不 double close、不泄漏 fd；
- 内部 short send 的部分发送不会被误报成完整发送。

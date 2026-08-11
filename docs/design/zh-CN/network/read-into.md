# `ReadInto`：转移所有权的单次读取

## 结论

`ReadInto` 已经是 CoroPact 的 **ownership-transfer read** 扩展：调用者把一个 move-only
`net::Buffer` 转移给 awaiter；awaiter 在整个 pending interval 内独占这块存储；每条
终态路径都把 buffer 和读取结果一起归还。

它不是 `AsyncStream` core，也不是 `BufferLease` 的替代品。三者解决的是不同的问题：

| 路径 | 调用者提供什么 | pending 期间谁拥有存储 | 完成后得到什么 |
| --- | --- | --- | --- |
| `ReadSome(span<byte>)` | 借用的连续内存 | 调用者 | `Result<size_t>` |
| `ReadInto(Buffer)` | 移交的 `Buffer` | awaiter / operation | `ReadIntoOutcome` |
| `RecvSource::Next()` | 不提供存储 | 后端 pool/ring | 带 `BufferLease` 的事件 |

因此，`ReadInto` 的价值不是把普通 receive 变为零拷贝；数据仍从内核复制进 destination
storage。它解决的是 C++ borrowed buffer 在 `co_await`、取消和 close 交错时的所有权风险。

## 接口与结果

后端实现的可选扩展为：

```cpp
auto awaiter = stream.ReadInto(io::Buffer buffer,
                               std::size_t reserve = 4096);

struct io::ReadIntoOutcome {
  base::Result<std::size_t> result;
  io::Buffer buffer;
};
```

`io::Buffer` 与 `io::ReadIntoOutcome` 都是公开 spelling；它们分别是 `net` 内部实现类型的
无成本别名。后端和 contract 内部仍使用 `net`，避免反向依赖 `io`。

调用模式应始终移动 owner：

```cpp
io::Buffer buffer;
auto outcome = co_await stream.ReadInto(std::move(buffer));

if (!outcome.result) {
  // buffer 仍然归调用者；本次 reservation 已经回滚。
  HandleError(outcome.result.error());
  return;
}

// outcome.buffer 已提交 outcome.result 所指定的新增可读字节。
Consume(outcome.buffer);
```

不能把返回类型改成单纯的 `Result<Buffer>`：那会让失败时 buffer 的归属不清楚。现在的
`ReadIntoOutcome` 明确表达两项独立事实：本次读取是否成功，以及 buffer owner 已经归还。

## 生命周期

```text
Caller owns Buffer
    |
    | ReadInto(std::move(buffer))
    v
Awaiter owns Buffer
    |
    | PrepareWrite(reserve)
    v
Buffer has one active write reservation
    |
    +-- Reactor: readv() / EAGAIN -> epoll readiness -> readv()
    |
    +-- io_uring: recv SQE -> CQE
    |
    v
physical read terminal
    |
    +-- success: CommitWrite(n)
    |
    +-- error, close, cancel, submit failure: AbortWrite()
    v
await_resume() returns { result, Buffer }
```

这给出必须维持的不变量：

1. `PrepareWrite()` 与 `CommitWrite()`/`AbortWrite()` 成对出现，且至多一次；
2. 在 physical read terminal 前，buffer 仍由 awaiter 持有，不能被调用者析构、移动或修改；
3. `await_resume()` 前 reservation 必须已经结算；
4. 无论成功、EOF、I/O error、关闭、取消还是提交失败，owner 都通过 `ReadIntoOutcome` 归还；
5. 对单次操作，业务 continuation 只恢复一次。

`reserve == 0` 不代表零容量读取：两个 Adapter 都把它交给
`Buffer::PrepareWrite(0, ...)`，由 Buffer 使用自身 block size。这样不会把成功返回 `0` 与空
destination 混淆；`0` 的读取结果仍按 stream 语义表示 EOF。

## 两个后端的实现映射

### Reactor

`ReactorStream::ReadIntoAwaiter` 保存 `Buffer` 与 `std::vector<iovec>`。它先
`PrepareWrite(reserve, 16)`，随后立即调用非阻塞 `readv()`：

- 立即成功、EOF 或错误：结算 reservation，`await_suspend()` 返回 `false`；
- `EAGAIN`：将 awaiter 记录为 pending read，启用 `EPOLLIN`；
- readiness、`Close()` 或 loop stop：经同一个 completion gate 结算 reservation，并由
  scheduler-bound continuation 恢复协程。

这里的 `ReadInto` 比 `ReadSome(Buffer&)` 更强：后者保留外部引用，调用者必须自行保证
buffer 在 await 返回前不被触碰；前者把这个要求编码进 move-only ownership。

### io_uring

`LUringStream::ReadIntoAwaiter` 也拥有 `Buffer`。它使用一个 writable iovec 的地址和长度
准备 `IORING_OP_RECV`：

- SQE 成功提交后，awaiter/frame 保持 buffer 和 writable span 存活；
- CQE 到达时，`OnComplete()` 先 `CommitWrite()` 或 `AbortWrite()`，然后由 operation hook
  按既有调度规则恢复 continuation；
- SQE 获取或提交失败时，failure callback 清除 `pending_read_`、回滚 reservation，并以
  immediate error 返回；
- `Close()` 等待 pending read 的物理完成/取消收敛后才关闭 fd，因此不会让迟到 CQE 访问
  已归还的 buffer。

这与 liburing 的单次 `recv` 语义匹配：destination buffer 是 request 的输入，CQE 的
`res` 才是业务结果；而 multishot `recv` 则要求 `IOSQE_BUFFER_SELECT` 并从 provided
buffer pool 为每个 CQE 取 buffer，属于 `RecvSource`/`BufferLease` 协议，而不是
`ReadInto`。参见 [io_uring_prep_recv(3)](https://man7.org/linux/man-pages/man3/io_uring_prep_recv.3.html)
与 [io_uring_prep_recv_multishot(3)](https://man7.org/linux/man-pages/man3/io_uring_prep_recv_multishot.3.html)。

## LRCI 边界审计

下表记录实现中的实际线性化顺序，而不是只比较两个方法是否同名：

| Adapter 路径 | Physical Terminal | Result Ready | Release Authorized | Continuation Authorized |
| --- | --- | --- | --- | --- |
| Reactor immediate | `readv()` 返回 | `FinishAttempt()` | syscall 返回后且 reservation 已结算 | 不挂起，`await_suspend()` 返回 `false` |
| Reactor pending | readiness 后重试 `readv()` 返回 | `CompleteRead()` 选择唯一 awaiter | `FinishAttempt()` commit/abort reservation | `CompletionGate` 胜出后提交 scheduler-bound continuation |
| Reactor close/stop | owner loop 撤销 pending read | `ECANCELED` 固定 | Channel 不再持有本次 awaiter，reservation 已 abort | 与普通 pending completion 使用同一个 gate |
| io_uring submit failure | SQE 未进入 pending/inflight | submit error 固定 | lane rollback，reservation abort | 不挂起，`await_suspend()` 返回 `false` |
| io_uring CQE | target CQE terminal | `LUringOp::TryRecordCqeCompletion()` 保存结果 | `OnComplete()` 结算 reservation 并释放 read lane | handler 返回后，loop 才调度 `ResumeWork` |
| io_uring close/stop | cancel acknowledgement 与 target CQE 收敛 | target CQE 解释为 `ECANCELED` 或已发生的结果 | target CQE 后结算 reservation；cancel CQE 单独不授权归还 | close/read continuation 分别且至多调度一次 |

共同的 read lane contract 是：同一 stream 同时最多一个 logical read；第二个 operation 稳定
返回 `EBUSY`。空 `span` 也必须经过 lane 与 loop-state 检查，不能因为无需 syscall/SQE 就绕过
生命周期协议。

## 与主流接口的关系

Boost.Asio 的 `async_read_some` 是典型 borrowed-buffer API：底层 buffer object 可以被复制，
但调用者必须保证其指向的内存直到 completion handler 被调用前都有效。`ReadSome(span)`
与它是同类契约；`ReadInto` 则把这项有效性保证转移进 operation。参见
[Boost.Asio `async_read_some`](https://www.boost.org/doc/libs/latest/doc/html/boost_asio/reference/basic_stream_socket/async_read_some.html)。

Tokio 的 `read_buf(&mut B)` 也向可增长 buffer 写入并推进内部 cursor，但 Rust 的独占可变
借用在类型系统中保证 buffer 不能在 `.await` 期间被另行移动或修改。C++ 无法从
`Buffer&`/`span` 获得同等保证，因此 `ReadInto(Buffer)` 是对应的显式所有权方案。参见
[Tokio `AsyncReadExt::read_buf`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncReadExt.html#method.read_buf)。

## 验证覆盖与后续约束

现有 Reactor/io_uring smoke tests 已覆盖：成功读回 buffer、close/cancel 后归还、io_uring
提交失败回滚并允许下一次读、恢复时 scheduler affinity，以及返回 buffer 不带活动
reservation。

后续不应把 `ReadInto` 扩张成 multishot 或 provided-buffer 的通用入口。若要增加能力，应遵守：

- 需要后端提供存储、多个事件或显式归还时，扩展 `RecvSource`/`BufferLease`；
- 需要 registered/fixed buffer 时，作为 backend-specific optimization profile，不改变
  `ReadIntoOutcome` 的 ownership 意义；
- 若引入“逻辑取消先返回、物理 I/O 后结束”的操作，不能复用本接口，除非 operation 继续
  独占 buffer 直到 physical release boundary。

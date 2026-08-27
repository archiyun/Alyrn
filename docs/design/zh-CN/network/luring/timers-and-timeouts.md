# timer、超时与停止

## SleepFor

`uring::SleepFor(loop, delay)` 或 connector 的 `SleepFor(delay)` 会把当前协程挂到
owner loop 的 timer queue，时间到期后在同一 loop 上恢复。它不是一个网络 fd，也不会把
timer 事件暴露为普通 stream completion。底层 `SleepFor(loop, delay)` 会返回
`Result<void>`；connector 的便捷包装是 `Task<void>`，适合不需要向调用方传播 timer
错误的内部健康检查循环。

运行时公共时间接口是单调的 `time::Deadline` 与 `time::Duration`。调用方以
`time::Seconds(5)`、`time::Milliseconds(250)` 等显式单位构造 delay；io_uring 绝对
timeout 使用 `IORING_TIMEOUT_ABS` 的单调时钟，不使用 `CLOCK_REALTIME`，因此墙钟校时不会
提前或推迟 I/O deadline。

```text
SleepFor
  -> timer queue
  -> timer driver/control operation
  -> 到期
  -> ScheduleCompletion
  -> 协程恢复
```

非正 delay 可以立即 ready。loop 尚未初始化、timer 提交失败等情况应通过底层 `Result`
返回，不应静默挂起。

`RunAfter()` 只有在新 timer 已进入逻辑树、且其首次 driver/update SQE 已成功准备后才返回
`TimerId`。若 preparation 失败，它会回滚该新 timer 并保留原先已 armed 的 deadline；不能把
“callback 已存入用户态树”误报成“timer 已接受”。已经处理过 CQE 后的重臂没有同步调用者可返回，
因此此时若 re-arm preparation 失败，owner loop 会进入 `Stopping` 并通过正常 cancel/drain 收敛，
而不是静默丢失仍在树中的 timer。

## ReadSomeFor

带超时的读是一个 composite operation：

```text
read SQE -----------\
                     -> logical read result
timeout SQE --------/
```

read 和 timeout 可能按任意顺序产生 CQE。业务协程只能恢复一次；两个 physical member 都
收敛后，awaiter 才能安全释放。read 先成功时，timeout 的迟到 CQE 仍要被正确消费；timeout
先到时，read 的迟到 CQE 也不能访问已释放的 buffer。

## loop 停止

`RequestStop()` 会让 owner loop 进入取消与 completion drain，不能替代应用资源的 `Close()`。
测试不能把“调用 `RequestStop()`”等同于“所有对象已经析构”：只有 pending CQE 和 ready work
收敛后 loop 才会进入 `Stopped`；fd、BufferLease 和 coroutine owner 仍必须遵循各自的释放协议。

## 测试观察点

- delay 到期后恢复一次，零 delay 不死锁；
- read-first、timeout-first、同一轮到达和提交失败都可结束；
- timeout 后 buffer 仍然不会被迟到 read CQE 写入；
- loop stop 后 timer callback 不再新增业务 work；
- timer 和 network operation 同时 pending 时，drain 不泄漏 CQE 或 coroutine frame。

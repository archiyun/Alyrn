# timer、超时与停止

## SleepFor

`luring::SleepFor(loop, delay)` 或 connector 的 `SleepFor(delay)` 会把当前协程挂到
owner loop 的 timer queue，时间到期后在同一 loop 上恢复。它不是一个网络 fd，也不会把
timer 事件暴露为普通 stream completion。底层 `SleepFor(loop, delay)` 会返回
`Result<void>`；connector 的便捷包装是 `Task<void>`，适合不需要向调用方传播 timer
错误的内部健康检查循环。

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

停止 loop 时，timer 和网络 operation 都经过 completion/cancel drain。测试不能用“调用
`Quit()` 后立刻析构 loop”替代停止协议；应等待 loop 观察到 quit、收割 pending CQE，并确认
`IsDrained()`。

## 测试观察点

- delay 到期后恢复一次，零 delay 不死锁；
- read-first、timeout-first、同一轮到达和提交失败都可结束；
- timeout 后 buffer 仍然不会被迟到 read CQE 写入；
- loop stop 后 timer callback 不再新增业务 work；
- timer 和 network operation 同时 pending 时，drain 不泄漏 CQE 或 coroutine frame。

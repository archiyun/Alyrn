## EventLoop + TimerQueue

 整个 Reactor 模型的心脏

### EventLoop 

// 状态
`looping_` Loop()运行的标志
`quit_` 退出请求， 用于优雅关闭
`calling_pending_fuctors` 正在执行跨线程投递的回调函数

// 一线程一事件循环
`thread_id_` 创建EventLoop的记录的线程tid

// IO 多路复用
`poller_`  底册IO复用工具(Select, Poll, EPoll) 默认EPoll
`active_channels_` epoll_wait 每轮返回的活跃 Channel 列表

// 跨线程投递
`wakeup_fd_` eventfd, 用于唤醒 `epoll_wait()` epoll_wait默认最高阻塞10s, 另外timerfd也可以唤醒它。
`wakeup_channel_` wakeup_fd 对应的Channel
`mutex_` + `pending_functors_` 其它线程投递进来的回调队列

// 定时器
`timer_queue_` TimerQueue, 基于 timerfd

### Loop()

每个线程持有一个事件循环`EventLoop`, 每个事件循环极其简洁。
事件循环天然串行处理IO， 一部分是自己管理fd的事件回调， 另一部分是其它线程投递的任务。

```cpp
while (!quit_) {
  active_channels_.clear();
  poll_return_time_ = poller_->Poll(kPollTimeMs, &active_channels_); // 1. 阻塞等待事件， 默认10s， 可被wakeup_fd, timerfd_唤醒。
  
  for(Channel* ch : active_channels_) {
    ch->HandleEvent(poll_return_time_);
  }
  DoPendingFunctors();
}
```


### 跨线程投递: RunInLoop / QueueInLoop


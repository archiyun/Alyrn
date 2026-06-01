## 网络层职责
如果你第一次读这个网络层，我建议按这个顺序：
先了解Reactor架构， 了解下面的类在该架构上充当什么角色。

1. `EventLoop`：理解事件循环、跨线程投递、定时器入口。

2. `Channel`：理解“一个 fd 对应一个事件分发单元”。
   `Channel` 是Reactor 的代理中间件， 往下对接网络接口， 往上持事件的回调函数。

3. `Poller` / `EPollPoller`：理解 `Channel` 如何注册到 epoll，epoll 结果如何回填到 `Channel`。

4. `Acceptor` + `TcpServer`：理解服务端如何 accept 新连接，并把新连接交给 `TcpConnection`。
   `TcpServer` 的注释说得很清楚：它管理监听 socket 和活跃连接，在 base loop accept，并把连接分配给 `EventLoopThreadPool` 中的 IO loop。 include/runtime/net/tcp_server.h:19-22

5. `TcpConnection` + `Buffer`：理解连接建立后的读写、缓冲、关闭。
   `TcpConnection` 代表一条已建立 TCP 连接，拥有 connected socket、`Channel`、输入/输出 buffer，并驱动 read/write/close/error callbacks。 include/runtime/net/tcp_connection.h:26-30

6. `TimerQueue`：理解 `RunAfter` / `RunEvery` 背后的 timerfd 机制。
   `TimerQueue` 是每个 `EventLoop` 的 timerfd 驱动定时器队列，拥有 `Timer` 对象，使用 `TimerTree` 按过期时间索引。 include/runtime/net/timer_queue.h:22-26

7. `Connector` + `TcpClient`：理解客户端侧非阻塞 connect、重试、包装成 `TcpConnection`。
   `Connector` 负责异步 outbound connect，并支持失败后的指数退避重试。 include/runtime/net/connector.h:11-18

---

# 3. 核心模型：Reactor

这个网络层是典型 Reactor 模型，核心链路是：

```text
EventLoop::Loop()
  -> Poller::Poll()
    -> epoll_wait()
    -> FillActiveChannels()
  -> Channel::HandleEvent()
    -> read/write/close/error callback
```

`EventLoop::Loop()` 会不断清空 active channel 列表，调用 `poller_->Poll()` 等待 IO 事件，然后逐个调用 `channel->HandleEvent()`，最后执行跨线程投递进来的 pending functors。 src/net/event_loop.cc:125-134

`EPollPoller::Poll()` 内部就是 `epoll_wait()`，拿到事件后调用 `FillActiveChannels()`，如果当前 event buffer 被填满，还会扩容 `events_`，以便下一轮吸收更大 burst。 src/net/epoll_poller.cc:74-98

`FillActiveChannels()` 从 `epoll_event.data.ptr` 取回 `Channel*`，把 epoll 事件转换成抽象事件写入 `channel->set_revents()`，再 push 到 `active_channels`。 src/net/epoll_poller.cc:100-109

`Channel::HandleEvent()` 会保证必须在 owning loop 线程执行；如果 `Tie()` 过 owner，则先尝试从 weak_ptr lock 出 shared_ptr，避免 owner 已析构后还分发 callback。 src/net/channel.cc:39-50

---

# 4. EventLoop：每个线程一个事件循环

`EventLoop` 有几个非常关键的成员：

```cpp
std::unique_ptr<Poller> poller_;
std::vector<Channel*> active_channels_;
int wakeup_fd_;
std::unique_ptr<Channel> wakeup_channel_;
std::mutex mutex_;
std::vector<Functor> pending_functors_;
std::unique_ptr<TimerQueue> timer_queue_;
```

这些成员直接体现了它的职责：IO multiplexing、active channel 分发、跨线程唤醒、pending functor 队列和定时器。 include/runtime/net/event_loop.h:87-98

构造函数里会创建默认 `Poller`、`eventfd`、`wakeup_channel_` 和 `TimerQueue`，并把 `wakeup_channel_` 的读事件 callback 设成 `HandleRead()`，然后注册读事件。 src/net/event_loop.cc:84-103

这里的 `eventfd` 是跨线程唤醒机制：别的线程调用 `QueueInLoop()` 后，如果 loop 正在 `epoll_wait()`，就写 `eventfd` 唤醒它。 src/net/event_loop.cc:158-169src/net/event_loop.cc:191-197

`RunInLoop()` 的语义是：如果调用者就在 loop 线程，立即执行；否则进入 `QueueInLoop()`，异步排队到 loop 线程执行。 src/net/event_loop.cc:150-156

`QueueInLoop()` 会把 callback 放进 `pending_functors_`，并在跨线程调用或当前正在执行 pending functors 时唤醒 loop。 src/net/event_loop.cc:158-169

`DoPendingFunctors()` 会先把共享队列 swap 到局部 vector，再执行 callback，这样执行 callback 时不持有 mutex，避免阻塞生产者继续投递任务。 src/net/event_loop.cc:199-215

定时器入口也挂在 `EventLoop` 上：`RunAt()`、`RunAfter()`、`RunEvery()` 最终都调用 `timer_queue_->AddTimer()`，`Cancel()` 调用 `timer_queue_->Cancel()`。 src/net/event_loop.cc:218-238

---

# 5. Channel：fd 的事件分发器

`Channel` 是一个很薄但非常重要的对象：它不拥有 fd，只负责记录这个 fd 感兴趣的事件、poller 返回的事件，以及读/写/关闭/错误 callback。 include/runtime/net/channel.h:23-31

`Channel` 的事件位是自己定义的一套抽象位：

```cpp
kReadEvent
kWriteEvent
kErrorEvent
kHupEvent
```

这些抽象事件随后会在 `EPollPoller` 里转换成 `EPOLLIN`、`EPOLLOUT`、`EPOLLERR`、`EPOLLHUP`。 include/runtime/net/channel.h:94-101src/net/epoll_poller.cc:36-51

当你调用 `EnableReading()` / `EnableWriting()` / `DisableWriting()` / `DisableAll()` 时，`Channel` 会更新本地 `events_`，然后调用私有 `Update()` 同步到底层 `Poller`。 include/runtime/net/channel.h:66-72src/net/channel.cc:28-31

`Channel::Update()` 和 `Channel::Remove()` 都要求在 owner loop 线程执行，这就是整个网络层线程模型的基础之一。 src/net/channel.cc:28-36

事件真正分发时，`HandleEventWithGuard()` 按顺序处理 close、error、read、write：如果是 HUP 且没有 read 事件，先调 close；有 error 调 error；有 read 调 read；有 write 调 write。 src/net/channel.cc:53-75

---

# 6. Poller / EPollPoller：epoll 适配层

`Poller` 是一个抽象基类，接口只有三个核心方法：

```cpp
Poll(timeout_ms, active_channels)
UpdateChannel(channel)
RemoveChannel(channel)
```

这说明上层 `EventLoop` 不关心底层是 epoll、poll 还是 select。 include/runtime/net/poller.h:29-39

默认 poller 在 Linux 下是 `EPollPoller`，但也可以通过环境变量 `RUNTIME_POLLER=poll|select|epoll` 覆盖。 src/net/poller.cc:22-40

`EPollPoller` 内部维护一个 epoll fd 和 `std::vector<epoll_event> events_`。 include/runtime/net/epoll_poller.h:34-40

`EPollPoller::UpdateChannel()` 用 `Channel::Index()` 保存 channel 在 epoll 里的状态：新建、已添加、已删除；新建或已删除时走 `EPOLL_CTL_ADD`，已添加但没有任何关心事件时走 `EPOLL_CTL_DEL`，否则走 `EPOLL_CTL_MOD`。 src/net/epoll_poller.cc:112-135

`EPollPoller::Update()` 会把 `Channel::Events()` 转换成 epoll events；如果 `Channel` 配了 edge-triggered，还会加 `EPOLLET`。 src/net/epoll_poller.cc:148-155

所以 `Channel` 和 `EPollPoller` 的分工是：

```text
Channel:
  维护 fd + interested events + callbacks

EPollPoller:
  把 Channel 注册进 epoll
  把 epoll 返回事件写回 Channel::revents_
```

---

# 7. 服务端路径：TcpServer + Acceptor

服务端监听入口是 `TcpServer`。

`TcpServer` 构造时创建 `Acceptor`，并给 `Acceptor` 注册 new connection callback；当 `Acceptor` accept 到新 fd 后，会调用 `TcpServer::NewConnection()`。 src/net/tcp_server.cc:13-30

`TcpServer::Start()` 会创建 `EventLoopThreadPool`，启动 sub loops，然后通过 base loop 的 `RunInLoop()` 调 `acceptor_->Listen()`。 src/net/tcp_server.cc:41-57

`Acceptor` 构造时创建非阻塞监听 socket，设置 `SO_REUSEADDR` / `SO_REUSEPORT`，bind 到监听地址，并给监听 fd 对应的 `accept_channel_` 设置 read callback。 src/net/acceptor.cc:16-31

`Acceptor::Listen()` 会调用底层 socket 的 `listen()`，然后让 `accept_channel_` 开始关注读事件。 src/net/acceptor.cc:39-44

当监听 fd 可读时，`Acceptor::HandleRead()` 调 `accept_socket_.Accept()` 拿到 connfd；如果有 `new_connection_callback_`，就把 connfd 和 peer address 交给上层，否则关闭 connfd。 src/net/acceptor.cc:47-67

这里还支持 LT / ET 两种触发模式：ET 模式必须循环 accept 到 `EAGAIN`，LT 模式 accept 一次即可。 src/net/acceptor.cc:70-78

---

# 8. 新连接如何分配到 IO 线程

`TcpServer::NewConnection()` 的第一步是从 `thread_pool_->GetNextLoop()` 拿一个 IO loop。 src/net/tcp_server.cc:60-62

`EventLoopThreadPool::GetNextLoop()` 要求只能在 main loop 线程调用，因为 round-robin 游标 `next_` 没有加锁；如果没有 sub loop，就返回 main loop，否则从 `loops_` 里轮询一个 sub loop。 src/net/event_loop_thread_pool.cc:52-68

拿到 IO loop 后，`TcpServer` 会创建 `TcpConnection`，保存到 `connections_`，设置连接回调、消息回调、写完成回调、ET 模式和 TCP_NODELAY，并设置 close callback 让连接关闭时回到 `TcpServer::RemoveConnection()`。 src/net/tcp_server.cc:67-85

最后，连接建立流程必须投递到该连接所属 IO loop 中执行：`ioLoop->RunInLoop([conn] { conn->ConnectEstablished(); });`。 src/net/tcp_server.cc:87-90

这就是 one-loop-per-thread：base loop 负责 accept，真正连接上的 IO 操作在分配到的 IO loop 中执行。

---

# 9. TcpConnection：连接生命周期

`TcpConnection` 拥有三个核心对象：

```cpp
Socket socket_;
Channel channel_;
Buffer input_buffer_;
Buffer output_buffer_;
```

它的头文件注释明确说：`TcpConnection` 代表一条已建立 TCP 连接，拥有 connected socket 和 `Channel`，并维护输入/输出 buffer。 include/runtime/net/tcp_connection.h:26-30include/runtime/net/tcp_connection.h:162-169

构造函数里，`TcpConnection` 会创建 `Socket` 和 `Channel`，然后把 channel 的 read/write/error/close callback 分别绑定到自己的 `HandleRead()`、`HandleWrite()`、`HandleError()`、`HandleClose()`。 src/net/tcp_connection.cc:43-61

`ConnectEstablished()` 是连接真正“上线”的地方：它要求在所属 loop 线程执行，把状态改成 `kConnected`，把 `Channel` tie 到 `shared_from_this()`，然后 `EnableReading()` 注册读事件，并触发用户 connection callback。 src/net/tcp_connection.cc:175-190

`ConnectDestroyed()` 是连接销毁前的清理：如果还没进入 disconnected 状态，就改状态、`DisableAll()`，必要时触发 connection callback，最后 `channel_->Remove()` 从 poller 里移除。 src/net/tcp_connection.cc:193-210

连接关闭时，`HandleClose()` 会把状态改成 disconnected，禁用所有事件，然后调用 connection callback 和 close callback；close callback 通常是 `TcpServer` 注册的删除逻辑。 src/net/tcp_connection.cc:354-367

`TcpServer::RemoveConnection()` 会把删除操作序列化回 base loop，然后在 `RemoveConnectionInLoop()` 中从 `connections_` map 删除连接，并把 `ConnectDestroyed()` 投递回连接所属 IO loop 执行。 src/net/tcp_server.cc:92-106

---

# 10. 读路径：socket → Buffer → MessageCallback

读事件到达时，`Channel` 调 `TcpConnection::HandleRead()`。 src/net/channel.cc:66-69src/net/tcp_connection.cc:212-286

如果是 ET 模式，`HandleRead()` 会循环 `ReadFd()` 直到 `EAGAIN` / `EWOULDBLOCK`；读到 0 表示对端关闭，调用 `HandleClose()`；读出错误则 `HandleError()`。 src/net/tcp_connection.cc:223-256

如果是 LT 模式，每次事件只读一次；读到数据后调用 `message_callback_(conn, input_buffer_, receive_time)`，读到 0 关闭，非 transient 错误则记录并处理错误。 src/net/tcp_connection.cc:257-285

`Buffer::ReadFd()` 用 `readv()`，第一个 iovec 指向 buffer 当前 writable 区域，第二个 iovec 指向 64KB 栈上临时缓冲区；这样如果当前 buffer writable 不够，仍然能一次 readv 尽量多读，溢出的部分再 append 回 buffer。 src/net/buffer_muduo.cc:23-49

`Buffer` 的内部布局是 `[prependable | readable | writable]`，用 `reader_index_` 和 `writer_index_` 维护可读和可写区域。 include/runtime/net/buffer.h:21-39

---

# 11. 写路径：Send → 直接写 / output_buffer_ → 关注可写事件

`TcpConnection::Send()` 可以跨线程调用：如果当前就在连接所属 loop 线程，直接 `SendInLoop()`；否则复制数据，并投递到 loop 线程执行。 src/net/tcp_connection.cc:84-95

`SendInLoop()` 的核心策略是：如果当前没有关注写事件且 output buffer 为空，就先尝试直接 `write()`；如果没写完，再把剩余数据 append 到 `output_buffer_`，并开启写事件。 src/net/tcp_connection.cc:103-160

这里还有 high water mark 机制：当 `output_buffer_` 从低于阈值增长到大于等于阈值时，会通过 `QueueInLoop()` 触发 high-water callback，供上层做背压处理。 src/net/tcp_connection.cc:140-152

当 fd 可写时，`HandleWrite()` 会把 `output_buffer_` 写出去；ET 模式会循环写到 buffer 为空或遇到 transient 错误，LT 模式写一次。 src/net/tcp_connection.cc:288-351

写完后会 `DisableWriting()`，触发 `write_complete_callback_`，如果连接状态已经是 `kDisconnecting`，则继续执行 `ShutdownInLoop()`。 src/net/tcp_connection.cc:333-345

`Shutdown()` 是优雅关闭写端：把状态改成 `kDisconnecting`，然后投递 `ShutdownInLoop()`；`ShutdownInLoop()` 只有在当前没有待写事件时才调用 socket 的 `ShutdownWrite()`。 src/net/tcp_connection.cc:167-172src/net/tcp_connection.cc:379-384

---

# 12. Buffer：网络层的字节容器

`Buffer` 默认实现是 `muduo` 风格，CMake 里通过 `RUNTIME_BUFFER_IMPL` 选择 `muduo` / `ringbuf` / `nginx`，默认是 `muduo`。 CMakeLists.txt:70-78

`Buffer` 的三个区域是：

```text
[ prependable | readable | writable ]
```

`reader_index_` 到 `writer_index_` 是 readable，`writer_index_` 到 vector end 是 writable，前面的 prependable 可以被复用。 include/runtime/net/buffer.h:21-39

`Append()` 会先 `EnsureWritableBytes()`，必要时 `MakeSpace()`，然后 memcpy 到 writable 区域并推进 writer index。 include/runtime/net/buffer.h:114-149

`MakeSpace()` 如果总空间不够就 resize；如果只是前面 prependable 空间够用，就把 readable 区域 memmove 到 `kCheapPrepend` 后面，复用已经消费掉的空间。 src/net/buffer_muduo.cc:91-105

`FindCRLF()` 和 `FindCRLFCRLF()` 使用 `memmem()` 在线性时间内查找 HTTP 分隔符，这一点对 HTTP 解析抗恶意输入比较重要。 include/runtime/net/buffer.h:170-185

---

# 13. TimerQueue：定时器如何挂进 EventLoop

`TimerQueue` 内部创建 `timerfd`，再创建一个 `Channel` 监听这个 `timerfd` 的读事件；timerfd 可读时调用 `HandleRead()`。 src/net/timer_queue.cc:41-49

`AddTimer()` 会从对象池创建 `Timer`，然后通过 `loop_->RunInLoop()` 把插入 timer tree 的动作放到 loop 线程执行；如果新 timer 是最早过期的，就重置 timerfd。 src/net/timer_queue.cc:62-76

`HandleRead()` 读取 timerfd，取出所有已经过期的 timer，逐个执行 `timer->Run()`，然后调用 `Reset()` 处理重复定时器或释放一次性定时器。 src/net/timer_queue.cc:89-98

`Reset()` 对 repeat timer 会 `Restart(now)` 后重新插入，对非 repeat timer 则释放回对象池；最后如果队列不空，会把 timerfd 设到下一次最早过期时间。 src/net/timer_queue.cc:106-124

所以 `EventLoop::RunAfter()` / `RunEvery()` 实际上不是单独开线程 sleep，而是把 timerfd 当成一个普通 fd 纳入 Reactor。 src/net/event_loop.cc:222-238src/net/timer_queue.cc:41-49

---

# 14. 客户端路径：Connector + TcpClient

客户端侧入口是 `TcpClient`，它内部持有一个 `Connector` 和当前 `TcpConnectionPtr`。 include/runtime/net/tcp_client.h:49-65

`TcpClient::Connect()` 把 `connect_` 标记为 true，然后调用 `connector_->Start()`。 src/net/tcp_client.cc:37-42

`Connector::Start()` 会通过 `loop_->RunInLoop()` 进入 loop 线程执行 `StartInLoop()`，再调用 `Connect()` 创建非阻塞 socket 并调用 `connect()`。 src/net/connector.cc:27-47

非阻塞 connect 的正常路径包括 `0`、`EINPROGRESS`、`EINTR`、`EISCONN`，这些都会进入 `Connecting(sockfd)`；可重试错误如 `ECONNREFUSED`、`ENETUNREACH` 会进入 `Retry(sockfd)`。 src/net/connector.cc:49-84

`Connecting()` 会创建临时 `Channel` 关注写事件；因为非阻塞 connect 完成通常表现为 fd writable。 src/net/connector.cc:86-96

`handleWrite()` 里不会直接认为连接成功，而是先 `getsockopt(SO_ERROR)` 确认 connect 结果；成功后把 fd 交给 `new_connection_cb_`，失败则 retry。 src/net/connector.cc:98-141

`Retry()` 关闭 fd，状态回到 disconnected，并用 `RunAfter()` 做指数退避，最大退避 30 秒。 src/net/connector.cc:158-177

`TcpClient::NewConnection()` 收到已连接 fd 后，创建 `TcpConnection`，设置回调，保存到 `connection_`，然后调用 `ConnectEstablished()` 注册读事件并触发 connection callback。 src/net/tcp_client.cc:52-82

---

# 15. 线程模型里最重要的约束

这个网络层的核心约束是：**每个 fd 的 Channel 操作必须发生在它所属的 EventLoop 线程。**

代码里有很多地方体现这个约束：

* `EventLoop` 构造时记录 `thread_id_`，`IsInLoopThread()` 用它判断当前线程是否是 owner 线程。 src/net/event_loop.cc:84-89src/net/event_loop.cc:187-189
* `Channel::Update()` / `Remove()` 要求在 owner loop 线程执行。 src/net/channel.cc:28-36
* `TcpConnection::ConnectEstablished()` 和 `ConnectDestroyed()` 要求在连接所属 loop 线程执行。 src/net/tcp_connection.cc:175-195
* `TcpServer::NewConnection()` 创建连接后，不直接在 base loop 注册连接 channel，而是投递到选中的 `ioLoop` 执行 `ConnectEstablished()`。 src/net/tcp_server.cc:87-90
* `TcpServer::RemoveConnection()` 先回到 base loop 删除 map，再投递到连接所属 IO loop 执行 `ConnectDestroyed()`。 src/net/tcp_server.cc:92-106

这套约束的好处是：每条连接的读写状态、buffer 和 context 都由单一 IO loop 独占，避免大部分锁。每条 `TcpConnection` 的状态通过 `set_context(std::any)` 绑定，由所属 sub loop 独享。

---

# 16. 从一次请求看完整链路

以服务端收到一条连接并读到数据为例，完整链路大概是：

```text
main()
  -> 创建 EventLoop base_loop
  -> 创建 TcpServer(&base_loop, addr, name)
  -> server.Start()
  -> base_loop.Loop()

TcpServer::Start()
  -> 创建 EventLoopThreadPool
  -> acceptor.Listen()

客户端连接进来：
  -> epoll_wait 返回 listen fd readable
  -> EventLoop 分发 accept_channel.HandleEvent()
  -> Acceptor::HandleRead()
  -> accept()
  -> TcpServer::NewConnection(connfd)

TcpServer::NewConnection()
  -> thread_pool.GetNextLoop()
  -> make_shared<TcpConnection>(ioLoop, connfd)
  -> 设置 callbacks
  -> ioLoop.RunInLoop(conn->ConnectEstablished)

IO loop 中：
  -> conn->ConnectEstablished()
  -> channel.EnableReading()
  -> epoll_ctl ADD connfd

客户端发数据：
  -> epoll_wait 返回 connfd readable
  -> TcpConnection::HandleRead()
  -> input_buffer.ReadFd()
  -> message_callback(conn, input_buffer, timestamp)

业务回包：
  -> conn->Send()
  -> 直接 write 或 append output_buffer
  -> 必要时 EnableWriting()
  -> writable 后 HandleWrite()
```

这个链路中的关键代码分别是 `TcpServer::Start()`、`Acceptor::HandleRead()`、`TcpServer::NewConnection()`、`TcpConnection::ConnectEstablished()`、`TcpConnection::HandleRead()` 和 `TcpConnection::SendInLoop()`。 src/net/tcp_server.cc:41-57src/net/acceptor.cc:47-78src/net/tcp_server.cc:60-90src/net/tcp_connection.cc:175-190src/net/tcp_connection.cc:212-286src/net/tcp_connection.cc:103-160

---

# 17. 读代码时可以重点留意的设计点

## 17.1 `Channel::Tie()` 防止回调期间对象析构

`TcpConnection::ConnectEstablished()` 会 `channel_->Tie(shared_from_this())`，`Channel::HandleEvent()` 分发前会 lock weak_ptr，只有 owner 还活着才继续处理事件。 src/net/tcp_connection.cc:180-182src/net/channel.cc:39-50

这是 Reactor 代码里非常常见也非常关键的生命周期保护。

---

## 17.2 ET 模式必须 drain

`Acceptor` 在 ET 模式下会循环 accept 到不能再 accept。 src/net/acceptor.cc:70-78

`TcpConnection::HandleRead()` 在 ET 模式下会循环读到 `EAGAIN` / `EWOULDBLOCK`。 src/net/tcp_connection.cc:223-256

`TcpConnection::HandleWrite()` 在 ET 模式下会循环写 output buffer 到写完或遇到 transient 错误。 src/net/tcp_connection.cc:322-337

这三处是 ET 模式正确性的核心。

---

## 17.3 跨线程只投递任务，不直接碰 fd 状态

`Send()` 跨线程调用时会复制数据，然后通过 `loop_->RunInLoop()` 投递到连接所属线程执行。 src/net/tcp_connection.cc:84-95

`EventLoop::QueueInLoop()` 使用 mutex 保护 pending functors，然后通过 eventfd 唤醒 loop。 src/net/event_loop.cc:158-169

这种设计把“线程安全边界”集中在 `QueueInLoop()` / `RunInLoop()`，业务侧不用直接给每条连接加锁。

---

## 17.4 Timer 也是 Reactor 的一部分

`TimerQueue` 把 timerfd 包装成 `Channel`，因此定时器和 socket fd 都走同一个 `EventLoop` 分发路径。 src/net/timer_queue.cc:41-49

这使得后续如果要做连接空闲超时、请求 header-read timeout、重试退避等能力，都可以自然挂到 `EventLoop::RunAfter()` / `RunEvery()` 上。 src/net/event_loop.cc:222-238

---

# 18. 一个简化类图

```text
EventLoop
  owns Poller
  owns TimerQueue
  owns wakeup Channel
  has active Channel*

Channel
  belongs to EventLoop
  points to fd
  has read/write/close/error callbacks

EPollPoller : Poller
  owns epoll fd
  maps fd -> Channel*

Acceptor
  owns listening Socket
  owns listening Channel
  callback -> TcpServer::NewConnection

TcpServer
  owns Acceptor
  owns EventLoopThreadPool
  owns map<string, TcpConnectionPtr>

EventLoopThreadPool
  owns EventLoopThread[]
  returns EventLoop* round-robin

TcpConnection
  owns connected Socket
  owns connected Channel
  owns input/output Buffer

Connector
  owns temporary connect Channel
  callback -> TcpClient::NewConnection

TcpClient
  owns Connector
  owns one TcpConnectionPtr
```

这些关系分别可以从对应类成员看出来：`EventLoop` 持有 `poller_`、`active_channels_`、`wakeup_channel_`、`timer_queue_`；`TcpServer` 持有 `acceptor_`、`thread_pool_` 和 `connections_`；`TcpConnection` 持有 `socket_`、`channel_`、`input_buffer_` 和 `output_buffer_`。 include/runtime/net/event_loop.h:87-98include/runtime/net/tcp_server.h:81-98include/runtime/net/tcp_connection.h:158-181

---

# 19. 如果继续深入，下一步建议

我建议下一轮可以按你最关心的方向继续拆：

1. **只读服务端收发路径**：从 `tests/integration/test_tcp_server_smoke.cc` 或 demo 入手，把 `TcpServer`、`TcpConnection` 和业务 callback 串起来。
2. **只读 epoll / Channel 状态机**：重点看 `Channel::index_`、`EPollPoller::UpdateChannel()` 和 `RemoveChannel()`。
3. **只读线程模型**：重点看 `EventLoopThread`、`EventLoopThreadPool`、`RunInLoop()`、`QueueInLoop()`。
4. **只读 Buffer 与 HTTP 解析连接处**：从 `Buffer` 进入 `HttpContext`，理解上层 HTTP 怎么消费网络层数据。
5. **读测试验证行为**：比如 `event_loop_smoke_test`、`tcp_server_smoke_test`、`trigger_mode`、`rst_storm_smoke_test`。

如果你愿意，我可以下一条直接带你逐行走一遍 **“TcpServer 收到连接并 echo 回包”** 的实际执行路径。

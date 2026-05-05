# 网络层架构设计

本文档描述 `runtime::net` 的整体设计、核心对象关系，以及一条 TCP 连接从建立到销毁的完整生命周期。

网络层采用典型的 Reactor 模型：
IO 多路复用 + 非阻塞 + LT模式/ET模式, 支持`Select`,`Poll`,`Epoll`三种模式

- `EventLoop` 负责事件循环、任务串行执行和定时器调度
- `Poller` 负责与内核多路复用机制交互
- `Channel` 负责把 fd 上发生的事件分发给上层回调
- `Acceptor` 负责监听 socket 和接收新连接
- `TcpServer` 负责组织监听、线程池和连接对象
- `TcpConnection` 负责单连接的读写、缓冲区和关闭流程
- `Socket` / `Buffer` / `InetAddress` 提供底层支撑

## 1. 总体分层

从职责上看，网络层可以分成 4 层：

### 1.1 系统调用薄封装

- `Socket`
- `InetAddress`
- `Buffer`

围绕 socket fd、地址结构和字节缓冲区工作，
把底层系统调用包装成更稳定的 C++ 接口。

### 1.2 事件抽象层

- `Channel`
- `Poller`

这一层负责把 “某个 fd 上发生了什么事件” 抽象成统一的数据结构和回调接口。

### 1.3 事件循环层

- `EventLoop`
- `TimerQueue`

这一层负责：

- 调用 `Poller` 等待 I/O 事件
- 串行执行回调
- 承载跨线程投递的任务
- 调度定时器

### 1.4 TCP 连接管理层

- `Acceptor`
- `TcpServer`
- `TcpConnection`
- `EventLoopThreadPool`

这一层面向 TCP 服务端语义，负责监听端口、接收连接、选择 I/O 线程、创建连接对象，以及驱动连接生命周期。

## 2. 核心对象关系

网络层里的核心对象关系如下：

```text
TcpServer
  ├── Acceptor
  │     ├── Socket(listen_fd)
  │     └── Channel(listen_fd)
  ├── EventLoopThreadPool
  │     ├── EventLoop(sub loop 1)
  │     ├── EventLoop(sub loop 2)
  │     └── ...
  └── TcpConnection(name -> shared_ptr)
        ├── Socket(conn_fd)
        ├── Channel(conn_fd)
        ├── Buffer(input/output)
        └── EventLoop(sub loop owner)
```

### 2.1 `EventLoop`

`EventLoop` 是网络层的调度中心，职责包括：

- 轮询 I/O 事件
- 执行激活 `Channel` 的回调
- 执行跨线程投递的任务
- 处理定时器回调

项目采用 **one loop per thread** 模型：

- 每个 `EventLoop` 只归属于一个线程
- 该线程内串行处理所有 I/O 回调和投递任务
- 跨线程调用通过 `RunInLoop()` / `QueueInLoop()` 回到所属线程执行

### 2.2 `Channel`

`Channel` 不是 socket，也不是 Poller。它是 fd 的事件代理对象, 可以认为它是绑定事件与对应回调的路由器。

```
fd_      ← 监听的文件描述符（不拥有，只借用）
events_  ← 当前感兴趣的事件（我想监听什么）
revents_ ← epoll 返回的就绪事件（内核说发生了什么）
index_   ← 在 Poller 中的注册状态（kNew/kAdded/kDeleted）
loop_    ← 所属 EventLoop 指针
tie_     ← weak_ptr，防止回调时 owner 已析构

```

`Channel` 的两个关键作用是：

1. 把 fd 事件翻译成上层回调
2. 保证 “本地感兴趣事件状态” 和 “Poller 中注册状态” 的一致性.

最初的设计中 Channel 高度绑定`Epoll`, 后续实现了事件抽象兼容了`select`,`poll` 典型的怀旧。

### 2.3 `Poller`

`Poller` 是对底层 I/O 复用机制的抽象。当前实现主要由 `epoll` 驱动。

它负责：

- 把 `Channel` 注册到内核
- 等待活跃事件
- 返回本轮活跃的 `Channel` 集合

`Poller` 本身不理解 TCP 连接，也不直接执行业务逻辑；它只提供事件发现能力。

### 2.4 `Socket`

`Socket` 是 socket fd 的 RAII 包装类，负责：

- 生命周期管理
- `bind` / `listen` / `accept`
- `shutdown`
- 常见 socket option 设置

它是系统调用的薄封装，不承载事件循环和连接状态机。

### 2.5 `Buffer`

`Buffer` 是用户态字节缓冲区，负责：

- 缓存 socket 读出的输入数据
- 缓存还未完全写出的输出数据
- 支持按读指针/写指针滑动管理内存

在 `TcpConnection` 中：

- `input_buffer_` 保存从内核读出来但上层还没完全消费的数据
- `output_buffer_` 保存已经准备发送但暂时还没完全写入 socket 的数据

## 3. 主线程与 I/O 线程模型

`TcpServer` 采用主从式线程模型：

- **主 loop**：负责监听 socket 和接收新连接
- **sub loop**：负责已建立连接的读写 I/O

线程分工如下：

### 3.1 主 loop

主 loop 上主要承载：

- `Acceptor`
- listen fd 对应的 `Channel`
- 新连接到来时的 `accept`

主 loop 不直接负责每条连接的后续读写处理。

### 3.2 sub loop

每条新连接会分配给某个 sub loop，之后该连接的：

- 可读事件
- 可写事件
- 关闭事件
- 错误事件

都在所属 sub loop 上处理。

### 3.3 线程池

`EventLoopThreadPool` 负责创建和管理多个 I/O 线程，每个线程内部持有一个 `EventLoop`。

`TcpServer` 在 `Start()` 时启动线程池，然后在每次新连接到来时，从线程池中选取一个 sub loop 作为连接归属线程。

## 4. 从监听到建立连接的流程

一条新连接的建立过程如下：

### 4.1 服务器启动

`TcpServer::Start()` 做两件事：

1. 启动 `EventLoopThreadPool`
2. 启动 `Acceptor`

### 4.2 `Acceptor` 开始监听

`Acceptor` 内部持有：

- 一个监听 socket
- 一个绑定在 listen fd 上的 `Channel`

调用 `Listen()` 后：

- socket 进入监听态
- listen fd 注册到主 loop 的 `Poller`
- 主 loop 开始监听新连接事件

### 4.3 新连接到来

当 listen fd 可读时：

1. 主 loop 收到活跃事件
2. `Channel` 触发 `Acceptor::HandleRead()`
3. `HandleRead()` 调用 `accept`
4. 拿到新的 conn fd 和对端地址
5. 调用 `TcpServer::NewConnection()`

## 5. `TcpConnection` 的建立流程

`TcpServer::NewConnection()` 会完成这些动作：

1. 从线程池里选择一个 sub loop
2. 为新连接生成唯一名字
3. 创建 `TcpConnection`
4. 给该连接设置各种回调：
   - connection callback
   - message callback
   - write complete callback
   - close callback
5. 把连接对象保存到 `connections_`
6. 将 `ConnectEstablished()` 投递到该连接所属的 sub loop

### 5.1 为什么要投递到 sub loop

因为连接的 `Channel` 最终归属于 sub loop，所以：

- 启用读事件
- 更新 Poller 注册状态
- 触发用户连接建立回调

这些动作都应该在连接所属的 `EventLoop` 线程中完成。

### 5.2 `ConnectEstablished()`

`TcpConnection::ConnectEstablished()` 会：

- 把状态切换到 `kConnected`
- 把 `Channel` 和自身绑定，避免回调期间对象被销毁
- 打开读事件
- 触发 connection callback

到这里，一条连接才真正进入可收发数据的状态。

## 6. 数据接收流程

当某条连接上有数据到来时，流程如下：

### 6.1 fd 可读

连接所属 sub loop 从 `Poller` 得到该 fd 的可读事件，调用：

`Channel::HandleEvent()`

### 6.2 `Channel` 分发读事件

`Channel` 根据 `revents_` 调用 `read_callback_`，也就是：

`TcpConnection::HandleRead()`

### 6.3 `TcpConnection::HandleRead()`

`HandleRead()` 做这些事：

1. 调用 `input_buffer_.ReadFd()` 从 socket 读取数据
2. 如果读到正数字节：
   - 调用 `message_callback_`
3. 如果读到 0：
   - 表示对端关闭，进入 `HandleClose()`
4. 如果读失败：
   - 区分可重试错误和真正错误
   - 需要时进入 `HandleError()`

### 6.4 message callback

网络层本身不解释协议内容。`message_callback_` 会把：

- `TcpConnection`
- `Buffer`
- `receive_time`

交给上层协议模块。

例如 HTTP 层会在这里读取 `Buffer`，把字节流解析成 `HttpRequest`。

## 7. 数据发送流程

发送数据分成两种情况：

### 7.1 直接写成功

调用 `TcpConnection::Send()` 后，如果当前：

- 连接已建立
- 在所属 loop 线程
- 没有正在监听写事件
- 输出缓冲区为空

那么会优先尝试一次 `write()` 直接发送。

如果一次性全部发完：

- 不需要进入 `output_buffer_`
- 不需要监听可写事件

### 7.2 需要缓存剩余数据

如果 `write()` 没写完，或者当前已经有待发送数据：

1. 把剩余数据追加到 `output_buffer_`
2. 打开 `Channel` 的写事件
3. 等待 fd 可写

### 7.3 fd 可写时

`Channel` 触发 `TcpConnection::HandleWrite()`：

1. 调用 `output_buffer_.WriteFd()`
2. 持续刷出待发送数据
3. 如果缓冲区清空：
   - 关闭写事件监听
   - 触发 write complete callback
   - 如果当前状态是 `kDisconnecting`，执行半关闭

## 8. 连接关闭流程

连接关闭有两类来源：

### 8.1 对端关闭

在 `HandleRead()` 中，如果 `read()` 返回 0，说明对端关闭连接：

1. 进入 `HandleClose()`
2. 状态切到 `kDisconnected`
3. 禁用所有事件
4. 触发 connection callback
5. 触发 close callback

其中 close callback 会通知 `TcpServer` 移除该连接。

### 8.2 本端主动关闭

调用 `TcpConnection::Shutdown()` 时：

1. 状态切到 `kDisconnecting`
2. 投递 `ShutdownInLoop()` 到所属 loop

如果当前没有待发送数据，则立即关闭写端。  
如果还有待发送数据，则等 `HandleWrite()` 把输出缓冲区刷空后再关闭写端。

这保证了：

- 连接关闭前尽量把已排队的响应发完
- 不会粗暴丢弃还没写出的数据

## 9. 连接对象的移除与销毁

`TcpServer` 使用 `connections_` 持有所有活动连接。

当 `TcpConnection` 通过 close callback 通知 `TcpServer` 后：

1. `TcpServer::RemoveConnection()` 把移除动作投递回主 loop
2. 主 loop 中执行 `RemoveConnectionInLoop()`
3. 从 `connections_` 删除该连接
4. 再把 `ConnectDestroyed()` 投递到连接所属 sub loop

`ConnectDestroyed()` 会：

- 最终关闭 `Channel`
- 取消 Poller 注册
- 触发必要的连接销毁回调

这套流程的重点是：

- 连接对象的所有权集中在 `TcpServer`
- Poller/Channel 的清理在所属 loop 中完成
- 避免跨线程直接销毁连接对象带来的竞态问题

## 10. 和 HTTP 层的关系

`runtime::net` 只负责 TCP 和事件驱动语义，不理解 HTTP 协议。

HTTP 层是在 `TcpServer` 之上叠加出来的：

- `HttpServer` 内部复用 `TcpServer`
- 每条连接上通过 `SetContext()` 挂一个 `HttpContext`
- `message_callback_` 中把 `Buffer` 解析成 HTTP 请求

也就是说：

- 网络层负责 “字节什么时候到了”
- HTTP 层负责 “这些字节是什么意思”

这种分层使网络层可以复用到：

- HTTP
- 自定义二进制协议
- RPC
- 纯 TCP echo 服务

## 11. 当前设计的优点

### 11.1 分层清晰

- fd 生命周期在 `Socket`
- 事件抽象在 `Channel`
- 事件循环在 `EventLoop`
- TCP 服务端语义在 `TcpServer` / `TcpConnection`

### 11.2 线程模型清楚

- 主 loop 只做 accept
- sub loop 只做已连接 I/O
- 连接始终归属于一个固定 loop

### 11.3 上层协议容易接入

通过 `message_callback_` 和 `context_`，上层可以方便地实现 HTTP 等协议，而无需侵入网络层内部。

### 11.4 生命周期控制较稳

连接的建立、关闭、移除和销毁都通过所属 loop 或主 loop 串行推进，避免了大量裸线程同步。

## 12. 当前边界与限制

当前网络层是一个比较完整的最小 Reactor TCP 框架，但还存在一些明显边界：

- 目前主要针对 Linux/epoll 场景设计
- 连接背压和高水位控制能力还较弱
- 错误码封装与统一错误模型仍较轻量
- 没有 TLS/SSL 层
- 观测指标和 tracing 钩子还不系统

这些都不影响当前架构成立，但属于后续可扩展方向。

## 13. 一句话总结

`runtime::net` 的核心设计是：

**用 `EventLoop + Poller + Channel` 构建 Reactor 事件驱动骨架，再用 `TcpServer + TcpConnection` 把它组织成一个支持多线程 I/O 的 TCP 服务端框架。**

上层协议只需要关注如何消费连接上的字节流，而不需要自己重新处理 epoll、回调分发和连接生命周期。

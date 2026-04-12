### 注意
该项目目前还未完善， 代码仅供参考

### 架构层次

┌──────────────────────────────────┐
│         HTTP Layer               │  runtime::http
│  HttpServer / HttpRequest/Response│
└──────────────┬───────────────────┘
               │ 依赖
┌──────────────▼───────────────────┐
│         TCP/Net Layer            │  runtime::net
│  TcpServer / TcpConnection       │
│  EventLoop / Channel / Poller    │
│  Buffer / Socket / Acceptor      │
│  TimerQueue / EventLoopThreadPool│
└──────────────┬───────────────────┘
               │ 依赖
┌──────────────▼───────────────────┐
│       Foundation Layer           │  runtime::log / runtime::time
│  Logger(Async) / Timestamp       │
│  ThreadPool / MemoryPool         │
└──────────────────────────────────┘

### 核心模块解析

#### 1. Reactor 事件循环
核心是 One Loop Per Thread 模型：

`EventLoop` : 事件驱动主循环， 持有 `Poller`, `Channel`, `TimerQueue`
`Channel` : fd的事件代理， 封装感兴趣的事件和回调分发
`Poller` : IO多路复用的接口， 与内核epoll 实现解耦
`EpollPoller`: epoll 的封装， 实现Poller 节接口。
`EventLoopThreadPoll` SubLoop 线程池， 连接IO

#### 2. 连接管理层

`TcpServer` : 监听端口、接受新连接并分配 IO线程
`TcpConnection` : 单条连接的全生命周期管理， 包括状态机
`Buffer` : 三段滑动索引缓冲区
`Acceptor` : 专门监听 accept, 建立连接后通知TcpServer

#### 3. 定时器
待完善部分: 
`TimerQueue` 使用`timerfd` + `std::set<expiration, Timer*>` 实现一个简单定时器。

#### 4. HTTP层
`HttpServer` 基于 TcpServer 构建，提供：

- 路由注册：Get() / Post() / AddRoute()
- Keep-Alive 连接复用
- 静态文件服务（SetStaticRoot）
- 空闲连接超时清理（std::jthread + SweepIdleConnections）
- X-Trace-Id 传递

#### 5. 异步日志
Logger 采用单例模式， LogMessage RAII + 宏实现零开销， 支持流式输出。
底层采用`AsyncLogger` 异步批量落盘， 支持按滚动日志文件。

#### 6. 基础设施
以下不保证一定用上， 有部分只是作为组件， 没有参与实际的项目中

`ThreadPool` : std::jthread 工作线程池， enqueue 返回 std::future
`MemoryPool` : 固定对象大小的空闲链表内存池， 线程安全
`ObjectPool` : 对象池变体， 同内存池
`SegmentLruCache` : 分段LRU缓存
`runtime::base::NonCopyable` : 禁止拷贝语义的基类。
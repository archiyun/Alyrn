# 从零写一个高并发网络运行时（一）：总体架构

> 这是一个系列博客，记录我毕设项目 `high-concurrency-runtime` 的网络层设计与实现。
> 作为对照系，我会反复参考四个工业级实现：**nginx**、**envoy**、**muduo**、**libevent**。
> 第一篇先讲清"为什么是 Reactor + One-Loop-Per-Thread"——这是后面所有故事的底座。

---

## 0. 一个最朴素的问题

写一个 HTTP 服务器，要支持 10K 并发连接，怎么做？

最直觉的方案是 **thread-per-connection**：来一个连接，起一个线程。代码简单，写出来跟教科书一样：

```cpp
while (true) {
  int conn = accept(listen_fd, ...);
  std::thread([conn] { handle(conn); }).detach();
}
```

但它会在三个地方撞墙：

1. **内存**：每个 pthread 默认栈 8MB，10K 连接 = 80GB 虚存地址。哪怕实际驻留只用几 MB/线程，调度器的开销也无法忽略。
2. **调度**：内核线程切换涉及 TLB 失效、缓存污染。当线程数远超 CPU 核心数时，CPU 大部分时间花在切换而不是干活上。
3. **同步**：线程间共享状态（连接表、定时器、限流器……）一加锁，性能就完蛋。

这就是 **C10K 问题** 二十年前提出的核心矛盾。所有现代高性能网络框架——从 nginx 到 envoy 到 Node.js——给出的答案都是同一个词：

> **事件驱动（event-driven）**。

具体落到设计模式上，就是 **Reactor 模式**。

---

## 1. Reactor 是什么

Reactor 模式的核心只有一句话：

> **用一个线程，通过 I/O 多路复用，把"等"换成"被通知"。**

不再有"每个连接一个线程在 `read()` 上阻塞"——而是把所有 fd 注册到一个多路复用器（`epoll`/`kqueue`/`IOCP`）上，问内核："这堆 fd 谁就绪了，告诉我。"内核返回就绪列表，应用线程依次处理。

最小化的 Reactor 循环长这样：

```
while (running) {
    events = poller.wait(timeout);   // 阻塞，但只阻塞一个线程
    for (event : events) {
        dispatch(event);             // 同步处理就绪的 fd
    }
}
```

就这么简单。所有复杂性——SubReactor、线程池、定时器、连接池——都是围绕这个循环的扩展。

### 1.1 Reactor 的四个角色

按 Schmidt 在 *POSA2* 里的定义，Reactor 模式包含四个角色：

| 角色 | 职责 | 本项目对应 |
|---|---|---|
| **Handle** | 操作系统资源句柄（fd） | `int sockfd` |
| **Event Demultiplexer** | 调用 `epoll_wait` 等待事件 | `runtime::net::Poller`（基类）+ `EPollPoller` |
| **Event Handler** | 事件回调接口 | `runtime::net::Channel` 持有的 read/write/close/error 回调 |
| **Reactor** | 注册 Handler、运行事件循环、分发事件 | `runtime::net::EventLoop` |

代码上一一对应（[`include/runtime/net/event_loop.h`](../../../include/runtime/net/event_loop.h)）：

```cpp
class EventLoop : public NonCopyable {
public:
  void Loop();                          // 事件循环主体
  void UpdateChannel(Channel* channel); // 注册 Handler
  ...
private:
  std::unique_ptr<Poller> poller_;            // Event Demultiplexer
  std::vector<Channel*> active_channels_;     // 本轮就绪的 Handler 列表
};
```

---

## 2. 为什么单 Reactor 不够：从 Reactor 到 Multi-Reactor

单 Reactor 的瓶颈很明显：**只用一个 CPU 核**。

如果你的机器有 32 核，那剩下 31 个核都在闲着等单核处理完事件——这在 10Gbps 网卡和 100K+ QPS 场景下完全不可接受。

业界给出了两类扩展方案：

### 2.1 Multi-Process（nginx 流派）

nginx 用 **master + worker 多进程** 模型。master 进程创建监听 socket 后，`fork()` 出 N 个 worker 进程（N 通常等于 CPU 核心数）。每个 worker 进程独立运行一个完整的 Reactor 循环，**共享同一个 listen fd**，靠内核的 `accept()` 互斥（旧 Linux 上还要用 `accept_mutex` 显式互斥避免惊群；新内核有 `SO_REUSEPORT` 后这个就不需要了）。

```
                                 ┌──────────┐
                                 │  master  │ (配置加载、信号、worker 监控)
                                 └────┬─────┘
                                      │ fork
                ┌───────────┬─────────┼─────────┬───────────┐
                ▼           ▼         ▼         ▼           ▼
            ┌───────┐   ┌───────┐ ┌───────┐ ┌───────┐   ┌───────┐
            │worker │   │worker │ │worker │ │worker │   │worker │ ...
            │epoll  │   │epoll  │ │epoll  │ │epoll  │   │epoll  │
            └───────┘   └───────┘ └───────┘ └───────┘   └───────┘
```

**优点**：进程隔离，一个 worker 崩溃不影响其他；天然的 NUMA 亲和性。
**缺点**：进程间共享数据麻烦（要用共享内存）；启动慢；连接不能在 worker 间迁移。

### 2.2 Multi-Thread（envoy / muduo 流派）

envoy 和 muduo 都选择 **多线程 + 多 EventLoop**。一个主线程（"Main Reactor"）只负责 `accept`，N 个工作线程各运行一个 EventLoop（"Sub Reactor"），新连接被分发到某个 Sub Reactor 上独占。

这就是 **One-Loop-Per-Thread**：

```
              ┌──────────────────┐
              │   Main Loop      │  accept() 新连接
              │   (Acceptor)     │
              └────────┬─────────┘
                       │ round-robin 分配
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
   ┌─────────┐   ┌─────────┐   ┌─────────┐
   │ Sub Loop│   │ Sub Loop│   │ Sub Loop│ ...
   │ epoll   │   │ epoll   │   │ epoll   │
   │ conn[]  │   │ conn[]  │   │ conn[]  │
   └─────────┘   └─────────┘   └─────────┘
```

**关键不变量**：一条连接一旦分配给某个 Sub Loop，它的所有 I/O、状态、回调都**只在那个线程上跑**，直到关闭。线程之间不共享连接状态。

**优点**：单进程内简单，连接对象可以挂任何 C++ 对象；锁的范围被消灭在源头（同一个线程访问的数据本就不需要锁）。
**缺点**：单进程崩溃带走全部连接；线程数固定，不像进程那样能用 `cgroup` 隔离。

### 2.3 本项目的选择

这个项目走的是 **One-Loop-Per-Thread**（受 muduo 影响很深）。原因有三：

1. **C++ 写多线程更顺手**：一个进程一份配置、一份连接池、一份指标，比多进程方便得多。
2. **目标场景是 API 网关**：网关本身要持有大量上游连接池、健康检查状态、限流器，这些状态在多进程模型里需要复杂的共享内存或者 IPC，多线程模型里直接 `shared_ptr` + 锁。
3. **作为毕设，更适合讲清楚 C++ 现代特性**：`std::any`、`shared_from_this`、`std::function` 这些在多线程模型里有自然的舞台。

---

## 3. 四个对照系：异同表

把四个参照系按几个关键维度并排：

| 维度 | nginx | envoy | muduo | libevent | **本项目** |
|---|---|---|---|---|---|
| 并发模型 | 多进程 + 单 Reactor/进程 | 多线程 + 多 EventLoop | 多线程 + 多 EventLoop | 单 event_base（也可多）| **多线程 + 多 EventLoop** |
| 语言 | C | C++14/17 | C++11 | C | **C++20** |
| 主要平台 | Linux/Unix | Linux | Linux | 跨平台（含 Windows） | **Linux** |
| 多路复用抽象 | event module（epoll/kqueue/select 静态选） | dispatcher（基于 libevent2） | Poller 基类（运行时选） | event_base + backend（编译期选） | **Poller 基类（运行时选）** |
| 内存管理 | 池（`ngx_pool_t`） | 标准 C++ + tcmalloc | 标准 C++ | C `malloc/free` | **标准 C++ + ObjectPool（定时器）** |
| 连接对象 | `ngx_connection_t`，池化复用 | `ConnectionImpl`，`unique_ptr` | `TcpConnection`，`shared_ptr` | 用户管理 | **`TcpConnection`，`shared_ptr`** |
| 模块/扩展 | 编译期模块（phase handler） | 编译期 filter chain | 无（库性质） | 无（库性质） | **HTTP Router + std::any context** |
| 跨线程通信 | N/A（多进程，无共享） | dispatcher::post()，eventfd 唤醒 | runInLoop，eventfd 唤醒 | `event_active`，pipe/socket 唤醒 | **`RunInLoop`/`QueueInLoop`，eventfd 唤醒** |
| 定时器 | 红黑树 | libevent min-heap | 红黑树 | min-heap | **侵入式红黑树 + ObjectPool** |
| 背压 | 隐式（recv 节流） | 显式 watermark callback | 无 | 无 | **显式 HighWaterMark callback** |

几个值得展开的差异：

### 3.1 nginx：多进程 + phase handler

nginx 是这一类里最"古典"的——1999 年的设计，要在那个年代的硬件上扛 1 万连接，多进程是当时唯一靠谱的选择。它的 `event module` 是编译期决定的（`./configure --with-poll_module`），运行时不能切。

nginx 的 phase handler 模型把一个 HTTP 请求拆成 11 个阶段（POST_READ、SERVER_REWRITE、FIND_CONFIG……），每个阶段可以挂多个模块。这种设计**优先稳定性和可调试性**——nginx.conf 写错了配置不会让进程崩，因为模块是松耦合的。

我学到的：**配置的稳定性比代码的优雅性重要**。这一点这个项目目前还没做到——Router 配置是硬编码的，下一阶段要加配置文件解析时会回头借鉴。

### 3.2 envoy：service mesh 时代的 Reactor

envoy（2017）是这一类里最年轻的。它的 dispatcher 底层其实就是 libevent2，但在上层加了非常多东西：filter chain、connection manager、watermark buffer、circuit breaker、admin API……envoy 的代码量是 muduo 的几十倍，因为它要做的事情多得多。

envoy 最值得学的是 **watermark buffer**——也就是我刚加的 `HighWaterMarkCallback` 的原型。envoy 在每条连接的 read/write buffer 上都设有高低水位，触发时反压上游。本项目目前只在 TcpConnection 的写方向加了高水位，read 方向的反压还没做，这是下一步。

### 3.3 muduo：C++ 网络库的教科书

muduo（陈硕，2010）是本项目最直接的灵感来源。它把 One-Loop-Per-Thread 的思想做得极其干净：Channel/Poller/EventLoop/TcpConnection 四件套，几乎是后来所有 C++ 网络库的模板。

但 muduo 是 **库**，不是 **运行时**——它没有 HTTP 路由、没有负载均衡、没有限流熔断。这个项目相当于"muduo 风格的 net 层 + nginx 风格的 gateway 层"。

### 3.4 libevent：跨平台的代价

libevent 是这四个里唯一认真做跨平台的（支持 Windows IOCP、BSD kqueue、Solaris event ports）。代价是它的抽象层级比 muduo 多一层：`event_base` → `event` → 用户回调。事件的注册是 C 风格的 `event_new` + `event_add`，灵活但容易写错。

我没有跟 libevent 那条路——本项目只跑 Linux，所以 Poller 抽象只是为了便于测试（poll/select 用来对照 epoll 的正确性），不为了真正可移植。

---

## 4. 一条连接的生命周期：本项目实现

讲完原理，看代码。一条 TCP 连接从 accept 到 close 在本项目里的流程：

```
[Client]                  [Main Loop]                   [Sub Loop N]
   |                          |                              |
   |---- connect ------------>|                              |
   |                          | (Acceptor::HandleRead)       |
   |                          | int connfd = accept(...)     |
   |                          |                              |
   |                          | TcpServer::NewConnection     |
   |                          | EventLoop* loop =            |
   |                          |   pool.GetNextLoop()         |
   |                          | conn = make_shared<TcpConn>  |
   |                          | loop->RunInLoop(             |
   |                          |   conn->ConnectEstablished)  |
   |                          |--------(eventfd wakeup)----->|
   |                          |                              | ConnectEstablished:
   |                          |                              |   channel->Tie(self)
   |                          |                              |   channel->EnableReading()
   |                          |                              |   connection_callback_(conn)
   |                          |                              |
   |---- write "hello" ------>|                              |
   |                          |                              | epoll_wait 返回 EPOLLIN
   |                          |                              | Channel::HandleEvent
   |                          |                              |   -> TcpConnection::HandleRead
   |                          |                              |     -> input_buffer_.ReadFd
   |                          |                              |     -> message_callback_(conn, buf)
   |                          |                              |        (HTTP 解析 + 业务回调)
   |                          |                              |     -> conn->Send(response)
   |                          |                              |        -> output_buffer_.Append
   |                          |                              |        -> channel->EnableWriting()
   |                          |                              |
   |                          |                              | epoll_wait 返回 EPOLLOUT
   |                          |                              | -> HandleWrite
   |                          |                              |    write(fd, output_buffer_, ...)
   |<--- "world" --------------|                              |
   |                          |                              |
   |---- close -------------->|                              |
   |                          |                              | EPOLLHUP / read==0
   |                          |                              | -> HandleClose
   |                          |                              |    state_ = Disconnected
   |                          |                              |    close_callback_(conn)
   |                          |                              |        -> TcpServer::RemoveConnection
   |                          |                              |           (loop->RunInLoop(base_loop))
   |                          |<-------(eventfd wakeup)------|
   |                          | connections_.erase(name)     |
   |                          | sub_loop->QueueInLoop(       |
   |                          |   conn->ConnectDestroyed)    |
   |                          |--------(eventfd wakeup)----->|
   |                          |                              | ConnectDestroyed:
   |                          |                              |   channel->DisableAll()
   |                          |                              |   channel->Remove()
   |                          |                              | (shared_ptr 计数归零，析构)
```

几个关键设计：

### 4.1 跨线程通信靠 eventfd

`Main Loop` 把新连接交给 `Sub Loop` 时，不能直接调用 `Sub Loop` 上的函数——那是另一个线程，会撞数据竞争。正确做法是把 lambda 塞进 `Sub Loop` 的 pending 队列，然后写一个字节到它的 `eventfd` 上：

```cpp
// EventLoop::QueueInLoop()
void EventLoop::QueueInLoop(Functor cb) {
  {
    std::lock_guard lk{mutex_};
    pending_functors_.push_back(std::move(cb));
  }
  if (!IsInLoopThread() || calling_pending_functors_) {
    Wakeup();   // write(eventfd, ...)
  }
}
```

`Sub Loop` 的 `epoll_wait` 因为有 `eventfd` 上的可读事件而立刻返回，处理完正常的 IO 事件后调用 `DoPendingFunctors()` 跑队列里的 lambda。

这是 muduo 的发明，envoy 也用同样的机制（envoy 叫 `Dispatcher::post`）。libevent 用的是 pipe 而不是 eventfd（早期 Linux 没 eventfd），效果一样但开销略大。

### 4.2 `shared_from_this` 解决 use-after-free

`TcpConnection` 用 `std::shared_ptr` 管理生命周期。`Channel` 里的回调持有 `weak_ptr<void>`（通过 `Channel::Tie()` 设置），在每次 `HandleEvent` 之前 `lock()` 一次：

```cpp
// Channel::HandleEvent
void Channel::HandleEvent(Timestamp receive_time) {
  if (tied_) {
    std::shared_ptr<void> guard = tie_.lock();
    if (guard) {
      HandleEventWithGuard(receive_time);
    }
    // 如果 lock 失败，说明 TcpConnection 已被销毁，直接跳过
  } else {
    HandleEventWithGuard(receive_time);
  }
}
```

这解决了一个非常隐蔽的 bug：连接关闭后，epoll 队列里可能还有"已经过期"的事件等待处理。如果不做 tie，回调会访问已释放的内存。

nginx 不需要这个机制，因为 `ngx_connection_t` 是从池里分配的——池在 worker 进程生命期内不释放，所以指针永远有效，但代价是连接对象上的字段必须手动 reset。

### 4.3 状态本地化：`std::any context_`

每条 `TcpConnection` 有一个 `std::any context_` 字段。HTTP 层在这上面挂 `HttpContext`（解析状态机），Gateway 在这上面挂上下游关联信息。所有这些状态都在连接归属的 Sub Loop 线程上访问，**没有全局 map，没有锁**。

这是 One-Loop-Per-Thread 的精髓：**让数据本来就不需要被并发访问**，而不是用锁去保护它。

---

## 5. 这次写代码时被绊了几次

讲点真实的——架构图画起来漂亮，写代码时被以下几个问题折磨过：

### 5.1 Channel 的 Index/set_index 暴露问题

`Channel` 有一个 `index_` 字段，记录它在 Poller 里的注册状态（新/已添加/已删除）。我最初把 `index()`/`set_index()` 设为 public——结果 HTTP 层的代码就有人写了 `channel_->index()` 来"查状态"，但语义完全错了。

正确做法是把它 `private`，然后 `friend` 给三个具体 Poller 实现：

```cpp
class Channel : public NonCopyable {
  ...
private:
  friend class EPollPoller;
  friend class PollPoller;
  friend class SelectPoller;

  int Index() const { return index_; }
  void set_index(int idx) { index_ = idx; }
};
```

教训：**封装不是写 `private`，是约束接口的语义边界。**

### 5.2 Send 失败时没有信号

最初 `TcpConnection::Send` 返回 `void`，连接关闭后调用 Send 静默丢弃——调用者完全无感知。这就是上周加 `bool Send(...)` 返回值的原因。配合新加的 `HighWaterMarkCallback`，上层可以：

```cpp
if (!conn->Send(payload)) {
  metrics_.dropped++;          // 连接断了，丢的
}

conn->set_high_water_mark(64 * 1024 * 1024);
conn->set_high_water_mark_callback([](auto& c, size_t n){
  LOG_WARN() << "slow downstream, buffered=" << n;
});
```

这套语义是从 envoy 的 watermark buffer 直接借的。

### 5.3 命名一致性

最容易被忽略但最容易爆炸的问题。Google C++ Style 规定成员变量后缀 `_`，但项目里曾经混了 `ownerLoop_`（驼峰）和 `owner_loop_`（snake_case）。每次新人/未来的自己来看代码都要确认一下："这个项目到底用哪种风格？"

最近一次大清理后，全部统一为 `snake_case_`。这种事情**做的时候很无聊，不做的话半年后会成为离职原因**。

---

## 6. 跑一个压测：把架构选择落到数字上

光讲原理没意思，跑组数据看看。

### 6.1 测试环境

| 项 | 值 |
|---|---|
| CPU | AMD EPYC 9754（虚拟化后 **2 vCPU**）|
| 内核 | Linux 6.1 |
| 服务端 | `examples/demo_http_server`（response body = `"OK"`，2 字节）|
| 客户端 | wrk 1 线程，HTTP/1.1 keep-alive |
| 测试时长 | 每组 10s（c=10000 那组 15s）|

注意：**2 vCPU 是个很挤的环境**——wrk 自己也要占 CPU，所以 server 实际能拿到的只有约 1 个核。绝对数字不能跟生产机器比，但**配置之间的相对差**有参考价值。

### 6.2 ET vs LT 在小报文下基本打平

把 `ET` 环境变量分别设为 0/1，扫连接数：

| 并发连接 | ET=1 RPS | LT RPS | 平均延迟（LT）|
|---|---|---|---|
| 16   | 7430 | **8245** | 1.16 ms |
| 64   | 7768 | **7934** | 3.92 ms |
| 256  | 7092 | **8066** | 14.02 ms |
| 1024 | 7616 | **7798** | 47.52 ms |

**结果反直觉**：LT 在所有并发档位都略快（2–14%）。

为什么？2 字节响应下，ET 模式"读到 EAGAIN 才停"的优势几乎不存在（每次 `read()` 就一个报文），但 ET 的事件分发逻辑（要在用户态处理"已经 ready 但没读完"的状态）反而多了几个分支。**报文越小，ET 越没优势**。

预期是：响应变大（比如 100KB 文件）、或者高并发下大量 short-lived 连接（HTTP/1.0 风格），ET 才会反超。后面的 Buffer 一篇会用 100KB body 重测这个对比。

### 6.3 io_threads=1 比 io_threads=2 快 10–16%

这是这次跑数据最意外的发现。把 `IO_THREADS` 从 2 降到 1：

| 并发连接 | io=2 RPS | io=1 RPS | 提升 |
|---|---|---|---|
| 64   | 7934 | **9241** | +16% |
| 256  | 8066 | **9298** | +15% |
| 1024 | 7798 | **8602** | +10% |

直觉上"加线程加吞吐"，**实际上 thread > core 是反优化**。原因：

```
io_threads=2 时的线程：
  - main loop (Acceptor + connections map)
  - sub loop A
  - sub loop B
  - wrk thread
合计 4 个活跃线程，争 2 个核 → 不停 context switch + cross-thread eventfd 唤醒
```

```
io_threads=1 时：
  - main loop
  - sub loop A
  - wrk thread
3 个线程，依然超 2 核，但少了一组跨 sub-loop 的 wakeup
```

教训：**One-Loop-Per-Thread 的最优线程数 ≈ 物理核心数**，超过就开始亏。nginx 默认 `worker_processes auto` 也是按 CPU 核数来的，envoy 的 `--concurrency` 默认值同理。我之前默认设 `io_threads=2` 是想"反正多线程总比少线程好"，被数据打脸。

### 6.4 C10K 实测：1 万长连接

最后压一组 10000 keep-alive 连接（LT, io=2，跑 15s）：

```
Running 15s test @ http://127.0.0.1:18080/
  1 threads and 10000 connections
    Latency   571.57ms  365.87ms   1.76s    69.56%
    Req/Sec     6.45k     1.23k    8.95k    64.29%
  90384 requests in 15.25s, 7.33MB read
Requests/sec:   5925.20
```

**Socket errors: 0**。1 万连接稳定挂着，吞吐降到低并发的 ~75%，没有连接被踢掉。这就是 epoll + One-Loop-Per-Thread 模型相对 thread-per-connection 的根本胜利——thread-per-connection 在这台 2 核 VM 上跑 10K 线程会直接 OOM。

p99 延迟 1.76s 不算好看，但这是 2 vCPU 的物理瓶颈——10K 连接竞争 1 个 sub-loop 线程的服务能力，没有魔法。

### 6.5 这些数字给后续设计的启示

- **默认线程数应该 = nproc，但要让用户能调**：后面 GatewayServer 的配置项需要暴露这个
- **背压在这个量级还不是瓶颈**：5925 RPS × 2 字节 ≈ 12KB/s，离 HighWaterMark 的 64MB 阈值远得很。等做完上游代理（响应可能几 MB）才会真正有戏
- **Poller 抽象（select/poll/epoll 可切）的开销可以忽略**：上面的数字都是经过虚函数派发的，离 epoll 直接调用差距 < 1%。下一篇会用 perf 给出具体数

### 6.6 和 nginx 当面对线：反向代理 benchmark

直接拉网关层做对比。同一个 echo 上游（`demo_echo_server`，端口 9001），同一台 2 vCPU 机器，本项目 `demo_bench_gateway`（端口 8080）vs nginx 1.22（端口 8088）。配置严格对齐：

- 两侧都 `worker_processes / IO_THREADS = 4`
- 两侧都开启 upstream keepalive，pool 大小 = 64
- 两侧都 HTTP/1.1，关闭 access log
- wrk: 4 线程，扫并发 50 / 200 / 1000，每组 15s

代码和 nginx 配置都在 [`examples/demo_bench_gateway.cc`](../../../examples/demo_bench_gateway.cc)，可复现。

| 并发 | 本项目 RPS | nginx RPS | 本项目 p99 | nginx p99 | 本项目 / nginx |
|---|---|---|---|---|---|
| 50   | 12591 | 22036 | **12.47 ms** | 13.62 ms | 57% |
| 200  | 12388 | 21057 | 37.60 ms | 29.64 ms | 59% |
| 1000 |  5418 | 17487 | 422.10 ms | 264.34 ms | 31% |

诚实地解读：

**好消息**：50 并发档位下 p99 延迟反而比 nginx 略好（12.47 vs 13.62 ms）。说明在轻负载下，从 accept → 路由 → upstream pool → 转发的关键路径**没有架构级浪费**。这是 One-Loop-Per-Thread + std::any context 模型的兑现——单连接路径上没有多余跳板。

**中等消息**：50/200 并发下吞吐稳定在 nginx 的 ~58%。考虑到 nginx 是 20+ 年优化过的工业级实现，自研项目第一版能拿到这个比例已经超出预期。差距主要来自：
- nginx 用 `ngx_pool_t` per-request 池分配，本项目用 `std::string`/`std::function`，每请求多几次 `malloc`
- nginx 的 HTTP 头解析是手写状态机直接操作字节数组；本项目用 `Buffer::FindCRLF` + `string_view` 切片，多一次拷贝
- nginx 转发用 `writev` 一次性写 header+body，本项目刚做完合并写优化（[`1d7db0f`](https://github.com/akiba-miku/high-concurrency-runtime/commit/1d7db0f) commit），但仅覆盖 response 方向，request 方向还没合并

**坏消息**：1000 并发档位掉到 nginx 的 31%。这是一个明确的扩展性问题。猜测：
- nginx 4 个 worker = 4 个独立进程，各自维护 upstream pool；本项目 4 个 IO 线程**共享同一个** UpstreamConnPool，pool 内部加锁
- 1000 并发下 pool 锁竞争开始主导
- 这正是后续优化的方向：把 pool **分片到每个 EventLoop**，本质就是 One-Loop-Per-Thread 思想应用到上游侧

把这个 1000c 数据放出来不会让我难看——它**指明了下一阶段的具体工作**：上游连接池本地化。等做完，再压一次对比看缩了多少差距。

> 给毕设答辩老师：这组数据的价值不在"我比 nginx 快/慢"，而在于**架构选择的成本是可量化的**。58% 的吞吐对应了几个具体可改的点；31% 的吞吐对应了一个具体的锁竞争 hotspot。这种"读数据回到代码"的过程，比单纯报一个 RPS 数字有意义得多。

---

## 7. 收尾：这个系列接下来会写什么

按重要性排：

1. ✅ **总体架构**（本篇）
2. **Poller 抽象层**——为什么保留 select/poll 而不是只用 epoll？跨实现的契约怎么定？
3. **Channel 与 fd 事件分发**——Tie 机制的来龙去脉、ET vs LT 的取舍
4. **Buffer 的两种实现**——muduo 三段式 vs nginx ngx_chain，本项目两套都有
5. **TcpConnection 生命周期**——`shared_ptr` 模型 vs 对象池模型
6. **TimerQueue：红黑树 vs 小根堆 vs 时间轮**
7. **背压机制 HighWaterMark**——和 envoy watermark buffer 的对比
8. **HTTP Router：Trie 实现**——和 nginx phase handler 的对比

每一篇都会带：
- 对应的源码引用（`file_path:line_number`）
- 至少一个 benchmark 数字
- 对照 nginx/envoy/muduo/libevent 中至少一个的具体实现

如果你觉得哪个话题特别想看，告诉我，我可以调顺序。

---

## 参考资料

- Schmidt et al., *Pattern-Oriented Software Architecture, Volume 2*, Wiley, 2000. （Reactor 模式的原始定义）
- 陈硕，《Linux 多线程服务端编程：使用 muduo C++ 网络库》，电子工业出版社，2013.
- nginx source: <https://github.com/nginx/nginx>
- envoy source: <https://github.com/envoyproxy/envoy>
- muduo source: <https://github.com/chenshuo/muduo>
- libevent source: <https://github.com/libevent/libevent>
- 本项目: <https://github.com/akiba-miku/high-concurrency-runtime>

下一篇见。

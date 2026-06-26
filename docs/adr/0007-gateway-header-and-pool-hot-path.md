# ADR-007: 消除网关 Header 重写与连接池字符串哈希热点

状态：已采用
日期：2026-06-26

## 背景 / 约束

Gateway 的 c1000 perf 基线显示，请求 Header 的小写、查找、删除和字符串哈希是
最明显的 CPU 热点：

- `tolower`：约 7.48% self。
- `std::_Hash_bytes`：约 4.02% self。
- `HttpRequest::RemoveHeader`：约 3.53% self。
- `HttpRequest::set_header`：约 2.66% self。
- `UpstreamConnPool::Acquire`：部分采样中约 3.69% self。

根因不只是哈希函数本身。Gateway 在每个请求进入路由前，会固定调用多次
`RemoveHeader()`、`header()` 和 `set_header()`：

1. 删除 hop-by-hop Header。
2. 删除 `Connection` 指定的动态 Header。
3. 删除客户端伪造的 `X-Forwarded-*`。
4. 写入 `X-Forwarded-*`、`Via` 和 `X-Request-ID`。

旧的 `HttpRequest` API 会为普通查找和删除构造 PMR 小写字符串，再执行哈希。
即使目标 Header 不存在，也会支付小写和哈希成本。404、限流拒绝和 direct route
同样会提前支付 Gateway Header 重写成本。

连接池则使用 peer name 字符串作为 key。每个代理请求至少进行一次
`Acquire()`，正常完成后还会进行一次 `Release()`，重复计算稳定字符串的哈希。

约束如下：

- HTTP Header 名大小写不敏感，但按协议只需要 ASCII folding，不需要 locale。
- `HttpRequest` 仍需保存拥有所有权的小写 Header key，不能改成悬空
  `string_view`。
- Upstream peer 在启动阶段创建，运行期间集合只读，peer 地址稳定。
- 本次不进行 `UpstreamRequest` 生命周期、TimerQueue 或 HTTP 容器整体替换等
  大规模重构。

## 决定

### 1. Gateway 在序列化时一次性过滤和注入 Header

删除 `GatewayServer` 中对 `HttpRequest` 的原地 `RewriteForUpstream()`。

仅在请求已经：

1. 通过限流；
2. 匹配 proxy route；
3. 找到 upstream；
4. 通过 circuit breaker；

之后，才由 `ProxyPass::BuildRequest()` 在序列化请求时处理 Header。

`BuildRequest()` 单次遍历 Header：

- 跳过固定 hop-by-hop Header。
- 跳过 `Connection` 值动态指定的 Header。
- 跳过客户端提供的 `X-Real-IP`、`X-Forwarded-Proto`、
  `X-Forwarded-Host` 和 `X-Forwarded-Port`。
- 保留并追加原始 `X-Forwarded-For`。
- 保留并追加原始 `Via`。
- 保留已有 `X-Request-ID`，不存在时写入 Gateway 生成的 ID。
- 重写 upstream `Host` 和 `Connection: keep-alive`。

原始 `HttpRequest` 不再为代理转发被反复修改。

### 2. HTTP 请求 Header map 使用透明、大小写不敏感查找

Header map 使用专用的 `HeaderNameHash` 和 `HeaderNameEqual`：

- 声明 `is_transparent`，允许 C++23 unordered container 直接使用
  `std::string_view` 查找。
- hash 和 equality 都执行 ASCII 大小写折叠。
- 不调用 locale-aware `std::tolower`。

API 行为调整为：

- `header()`：直接用 `string_view` 查找，不创建临时 key。
- `RemoveHeader()`：直接查找 iterator 后删除，不创建临时 key。
- `set_header()`：先透明查找；仅当 Header 不存在时，才创建拥有所有权的小写
  PMR key。
- 解析阶段继续保存小写 owning key。

C++23 提供异类 unordered lookup，但标准库不知道 HTTP Header 的 ASCII
大小写规则，因此仍然需要显式提供透明 hash/equality 类型。

### 3. 连接池使用稳定 peer 指针作为 key

将：

```cpp
std::unordered_map<std::string, IdleQueue>
```

改为：

```cpp
std::unordered_map<const UpstreamPeer*, IdleQueue>
```

`Acquire()` 和 `Release()` 直接接收 `peer.get()`。这消除了连接池热路径上的
peer name 字符串哈希和首次插入时的字符串复制。

该设计依赖现有生命周期约束：Upstream peer 在启动后不再增删，并且 Gateway
运行期间由 Registry/Upstream 持有。

## 排除什么 / 为什么

- **只替换成更快的字符串哈希算法**：不能消除多次无效查找、临时 key 构造和
  Gateway 原地重写，治标不治本。
- **只给连接池增加透明字符串查找**：调用方本来已经传递
  `const std::string&`，主要成本是哈希字符串内容，而不是 key 构造。
- **把所有常见 Header 都做成员缓存**：会扩大 `HttpRequest` 状态和一致性维护
  成本，目前 Host、Connection、Content-Length 缓存已经足够。
- **立即替换 `unordered_map` 为 flat map 或自定义容器**：当前 Header 查找热点
  已消失，容器整体重构的收益证据不足。
- **本轮同时改 Timer 和 `UpstreamRequest` 生命周期**：两者影响异步状态机和
  资源所有权，风险远高于本 ADR 的局部优化。

## 验证与结果

功能测试覆盖：

- hop-by-hop Header 过滤。
- `Connection` 动态指定 Header 的过滤。
- spoofed forwarded Header 的替换。
- `X-Forwarded-For` 和 `Via` 的追加。
- 已有 `X-Request-ID` 的保留。
- Header 混合大小写的查找、更新和删除。
- upstream keep-alive 连接复用。

全部 26 个已构建测试通过。两个 GoogleTest 聚合目标因环境未发现 GTest 而未
构建，不属于测试失败。

新的 perf 数据保存在：

- `docs/benchmark/perf-after-header-pool-c500/`
- `docs/benchmark/perf-after-header-pool-c1000-f49/`

c500 图更适合观察正常热路径；c1000 在 DWARF 调用栈采样时出现 timeout 和
deadline 日志扰动，只用于观察压力状态，不作为正式延迟基准。

新的 c500 profile 中：

- `HttpRequest::RemoveHeader` 和 `HttpRequest::set_header` 不再是热点。
- `UpstreamConnPool::Acquire` 的字符串哈希热点消失。
- `std::_Hash_bytes` 从旧基线约 4.02% 降至约 0.25%。
- `tolower` 从旧基线约 7.48% 降至约 1.18%。
- `ProxyPass::BuildRequest` children 约 0.60%，不再是首要瓶颈。

上述比例来自不同采样轮次，只用于判断热点迁移，不应当视为严格的同比性能
基准。

## 代价转移

- Gateway Header 规则集中到了 `ProxyPass::BuildRequest()`，序列化函数承担了
  更多协议职责。
- `HeaderNameHash` 使用自定义 FNV-1a 风格 ASCII folding hash，必须保证 hash
  和 equality 始终使用相同的大小写规则。
- 连接池 key 不再自带可读名称，调试输出仍应通过 `UpstreamPeer::config().name`
  获取。
- 连接池使用裸指针作为非 owning key，未来若支持运行时动态删除 peer，必须先
  清理每个 EventLoop 的对应 idle queue，或改用稳定整数 ID。

## 后续瓶颈 / 待决策

### 1. 每请求 deadline Timer

当前最明确的结构性热点是每个 upstream 请求都执行：

```text
ArmDeadline
  -> EventLoop::RunAfter
  -> TimerQueue::AddTimer
  -> intrusive RBTree insert

Finalize
  -> CancelDeadline
  -> TimerQueue::Cancel
  -> intrusive RBTree erase
```

c500 profile 中：

- `UpstreamRequest::ArmDeadline` children 约 1.19%。
- `TimerQueue::AddTimer` children 约 0.81%。
- `UpstreamRequest::Finalize` children 约 2.61%。
- `TimerQueue::Cancel` children 约 1.41%。

大部分请求在 1～2ms 内结束，却为 5 秒 deadline 创建并立即取消 Timer。
后续应评估粗粒度 deadline bucket、时间轮或 EventLoop 共享 deadline 扫描。
不能为了性能直接删除 timeout。

### 2. `shared_ptr` / `weak_ptr` 与 callback 重绑

当前可见成本包括：

- `shared_ptr` strong/weak 原子引用计数。
- `make_shared<UpstreamRequest>` 控制块。
- Timer 和网络 callback 捕获 `weak_ptr`。
- pooled connection 每次复用时重新安装 message/close callback。

彻底解决通常需要 EventLoop 单线程 owning、intrusive lifetime 或 request slab，
属于中型重构，需要单独 ADR。

### 3. 请求 ID 格式化

`GenRequestId()` 每请求使用 `snprintf` 格式化两个固定宽度的 64 位十六进制数。
c500 profile 中该调用链约 1.22%。

可用固定 hex lookup table 直接写入 33 字节缓冲区。这是低风险、局部优化。

### 4. HTTP Header 解析插入

查找热点消失后，剩余 Header 成本集中在解析阶段：

```text
HttpContext::ProcessHeaderLine
  -> HttpRequest::AddHeaderLowered
  -> unordered_map node allocation / insert
```

下一步低风险修改是把 `header_utils.h` 中剩余 locale-aware `std::tolower`
替换为 ASCII folding。是否替换 Header 容器，应在新 profile 证明解析插入再次
成为主要热点后再决定。

### 5. 上游响应 Header 被扫描两次

`OnUpstreamMessage()` 当前分别调用：

- `ParseFraming()`：查找 Content-Length、Transfer-Encoding、Connection。
- `RewriteHeaders()`：再次扫描并重写 Server Header。

二者可合并成一次 Header block 遍历，但预期收益约 1%，优先级低于 deadline
Timer 和请求生命周期。

## 后续顺序

建议按风险和收益排序：

1. 用固定十六进制编码替换 request ID 的 `snprintf`。
2. 清除 Header 解析路径中剩余的 locale-aware `std::tolower`。
3. 为每请求 deadline Timer 设计 batching/bucket 方案并单独记录 ADR。
4. 合并 upstream response framing 解析和 Header 重写。
5. 最后评估 `UpstreamRequest` 的 EventLoop-owned/intrusive 生命周期。

## 2026-06-26 复测：wrk 吞吐、wrk2 尾延迟与内核开销

本节覆盖此前保存的旧 benchmark 摘要。原始数据分别位于：

- `docs/benchmark/results/`
- `docs/benchmark/results-expanded/`
- `docs/benchmark/results-wrk2-c1000/`
- `docs/benchmark/perf/`

所有表格均为三轮算术平均。测试在同一台机器完成，wrk、Gateway 和两个 Nginx
实例共享 CPU 与 loopback 网络栈，因此绝对数值只适合本机回归和相对比较。

### wrk 闭环吞吐对比

参数：

```text
wrk -t4 -d15s --latency --timeout 5s
connections = 100, 500, 1000, 5000, 10000
rounds = 3
Gateway IO threads = 4
Nginx workers = 4
```

Gateway 默认 `max_concurrent_requests=1024` 时：

| 并发 | Gateway RPS | Gateway P99 | 非 2xx | Nginx RPS | Nginx P99 |
|---:|---:|---:|---:|---:|---:|
| 100 | 296,139 | 1.337 ms | 0 | 297,730 | 1.013 ms |
| 500 | 228,754 | 3.307 ms | 0 | 228,555 | 3.683 ms |
| 1,000 | 207,755 | 6.643 ms | 0 | 195,994 | 8.980 ms |
| 5,000 | 69,485 | 287.397 ms | 518,064 | 170,326 | 53.663 ms |
| 10,000 | 61,140 | 792.043 ms | 300,635 | 155,004 | 110.503 ms |

`c=5000/10000` 的默认配置结果是 bulkhead 拒绝测试，不是公平的代理吞吐
对比。临时将 benchmark upstream 的 `max_concurrent_requests` 提高到 `20000`
后重新测试：

| 并发 | Gateway RPS | Gateway P99 | 非 2xx | Nginx RPS | Nginx P99 |
|---:|---:|---:|---:|---:|---:|
| 5,000 | 118,498 | 84.400 ms | 0 | 170,326 | 53.663 ms |
| 10,000 | 98,353 | 174.593 ms | 0 | 155,004 | 110.503 ms |

临时容量修改已在测试后恢复。该结果说明：

- `1024` bulkhead 是高并发非 2xx 的直接原因。
- 去除该限制后，Gateway 高并发吞吐仍只有 Nginx 的约 `69.6%` 和 `63.5%`。
- Gateway P99 仍高约 `57%` 和 `58%`，剩余差距属于真实执行路径和内核开销。
- Gateway RSS 仍更低：扩大容量后约 `88.6 MB` 和 `142.0 MB`，对应 Nginx
  约 `170.8 MB` 和 `210.3 MB`。

聚合摘要已更新为 `docs/benchmark/results/summary.csv`。其中
`gateway-default` 保留默认 bulkhead 行为，`gateway-expanded` 仅表示临时将
并发槽提高到 `20000` 的高并发结果。

### wrk2 固定速率尾延迟

wrk2 使用开环固定速率模型，用于回答“在给定请求速率下尾延迟是多少”，不能和
wrk 的最大吞吐闭环结果直接混合解释。

参数：

```text
wrk2 -t4 -c1000 -d15s -L -R<rate> --timeout 5s
rates = 50000, 80000, 100000, 120000, 150000
rounds = 3
```

| 目标速率 | Gateway 实际 RPS | Gateway P99 | Gateway P99.9 | Nginx 实际 RPS | Nginx P99 | Nginx P99.9 |
|---:|---:|---:|---:|---:|---:|---:|
| 50,000 | 47,835 | 4.743 ms | 10.703 ms | 47,948 | 4.480 ms | 10.053 ms |
| 80,000 | 76,703 | 3.453 ms | 5.663 ms | 76,701 | 3.497 ms | 7.687 ms |
| 100,000 | 95,858 | 5.583 ms | 6.987 ms | 95,641 | 8.187 ms | 14.650 ms |
| 120,000 | 115,034 | 3.560 ms | 5.080 ms | 115,038 | 3.357 ms | 6.300 ms |
| 150,000 | 143,784 | 4.057 ms | 5.303 ms | 143,787 | 5.557 ms | 11.650 ms |

所有轮次均为零非 2xx、零 socket error、零 timeout。

曾使用 `-c10000` 跑过一组 wrk2，但客户端在连接建立和调度上无法达到设定
速率，例如 `-R100000` 实际仅约 `58k RPS`。该组保存在
`docs/benchmark/results-wrk2/`，只作为客户端饱和反例，不作为正式结论。

### 优化后的用户态热点

旧 perf 数据早于透明 Header hash/equality 改动，不能用于评价当前实现。
重新编译并采样当前工作区后，主要变化为：

| 热点 | 优化前 self | 优化后 self |
|---|---:|---:|
| `tolower` | 约 5%～7.5% | 约 0.3%～1.4% |
| `std::_Hash_bytes` | 约 3%～4% | 约 0.1%～0.3% |
| `HttpRequest::RemoveHeader` | 约 2.7%～3.5% | 跌出主要热点 |
| `HttpRequest::set_header` | 约 2.7% | 跌出主要热点 |

当前主要用户态热点已经转移到：

- `GatewayServer::OnMessage`
- `UpstreamRequest::Start`
- `shared_ptr` / `weak_ptr` 引用计数
- `malloc` / `free` 与 `UpstreamRequest` 析构
- pooled connection callback 重绑
- 每请求 deadline Timer 创建和取消

当前优化后 c1000 perf 负载约 `201,969 RPS`、P99 `6.74 ms`。

### 内核态瓶颈

在 `kernel.perf_event_paranoid=1` 下，对当前优化后二进制进行：

```text
wrk -t4 -c1000 -d40s
perf record -e cycles
perf record -e cpu-clock:k
```

采样期间负载约 `206,515 RPS`、P99 `6.76 ms`、零错误。

全栈样本按 CPU mode 统计：

```text
内核态约 68.6%
用户态约 31.4%
```

内核专用调用链按入口路径归类：

```text
send/TCP 发送路径    72.2%
readv/TCP 接收路径   17.7%
epoll_wait 路径       8.2%
其他                  1.9%
```

主要内核 self 热点包括：

- `_raw_spin_unlock_irqrestore`：8.84%，主要来自 `sock_def_readable`。
- `tcp_ack`：4.38%。
- `__inet_lookup_established`：3.26%。
- `tcp_rcv_established`：3.26%。
- `sock_poll`：2.84%。
- `tcp_poll`：2.08%。
- `vfs_readv`：2.00%。
- `nf_conntrack`：合计约 3.46%。
- `tcp_sendmsg_locked`：1.50%。
- `skb_release_data`：1.40%。
- `tcp_recvmsg_locked`：1.38%。

系统调用入口本身不是主要成本；`do_syscall_64` self 约 1.68%。主要成本发生在
进入内核后的 TCP ACK、socket 唤醒、SKB 生命周期、loopback 软中断、epoll
扫描和 conntrack。

火焰图：

- `docs/benchmark/perf/optimized-c1000-flamegraph.svg`
- `docs/benchmark/perf/kernel-c1000-all-flamegraph.svg`
- `docs/benchmark/perf/kernel-c1000-kernel-flamegraph.svg`

tracefs 在该环境仍是只读，因此本轮没有精确的逐类 syscall 调用次数；上述比例
来自 CPU 栈采样，不等同于 syscall 次数占比。

## 内核加速方向决策

### 决定

当前不引入 XDP、AF_XDP 或 DPDK，不将它们视为下一阶段的默认优化方向。

保留 io_uring backend 的实验价值，但将其定位为渐进优化，不假设它能解决当前
主要差距。原因：

1. epoll 内核路径只占内核样本约 8.2%，即使完全消除也不足以解释高并发差距。
2. 最大成本位于 TCP send/ACK/socket wakeup/SKB 路径，io_uring 不会绕过 TCP
   协议栈。
3. Gateway 是需要 HTTP stream 解析、Header 重写、重试、deadline 和 upstream
   连接池的 L7 proxy。XDP 不提供完整 TCP stream 语义。
4. DPDK/AF_XDP 要获得完整收益，需要用户态 TCP 栈、独占队列和 CPU，并显著
   增加路由、防火墙、可观测性和运维复杂度。
5. 当前测试把 wrk、Gateway 和 upstream Nginx 放在同一台机器，loopback 两段
   TCP 和软中断被集中计入本机，内核比例会高于跨机器部署。

### 下一步

1. 将 wrk、Gateway、upstream 拆到不同机器重新测量，建立生产形态基线。
2. 统计每请求 `send/readv/epoll_wait` 次数，确认是否存在小包和重复 syscall。
3. 优先合并请求/响应 buffer，减少 send 次数和 packet 数量。
4. 在隔离测试环境评估关闭 benchmark 端口 conntrack 的收益。
5. 以相同 HTTP、连接池和 Timer 逻辑对比 epoll 与 io_uring backend。
6. 只有在跨机器测试仍证明内核协议栈是主要限制，并且业务目标需要显著高于当前
   `100k～200k RPS` 时，再为 AF_XDP/DPDK 编写独立 ADR。

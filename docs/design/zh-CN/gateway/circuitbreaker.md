## 熔断 CircuitBreaker — 无锁 3 状态状态机

如果不了解降级， 推荐先了解一下降级。
熔断（`Circuit Breaker`）是降级的一种自动触发机制，它不是降级本身。
熔断三态（`Closed/Open/Half-Open`）描述的是"要不要把请求放过去"，
而降级描述的是"放不过去的时候干什么"。

那么熔断应该管什么范围？ 

在 `EventLoop` → `TcpConnection` → 转发逻辑 → 选择 upstream 节点（LB） → 建立/复用连接池里的连接 → 发出请求。

`UpstreamPeer` 的 `max_fails` + `fail_timeout` 本质就是一个简陋的熔断器 - 失败达到阈值 -> 标记为 `down`, 超时后自动恢复。 区别是它只管单个上游节点的状态。

熔断器应该考虑互补， 而不是替代它。 
采用`Per-upstream` 的方案， 保护整个后端集群， 快路径放行通过或者快速失败避免浪费资源。

熔断器**挂在 LB 选节点之后、实际发请求之前**。更准确地说，它是每个 upstream 节点的一个属性——不是全局的，是 per-node 的。你有五种 LB 算法，选出节点之后，先问这个节点的熔断器：**你现在开着吗？**开着就跳过这个节点，重选或者直接降级。

所以熔断器在代码里的形状是：一个附着在 `UpstreamNode`（或者你项目里对应的结构体）上的状态机对象。

---

## 三态状态机，精确定义

```
         失败率超阈值
Closed ─────────────→ Open
  ↑                     │
  │   探测成功           ｜等待 reset_timeout
  │                     ↓
  └────────────── Half-Open
                    │
                    └─→ Open（探测失败）
```

**Closed**：正常放行所有请求，同时在滑动窗口里统计成功/失败。失败率或失败次数超过阈值 → 转 Open。

**Open**：拒绝所有请求，直接返回降级响应，**不访问上游**。等待一个固定时长（`reset_timeout`，通常 5~30s）后 → 转 Half-Open。

**Half-Open**：放行**一个**（或少量）探测请求。探测成功 → 转 Closed，清空统计窗口。探测失败 → 立刻回 Open，重置计时器。

Half-Open 的"放行一个"是实现难点。多线程环境下多个 IO 线程同时看到 Half-Open，必须保证只有一个线程能发出探测请求，其余的要么等待要么直接降级。这是一个典型的 CAS 场景。

---

## 统计窗口：计数器 vs 滑动窗口

熔断器的灵魂在统计，统计的质量决定触发的准确性。

**固定窗口计数器**是最简单的实现：维护一个周期内的成功数和失败数，周期结束重置。问题是边界效应——窗口切换瞬间统计清零，刚才的连续失败被遗忘。

**滑动窗口**有两种：

时间滑动窗口：记录每个请求的时间戳和结果，统计时只看最近 N 秒内的数据。精确，但内存开销随 QPS 线性增长，高并发下不实用。

**环形缓冲区（Ring Buffer）滑动窗口**：这是 Hystrix 和 Resilience4j 的实际做法，也是你应该用的。固定大小的环形数组（比如 100 个槽），每个槽记录一个请求的结果（成功/失败/超时）。新请求写入时覆盖最老的槽，同时维护全局的成功数和失败数计数器——写入新槽时加上新结果，减去被覆盖的旧结果。

```cpp
struct Slot {
    enum class Result : uint8_t { kSuccess, kFailure, kTimeout };
    Result result;
};

class RingBufferWindow {
    static constexpr int kSize = 100;
    std::array<Slot, kSize> buffer_;
    std::atomic<int> head_{0};       // 下一个写入位置
    std::atomic<int> total_{0};      // 已记录请求总数
    std::atomic<int> failure_count_{0};
    // ...
};
```

环形缓冲区的好处：O(1) 写入，O(1) 读取失败率，内存固定。缺点是窗口是"最近 N 次请求"而非"最近 N 秒"，低 QPS 场景下窗口时间跨度不确定。两种语义各有适用场景，网关通常用请求数窗口更合理。

---

## 并发安全的状态转移

这是实现里最容易出 bug 的地方。你的每个 EventLoop 线程都可能同时在读熔断器状态，状态转移逻辑必须无锁或细粒度加锁。

核心字段：

```cpp
enum class State : uint8_t { kClosed, kOpen, kHalfOpen };

class CircuitBreaker {
    std::atomic<State> state_{State::kClosed};
    std::atomic<int64_t> open_timestamp_{0};  // 进入 Open 的时间点
    std::atomic<bool> probe_in_flight_{false}; // Half-Open 探测令牌
    RingBufferWindow window_;
    // 配置
    int failure_threshold_;     // 触发熔断的失败率阈值，如 50%
    int min_requests_;          // 最小请求数，防止样本太少就触发
    int64_t reset_timeout_ms_;  // Open 持续时长
};
```

**判断是否放行请求**（IO 线程调用，必须极快）：

```cpp
bool CircuitBreaker::AllowRequest() {
    State s = state_.load(std::memory_order_acquire);
    
    if (s == State::kClosed) return true;
    
    if (s == State::kOpen) {
        int64_t now = CurrentTimeMs();
        if (now - open_timestamp_.load() >= reset_timeout_ms_) {
            // 尝试转为 Half-Open
            State expected = State::kOpen;
            if (state_.compare_exchange_strong(expected, State::kHalfOpen,
                    std::memory_order_acq_rel)) {
                // 我赢得了状态转移，顺便拿探测令牌
                probe_in_flight_.store(true);
                return true;  // 我来做探测
            }
        }
        return false;  // 还在 Open 期，拒绝
    }
    
    // Half-Open：只允许一个探测请求
    bool expected = false;
    return probe_in_flight_.compare_exchange_strong(expected, true,
               std::memory_order_acq_rel);
    // CAS 成功：我拿到令牌，允许通过
    // CAS 失败：已有探测在飞，拒绝
}
```

**记录请求结果**（请求完成后调用）：

```cpp
void CircuitBreaker::RecordResult(bool success) {
    State s = state_.load(std::memory_order_acquire);
    
    if (s == State::kHalfOpen) {
        if (success) {
            // 探测成功，恢复
            window_.Reset();
            probe_in_flight_.store(false);
            state_.store(State::kClosed, std::memory_order_release);
        } else {
            // 探测失败，重新打开
            probe_in_flight_.store(false);
            open_timestamp_.store(CurrentTimeMs());
            state_.store(State::kOpen, std::memory_order_release);
        }
        return;
    }
    
    window_.Record(success);
    
    if (s == State::kClosed && ShouldTrip()) {
        State expected = State::kClosed;
        if (state_.compare_exchange_strong(expected, State::kOpen,
                std::memory_order_acq_rel)) {
            open_timestamp_.store(CurrentTimeMs());
        }
    }
}
```

---

## 阈值设计的陷阱

两个常见错误：

**只看失败率，不看最小样本数。** 冷启动阶段第一个请求失败，失败率 100%，直接触发熔断。正确做法是窗口内请求数未达到 `min_requests`（比如 20）时不触发，无论失败率多高。

**阈值太低导致振荡。** 上游抖动恢复，Half-Open 探测成功回 Closed，流量涌入又把它打挂，反复横跳。解决方案是 Half-Open 阶段不只放一个请求，而是放一个**缓慢增加的比例**（比如先放 5%，成功后放 20%，再成功后全量）。这就是所谓的"渐进式恢复"，实现复杂度更高，但稳定性好很多。

---

你的项目里主动健康检查已经在做周期性探活，和熔断器的 Half-Open 探测有功能重叠。下一步想聊的是：怎么把这两套机制整合，避免重复轮子，还是先看限流的设计？
# ADR-0011: 定时器索引改为可注入策略

2026-08-26.

## 背景 / 约束

TimerQueue 原先把 `IntrusiveTree` 写死为唯一索引。四叉堆已经作为对等的有序索引存在，
但不能在不改 TimerQueue 的情况下替换。Splay tree 没有运行时调用方。

Runtime 的 composition root 不能增加调优开关（ADR-0010）。索引选择属于手动组合
`Loop` 的实现细节。

## 决定

- 删除未使用的 `IntrusiveSplayTree`。
- `time::Timer` 同时携带 rbtree 与 quadheap hook；同一时刻只链接其中一个。
- `time::TimerIndex` 以构造期 `TimerIndexKind` 在红黑树（默认）和四叉堆之间选择。
- 三个 backend 的 `TimerQueue` 与 `Loop` 接收该策略；默认仍是红黑树。

## 排除什么 / 为什么

- 不为 Runtime Builder 增加 timer 索引开关：那会把实现调优提升成跨后端应用接口。
- 不把索引做成开放虚接口：当前只有两个适配器，关闭的 `variant` 策略足够深。
- 不把 `Loop` 做成索引模板：模板会传染 worker 与 Runtime。

## 代价转移

`Timer` 为未使用的那条索引 hook 多占一个 heap index（或 rbtree 节点）。默认路径仍是
红黑树，取消成本与 ADR-0001 一致。四叉堆路径用反向索引换更矮的树和更好的缓存行为。

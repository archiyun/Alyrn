# Intrusive-model

本文将快速帮助读者熟悉侵入式结构并了解项目核心 `ds` 模块的侵入式结构。

## 1. 适用范围

本文涵盖核心容器：

 - `IntrusiveList`
 - `IntrusiveQueue`
 - `IntrusiveTree`

很好区分，前缀带有 `Intrusive`；它们由 `coropact/ds.h` 导出。

## 2. Intrusive 模型

如何理解侵入式容器?
以C++的双向链表`std::list`为例:

为了描述这种链式结构, 采用了"节点即对象"的写法, 准确来说容器创建一个额外的内部节点，节点中保存链接字段和用户对象。
```
`std::list`
--->----------- --->-----------
    |         |     |         |
    | Object  |     | Object  |
    |         |     |         |
    -----------     -----------
```

但事实上, 可以把节点作为对象的一部分, 这时候称其为hook.
如下, 同样描述了一个链表.
```
    -----------     -----------
    |         |     |         |
    | Object  |     | Object  |
    |  hook   | --->|  hook   |
    -----------     -----------
```
所以,
`Intrusive` 容器不保存独立的包装节点. 节点 hook 嵌入到用户对象.

实现侵入式结构常见有两种 挂载成员和继承.
前者在`Linux`很常见, hlist, list, 红黑树都是基于成员风格.
后者是我的风格, 受经典Boost库的启发.

以一个简单的例子演示
```cpp
struct PeerTag {};

struct Connection : public coropact::ds::ListNode<Connection, PeerTag> {
    int fd{-1};
};

coropact::ds::IntrusiveList<Connection, PeerTag> connections;
Connection connection;

connections.PushBack(&connection);
```

第一行
`struct PeerTag{};` 是典型的空类标签, 它解决了如何一个对象如果挂多个相同类型容器.
比如上面示例 有两个链表需要挂载, 如果标签不存在那么只能挂一个链表.
但是通过标签就可以区分.
```cpp
struct PeerTag {};
struct RequestTag {};

// 多继承区分.
struct Connection : public coropact::ds::ListNode<Connection, PeerTag>,
                    public coropact::ds::ListNode<Connection, RequestTag> {};
```

注意对象本身存储业务字段和容器链接字段(继承风格需要基础 base hook), 容器只负责描述如何组织数据.

## 3. 所有权与生命周期

这不仅在设计上是难点, 对于使用者更是如此, 牢记没有免费的午餐.

以下将介绍从标准库通用容器到写侵入式容器的取舍.

### 3.1 容器不拥有对象

容器不会:

- 分配对象
- 释放对象
- 调用对象析构函数
- 延长对象的生命周期

很遗憾, 但是你必须手动管理对象的生命周期, 侵入式容器不会提供这种服务了.

以下有几点注意事项:
- 按照“创建对象 -> 加入容器 -> 从容器移除 -> 销毁对象”的顺序管理生命周期。
如果对象已经销毁但是还在容器里面, 由于  hook 与对象同生命周期, 容器会有悬空地址, 这是引起BUG的地方, 不同侵入式容器可能表现的行为不同.
- 不要重复加入同一个 hook。实现通常会通过 `InList()`、`InTree()`、`InHeap()` 等接口检查元素是否已经加入；重复加入可能返回失败或触发断言。
- 不要将对象加入到同一类型容器, 这是未定义行为且绝对不要这么干! 详见 第五个标题.

### 3.2 对象不能被拷贝与移动

这是设计的妥协
容器内部保存的是 hook 的地址，而 hook 是对象的一部分。对象移动后，hook 地址也随之改变，容器中原有的指针便会失效。Intrusive hook 通过编译器来推导对象地址, 因此元素地址必须稳定.
具体说一下,
在所有侵入式容器都能看到这么两行接口
```cpp
  static T* ElemOf(Node* node) noexcept { return static_cast<T*>(node); }
  static Node* NodeOf(T* elem) noexcept { return static_cast<Node*>(elem); }
```
优雅的对称结构
hook -> object
object -> hook

继承 intrusive node 的对象：
- 不应被复制；
- 不应被移动；
- 不应放入会导致地址改变的容器中。(这条在目前实现的容器不会发生)

更严谨的说法:
已经挂入容器的对象不能移动；复制和移动是否被允许，取决于 hook 类型的实现。

## 4. Hook 与 Tag

  每个 intrusive 容器都通过一个 hook 表示成员关系：

   容器                  Hook
  ━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━
   IntrusiveList         ListNode<T, Tag>
  ────────────────────  ───────────────────
   IntrusiveQueue        QueueNode<T, Tag>
  ────────────────────  ───────────────────
   IntrusiveTree       RBTreeNode<T, Tag>
  ────────────────────  ───────────────────

如前面的例子, 同一个对象可以通过不同的 Tag 同时加入多个同类型的容器

```cpp
struct GlobalTag {};
struct PeerTag {};

struct Item
    : public coropact::ds::ListNode<Item, GlobalTag>,
     public coropact::ds::ListNode<Item, PeerTag> {};

coropact::ds::IntrusiveList<Item, GlobalTag> global_list;
coropact::ds::IntrusiveList<Item, PeerTag> peer_list;
```

**Tag 只用于区分不同的 hook**

## 5. Membership 规则
一个hook 在同一时刻最多属于一个容器.

比如一个没有用Tag区分
list_a ,list_b 的类型都是 `coropact::ds::IntrusiveList<Item>;`.
```cpp
Item item;
list_a.PushBack(&item);

list_b.PushBack(&item); // 不能再次加入
```

跨容器归属检查并非所有结构都提供；部分结构仅在 Debug 模式下记录 owner，这是出于内存开销的考量。

需要注意, 侵入式实现里面的`InList()`,`InTree()`,`InHeap()`等均表示:
`该 hook 当前处于某个容器中`.
hook 的链接字段只能描述一份成员关系，所以同一 hook 同时加入多个容器属于未定义行为。

因此, 下面操作需要注意
```cpp
if (item.InList()) {
    another_list.Erase(&item);
}
```
每个 `<T, Tag>` 组合表示一份独立的 hook。同一个对象只有拥有多份不同 Tag 的 hook，才能同时加入多个同类型容器。
如果要挂多个相同容器, 用 Tag 区分。以上跨容器删除属于未定义行为；即使 Debug 模式能够检查，也不能把检查当成 Release 模式下的运行时保证。

## 6. 容器析构与 Clear

所有当前的 intrusive 容器都提供 `Clear()`，并且 `Clear()` 会解除容器中每个节点的 hook；容器析构时也会执行同样的清理。

| 容器 | 析构时是否解除节点 hook |
| --- | --- |
| `IntrusiveList` | 是 |
| `IntrusiveQueue` | 是 |
| `IntrusiveTree` | 是 |

当容器与元素生命周期存在交叉时, 仍建议显式写出
`xxx.Clear()`
另外最重要的是，容器 `Clear()` 时其中的 hook 仍然有效，并会被重置为未加入状态。如果对象即将析构，必须先让它脱离所有容器。

## 7. 排序,键与比较器

有序容器要求比较器满足严格弱序，且比较结果在元素加入容器期间保持稳定。

容器不能在元素已经加入后修改会影响排序或 bucket 位置的字段，除非先移除元素再重新插入。

## 8. 并发模型

除非具体文档另有说明, 否则 intrusive 容器不提供内部同步:

- 并发读写需要外部同步
- 并发写入考虑上锁
- 一个线程销毁元素时,其它线程不能访问该元素
- 容器析构时应停止所有并发访问行为

## 9. 统一错误与前置条件

返回值只表示接口文档明确声明的正常失败。违反生命周期、成员归属和结构不变量的行为属于前置条件违约，不应依赖返回值恢复。

## 10. 项目中的使用
这不是侵入式数据结构实现图鉴
以下是 Intrusive 在项目中的使用

   结构                  使用位置
  ━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   IntrusiveList         连接池、IO buffer
  ────────────────────  ─────────────────────────────────
   IntrusiveQueue        协程 WorkQueue
  ────────────────────  ─────────────────────────────────
   IntrusiveTree       默认 TimerIndex、deadline 调度
  ────────────────────  ─────────────────────────────────
  ────────────────────  ─────────────────────────────────

`time::TimerIndex` 在构造时选择红黑树或四叉堆；backend `TimerQueue` 注入该索引，
默认仍是红黑树。`time::Timer` 同时携带两种 hook，但同一时刻只链接其中一个。

## 11. 相关实现与测试

实现位于 :
  - include/coropact/ds/
  - include/coropact/time/

  验证测试位于：

  - tests/unit/test_intrusive_list_smoke.cc
  - tests/unit/test_intrusive_queue_smoke.cc
  - tests/unit/test_rbtree_validator.cc
  - tests/unit/test_quad_heap.cc
  - tests/unit/test_timer_tree_smoke.cc
  - tests/unit/test_timer_index_smoke.cc
  - tests/unit/test_mpsc_bounded_queue_smoke.cc

完.

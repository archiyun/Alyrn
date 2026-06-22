# ADR-001: 定时器红黑树采用侵入式而非 std::set v1->v4演化

与其说是单个决策，不如说这是v1-v4的决策链，早期没写，这次一并补上。尽力回忆早期编写的思考过程， v4是最近编写的，印象较深。 2026-06-15.


侵入式结构早期的了解可能从内存池开始或者 Linux 源码， 具体学习时间无从考证。


## 背景 / 约束
定时器管理 Timer, 一般不会等到过期触发回调， 取消是比较高频的。 每个 timer 节点用 std::set 包装， 节点要额外分配。

## 决定
了解了侵入式结构，看算法导论编写红黑树。

翻了 git 历史
[05-06:7b77556](https://github.com/akiba-miku/high-concurrency-runtime/commit/7b775564e98d20ab1e895fe1bfd27d8c2df632c9#diff-4c51713d85ef5b5b11ba8cc9d05af608a89b6ae136d09864a1888be8c45a0bd2): 节点嵌入Timer, 过于耦合但是第一次开始写完， 实现了标准CLRS红黑树。具体看了Nginx红黑树源码。--缺点 过耦合，目前就定时器使用但或许考虑未来的使用。
[05-11](https://github.com/akiba-miku/high-concurrency-runtime/commit/29e967052022cfa9066b61bf8a85cf1c64a36995): 实现了可插拔红黑树，实现解耦， 形成了初代Member hook.
后续，缓存了最小值指针。--缺点 考虑有虚函数多继承甚至菱形继承的场景， 内存布局变得复杂， 考虑到前者节点内部缓存了对象指针以逃避这个问题。
[06-02](https://github.com/akiba-miku/high-concurrency-runtime/commit/744c02cfc7f3e7f60744e12185d64350a016dd6f#diff-4e534af418bb39b0da64637d67499c40320d3d87274954bccb1e54548e9fe7c8): 数据结构和哈希算法统一挪到`vexo::ds`模块。且加入了跨树检查和每树独立的哨兵节点。--缺点 节点占了48字节， 颜色， 当前树的指针， intree标识， 内部的对象指针索引。 太费内存。
[06-10](https://github.com/akiba-miku/high-concurrency-runtime/commit/d276766ca18d0ec961a967e85f7adaf951b57bf6#diff-4e534af418bb39b0da64637d67499c40320d3d87274954bccb1e54548e9fe7c8): 面试后的几天，了解到CRTP的设计模式，改写成 base hook, 指针位压缩， 哨兵缓存， Tag支持挂多棵树， 支持多继承base hook。 --目前暂用， 考虑加入DEBUG下加入宏。

节点占了48字节， 颜色， 当前树的指针， intree标识。 太费内存。
节点内存压缩到了24字节， 树本身也只维护哨兵`header`和大小。

## 排除什么 / 为什么
- std::set/std::map：非侵入，每次插入一次额外堆分配，内存碎片; 节点和业务对象分离、缓存不友好；取消按 key 查找。
- 最小堆和时间轮方案， 最小堆要考虑反向索引否则需要遍历堆不划算， 时间轮方案支持更高效的插入删除但精度有限暂不考虑。

## 代价转移
为了省内存，跨树检查删了，靠使用者约定。
节点先从树上摘下来， 然后析构。 防 use-after-free.
CheckInvariants() 在 debug 下保证安全， release不一定保证。

后续有待观察， 具体优化方案看 [PR #21](https://github.com/akiba-miku/high-concurrency-runtime/pull/21#issuecomment-4662142818)
考虑单独维护一套时间轮更高效但精度支持有效。

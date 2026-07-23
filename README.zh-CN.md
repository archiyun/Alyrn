# CoroPact⚡

![C++](https://img.shields.io/badge/C++-23-blue)
![Platform](https://img.shields.io/badge/platform-Linux-lightgrey)
![License](https://img.shields.io/github/license/akiba-miku/high-concurrency-runtime)
![Stars](https://img.shields.io/github/stars/akiba-miku/high-concurrency-runtime?style=social)

***面向 Linux 的 C++23 异步网络运行时与 L7 网关，由协程、epoll 与 io_uring 驱动***

CoroPact 致力于在相互独立的 Reactor 与 io_uring 网络后端之上，提供统一、直观且高性能的协程编程模型：

* 🔀 **统一的异步 I/O 契约**
  epoll 与 io_uring 模块拥有各自独立的事件循环，但对外提供一致的协程 Stream 语义。业务代码依赖 `Task<T>`、`AsyncStream` 与 `AsyncListener`，而不需要接触 `epoll_event`、SQE 或 CQE 等后端细节。

* 🧩 **明确的所有权与完成语义**
  每个 Worker 独占自己的线程、事件循环、连接与 I/O 操作。操作在所属执行上下文中完成，协程 continuation 也在相同上下文中恢复，同时明确约束 buffer 生命周期、取消行为与异步关闭流程。

* 🚀 **网络运行时与 L7 网关**
  CoroPact 提供异步 accept、connect、read、write 与 close，并实现了增量式 HTTP/1.1 解析器、反向代理、上游连接池、负载均衡、健康检查、熔断与限流等能力。

事实上, 它不仅限于Linux, 你可以遵守协程/IO抽象的契约机制, 编写支持MacOS(kqueue), Windows(IOCP)等其它OS后端.

## 快速开始

```cpp
#include <array>
#include <cstddef>

#include "coropact/coro/task.h"
#include "coropact/io/stream_algorithms.h"

coropact::coro::Task<void> Echo(coropact::io::AsyncStream auto& stream) {
    std::array<std::byte, 4096> buffer{};

    for (;;) {
        auto read = co_await stream.ReadSome(buffer);
        if (!read.has_value() || *read == 0) {
            break;
        }

        auto written =
            co_await coropact::io::WriteAll(stream, buffer.first(*read));

        if (!written.has_value()) {
            break;
        }
    }

    co_await stream.Close();
}
```

构建 Reactor 后端：

```bash
cmake -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=ON \
  -DBUILD_EXAMPLES=ON

cmake --build build -j"$(nproc)"
ctest --test-dir build --output-on-failure
```

构建并启用 io_uring 后端：

```bash
# 请确保系统已经安装 liburing。

cmake -B build-uring \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=ON \
  -DBUILD_EXAMPLES=ON \
  -DCOROPACT_ENABLE_URING=ON

cmake --build build-uring -j"$(nproc)"
ctest --test-dir build-uring --output-on-failure
```

## 架构

```text
HTTP / Gateway / 自定义 Session
               |
               v
 Task<T> + Scheduler + AsyncStream
               |
       Submit -> Suspend
       Complete -> Resume
               |
        +------+------+
        |             |
        v             v
 Reactor / epoll   luring / io_uring
   coropact::net         coropact::luring
```

两个后端不共享事件循环，内部状态机也不需要完全一致。它们只需要遵守相同的业务可观察异步 I/O 契约。

io_uring Server 采用 thread-per-ring 模型：

```text
LUringServer
  |
  +-- Worker 0 -> Thread 0 -> LUringLoop 0 -> Ring 0
  +-- Worker 1 -> Thread 1 -> LUringLoop 1 -> Ring 1
  `-- Worker N -> Thread N -> LUringLoop N -> Ring N
```

连接、I/O 操作与协程 continuation 始终归属于创建它们的 Worker 和 Ring，不会在运行过程中跨 Ring 迁移。

## 性能测试

CoroPact 提供了可复现的 `wrk` 性能测试，用于比较：

* Reactor 与 io_uring 后端
* raw liburing
* standalone Asio
* Monoio
* Compio
* libaio poll 兼容路径
* Nginx 参考配置

性能测试结果与具体 workload 密切相关，不应被解释为网络框架的综合排名。测试报告会完整保留实验拓扑、原始轮次、延迟异常、CPU 使用率、内存占用与错误数量。

完整的七个网络库统一公平压测报告（包含图表、汇总数据和每轮关键数据）请参阅
[网络库统一公平压测](docs/benchmark/network-libraries.md)。其它测试脚本、原始结果和专项优化记录请参阅 [`docs/benchmark`](docs/benchmark/)。

## 文档

目前多数内容仍然在编写中, 且随版本更新内容会出现迟滞, 仅供参考.

* **[网络架构](docs/design/zh-CN/network/index.md)：** 运行时分层、后端边界与所有权模型。
* **[协程六元组状态机形式化建模](docs/design/zh-CN/theory/index.md):**
* **[AsyncStream 语义契约](docs/design/zh-CN/network/async-stream-contract.md)：** read、write、close、取消与 buffer 生命周期语义。
* **[网关架构](docs/design/zh-CN/gateway/index.md)：** HTTP 处理、反向代理与上游管理。
* **[数据结构](docs/design/zh-CN/datastructure/index.md):** C++现代风格的侵入式数据结构, 侵入式红黑树, 侵入式链表, MSPC队列的设计与实现, 以及它们在项目各处的应用.
* **[性能测试](docs/benchmark/network-libraries.md)：** Reactor、CoroPact luring、raw liburing、Asio、Monoio、Compio 与 libaio 的统一网络压测报告；其它测试方法、原始结果与性能优化记录见 [`docs/benchmark`](docs/benchmark/)。
* **[示例](examples/)：** Reactor、io_uring 与 Gateway 使用示例。
* **[测试](tests/)：** 协程、网络、生命周期与网关行为验证。

## 当前状态

CoroPact 目前仍是一个实验性网络运行时，尚不适合作为成熟网络框架的生产级替代方案。

当前正在推进的方向包括：

* 给出状态机的形式化证明, 并编写不变量测试和并发检验接入的后端.
* io_uring, 提供liburing库最新版本的网络选项配置和现代优化.
* 更贴近实际环境的数据压测和瓶颈分析.

## 参与项目

* 遇到问题、发现 Bug 或希望提出新功能，请创建 [Issue](https://github.com/akiba-miku/high-concurrency-runtime/issues)。
* 欢迎提交 [Pull Request](https://github.com/akiba-miku/high-concurrency-runtime/pulls)。
* 本项目使用 [MIT License](LICENSE)。

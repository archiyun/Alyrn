# Alyrn⚡

![C++](https://img.shields.io/badge/C++-23-blue)
![Platform](https://img.shields.io/badge/platform-Linux-lightgrey)
![License](https://img.shields.io/github/license/archiyun/Alyrn)
![Stars](https://img.shields.io/github/stars/archiyun/Alyrn?style=social)

***C++23 协程网络运行时：epoll 与 io_uring 作为平行后端。***

Alyrn 在相互独立的网络后端之上，提供统一、直观且高性能的 C++23 协程编程模型。它让默认路径像常规网络库一样不暴露底层事件机制，并通过 `Runtime` 提供类似 Tokio 的快速启动方式；需要时，应用仍可显式使用后端原生扩展与配置。

这些后端是**平行 adapter**，不是带预处理器分支的同一套实现：

| 后端 | 宿主 | 分发模型 | 多 worker 拓扑 |
|---|---|---|---|
| `epoll` | Linux | `epoll` readiness | 独立 listener + `SO_REUSEPORT` |
| `uring` | Linux | `io_uring` completion | thread-per-ring Proactor |

Alyrn 使用[生命周期精化协程 I/O（LRCI）](docs/design/zh-CN/network/lifecycle-refined-coroutine-io.md)：readiness 与 CQE 等后端事件不会直接等同于协程完成，而是被精化到一套共享逻辑生命周期，分别确定结果何时 ready、continuation 何时恢复、资源何时释放。

* 🔀 **统一的异步 I/O 契约**
  各后端保留各自的线程、事件循环与完成模型，但通过 `backend` 的 `AsyncStream`、`AsyncListener` 与 `AsyncConnector` concept 提供一致的业务可观察语义（应用侧使用 `io` 别名）。`coro` 以同步代码形式表达异步控制流，并隐藏协程帧、挂起、恢复与生命周期细节；业务代码无需接触 `epoll_event`、SQE 或 CQE。

* 🧩 **明确的所有权与完成语义**
  每个 Worker 独占自己的线程、事件循环、连接与 I/O 操作。操作在所属执行上下文中完成，协程 continuation 也在相同上下文中恢复，同时明确约束 buffer 生命周期、取消行为与异步关闭流程。协程帧不跨 loop 迁移。

* 🚀 **基础功能与高级扩展**
  Alyrn 提供异步 accept、connect、read、write、close 与 timer。Epoll 可选择 LT/ET；uring 还提供 multishot receive、zero-copy send 等扩展。HTTP 与网关策略已迁移至 [CoroGateway](https://github.com/archiyun/CoroGateway)。

Linux 是 CI 验证宿主。IOCP 尚未实现。

## 快速开始

### 1. 选择头文件

应用通常按需包含后端无关模块与一个具体 backend：

```cpp
#include "alyrn/coro.h"
#include "alyrn/io.h"
#include "alyrn/net.h"
#include "alyrn/epoll.h"  // 默认 Linux Epoll backend
```

请按实际使用的模块包含头文件。

| 后端 | 伞头文件 | Runtime tag | CMake 选项 |
|---|---|---|---|
| Epoll | `alyrn/epoll.h` | `runtime::Epoll` | Linux 默认 |
| uring / io_uring | `alyrn/uring.h` | `runtime::Uring` | `-DALYRN_ENABLE_URING=ON` |

### 2. 后端无关的连接处理协程

以 echo server 为例。该协程只依赖 `AsyncStream`，可同时服务 `epoll::Stream` 与 `uring::Stream`；Linux 可运行版本见 [`examples/simple_echo`](examples/simple_echo)。

```cpp
#include <array>
#include <cstddef>
#include <cstdio>
#include <expected>
#include <print>
#include <span>
#include <utility>

namespace cp = alyrn;

template <cp::io::AsyncStream Stream>
auto EchoSession(Stream stream) -> cp::Task<cp::Result<void>> {
  std::array<std::byte, 4096> buffer{};

  for (;;) {
    auto read = co_await stream.Read(buffer);
    if (!read.HasValue()) {
      co_return std::unexpected(read.Error());
    }
    if (*read == 0) {  // EOF
      co_return cp::Result<void>{};
    }

    auto payload = std::span<const std::byte>(buffer.data(), *read);
    auto written = co_await stream.Write(payload);
    if (!written.HasValue()) {
      co_return std::unexpected(written.Error());
    }
  }
}

template <cp::io::AsyncStream Stream>
auto HandleConnection(Stream stream) -> cp::DetachedTask {
  auto result = co_await EchoSession(std::move(stream));
  if (!result.HasValue()) {
    std::println(stderr, "session failed: {}", result.Error().message());
  }
  co_return;
}
```

### 3. 通过 Runtime 快速启动

```cpp
#include <stop_token>

int main() {
  constexpr int kPort = 19090;
  std::stop_source stop_source;

  auto runtime = cp::Runtime::Create<cp::runtime::Epoll>(
      cp::net::Endpoint::Loopback(kPort),
      [](auto stream) { return HandleConnection(std::move(stream)); });

  // signal handler、管理线程或测试代码随后调用 stop_source.request_stop()。
  auto result = runtime.Run(stop_source.get_token());
  return result.HasValue() ? 0 : 1;
}
```

要使用 io_uring，只需在启用 `ALYRN_ENABLE_URING=ON` 的构建中包含 `alyrn/uring.h`，并将 tag 改为 `cp::runtime::Uring`。handler 中的 `stream` 仍保持对应后端的静态类型，不会引入虚调用。

### 4. 需要时显式配置

`Create` 使用保守默认值（一个 worker）。需要控制 worker 数量时，使用同一个 Runtime 的 backend-specific Builder：

```cpp
auto runtime = cp::Runtime::Builder<cp::runtime::Epoll>{
                   cp::net::Endpoint::Loopback(19090)}
                   .AutoWorkers()
                   .OnConnection([](auto stream) {
                     return HandleConnection(std::move(stream));
                   })
                   .Build();
```

Backend tag 仍在编译期选择实现；ring 深度、provided buffer、zero-copy 等改变后端资源或生命周期语义的选项不伪装成通用 Runtime 配置。

`Workers(n)` 始终表示 *n 条线程*。其背后的拓扑由后端决定：Epoll 在 `n > 1` 时用 `SO_REUSEPORT` 共享监听端口；uring 保持每个 worker 一个 ring。

### 5. 使用 uring 原生能力

`Runtime` 只负责默认 TCP server 的 worker 生命周期，不是通用的 io_uring 配置接口。它可以选择安全的默认策略（例如带 fallback 的 multishot accept），但应用若要**显式**控制 ring 深度、SQPOLL、provided-buffer ring、multishot receive 或 zero-copy send，应直接组合 `uring::Loop`、`Options` 与对应的 listener、stream 或 source：

```cpp
alyrn::uring::Loop loop;
alyrn::uring::Options options;
options.entries = 8192;
options.shared_buffer_capacity = 256;  // RecvSource 的 provided buffers

auto initialized = loop.Init(options);
// 在 loop 所属线程上创建 listener/source，SpawnDetach(...) 后调用 loop.Run(...)
```

这条原生路径让应用明确承担每个 ring、buffer lease 与操作生命周期；参考 [`examples/uring`](examples/uring) 及 uring 的公开头文件。不要把这些能力增加为 `Runtime` 的跨后端开关。

## 运行容器示例

发布的容器运行一个基于 Alyrn Epoll 后端的 TCP echo server。通过 Docker
映射端口后可直接从宿主访问：

```bash
docker run --rm -p 9090:9090 ghcr.io/archiyun/alyrn:latest
```

在另一个终端验证：

```bash
printf 'hello\n' | nc 127.0.0.1 9090
```

在本地 checkout 中构建同一个镜像：`docker build -t alyrn:local .`。
该镜像是可运行的演示程序；实际应用仍应以自己的、链接 Alyrn 的可执行文件
作为最终镜像入口。

## 构建

### Makefile 快捷方式（Linux）

仓库内的 Makefile 使用 Ninja 配置构建，并将 `compile_commands.json`
指向当前构建目录，供 clangd 使用。

```bash
make build                  # 配置并构建默认的 Debug epoll
make test                   # 构建后运行对应测试
make run                    # 构建后启动 examples/simple_echo
make run EXAMPLE=epoll/demo_epoll_coro_echo
                            # 构建后启动指定的 epoll 示例
make release                # 配置并构建 Release epoll

# 需要 liburing >= 2.6。
make uring                  # 配置并构建 Debug io_uring 后端
make test-uring             # 构建后运行启用 io_uring 的测试集
make run-uring              # 构建后启动 examples/simple_echo_luring
make run-uring URING_EXAMPLE=uring/demo_luring_recv_echo
                            # 构建后启动指定的 io_uring 示例
make uring TYPE=Release     # Release io_uring 构建
```

需要额外传递 CMake cache 选项时，使用下方的原始 CMake 命令。

构建默认的 Linux Epoll 后端：

```bash
cmake -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=ON \
  -DBUILD_EXAMPLES=ON

cmake --build build -j"$(nproc)"
ctest --test-dir build --output-on-failure
```

若使用 GCC 或 Clang 进行严格诊断构建，可额外加入
`-DALYRN_STRICT_WARNINGS=ON`。

构建并启用 io_uring 后端：

```bash
# 请确保系统已经安装 liburing >= 2.6。

cmake -B build-uring \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=ON \
  -DBUILD_EXAMPLES=ON \
  -DALYRN_ENABLE_URING=ON

cmake --build build-uring -j"$(nproc)"
ctest --test-dir build-uring --output-on-failure
```

### CMake 选项

| 选项 | 默认 | 作用 |
|---|---|---|
| `ALYRN_ENABLE_URING` | `OFF` | Linux io_uring 后端（`liburing >= 2.6`） |
| `ALYRN_STRICT_WARNINGS` | `OFF` | GCC/Clang 下 `-Wall -Wextra -Wpedantic -Werror` |
| `ALYRN_SANITIZER` | 空 | 例如 `address,undefined` 或 `thread` |
| `BUILD_TESTS` | `ON` | 单元与 smoke 测试 |
| `BUILD_EXAMPLES` | `ON` | Linux 示例；没有原生 readiness 后端时关闭 |
| `BUILD_BENCHMARKS` | `OFF` | 独立微基准 |
| `BUILD_FUZZERS` | `OFF` | 需要 Clang/libFuzzer 的状态机 fuzz target；自带 ASan/UBSan |

### Fuzz 与微基准

用 Clang 构建 receive-source 生命周期 fuzzer。该 target 已启用 AddressSanitizer
与 UndefinedBehaviorSanitizer，因此不要同时设置 `ALYRN_SANITIZER`：

```bash
CC=clang CXX=clang++ cmake -S . -B build-fuzz -G Ninja \
  -DBUILD_TESTS=OFF -DBUILD_EXAMPLES=OFF -DBUILD_FUZZERS=ON
cmake --build build-fuzz --target recv_source_state_fuzzer -j"$(nproc)"
ASAN_OPTIONS=detect_leaks=0 build-fuzz/fuzz/recv_source_state_fuzzer -runs=100000
```

`detect_leaks=0` 只适合 ptrace/受限环境中 LeakSanitizer 无法正常工作时使用；普通
CI 中应保留 leak 检测。

Channel 的 owner-thread buffer 微基准：

```bash
cmake -S . -B build-bench -DCMAKE_BUILD_TYPE=Release -DBUILD_BENCHMARKS=ON
cmake --build build-bench --target coro_channel_microbenchmark -j"$(nproc)"
ITERATIONS=1000000 build-bench/benchmarks/coro_channel_microbenchmark
```

### 环境要求

* CMake 3.20+；支持 C++23 coroutine 的编译器。
* Epoll 使用 `epoll`，是 Linux 默认后端，不依赖额外网络库。
* uring 需要 Linux 与 `liburing >= 2.6`，并建议使用 Linux 5.19 或更新内核。
* `net` 与后端无关契约只使用可移植的 POSIX socket 设施。

可安装的 Debian/tarball 产物与 Docker 发布构建见[打包与安装](docs/packaging.md)。

## 架构

```text
自定义 Session / 应用
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
   Epoll/epoll      uring
   alyrn::epoll     ::uring
```

两个后端不共享事件循环，内部状态机也不需要完全一致。它们只需要遵守相同的业务可观察异步 I/O 契约。依赖边界以 [`docs/SUBSYSTEMS.md`](docs/SUBSYSTEMS.md) 为准。

Epoll 多 worker（`Workers(n>1)`）在同一端口上使用独立 listener：

```text
WorkerGroup
  |
  +-- Worker 0 -> Thread 0 -> Loop 0 -> listen :port (SO_REUSEPORT)
  +-- Worker 1 -> Thread 1 -> Loop 1 -> listen :port (SO_REUSEPORT)
  `-- Worker N -> Thread N -> Loop N -> listen :port (SO_REUSEPORT)
```

io_uring Server 采用 thread-per-ring 模型：

```text
Server
  |
  +-- Worker 0 -> Thread 0 -> Loop 0 -> Ring 0
  +-- Worker 1 -> Thread 1 -> Loop 1 -> Ring 1
  `-- Worker N -> Thread N -> Loop N -> Ring N
```

连接、I/O 操作与协程 continuation 始终归属于运行它们的 Worker 和 loop。`Stream` 不能跨 loop 移动。

## 性能测试

Alyrn 提供了可复现的 `wrk` 性能测试，用于比较：

* Epoll 与 io_uring 后端
* raw liburing
* standalone Asio
* Monoio
* Compio
* libaio poll 兼容路径
* libuv、libevent 与 libev 参考适配器
* Nginx 参考配置

性能测试结果与具体 workload 密切相关，不应被解释为网络框架的综合排名。测试报告会完整保留实验拓扑、原始轮次、延迟异常、CPU 使用率、内存占用与错误数量。

最新的当前源码 C++ 对照基线见[2026-08-10 网络库基线](docs/benchmark/network-libraries-20260810.md)；它记录了 `wrk` 的文件描述符前置条件和无效样本。完整的十个网络库统一公平压测报告（包含图表、汇总数据和每轮关键数据）请参阅[网络库统一公平压测](docs/benchmark/network-libraries.md)。其它测试脚本、原始结果和专项优化记录请参阅 [`docs/benchmark`](docs/benchmark/)。

## 文档

文档目录见 [`docs/index.md`](docs/index.md)。设计说明目前以中文编写，是契约与所有权的依据。`CONTEXT.md` 是仅供本地开发使用的领域词汇表，不随仓库发布。

* **[网络架构](docs/design/zh-CN/network/index.md)：** 运行时分层、后端边界与所有权模型。
* **[Runtime Builder](docs/design/zh-CN/network/runtime-builder.md)：** 编译期 backend tag 与启停生命周期。
* **[生命周期精化协程 I/O](docs/design/zh-CN/network/lifecycle-refined-coroutine-io.md)：** 逻辑 I/O 规范、三条授权边界，以及 epoll / io_uring 精化。
* **[AsyncStream 语义契约](docs/design/zh-CN/network/async-stream-contract.md)：** read、write、close、取消与 buffer 生命周期语义。
* **[数据结构](docs/design/zh-CN/datastructure/index.md)：** C++ 现代风格的侵入式数据结构，侵入式红黑树，侵入式链表，MPSC 队列的设计与实现，以及它们在项目各处的应用。四叉堆是通过 `time::TimerIndex` 注入的一等 timer-index 适配器。
* **[性能测试](docs/benchmark/network-libraries-20260810.md)：** 最新的当前源码 C++ 对照基线；完整统一网络库报告、测试方法、原始结果与优化记录见 [`docs/benchmark`](docs/benchmark/)。
* **[示例](examples/)：** Linux 上的 Epoll 与 io_uring 使用示例。
* **[测试](tests/)：** 协程、网络、生命周期与后端行为验证。

## 当前状态

Alyrn 目前仍是一个实验性网络运行时，尚不适合作为成熟网络框架的生产级替代方案。

当前正在推进的方向包括：

* 给出状态机的形式化证明，并编写不变量测试和并发检验接入的后端。
* io_uring：提供 liburing 较新版本的网络选项配置和现代优化。
* 更贴近实际环境的数据压测和瓶颈分析。

## 参与项目

* 遇到问题、发现 Bug 或希望提出新功能，请创建 [Issue](https://github.com/archiyun/Alyrn/issues)。
* 欢迎提交 [Pull Request](https://github.com/archiyun/Alyrn/pulls)。
* 本项目使用 [MIT License](LICENSE)。

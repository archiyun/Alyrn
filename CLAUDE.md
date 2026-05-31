# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> 项目特有约束、命名规范、模块→测试映射等详细规则请参见 [`.claude/rules/`](.claude/rules/)：
> - `runtime-project.md` — 架构硬约束与禁止事项
> - `coding-style.md` — Google C++ Style 项目化版本
> - `code-review.md` — 模块到 smoke test 的映射表
> - `hooks.md` — 编辑/加锁/重构/提交前的检查清单

## Build

`build-tests` 是当前活跃的构建目录（CTest + sanitizer 友好），`build-perf` 用于 release 性能验证（`compile_commands.json` 默认软链到它）。

```bash
# 日常开发（含测试 + examples）
cmake -B build-tests
cmake --build build-tests -j$(nproc)

# Release / 性能验证
cmake -B build-perf -DCMAKE_BUILD_TYPE=Release
cmake --build build-perf -j$(nproc)
```

### 可选特性开关

| 选项 | 默认 | 依赖 |
|---|---|---|
| `BUILD_TESTS` | ON | - |
| `BUILD_EXAMPLES` | ON | - |
| `RUNTIME_ENABLE_SSL` | OFF | OpenSSL |
| `RUNTIME_ENABLE_HTTP2` | OFF | OpenSSL + libnghttp2 |
| `RUNTIME_BUFFER_IMPL` | `muduo` | 可选 `ringbuf` / `nginx` |

## 测试

测试二进制都在 `build-tests/tests/`（CMake 子目录布局），examples 在 `build-tests/examples/`。

```bash
# 全量
cd build-tests && ctest --output-on-failure

# Smoke tests（不依赖 GTest，始终编译）
./build-tests/tests/buffer_smoke_test
./build-tests/tests/http_smoke_test
./build-tests/tests/pool_smoke_test
./build-tests/tests/pmr_pool_resource_smoke_test
./build-tests/tests/load_balancer_smoke_test
./build-tests/tests/proxy_e2e_smoke_test

# GTest（需要系统 libgtest-dev）
./build-tests/tests/runtime_unit_tests
./build-tests/tests/runtime_integration_tests
./build-tests/tests/runtime_integration_tests --gtest_filter="HttpRouterTest.*"
```

**GTest 是可选依赖**：`tests/CMakeLists.txt` 用 `find_package(GTest CONFIG QUIET)` 检测，找不到时跳过 `runtime_unit_tests` 和 `runtime_integration_tests`，但 smoke tests 始终构建。

完整的"模块 → 测试"映射见 [`.claude/rules/code-review.md`](.claude/rules/code-review.md)。

## 架构

五个静态库，上层依赖下层，**不可反向依赖**：

```
runtime_gateway   (src/gateway/, include/runtime/gateway/)
    └── runtime_http   (src/http/, include/runtime/http/)
            └── runtime_net    (src/net/, include/runtime/net/)
                    └── runtime_task   (src/task/, include/runtime/task/)
                            └── runtime_foundation
                                    (src/base/, src/log/, src/time/, src/memory/)
```

`runtime_task` 与 `runtime_net` 同级、互不依赖；`runtime_http` 同时依赖二者。

### Foundation 层

- `runtime::base` — `NonCopyable`、`current_thread` 等基础设施
- `runtime::log` — `AsyncLogger` 后端 + `LOG_INFO/WARN/ERROR/DEBUG/FATAL` 流式宏
- `runtime::time` — `Timestamp` 时间戳
- `runtime::memory` — `Pool`（nginx 风格 arena）、`MemoryPool`、`ObjectPool`、`SegmentLruCache`、`pmr_pool_resource`（`std::pmr::memory_resource` 适配器）

### Task 层

`runtime::task` 提供线程池与协作式取消：`Scheduler`（带 metrics）、`ThreadPool`、`WorkQueue`（优先级队列）、`TimerScheduler`、`Task`/`TaskHandle`/`TaskHistory`、`CancellationToken`、`TaskGroup`。

### Net 层 — One-Loop-Per-Thread

- `EventLoop` 持有 `EpollPoller` + `Channel` 列表 + `TimerQueue`，是单线程驱动的核心
- `TcpServer` 跑在 Main Loop 只负责 accept；`EventLoopThreadPool` 管理 Sub Loop 线程池，新连接 round-robin 分配
- **IO 线程间无共享数据**：每条 `TcpConnection` 的状态通过 `conn->set_context(std::any)` 绑定，由所属 Sub Loop 独享
- `Buffer` 实现可在编译期切换（`muduo` / `ringbuf` / `nginx`，见 `RUNTIME_BUFFER_IMPL`）
- 客户端侧：`Connector` + `TcpClient`
- 定时器双实现：`TimerQueue`（基于 timerfd）+ `timer_rbtree`（自研红黑树，配 `rbtree_validator`）

### HTTP 层

**解析**：`HttpContext` 是有状态的增量状态机，直接消费 `Buffer&` 字节（零中间拷贝）。keep-alive 场景下 `ctx.Reset()` 复用同一上下文。

**请求 arena**：`HttpRequest` 内嵌一个 `Pool::Ptr`（nginx 风格 arena），请求解析期间所有临时分配走 pool，请求结束随 request 析构整块释放。pool 同时暴露为 `std::pmr::memory_resource`，可作为 STL pmr 容器的上游。

**路由**：`Router` 用前缀树（Trie），支持 `:param` 动态段：
- `Add()` 按 `/` 分段，`:` 前缀走 `param_child`，其余走 `static_child`
- `MatchNode()` DFS，**静态分支优先于参数分支**（`/users/me` 不会被 `/users/:id` 错误命中）；参数分支匹配失败时回溯，擦除已写入 `result.params` 的值
- `Match()` 返回 `RouteMatch`：`.handler != null` → 命中；`.path_matched == true` 且 handler 为空 → 405；两者均为默认值 → 404
- `HttpServer::OnMessage()` 调用 handler 前 `request.set_path_params(std::move(match.params))`，handler 内通过 `req.path_param("key")` 取值

**HTTP/2**：`RUNTIME_ENABLE_HTTP2` 打开时编译 `src/http/http2_session.cc`（基于 libnghttp2）。

**内置端点**：`metrics_handler`（Prometheus 风格指标）、`debug_handler`。

**Handler 签名是同步的**：
```cpp
using Handler = std::function<void(const HttpRequest&, HttpResponse&)>;
```

### Gateway 层

`runtime::gateway` 在 HTTP 层之上构建反向代理：
- `GatewayServer` — 主入口
- `ProxyPass` — 上游转发
- `UpstreamRegistry` / `Upstream` / `UpstreamPeer` / `UpstreamConnPool` — 上游池
- `LoadBalancer` — 负载均衡策略（含一致性哈希）
- `HealthChecker` — 主动健康检查
- `CircuitBreaker` / `RateLimiter` / `FallbackConfig` — 韧性能力
- `gateway_metrics` — 网关侧 Prometheus 指标

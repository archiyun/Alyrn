# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build

```bash
# 初次配置（build-tests 是当前活跃的构建目录）
cmake -B build-tests
cmake --build build-tests -j$(nproc)

# Release 构建
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

## 测试

```bash
# 运行所有 CTest（包含不依赖 GTest 的 smoke tests）
cd build-tests && ctest --output-on-failure

# 直接运行某个 smoke test（不需要 GTest）
./build-tests/http_smoke_test
./build-tests/buffer_smoke_test

# GTest 测试套件（需要系统安装 libgtest-dev）
./build-tests/runtime_unit_tests          # 单元测试
./build-tests/runtime_integration_tests   # 集成测试（含 HTTP 路由测试）

# 运行单个 GTest 用例
./build-tests/runtime_integration_tests --gtest_filter="HttpRouterTest.*"
```

**注意**：GTest 是可选依赖，`tests/CMakeLists.txt` 用 `find_package(GTest CONFIG QUIET)` 检测，找不到时跳过 `runtime_unit_tests` 和 `runtime_integration_tests`，但 smoke tests 始终构建。

## 架构

三层静态库，上层依赖下层，不可反向依赖：

```
runtime_http   (src/http/, include/runtime/http/)
    └── runtime_net    (src/net/, include/runtime/net/)
            └── runtime_foundation  (src/log/, src/base/, src/time/)
```

### Net 层核心：One-Loop-Per-Thread

- `EventLoop` 持有 `EpollPoller` + `Channel` 列表 + `TimerQueue`，是单线程驱动的核心。
- `TcpServer` 运行在 Main Loop，只负责 accept；`EventLoopThreadPool` 管理 Sub Loop 线程池，新连接 round-robin 分配给 Sub Loop。
- **IO 线程间无共享数据**：每条 `TcpConnection` 的状态（包括 HTTP 解析上下文）通过 `conn->SetContext(std::any)` 绑定在连接对象上，Sub Loop 线程独享。

### HTTP 层核心

**解析**：`HttpContext` 是一个有状态的增量状态机解析器，直接消费 `Buffer&` 的字节（零中间拷贝）。keep-alive 场景下调用 `ctx.Reset()` 复用同一上下文。

**路由**：`Router` 使用前缀树（Trie）实现，支持动态路径参数（`:param` 语法）。

- `Add()` 按 `/` 分段插入 Trie，`:` 开头的段走 `param_child`，其余走 `static_child`。
- `MatchNode()` DFS 匹配，**静态分支优先于参数分支**（`/users/me` 不会被 `/users/:id` 错误命中）；参数分支匹配失败时回溯，擦除已写入 `result.params` 的值。
- `Match()` 返回 `RouteMatch`：`.handler != null` → 命中；`.path_matched == true` 且 handler 为空 → 405；两者均为默认值 → 404。

**参数传递**：`HttpServer::OnMessage()` 在调用 handler 前执行 `request.SetPathParams(std::move(match.params))`，handler 内通过 `req.PathParam("key")` 取值。

**Handler 签名是同步的**，不支持异步操作：
```cpp
using Handler = std::function<void(const HttpRequest&, HttpResponse&)>;
```

### 待更新

`tests/integration/test_http_server.cpp` 中的 `HttpRouterTest::Distinguishes404And405` 仍使用旧的 `Match(method, path, bool&)` 签名（返回 `optional<Handler>`），需要改为新的 `Match(method, path)` → `RouteMatch` API。

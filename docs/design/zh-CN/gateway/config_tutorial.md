# 配置化网关教程

这篇教程说明如何用 YAML 文件启动 `vexo::gateway`，把上游、路由、健康检查、限流、熔断和 fallback 从 C++ 代码里移到配置文件里。

当前配置加载是启动期能力：进程启动时读取并校验配置，校验失败直接退出；运行中热更新还没有实现。

## 一、准备依赖

配置解析使用 `yaml-cpp`。如果系统没有这个库，`vexo_gateway_config` 目标不会构建，但核心 `vexo_gateway` 仍然可用。

Ubuntu / Debian：

```bash
sudo apt update
sudo apt install -y libyaml-cpp-dev
```

Arch Linux：

```bash
sudo pacman -S yaml-cpp
```

## 二、构建配置版网关

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_EXAMPLES=ON
cmake --build build --target demo_gateway_config -j"$(nproc)"
```

`VEXO_ENABLE_GATEWAY_YAML_CONFIG` 默认是 `ON`。如果你只想构建核心库，不需要 YAML 配置能力，可以关闭：

```bash
cmake -B build -DVEXO_ENABLE_GATEWAY_YAML_CONFIG=OFF
```

## 三、先校验配置

示例配置在 `examples/gateway/gateway.yaml`。启动前先跑一次校验：

```bash
./build/examples/gateway/demo_gateway_config --check examples/gateway/gateway.yaml
```

成功时输出：

```text
config ok: examples/gateway/gateway.yaml
```

这个模式只会解析配置、校验字段、构建 `UpstreamRegistry`，不会监听端口，适合 CI 和部署脚本使用。

## 四、启动完整示例

先启动两个监听 9001 / 9002 的 HTTP 服务作为上游：

```bash
# 使用部署环境中的 HTTP 服务，分别监听 9001 和 9002
```

启动配置版网关：

```bash
./build/examples/gateway/demo_gateway_config examples/gateway/gateway.yaml
```

验证：

```bash
curl -i http://127.0.0.1:8080/healthz
curl -i http://127.0.0.1:8080/api/health
curl -i http://127.0.0.1:8080/api/kv
```

其中 `/healthz` 是网关直接响应，`/api/...` 会按配置转发到 `user_service` 上游。

## 五、最小配置

只代理一个 upstream 时，可以从这份配置开始：

```yaml
server:
  name: gateway
  listen: 0.0.0.0:8080
  threads: 4

status_endpoint:
  enabled: true
  path: /healthz

upstreams:
  - name: user_service
    peers:
      - host: 127.0.0.1
        port: 9001
      - host: 127.0.0.1
        port: 9002

routes:
  - path: /api
    match: prefix
    upstream: user_service
    load_balance: round_robin
```

`routes[].path` 当前只支持前缀代理匹配。`/api` 会匹配 `/api`、`/api/health`，不会匹配 `/apifoo`。

## 六、完整示例

```yaml
server:
  name: gateway
  listen: 0.0.0.0:8080
  threads: 4

status_endpoint:
  enabled: true
  path: /healthz

health_check:
  enabled: true
  path: /api/health
  interval_sec: 10
  timeout_sec: 3
  unhealthy_threshold: 3
  healthy_threshold: 2

rate_limit:
  global:
    enabled: true
    rate: 1000
    burst: 2000
  per_ip:
    enabled: true
    rate: 100
    burst: 200
    max_buckets: 65536

upstreams:
  - name: user_service
    max_concurrent_requests: 1024
    request_timeout_ms: 5000
    circuit_breaker:
      enabled: true
      failure_threshold: 5
      success_threshold: 2
      open_timeout_ms: 10000
      half_open_max_requests: 1
    peers:
      - name: user-1
        host: 127.0.0.1
        port: 9001
        weight: 1
        max_fails: 3
        fail_timeout_ms: 10000
      - name: user-2
        host: 127.0.0.1
        port: 9002
        weight: 1
        max_fails: 3
        fail_timeout_ms: 10000

routes:
  - path: /api
    match: prefix
    upstream: user_service
    load_balance: round_robin
    circuit_breaker: true
    fallback:
      enabled: true
      status: 503
      content_type: application/json; charset=utf-8
      body: '{"error":"user service unavailable"}'
```

## 七、字段说明

### `server`

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `name` | `gateway` | 网关实例名 |
| `listen` | `127.0.0.1:8080` | 监听地址，当前只支持数字 IPv4 |
| `host` | `127.0.0.1` | 可替代 `listen` 的拆分写法 |
| `port` | `8080` | 可替代 `listen` 的拆分写法 |
| `threads` | `0` | I/O 子线程数，`0` 表示不显式设置 |

### `upstreams`

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `name` | 必填 | 上游组名称，路由通过它引用 upstream |
| `max_concurrent_requests` | `1024` | 上游组并发上限，`0` 表示不限制 |
| `request_timeout_ms` | `5000` | 单个上游请求超时 |
| `circuit_breaker` | 关闭 | 上游组熔断器配置 |
| `peers` | 必填 | 后端节点列表 |

### `peers`

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `name` | `host:port` | 节点名，在同一个 upstream 内必须唯一 |
| `host` | 必填 | 后端 IPv4 地址 |
| `port` | 必填 | 后端端口 |
| `weight` | `1` | 权重，供加权算法使用 |
| `max_fails` | `3` | 被动失败次数阈值 |
| `fail_timeout_ms` | `10000` | 被动失败冷却时间 |

### `routes`

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `path` | 必填 | 代理路由前缀，必须以 `/` 开头 |
| `match` | `prefix` | 当前只支持 `prefix` |
| `upstream` | 必填 | 目标 upstream 名称 |
| `load_balance` | `p2c` | 负载均衡算法 |
| `circuit_breaker` | `false` | 是否让这条路由启用 upstream 熔断器 |
| `fallback` | 关闭 | 上游不可用时返回的静态响应 |

支持的 `load_balance`：

- `round_robin`
- `smooth_weighted_round_robin`
- `least_connection`
- `weighted_least_connection`
- `random`
- `weighted_random`
- `ip_hash`
- `consistent_hash`
- `maglev_hash`
- `p2c`

### `health_check`

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `enabled` | `false` | 是否启用主动健康检查 |
| `path` | `/health` | 探针请求路径 |
| `interval_sec` | `10` | 探测间隔 |
| `timeout_sec` | `3` | 单次探测超时 |
| `unhealthy_threshold` | `3` | 连续失败多少次后标记 down |
| `healthy_threshold` | `2` | 连续成功多少次后恢复 up |

主动健康检查会探测所有 upstream 的所有 peer，所以 `path` 应该是所有后端都能处理的健康检查路径。

### `rate_limit`

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `global.enabled` | `false` | 是否启用全局限流 |
| `global.rate` | `1000` | 全局 token/s |
| `global.burst` | `2000` | 全局桶容量 |
| `per_ip.enabled` | `false` | 是否启用单 IP 限流 |
| `per_ip.rate` | `10` | 每个 IP 的 token/s |
| `per_ip.burst` | `20` | 每个 IP 的桶容量 |
| `per_ip.max_buckets` | `65536` | 最多保留多少个 IP 桶，`0` 表示不限制 |

## 八、加一组新服务

假设新增订单服务，监听 `9101` 和 `9102`：

```yaml
upstreams:
  - name: order_service
    peers:
      - host: 127.0.0.1
        port: 9101
      - host: 127.0.0.1
        port: 9102

routes:
  - path: /orders
    match: prefix
    upstream: order_service
    load_balance: least_connection
```

如果已有其他 upstream/route，把这两段追加到原来的列表里即可。追加后先运行：

```bash
./build/examples/gateway/demo_gateway_config --check path/to/gateway.yaml
```

校验通过再重启网关进程。

## 九、常见错误

### `yaml-cpp was not found`

CMake 找不到 `yaml-cpp`，只会跳过 `vexo_gateway_config`。安装依赖后重新配置：

```bash
cmake -B build -DBUILD_EXAMPLES=ON
```

### `unknown upstream`

`routes[].upstream` 引用了不存在的 `upstreams[].name`。检查名字是否完全一致。

### `unknown load balancer`

`routes[].load_balance` 不在支持列表里。先用 `round_robin` 或 `p2c` 验证配置。

### `expected a numeric IPv4 address`

当前监听地址和 peer 地址只接受数字 IPv4，例如 `127.0.0.1`、`0.0.0.0`。暂不做域名解析和 IPv6。

### `only 'prefix' proxy routes are supported`

配置层当前只暴露前缀代理路由。要支持 exact proxy、path rewrite、header rewrite，需要先扩展 `GatewayServer` 的公开 API，再扩展配置 schema。

## 十、在自己的程序中使用配置加载器

链接 `vexo_gateway_config`：

```cmake
target_link_libraries(my_gateway PRIVATE vexo_gateway_config)
```

启动代码：

```cpp
#include "vexo/gateway/gateway_config.h"
#include "vexo/gateway/gateway_server.h"
#include "vexo/gateway/upstream_registry.h"
#include "vexo/net/event_loop.h"

int main() {
  auto config = vexo::gateway::LoadGatewayConfigFromYaml("gateway.yaml");

  vexo::gateway::UpstreamRegistry registry;
  vexo::gateway::BuildGatewayUpstreamRegistry(config, registry);

  vexo::net::EventLoop loop;
  vexo::gateway::GatewayServer gateway(
      &loop,
      vexo::gateway::MakeGatewayListenAddress(config),
      config.server.name,
      registry);

  vexo::gateway::ApplyGatewayConfig(config, gateway);
  gateway.Start();
  loop.Loop();
}
```

这个流程有三个阶段：加载配置、构建 upstream 注册表、把配置应用到网关。后续做热更新时也应该保持这个边界：先完整构建新配置，成功后再替换运行时状态。

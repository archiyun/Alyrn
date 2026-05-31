## 降级 Fallback - 预渲染响应

网关可靠性的三件套保证之一， 预渲染响应。
采用一种运行前静态配置， 提前渲染好。 运行时错误的兜底机制。 这是降级返回结果。


## 阅读 `fallback_config.h`

这是一个 `header-only` 文件， 无任何前置依赖。
虽然代码很简单， 但还是容我介绍一下我编写的思路。 这样避免歧义。

### `FallbakcConfig`

FallbackConfig, 可以手动配置状态码， Content-Type, Body.
可以采用默认， 默认返回 503 `service temporarily unavailable`.

```cpp
// 配置：为 /api/users 路由设置自定义降级
gw.AddProxyRoute("/api/users", "user_service",
    FallbackConfig{
        .enabled = true, // 只需要显示指定为 true, 赋予路由降级功能。
        .status_code = runtime::http::StatusCode::ServiceUnavailable,
        .body = R"({"code":-1,"msg":"用户服务暂时不可用，请稍后重试"})",
    });

```

`pre_rendered` 是预渲染的字符串。 它是由`Init()`函数将预渲染的结果放置的内存位置。
服务期间，如果启用了降级。 它会默认返回pre_rendered. 
由于配置阶段(单线程)环境， 服务阶段不再修改只读访问。 "零"拷贝，零内存分配，天然无锁竞争。

`Init`调用时机？ 
你不需要显示调用它， 如前面所说。 你只需要指定降级配置 `.enabled = true`, 这是最小步骤。
在`gateway_server.h`中重载了`AddProxyRoute`, 这是为每个代理路由配置的接口。
```cpp
// NEW: 带降级和熔断配置
  void AddProxyRoute(std::string_view path,
                     std::string_view upstream_name,
                     FallbackConfig fallback, // 降级
                     bool circuit_breaker_enabled = false,
                     std::string_view algo = "round_robin");
```

### 集成配置
`gateway_server.h`中的`Route`结构体新增一个字段。
```cpp
struct Route {
  // ... 前面的成员字段

  FallbackConfig fallback;      // NEW: 降级配置
};
```

`gateway_server.cc`中 新增一个 `RenderFallback` 用于封装 "优先用自定义降级, 否则回退到通用错误" 的逻辑.


```cpp
std::string GatewayServer::RenderFallback(const Route& route,
                                          std::string_view reason) const {
  if (route.fallback.enabled) {
    return route.fallback.pre_rendered;
  }
  return MakeError(runtime::http::StatusCode::ServiceUnavailable, reason).ToString();
}
```

如果采用 MakeError 再转字符串会额外产生拷贝， 采用自定义降级配置可以优化性能。

`gateway_server.cc` 中的 `OnMessgae` 代理处理块
如果上游集群不存在，或者负载均衡没选出有效的上游节点， 那么统一走降级处理。
```cpp
// gateway_server.cc OnMessage ProxtPass 分支
if (route->type == RouteType::Proxy) {
  auto upstream = registry_.Find(route->upstream_name);
  if (!upstream) {
    // ① upstream 不存在 → 走降级
    conn->Send(RenderFallback(*route, "upstream not found"));
    continue;
  }

  // ...

  ctx.upstream_req = ProxyPass::Forward(conn, req, *upstream, *route->lb, ...);
  if (!ctx.upstream_req) {
    // ② 没有可用 peer → 走降级
    conn->Send(RenderFallback(*route, "no available upstream peer"));
  }
}

```

## 声明

本篇代表当时编写的代码版本思路阐述， 不代表最终代码实现，也不应直接用于工程实现， 仅供参考。
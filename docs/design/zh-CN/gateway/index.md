# 网关

`vexo::gateway` 是运行时的反向代理层，负责把 HTTP 请求转发到上游服务，并提供负载均衡、健康检查、限流、熔断和 fallback。

## 教程

- [配置化网关教程](config_tutorial.md)：用 YAML 文件声明 upstream、route、health check、rate limit 和 fallback。
- [负载均衡设计](load_balance.md)：负载均衡模型和算法背景。

## 当前边界

- 配置加载是启动期能力，暂不支持热更新。
- 配置文件里的代理路由当前只支持 `prefix` 匹配。
- 监听地址和 upstream peer 地址当前只支持数字 IPv4。

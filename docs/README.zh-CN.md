# HTTP Demo 观测与测试

仓库里的 `demo_http_server` 
- `Keep-Alive`
- 简单 JSON API
- 精确路径路由分发
- 常见错误码返回

## 一键测试

直接运行：

```bash
bash scripts/test_http_demo.sh
```

可选环境变量：

```bash
PORT=19081 IO_THREADS=2 bash scripts/test_http_demo.sh
```

脚本会自动完成这些检查：

- 构建并启动 `demo_http_server`
- 请求 `/api/health`
- 请求 `/api/echo`
- 请求一个不存在的路由验证 `404`
- 对已存在路径使用错误方法验证 `405`
- 用 `curl -v` 验证连接复用
- 打印最近日志
- 打印 `ss` / `lsof` 观测结果

## 手工命令

启动 demo：

```bash
cmake -S . -B build-tests
cmake --build build-tests --target demo_http_server
HOST=127.0.0.1 PORT=18081 IO_THREADS=2 \
  ./build-tests/examples/demo_http_server
```

健康检查：

```bash
curl -i --http1.1 http://127.0.0.1:18081/api/health
```

JSON API：

```bash
curl -i --http1.1 \
  -H 'Content-Type: application/json' \
  -d '{"hello":"runtime"}' \
  'http://127.0.0.1:18081/api/echo?src=manual'
```

错误码：

```bash
curl -i --http1.1 http://127.0.0.1:18081/missing
curl -i --http1.1 -X PUT http://127.0.0.1:18081/api/health
```

Keep-Alive 复用：

```bash
curl -sv --http1.1 \
  -H 'Connection: keep-alive' \
  http://127.0.0.1:18081/api/health \
  http://127.0.0.1:18081/api/health \
  -o /dev/null 2>&1 | grep -E 'Re-using existing connection|Connected to'
```

## 观测命令

日志：

```bash
tail -f build-tests/demo_http_server.log
```

连接状态：

```bash
ss -tanp | grep :18081
```

进程占用：

```bash
lsof -n -P -iTCP:18081
```

当前版本的 `demo_http_server` 只覆盖最小 HTTP 能力，不包含静态文件服务、空闲连接超时和 Trace ID 回传。如果后续把这些能力重新加回 `HttpServer`，再扩展脚本和文档会更稳妥。

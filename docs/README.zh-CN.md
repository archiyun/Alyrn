# HTTP Demo 观测与测试

仓库里的 `demo_http_server` 
- `Keep-Alive`
- 简单 JSON API
- 精确路径路由分发
- 常见错误码返回

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

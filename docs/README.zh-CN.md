# HTTP Demo 观测与测试

仓库里的 `simple_http_server` 
- `GET` 静态文件
- `Keep-Alive`
- 简单 JSON API
- 路由分发
- 空闲连接超时关闭
- 常见错误码返回
- `X-Trace-Id` 回传

## 一键测试

直接运行：

```bash
bash scripts/test_http_demo.sh
```

可选环境变量：

```bash
PORT=19081 IO_THREADS=2 STATIC_ROOT="$PWD/examples/www" bash scripts/test_http_demo.sh
```

脚本会自动完成这些检查：

- 构建并启动 `simple_http_server`
- 请求 `/api/health`
- 请求 `/api/echo`
- 请求 `/static/index.html`
- 请求一个不存在的路由验证 `404`
- 用 `curl -v` 验证连接复用
- 用半包请求验证空闲连接超时关闭
- 打印最近日志
- 打印 `ss` / `lsof` 观测结果

## 手工命令

启动 demo：

```bash
cmake -S . -B build-tests
cmake --build build-tests --target simple_http_server
HOST=127.0.0.1 PORT=18081 IO_THREADS=2 STATIC_ROOT="$PWD/examples/www" \
  ./build-tests/examples/simple_http_server
```

健康检查：

```bash
curl -i --http1.1 http://127.0.0.1:18081/api/health
```

检查 Trace ID 回传：

```bash
curl -i --http1.1 \
  -H 'X-Trace-Id: trace-demo-001' \
  http://127.0.0.1:18081/api/health
```

JSON API：

```bash
curl -i --http1.1 \
  -H 'Content-Type: application/json' \
  -d '{"hello":"runtime"}' \
  'http://127.0.0.1:18081/api/echo?src=manual'
```

静态文件：

```bash
curl -i --http1.1 http://127.0.0.1:18081/static/index.html
```

错误码：

```bash
curl -i --http1.1 http://127.0.0.1:18081/missing
curl -i --http1.1 -X PUT http://127.0.0.1:18081/api/health
curl -i --http1.1 -X POST http://127.0.0.1:18081/static/index.html
```

Keep-Alive 复用：

```bash
curl -sv --http1.1 \
  -H 'Connection: keep-alive' \
  http://127.0.0.1:18081/api/health \
  http://127.0.0.1:18081/api/health \
  -o /dev/null 2>&1 | grep -E 'Re-using existing connection|Connected to'
```

空闲超时关闭：

```bash
exec 3<>/dev/tcp/127.0.0.1/18081
printf 'GET /api/health HTTP/1.1\r\nHost: demo\r\n' >&3
sleep 17
cat <&3
exec 3<&-
exec 3>&-
```

## 观测命令

日志：

```bash
tail -f build-tests/simple_http_server.log
```

连接状态：

```bash
ss -tanp | grep :18081
```

进程占用：

```bash
lsof -n -P -iTCP:18081
```

如果当前环境不能绑定本地端口，demo 进程会在启动阶段失败，这时候需要换到允许 loopback 监听的本机或容器里运行这些命令。

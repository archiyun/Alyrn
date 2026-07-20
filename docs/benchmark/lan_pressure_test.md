# 局域网压测运行手册

本文用于让局域网内的压测机 Agent 压测本机上的 Vexo 上游和 io_uring 网关。

拓扑如下：

```text
压测机
   │ HTTP 压测
   ▼
本机 LAN_IP:8081  Vexo io_uring Gateway
   │ 仅本机回环连接
   ▼
127.0.0.1:9001  Vexo HTTP upstream
```

上游只绑定 `127.0.0.1`，不会暴露给局域网；网关绑定 `0.0.0.0:8081`，压测机访问本机的 LAN 地址即可。

## 本机准备

在仓库根目录执行：

```bash
cmake -S . -B build-uring \
  -DVEXO_ENABLE_URING=ON \
  -DBUILD_TESTS=ON \
  -DBUILD_EXAMPLES=ON

cmake --build build-uring -j"$(nproc)" \
  --target demo_http_server demo_bench_gateway_luring
```

确认本机 LAN 地址：

```bash
hostname -I
```

选择压测机能够访问的 IPv4 地址，例如 `192.168.1.20`，记为：

```text
LAN_IP=192.168.1.20
```

如果主机有防火墙，需要允许 TCP 8081：

```bash
sudo ufw allow 8081/tcp
```

## 启动本机上游

启动一个本机 HTTP 上游：

```bash
UPSTREAM_PIDFILE=/tmp/vexo-upstream-9001.pid

env PORT=9001 IO_THREADS=4 \
  ./build-uring/examples/http/demo_http_server \
  > /tmp/vexo-upstream-9001.log 2>&1 &
echo $! > "$UPSTREAM_PIDFILE"
```

验证上游：

```bash
curl --noproxy '*' -fsS http://127.0.0.1:9001/
curl --noproxy '*' -fsS http://127.0.0.1:9001/api/health
```

预期第一个请求返回：

```text
OK
```

## 启动本机网关

先使用单 worker 做连通性验证：

```bash
GATEWAY_PIDFILE=/tmp/vexo-gateway-8081.pid

env BIND_HOST=0.0.0.0 \
  PORT=8081 \
  UPSTREAM_PORTS=9001 \
  LB_ALGO=round_robin \
  FRAME_POOL=1 \
  URING_WORKERS=1 \
  URING_ENTRIES=8192 \
  MAX_CONCURRENT_REQUESTS=20000 \
  ./build-uring/examples/gateway/demo_bench_gateway_luring \
  > /tmp/vexo-gateway-8081.log 2>&1 &
echo $! > "$GATEWAY_PIDFILE"
```

确认监听：

```bash
ss -lntp | grep -E ':8081|:9001'
```

确认本机完整转发链路：

```bash
curl --noproxy '*' -i --max-time 5 http://127.0.0.1:8081/
```

预期返回 `HTTP/1.1 200 OK`，响应体为 `OK`。

连通性验证通过后，再按机器 CPU 和 ring 压力逐步增加 worker：

```bash
env BIND_HOST=0.0.0.0 \
  PORT=8081 \
  UPSTREAM_PORTS=9001 \
  FRAME_POOL=1 \
  URING_WORKERS=4 \
  URING_ENTRIES=32768 \
  MAX_CONCURRENT_REQUESTS=20000 \
  ./build-uring/examples/gateway/demo_bench_gateway_luring \
  > /tmp/vexo-gateway-8081.log 2>&1 &
```

不要同时启动两个占用 8081 的网关实例。若需要重启，先按 PID 清理旧实例。

## 交给压测机 Agent 的信息

把下面的变量替换成实际地址：

```bash
export TARGET_HOST=192.168.1.20
export TARGET_PORT=8081
```

压测机先做低负载验证：

```bash
curl --noproxy '*' -i --max-time 5 "http://${TARGET_HOST}:${TARGET_PORT}/"
```

然后使用 `wrk`：

```bash
NO_PROXY="${TARGET_HOST},localhost,127.0.0.1" \
no_proxy="${TARGET_HOST},localhost,127.0.0.1" \
wrk -t4 -c100 -d30s --latency \
  "http://${TARGET_HOST}:${TARGET_PORT}/"
```

建议压测机 Agent 按以下阶梯执行，每一级至少运行 30 秒，记录完整 stdout：

```bash
for C in 100 500 1000 2000 5000; do
  echo "===== concurrency=${C} ====="
  wrk -t8 -c"${C}" -d30s --latency \
    "http://${TARGET_HOST}:${TARGET_PORT}/"
done
```

每一级记录：

- Requests/sec
- 平均延迟、p50、p90、p99、最大延迟
- Socket errors：connect、read、write、timeout
- HTTP 非 2xx 数量
- 压测机 CPU、网卡吞吐和重传
- 网关 CPU、RSS、线程数、softirq
- 上游服务 RPS 和 active connections

本机可以观察：

```bash
pidstat -p "$(cat /tmp/vexo-gateway-8081.pid)" -t 1
vmstat 1
mpstat -P ALL 1
sar -n DEV 1
tail -f /tmp/vexo-gateway-8081.log /tmp/vexo-upstream-9001.log
```

## 多上游负载均衡压测

如果需要观察 round-robin，可以额外启动 9002、9003、9004：

```bash
for P in 9002 9003 9004; do
  env PORT="$P" IO_THREADS=4 \
    ./build-uring/examples/http/demo_http_server \
    > "/tmp/vexo-upstream-${P}.log" 2>&1 &
  echo $! > "/tmp/vexo-upstream-${P}.pid"
done
```

重启网关时使用：

```bash
UPSTREAM_PORTS=9001,9002,9003,9004
```

## 停止与清理

只按保存的 PID 停止本次压测进程，不要使用宽泛的 `pkill -f`：

```bash
for F in /tmp/vexo-gateway-8081.pid \
         /tmp/vexo-upstream-9001.pid \
         /tmp/vexo-upstream-9002.pid \
         /tmp/vexo-upstream-9003.pid \
         /tmp/vexo-upstream-9004.pid; do
  if [ -f "$F" ]; then
    PID=$(cat "$F")
    kill -TERM "$PID" 2>/dev/null || true
  fi
done
```

等待几秒后检查：

```bash
ss -lntp | grep -E ':8081|:900[1-4]' || true
```

如果某个本次启动的进程仍未退出，再针对该 PID 执行：

```bash
kill -KILL "$PID"
```

## 故障排查

### 压测机连接被拒绝

检查：

```bash
ss -lntp | grep ':8081'
curl --noproxy '*' -i --max-time 5 "http://${TARGET_HOST}:8081/"
```

网关必须显示监听 `0.0.0.0:8081`，如果显示 `127.0.0.1:8081`，说明没有设置 `BIND_HOST=0.0.0.0`。

### 网关返回 502

检查本机上游：

```bash
curl --noproxy '*' -i --max-time 5 http://127.0.0.1:9001/
tail -100 /tmp/vexo-gateway-8081.log
```

网关到上游使用 `127.0.0.1:9001`，不需要压测机访问 9001。

### 压测机走了代理

压测 HTTP 请求必须绕过代理：

```bash
curl --noproxy '*'
```

或者设置：

```bash
export NO_PROXY="${TARGET_HOST},localhost,127.0.0.1"
export no_proxy="$NO_PROXY"
```

### 结果出现大量 timeout

先降到 `-c100`，确认低负载下没有错误，再逐步提升并发。同步记录网关和上游的 CPU、RSS、FD 数、ring entries、worker 数及内核版本，不要只比较 RPS。

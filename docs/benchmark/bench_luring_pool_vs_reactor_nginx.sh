#!/usr/bin/env bash
# Compare the memory-pool io_uring gateway with the existing Reactor gateway
# and the repository's Nginx gateway configuration.
#
# The three targets proxy to the same four local Nginx upstreams. The runtime
# Reactor gateway uses one event-loop worker because
# demo_bench_gateway_multi::set_thread_num is currently a compatibility no-op.
# The io_uring gateway uses four independent rings/listeners with SO_REUSEPORT;
# the kernel distributes incoming connections among them. Nginx keeps its
# existing four-worker configuration for continuity with historical results.
#
# Usage:
#   docs/benchmark/bench_luring_pool_vs_reactor_nginx.sh
#
# Environment variables are forwarded to run_bench.sh. Typical quick run:
#   DURATION=10s ROUNDS=2 LEVELS="1000 5000 10000" \
#     docs/benchmark/bench_luring_pool_vs_reactor_nginx.sh
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BD="$ROOT/docs/benchmark"
OUTDIR=${OUTDIR:-"$BD/results-luring-pool-vs-reactor-nginx-$(date +%Y%m%d-%H%M%S)"}
REACTOR_BIN="$ROOT/build/examples/gateway/demo_bench_gateway_multi"
LURING_BIN="$ROOT/build-uring/examples/gateway/demo_bench_gateway_luring"
MAX_CONCURRENT_REQUESTS=${MAX_CONCURRENT_REQUESTS:-20000}
URING_WORKERS=${URING_WORKERS:-4}
URING_ENTRIES=${URING_ENTRIES:-8192}
MAX_IDLE_PER_PEER=${MAX_IDLE_PER_PEER:-16}

export NO_PROXY="127.0.0.1,localhost${NO_PROXY:+,$NO_PROXY}"
export no_proxy="$NO_PROXY"

if ! ulimit -n 200000 2>/dev/null; then
  echo "unable to raise RLIMIT_NOFILE; current limit: $(ulimit -n)" >&2
  exit 1
fi
echo "==> RLIMIT_NOFILE: $(ulimit -n)"

REACTOR_PID=""
LURING_PID=""

cleanup() {
  set +e
  if [[ -n "$REACTOR_PID" ]]; then
    kill "$REACTOR_PID" 2>/dev/null
    wait "$REACTOR_PID" 2>/dev/null
  fi
  if [[ -n "$LURING_PID" ]]; then
    kill -TERM "$LURING_PID" 2>/dev/null
    wait "$LURING_PID" 2>/dev/null
  fi
  nginx -s stop -c "$BD/nginx_gateway.conf" 2>/dev/null
  nginx -s stop -c "$BD/nginx_upstream.conf" 2>/dev/null
}
trap cleanup EXIT

[[ -x "$REACTOR_BIN" ]] || { echo "missing $REACTOR_BIN; build first" >&2; exit 1; }
[[ -x "$LURING_BIN" ]] || { echo "missing $LURING_BIN; build-uring first" >&2; exit 1; }

mkdir -p \
  /tmp/nginx_upstream_tmp/client /tmp/nginx_upstream_tmp/proxy \
  /tmp/nginx_upstream_tmp/fastcgi /tmp/nginx_upstream_tmp/uwsgi \
  /tmp/nginx_upstream_tmp/scgi \
  /tmp/nginx_gateway_tmp/client /tmp/nginx_gateway_tmp/proxy \
  /tmp/nginx_gateway_tmp/fastcgi /tmp/nginx_gateway_tmp/uwsgi \
  /tmp/nginx_gateway_tmp/scgi

mkdir -p "$OUTDIR"

echo "==> starting shared upstream nginx (9001-9004)"
nginx -s stop -c "$BD/nginx_gateway.conf" 2>/dev/null || true
nginx -s stop -c "$BD/nginx_upstream.conf" 2>/dev/null || true
nginx -t -c "$BD/nginx_upstream.conf"
nginx -c "$BD/nginx_upstream.conf"
sleep 1
curl -fsS http://127.0.0.1:9001/ >/dev/null

run_target() {
  local name=$1 url=$2 cpu_match=$3
  echo "==> benchmarking $name ($url)"
  "$BD/run_bench.sh" "$url" "$name" "$cpu_match" "$OUTDIR"
}

echo "==> starting Reactor gateway (8080)"
UPSTREAM_PORTS=9001,9002,9003,9004 LB_ALGO=round_robin IO_THREADS=4 \
  MAX_CONCURRENT_REQUESTS="$MAX_CONCURRENT_REQUESTS" PORT=8080 \
  "$REACTOR_BIN" >"$OUTDIR/reactor.log" 2>&1 &
REACTOR_PID=$!
sleep 1
curl -fsS http://127.0.0.1:8080/ >/dev/null
run_target reactor http://127.0.0.1:8080/ demo_bench_gateway_multi
kill "$REACTOR_PID" 2>/dev/null
wait "$REACTOR_PID" 2>/dev/null || true
REACTOR_PID=""

echo "==> starting memory-pool io_uring gateway (8081)"
UPSTREAM_PORTS=9001,9002,9003,9004 LB_ALGO=round_robin FRAME_POOL=1 \
  URING_WORKERS="$URING_WORKERS" URING_ENTRIES="$URING_ENTRIES" \
  MAX_IDLE_PER_PEER="$MAX_IDLE_PER_PEER" \
  MAX_CONCURRENT_REQUESTS="$MAX_CONCURRENT_REQUESTS" PORT=8081 \
  "$LURING_BIN" >"$OUTDIR/luring_pool.log" 2>&1 &
LURING_PID=$!
sleep 1
curl -fsS http://127.0.0.1:8081/ >/dev/null
run_target luring_pool http://127.0.0.1:8081/ demo_bench_gateway_luring
kill -TERM "$LURING_PID" 2>/dev/null
wait "$LURING_PID" 2>/dev/null || true
LURING_PID=""

echo "==> starting Nginx gateway (8088)"
nginx -t -c "$BD/nginx_gateway.conf"
nginx -c "$BD/nginx_gateway.conf"
sleep 1
curl -fsS http://127.0.0.1:8088/ >/dev/null
run_target nginx http://127.0.0.1:8088/ @/tmp/nginx_gateway.pid
nginx -s stop -c "$BD/nginx_gateway.conf"

echo "==> done; raw CSVs are under $OUTDIR"

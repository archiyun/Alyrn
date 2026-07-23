#!/usr/bin/env bash
# Compare the io_uring gateway with the existing Reactor gateway and the
# repository's Nginx gateway configuration.
#
# The three targets proxy to the same four local Nginx upstreams. Reactor and
# io_uring use the same number of independent workers, listeners, and I/O
# loops/rings. Nginx keeps its existing four-worker configuration as an
# independent reference and is not part of the fair runtime comparison.
#
# The fair runtime comparison also disables the io_uring-only frame pool and
# uses the same Worker-local upstream idle-pool size in both gateways. Nginx's
# keepalive pool is 64 total per worker, so luring and Reactor use a shared
# total budget of 64 rather than four independent per-peer limits.
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
OUTDIR=${OUTDIR:-"$BD/results-luring-vs-reactor-nginx-$(date +%Y%m%d-%H%M%S)"}
REACTOR_BIN="$ROOT/build/examples/gateway/demo_bench_gateway_multi"
LURING_BIN=${LURING_BIN:-"$ROOT/build-uring/examples/gateway/demo_bench_gateway_luring"}
MAX_CONCURRENT_REQUESTS=${MAX_CONCURRENT_REQUESTS:-20000}
WORKERS=${WORKERS:-4}
URING_WORKERS=${URING_WORKERS:-$WORKERS}
URING_ENTRIES=${URING_ENTRIES:-8192}
MAX_IDLE_PER_PEER=${MAX_IDLE_PER_PEER:-0}
MAX_IDLE_TOTAL=${MAX_IDLE_TOTAL:-64}
FRAME_POOL=${FRAME_POOL:-0}
URING_SQPOLL=${URING_SQPOLL:-0}
URING_SQPOLL_IDLE_MS=${URING_SQPOLL_IDLE_MS:-1000}
URING_DEFER_TASKRUN=${URING_DEFER_TASKRUN:-0}
MAX_CQE_PER_TURN=${MAX_CQE_PER_TURN:-256}
MAX_READY_WORK_PER_TURN=${MAX_READY_WORK_PER_TURN:-256}
MAX_READY_TIME_US=${MAX_READY_TIME_US:-50}
MAX_COMPLETION_WORK_PER_TURN=${MAX_COMPLETION_WORK_PER_TURN:-64}
COMPLETION_AGE_THRESHOLD_US=${COMPLETION_AGE_THRESHOLD_US:-0}
MAX_URGENT_COMPLETION_WORK_PER_TURN=${MAX_URGENT_COMPLETION_WORK_PER_TURN:-80}
NORMAL_QUEUE_AGE_THRESHOLD_US=${NORMAL_QUEUE_AGE_THRESHOLD_US:-5000}
LURING_DUMP_STATS=${LURING_DUMP_STATS:-0}
LURING_FRAME_STATS=${LURING_FRAME_STATS:-0}

if [[ "$WORKERS" != "$URING_WORKERS" ]]; then
  echo "fair comparison requires WORKERS=URING_WORKERS" >&2
  exit 1
fi
if [[ "$WORKERS" != 4 ]]; then
  echo "this benchmark is configured for the four-worker architecture; use WORKERS=4" >&2
  exit 1
fi
if [[ "$MAX_IDLE_PER_PEER" != 0 || "$MAX_IDLE_TOTAL" != 64 ]]; then
  echo "fair comparison requires MAX_IDLE_PER_PEER=0 and MAX_IDLE_TOTAL=64" >&2
  exit 1
fi
if [[ "$FRAME_POOL" != 0 ]]; then
  echo "fair comparison requires FRAME_POOL=0; Reactor has no frame pool" >&2
  exit 1
fi

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

echo "==> starting Reactor gateway (8080, ${WORKERS} workers)"
UPSTREAM_PORTS=9001,9002,9003,9004 LB_ALGO=round_robin \
  MAX_CONCURRENT_REQUESTS="$MAX_CONCURRENT_REQUESTS" PORT=8080 \
  MAX_IDLE_PER_PEER="$MAX_IDLE_PER_PEER" MAX_IDLE_TOTAL="$MAX_IDLE_TOTAL" \
  WORKERS="$WORKERS" \
  "$REACTOR_BIN" >"$OUTDIR/reactor.log" 2>&1 &
REACTOR_PID=$!
sleep 1
curl -fsS http://127.0.0.1:8080/ >/dev/null
run_target reactor http://127.0.0.1:8080/ demo_bench_gateway_multi
kill "$REACTOR_PID" 2>/dev/null
wait "$REACTOR_PID" 2>/dev/null || true
REACTOR_PID=""

echo "==> starting io_uring gateway (8081, ${URING_WORKERS} workers/rings, frame pool disabled)"
UPSTREAM_PORTS=9001,9002,9003,9004 LB_ALGO=round_robin FRAME_POOL="$FRAME_POOL" \
  URING_WORKERS="$URING_WORKERS" URING_ENTRIES="$URING_ENTRIES" \
  URING_SQPOLL="$URING_SQPOLL" URING_SQPOLL_IDLE_MS="$URING_SQPOLL_IDLE_MS" \
  URING_DEFER_TASKRUN="$URING_DEFER_TASKRUN" \
  MAX_IDLE_PER_PEER="$MAX_IDLE_PER_PEER" MAX_IDLE_TOTAL="$MAX_IDLE_TOTAL" \
  MAX_CQE_PER_TURN="$MAX_CQE_PER_TURN" \
  MAX_READY_WORK_PER_TURN="$MAX_READY_WORK_PER_TURN" \
  MAX_READY_TIME_US="$MAX_READY_TIME_US" \
  MAX_COMPLETION_WORK_PER_TURN="$MAX_COMPLETION_WORK_PER_TURN" \
  COMPLETION_AGE_THRESHOLD_US="$COMPLETION_AGE_THRESHOLD_US" \
  MAX_URGENT_COMPLETION_WORK_PER_TURN="$MAX_URGENT_COMPLETION_WORK_PER_TURN" \
  NORMAL_QUEUE_AGE_THRESHOLD_US="$NORMAL_QUEUE_AGE_THRESHOLD_US" \
  LURING_DUMP_STATS="$LURING_DUMP_STATS" LURING_FRAME_STATS="$LURING_FRAME_STATS" \
  MAX_CONCURRENT_REQUESTS="$MAX_CONCURRENT_REQUESTS" PORT=8081 \
  "$LURING_BIN" >"$OUTDIR/luring.log" 2>&1 &
LURING_PID=$!
sleep 1
curl -fsS http://127.0.0.1:8081/ >/dev/null
run_target luring http://127.0.0.1:8081/ demo_bench_gateway_luring
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

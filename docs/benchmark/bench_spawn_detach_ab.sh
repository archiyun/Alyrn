#!/usr/bin/env bash
# Compare two io_uring gateway binaries with alternating target order.
#
# The baseline and current binaries are started and stopped once per outer
# round. Odd rounds run baseline first; even rounds run current first. Each
# target still sweeps every LEVELS value in the same order, so the target order
# is not permanently coupled to the machine's first/last-run state.
#
# Required:
#   BASELINE_LURING_BIN=/path/to/old/demo_bench_gateway_luring
#
# Typical run:
#   BASELINE_LURING_BIN=/tmp/coropact-spawn-baseline-max-build/examples/gateway/demo_bench_gateway_luring \
#   CURRENT_LURING_BIN=build/examples/gateway/demo_bench_gateway_luring \
#   DURATION=10s WARMUP=2 ROUNDS=5 THREADS=4 \
#   docs/benchmark/bench_spawn_detach_ab.sh

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BD="$ROOT/docs/benchmark"
OUTDIR=${OUTDIR:-"$BD/results-spawn-detach-ab-$(date +%Y%m%d-%H%M%S)"}
BASELINE_LURING_BIN=${BASELINE_LURING_BIN:?set BASELINE_LURING_BIN to the Spawn().Detach() binary}
CURRENT_LURING_BIN=${CURRENT_LURING_BIN:-"$ROOT/build/examples/gateway/demo_bench_gateway_luring"}
ROUNDS=${ROUNDS:-5}
DURATION=${DURATION:-10s}
WARMUP=${WARMUP:-2}
THREADS=${THREADS:-4}
LEVELS=${LEVELS:-"100 500 1000 2000 5000 10000"}
MAX_CONCURRENT_REQUESTS=${MAX_CONCURRENT_REQUESTS:-20000}
URING_WORKERS=${URING_WORKERS:-4}
URING_ENTRIES=${URING_ENTRIES:-8192}
URING_SQPOLL=${URING_SQPOLL:-0}
URING_DEFER_TASKRUN=${URING_DEFER_TASKRUN:-1}
URING_CPU_AFFINITY=${URING_CPU_AFFINITY:-0,2,4,6}
FRAME_POOL=${FRAME_POOL:-0}
MAX_IDLE_PER_PEER=${MAX_IDLE_PER_PEER:-0}
MAX_IDLE_TOTAL=${MAX_IDLE_TOTAL:-64}
MAX_CQE_PER_TURN=${MAX_CQE_PER_TURN:-0}
MAX_READY_WORK_PER_TURN=${MAX_READY_WORK_PER_TURN:-0}
MAX_READY_TIME_US=${MAX_READY_TIME_US:-0}
MAX_COMPLETION_WORK_PER_TURN=${MAX_COMPLETION_WORK_PER_TURN:-0}
COMPLETION_AGE_THRESHOLD_US=${COMPLETION_AGE_THRESHOLD_US:-0}
MAX_URGENT_COMPLETION_WORK_PER_TURN=${MAX_URGENT_COMPLETION_WORK_PER_TURN:-0}
NORMAL_QUEUE_AGE_THRESHOLD_US=${NORMAL_QUEUE_AGE_THRESHOLD_US:-0}

if [[ "$FRAME_POOL" != 0 ]]; then
  echo "fair A/B comparison requires FRAME_POOL=0" >&2
  exit 1
fi
if [[ "$URING_WORKERS" != 4 ]]; then
  echo "this benchmark is configured for four io_uring workers" >&2
  exit 1
fi
if ! [[ "$ROUNDS" =~ ^[1-9][0-9]*$ ]]; then
  echo "ROUNDS must be a positive integer" >&2
  exit 1
fi

[[ -x "$BASELINE_LURING_BIN" ]] || {
  echo "missing baseline binary: $BASELINE_LURING_BIN" >&2
  exit 1
}
[[ -x "$CURRENT_LURING_BIN" ]] || {
  echo "missing current binary: $CURRENT_LURING_BIN" >&2
  exit 1
}

export NO_PROXY="127.0.0.1,localhost${NO_PROXY:+,$NO_PROXY}"
export no_proxy="$NO_PROXY"

if ! ulimit -n 200000 2>/dev/null; then
  echo "unable to raise RLIMIT_NOFILE; current limit: $(ulimit -n)" >&2
  exit 1
fi

mkdir -p \
  /tmp/nginx_upstream_tmp/client /tmp/nginx_upstream_tmp/proxy \
  /tmp/nginx_upstream_tmp/fastcgi /tmp/nginx_upstream_tmp/uwsgi \
  /tmp/nginx_upstream_tmp/scgi \
  /tmp/nginx_gateway_tmp/client /tmp/nginx_gateway_tmp/proxy \
  /tmp/nginx_gateway_tmp/fastcgi /tmp/nginx_gateway_tmp/uwsgi \
  /tmp/nginx_gateway_tmp/scgi \
  "$OUTDIR"

UPSTREAM_CONF="$BD/nginx_upstream.conf"
GATEWAY_CONF="$BD/nginx_gateway.conf"
LURING_PID=""

cleanup() {
  set +e
  if [[ -n "$LURING_PID" ]]; then
    kill -TERM "$LURING_PID" 2>/dev/null
    wait "$LURING_PID" 2>/dev/null
  fi
  nginx -s stop -c "$GATEWAY_CONF" 2>/dev/null
  nginx -s stop -c "$UPSTREAM_CONF" 2>/dev/null
}
trap cleanup EXIT

nginx -s stop -c "$GATEWAY_CONF" 2>/dev/null || true
nginx -s stop -c "$UPSTREAM_CONF" 2>/dev/null || true
nginx -t -c "$UPSTREAM_CONF"
nginx -c "$UPSTREAM_CONF"
sleep 1
curl -fsS http://127.0.0.1:9001/ >/dev/null

start_luring() {
  local label=$1
  local binary=$2
  local log="$OUTDIR/$label.log"

  echo "==> starting $label"
  env BIND_HOST=127.0.0.1 PORT=8081 \
    UPSTREAM_PORTS=9001,9002,9003,9004 LB_ALGO=round_robin \
    FRAME_POOL="$FRAME_POOL" URING_WORKERS="$URING_WORKERS" \
    URING_ENTRIES="$URING_ENTRIES" URING_SQPOLL="$URING_SQPOLL" \
    URING_DEFER_TASKRUN="$URING_DEFER_TASKRUN" \
    URING_CPU_AFFINITY="$URING_CPU_AFFINITY" \
    MAX_IDLE_PER_PEER="$MAX_IDLE_PER_PEER" MAX_IDLE_TOTAL="$MAX_IDLE_TOTAL" \
    MAX_CQE_PER_TURN="$MAX_CQE_PER_TURN" \
    MAX_READY_WORK_PER_TURN="$MAX_READY_WORK_PER_TURN" \
    MAX_READY_TIME_US="$MAX_READY_TIME_US" \
    MAX_COMPLETION_WORK_PER_TURN="$MAX_COMPLETION_WORK_PER_TURN" \
    COMPLETION_AGE_THRESHOLD_US="$COMPLETION_AGE_THRESHOLD_US" \
    MAX_URGENT_COMPLETION_WORK_PER_TURN="$MAX_URGENT_COMPLETION_WORK_PER_TURN" \
    NORMAL_QUEUE_AGE_THRESHOLD_US="$NORMAL_QUEUE_AGE_THRESHOLD_US" \
    MAX_CONCURRENT_REQUESTS="$MAX_CONCURRENT_REQUESTS" \
    "$binary" >"$log" 2>&1 &
  LURING_PID=$!
  sleep 1
  curl -fsS http://127.0.0.1:8081/ >/dev/null
}

stop_luring() {
  if [[ -n "$LURING_PID" ]]; then
    kill -TERM "$LURING_PID" 2>/dev/null || true
    wait "$LURING_PID" 2>/dev/null || true
    LURING_PID=""
  fi
}

run_luring_round() {
  local label=$1
  local binary=$2
  local round=$3

  start_luring "$label" "$binary"
  DURATION="$DURATION" ROUNDS=1 WARMUP="$WARMUP" THREADS="$THREADS" LEVELS="$LEVELS" \
    "$BD/run_bench.sh" http://127.0.0.1:8081/ "$label" demo_bench_gateway_luring "$OUTDIR"
  stop_luring

  awk -F, -v label="$label" -v round="$round" 'FNR > 1 {
      print label "," round "," $0
    }' "$OUTDIR/$label.csv" >> "$OUTDIR/alternating.csv"
}

printf 'label,outer_round,level,inner_round,rps,lat_avg_ms,p50_ms,p90_ms,p99_ms,non2xx,sock_err,timeout,cpu_pct_avg,rss_mb_max\n' \
  > "$OUTDIR/alternating.csv"

for ((round = 1; round <= ROUNDS; ++round)); do
  if ((round % 2 == 1)); then
    run_luring_round "baseline-r$round" "$BASELINE_LURING_BIN" "$round"
    run_luring_round "current-r$round" "$CURRENT_LURING_BIN" "$round"
  else
    run_luring_round "current-r$round" "$CURRENT_LURING_BIN" "$round"
    run_luring_round "baseline-r$round" "$BASELINE_LURING_BIN" "$round"
  fi
done

echo "==> starting Nginx gateway reference"
nginx -t -c "$GATEWAY_CONF"
nginx -c "$GATEWAY_CONF"
sleep 1
curl -fsS http://127.0.0.1:8088/ >/dev/null
DURATION="$DURATION" ROUNDS="$ROUNDS" WARMUP="$WARMUP" THREADS="$THREADS" LEVELS="$LEVELS" \
  "$BD/run_bench.sh" http://127.0.0.1:8088/ nginx "@/tmp/nginx_gateway.pid" "$OUTDIR"
nginx -s stop -c "$GATEWAY_CONF"

echo "==> alternating A/B results: $OUTDIR/alternating.csv"

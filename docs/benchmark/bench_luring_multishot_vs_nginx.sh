#!/usr/bin/env bash
# Compare one io_uring gateway configuration with the repository's Nginx
# gateway configuration. The caller selects the luring completion path through
# the URING_MULTISHOT_ACCEPT, URING_MULTISHOT_RECV, and URING_SEND_ZEROCOPY
# environment variables.
#
# Both gateways proxy to the same four local Nginx upstreams. The runtime
# configuration follows the established gateway comparison: four workers,
# ring entries 8192, FRAME_POOL=0, SQPOLL/DEFER_TASKRUN disabled, and one
# shared idle-connection budget of 64 per worker. The only luring-specific
# difference is the selected luring completion path; Nginx is started only
# when RUN_NGINX=1.
#
# Default sweep: wrk -t8, 10 seconds per round, three rounds, 3-second warmup,
# at concurrency 100/500/1000/2000/5000/10000.
#
# Usage:
#   docs/benchmark/bench_luring_multishot_vs_nginx.sh
#
# Typical shorter run:
#   DURATION=3s ROUNDS=1 LEVELS="1000 5000" \
#     docs/benchmark/bench_luring_multishot_vs_nginx.sh
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BD="$ROOT/docs/benchmark"
OUTDIR=${OUTDIR:-"$BD/results-luring-multishot-vs-nginx-$(date +%Y%m%d-%H%M%S)"}
LURING_BIN=${LURING_BIN:-"$ROOT/build-uring/examples/gateway/demo_bench_gateway_luring"}

MAX_CONCURRENT_REQUESTS=${MAX_CONCURRENT_REQUESTS:-20000}
WORKERS=${WORKERS:-4}
URING_WORKERS=${URING_WORKERS:-$WORKERS}
URING_ENTRIES=${URING_ENTRIES:-8192}
MAX_IDLE_PER_PEER=${MAX_IDLE_PER_PEER:-0}
MAX_IDLE_TOTAL=${MAX_IDLE_TOTAL:-64}
FRAME_POOL=${FRAME_POOL:-0}
ALLOW_FRAME_POOL_BENCH=${ALLOW_FRAME_POOL_BENCH:-0}
URING_SQPOLL=${URING_SQPOLL:-0}
URING_SQPOLL_IDLE_MS=${URING_SQPOLL_IDLE_MS:-1000}
URING_DEFER_TASKRUN=${URING_DEFER_TASKRUN:-0}
URING_SEND_ZEROCOPY=${URING_SEND_ZEROCOPY:-0}
URING_MULTISHOT_ACCEPT=${URING_MULTISHOT_ACCEPT:-0}
URING_MULTISHOT_RECV=${URING_MULTISHOT_RECV:-0}
LURING_LABEL=${LURING_LABEL:-luring}
RUN_NGINX=${RUN_NGINX:-1}
MAX_CQE_PER_TURN=${MAX_CQE_PER_TURN:-256}
MAX_READY_WORK_PER_TURN=${MAX_READY_WORK_PER_TURN:-256}
MAX_READY_TIME_US=${MAX_READY_TIME_US:-50}
MAX_COMPLETION_WORK_PER_TURN=${MAX_COMPLETION_WORK_PER_TURN:-64}
COMPLETION_AGE_THRESHOLD_US=${COMPLETION_AGE_THRESHOLD_US:-0}
MAX_URGENT_COMPLETION_WORK_PER_TURN=${MAX_URGENT_COMPLETION_WORK_PER_TURN:-80}
NORMAL_QUEUE_AGE_THRESHOLD_US=${NORMAL_QUEUE_AGE_THRESHOLD_US:-5000}
LURING_FRAME_STATS=${LURING_FRAME_STATS:-0}

# These defaults match the prior Nginx comparison. They remain overridable for
# smoke runs, but the script still records the actual values in run_bench CSVs.
DURATION=${DURATION:-10s}
ROUNDS=${ROUNDS:-3}
THREADS=${THREADS:-8}
LEVELS=${LEVELS:-"100 500 1000 2000 5000 10000"}
WARMUP=${WARMUP:-3}

BoolText() {
  if [[ "$1" == 1 ]]; then
    echo on
  else
    echo off
  fi
}

if [[ "$WORKERS" != "$URING_WORKERS" ]]; then
  echo "fair comparison requires WORKERS=URING_WORKERS" >&2
  exit 1
fi
if [[ "$WORKERS" != 4 ]]; then
  echo "this benchmark is configured for four workers; use WORKERS=4" >&2
  exit 1
fi
if [[ "$MAX_IDLE_PER_PEER" != 0 || "$MAX_IDLE_TOTAL" != 64 ]]; then
  echo "fair comparison requires MAX_IDLE_PER_PEER=0 and MAX_IDLE_TOTAL=64" >&2
  exit 1
fi
if [[ "$FRAME_POOL" != 0 && "$ALLOW_FRAME_POOL_BENCH" != 1 ]]; then
  echo "set ALLOW_FRAME_POOL_BENCH=1 to measure FRAME_POOL=1" >&2
  exit 1
fi

export DURATION ROUNDS THREADS LEVELS WARMUP
export NO_PROXY="127.0.0.1,localhost${NO_PROXY:+,$NO_PROXY}"
export no_proxy="$NO_PROXY"

if ! ulimit -n 200000 2>/dev/null; then
  echo "unable to raise RLIMIT_NOFILE; current limit: $(ulimit -n)" >&2
  exit 1
fi
echo "==> RLIMIT_NOFILE: $(ulimit -n)"

LURING_PID=""

cleanup() {
  set +e
  if [[ -n "$LURING_PID" ]]; then
    kill -TERM "$LURING_PID" 2>/dev/null
    wait "$LURING_PID" 2>/dev/null
  fi
  nginx -s stop -c "$BD/nginx_gateway.conf" 2>/dev/null
  nginx -s stop -c "$BD/nginx_upstream.conf" 2>/dev/null
}
trap cleanup EXIT

[[ -x "$LURING_BIN" ]] || { echo "missing $LURING_BIN; build-uring first" >&2; exit 1; }
command -v nginx >/dev/null || { echo "missing nginx" >&2; exit 1; }
command -v wrk >/dev/null || { echo "missing wrk" >&2; exit 1; }
command -v curl >/dev/null || { echo "missing curl" >&2; exit 1; }
command -v stdbuf >/dev/null || { echo "missing stdbuf" >&2; exit 1; }

mkdir -p \
  /tmp/nginx_upstream_tmp/client /tmp/nginx_upstream_tmp/proxy \
  /tmp/nginx_upstream_tmp/fastcgi /tmp/nginx_upstream_tmp/uwsgi \
  /tmp/nginx_upstream_tmp/scgi \
  /tmp/nginx_gateway_tmp/client /tmp/nginx_gateway_tmp/proxy \
  /tmp/nginx_gateway_tmp/fastcgi /tmp/nginx_gateway_tmp/uwsgi \
  /tmp/nginx_gateway_tmp/scgi \
  "$OUTDIR"

echo "==> starting shared upstream nginx (9001-9004)"
nginx -s stop -c "$BD/nginx_gateway.conf" 2>/dev/null || true
nginx -s stop -c "$BD/nginx_upstream.conf" 2>/dev/null || true
nginx -t -c "$BD/nginx_upstream.conf"
nginx -c "$BD/nginx_upstream.conf"
sleep 1
curl -fsS http://127.0.0.1:9001/ >/dev/null

echo "==> starting ${LURING_LABEL} io_uring gateway (8081, ${URING_WORKERS} workers/rings)"
UPSTREAM_PORTS=9001,9002,9003,9004 LB_ALGO=round_robin FRAME_POOL="$FRAME_POOL" \
  URING_MULTISHOT_ACCEPT="$URING_MULTISHOT_ACCEPT" \
  URING_MULTISHOT_RECV="$URING_MULTISHOT_RECV" \
  URING_SEND_ZEROCOPY="$URING_SEND_ZEROCOPY" \
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
  LURING_FRAME_STATS="$LURING_FRAME_STATS" \
  MAX_CONCURRENT_REQUESTS="$MAX_CONCURRENT_REQUESTS" PORT=8081 \
  stdbuf -oL "$LURING_BIN" >"$OUTDIR/${LURING_LABEL}.log" 2>&1 &
LURING_PID=$!
sleep 1
curl -fsS http://127.0.0.1:8081/ >/dev/null
for _ in $(seq 1 20); do
  if rg -q "multishot_accept=$(BoolText "$URING_MULTISHOT_ACCEPT")" \
      "$OUTDIR/${LURING_LABEL}.log"; then
    break
  fi
  sleep 0.1
done
if ! rg -q "multishot_accept=$(BoolText "$URING_MULTISHOT_ACCEPT")" \
    "$OUTDIR/${LURING_LABEL}.log"; then
  echo "luring gateway did not report requested multishot_accept mode" >&2
  exit 1
fi
if ! rg -q "multishot_recv=$(BoolText "$URING_MULTISHOT_RECV")" \
    "$OUTDIR/${LURING_LABEL}.log"; then
  echo "luring gateway did not report requested multishot_recv mode" >&2
  exit 1
fi
if [[ "$URING_SEND_ZEROCOPY" == 1 ]] &&
   ! rg -q 'send_zerocopy=on' "$OUTDIR/${LURING_LABEL}.log"; then
  echo "luring gateway did not report send_zerocopy=on" >&2
  exit 1
fi

"$BD/run_bench.sh" \
  http://127.0.0.1:8081/ "$LURING_LABEL" demo_bench_gateway_luring "$OUTDIR"

kill -TERM "$LURING_PID" 2>/dev/null || true
wait "$LURING_PID" 2>/dev/null || true
LURING_PID=""

if [[ "$URING_SEND_ZEROCOPY" == 1 ]]; then
  zc_stats=$(rg '\[luring\.send_zc\]' "$OUTDIR/${LURING_LABEL}.log" | tail -n 1 || true)
  if [[ -z "$zc_stats" ]]; then
    echo "luring gateway did not emit send-zero-copy diagnostics" >&2
    exit 1
  fi

  zc_attempts=$(sed -n 's/.*attempts=\([0-9][0-9]*\).*/\1/p' <<<"$zc_stats")
  zc_completions=$(sed -n 's/.*logical_completions=\([0-9][0-9]*\).*/\1/p' <<<"$zc_stats")
  zc_primary=$(sed -n 's/.*primary_events=\([0-9][0-9]*\).*/\1/p' <<<"$zc_stats")
  zc_notifications=$(sed -n 's/.*notifications=\([0-9][0-9]*\).*/\1/p' <<<"$zc_stats")
  zc_fallbacks=$(sed -n 's/.*copy_fallbacks=\([0-9][0-9]*\).*/\1/p' <<<"$zc_stats")
  zc_primary_errors=$(sed -n 's/.* primary=\([0-9][0-9]*\).*/\1/p' <<<"$zc_stats")
  zc_primary_enomem=$(sed -n 's/.*primary_enomem=\([0-9][0-9]*\).*/\1/p' <<<"$zc_stats")
  zc_primary_other=$(sed -n 's/.*primary_other=\([0-9][0-9]*\).*/\1/p' <<<"$zc_stats")
  if [[ "$zc_attempts" != "$zc_completions" || "$zc_attempts" != "$zc_primary" ||
        "$zc_attempts" != "$zc_notifications" || ! "$zc_stats" =~ errors=0 ||
        ! "$zc_stats" =~ protocol=0 || "$zc_primary_errors" != "$zc_primary_enomem" ||
        "$zc_primary_errors" != "$zc_fallbacks" || "$zc_primary_other" != 0 ]]; then
    echo "send-zero-copy lifecycle validation failed: $zc_stats" >&2
    exit 1
  fi
  echo "==> send-zero-copy lifecycle validated: $zc_stats"
fi

if [[ "$RUN_NGINX" != 1 ]]; then
  echo "==> done; raw CSVs are under $OUTDIR"
  exit 0
fi

echo "==> starting Nginx gateway (8088)"
nginx -t -c "$BD/nginx_gateway.conf"
nginx -c "$BD/nginx_gateway.conf"
sleep 1
curl -fsS http://127.0.0.1:8088/ >/dev/null
"$BD/run_bench.sh" http://127.0.0.1:8088/ nginx @/tmp/nginx_gateway.pid "$OUTDIR"
nginx -s stop -c "$BD/nginx_gateway.conf"

echo "==> done; raw CSVs are under $OUTDIR"

#!/usr/bin/env bash
set -euo pipefail

# Fixed HTTP keep-alive benchmark for the current CoroPact checkout.
# Generated wrk logs and CSV are written to OUTDIR, which defaults to /tmp.

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
OUTDIR=${OUTDIR:-/tmp/coropact-luring-vs-raw-liburing-$(date +%Y%m%d-%H%M%S)}
BUILD_DIR=${BUILD_DIR:-"$ROOT_DIR/build-uring"}
WRK=${WRK:-wrk}
THREADS=${THREADS:-8}
LEVELS=${LEVELS:-"1000 5000 10000"}
WARMUP=${WARMUP:-5s}
DURATION=${DURATION:-10s}
ROUNDS=${ROUNDS:-3}
TIMEOUT=${TIMEOUT:-5s}
WORKERS=${URING_WORKERS:-4}
ENTRIES=${URING_ENTRIES:-8192}
FRAME_POOL=${FRAME_POOL:-0}
PORT=${PORT:-19090}

LURING_BIN=${LURING_BIN:-"$BUILD_DIR/examples/luring/demo_bench_http_luring"}
RAW_BIN=${RAW_BIN:-"$BUILD_DIR/examples/luring/demo_bench_http_liburing"}

mkdir -p "$OUTDIR/raw"
printf 'target,concurrency,round,cpu_percent,rss_kb\n' >"$OUTDIR/resources.csv"

server_pid=''
cleanup() {
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    kill -TERM "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

run_target() {
  local target=$1
  local binary=$2
  local port=$3
  local server_log="$OUTDIR/${target}.log"

  echo "starting $target on port $port"
  if [[ "$target" == "luring" ]]; then
    BIND_HOST=127.0.0.1 PORT="$port" URING_WORKERS="$WORKERS" \
      URING_ENTRIES="$ENTRIES" FRAME_POOL="$FRAME_POOL" \
      "$binary" >"$server_log" 2>&1 &
  else
    PORT="$port" URING_WORKERS="$WORKERS" URING_ENTRIES="$ENTRIES" \
      "$binary" >"$server_log" 2>&1 &
  fi
  server_pid=$!

  local ready=0
  for _ in $(seq 1 100); do
    if ! kill -0 "$server_pid" 2>/dev/null; then
      echo "$target exited before readiness; see $server_log" >&2
      return 1
    fi
    if NO_PROXY=127.0.0.1 no_proxy=127.0.0.1 \
      curl --noproxy '*' --max-time 1 -fsS "http://127.0.0.1:${port}/" \
      -o /dev/null; then
      ready=1
      break
    fi
    sleep 0.1
  done
  if (( ready == 0 )); then
    echo "$target did not become ready; see $server_log" >&2
    return 1
  fi

  for concurrency in $LEVELS; do
    echo "$target warmup c=$concurrency"
    "$WRK" -t"$THREADS" -c"$concurrency" -d"$WARMUP" --timeout "$TIMEOUT" \
      "http://127.0.0.1:${port}/" >"$OUTDIR/raw/${target}-${concurrency}-warmup.txt" 2>&1

    for round in $(seq 1 "$ROUNDS"); do
      local raw="$OUTDIR/raw/${target}-${concurrency}-r${round}.txt"
      echo "$target round=$round c=$concurrency"
      "$WRK" -t"$THREADS" -c"$concurrency" -d"$DURATION" --latency \
        --timeout "$TIMEOUT" "http://127.0.0.1:${port}/" >"$raw" 2>&1

      local cpu rss
      read -r cpu rss < <(ps -p "$server_pid" -o %cpu=,rss=)
      printf '%s,%s,%s,%s,%s\n' "$target" "$concurrency" "$round" "$cpu" "$rss" \
        >>"$OUTDIR/resources.csv"
    done
  done

  kill -TERM "$server_pid"
  wait "$server_pid" || true
  server_pid=''
}

run_target luring "$LURING_BIN" "$PORT"
run_target raw-liburing "$RAW_BIN" "$((PORT + 1))"

echo "results: $OUTDIR"

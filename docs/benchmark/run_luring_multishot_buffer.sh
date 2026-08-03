#!/usr/bin/env bash
set -euo pipefail

# Compare the regular LUringStream receive path with the multishot provided
# buffer path. Results are intentionally written outside the repository.

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BUILD_DIR=${BUILD_DIR:-"$ROOT_DIR/build-uring"}
OUTDIR=${OUTDIR:-/tmp/coropact-luring-multishot-buffer-$(date +%Y%m%d-%H%M%S)}
WRK=${WRK:-wrk}
THREADS=${THREADS:-8}
LEVELS=${LEVELS:-"100 200 500 1000 2000 5000 10000"}
WARMUP=${WARMUP:-2s}
DURATION=${DURATION:-5s}
ROUNDS=${ROUNDS:-3}
TIMEOUT=${TIMEOUT:-5s}
CONSUMER=${CONSUMER:-adapter}
WORKERS=${URING_WORKERS:-4}
ENTRIES=${URING_ENTRIES:-1024}
BASE_PORT=${BASE_PORT:-19190}
SHARED_BUFFER_CAPACITY=${SHARED_BUFFER_CAPACITY:-1024}

BASELINE_BIN=${BASELINE_BIN:-"$BUILD_DIR/examples/luring/demo_bench_http_luring"}
MULTISHOT_BIN=${MULTISHOT_BIN:-"$BUILD_DIR/examples/luring/demo_bench_http_luring_multishot"}

mkdir -p "$OUTDIR/raw"
printf 'target,concurrency,round,cpu_percent,rss_kb\n' >"$OUTDIR/resources.csv"

server_pid=''
cleanup() {
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    kill -TERM "$server_pid" 2>/dev/null || true
    for _ in $(seq 1 20); do
      kill -0 "$server_pid" 2>/dev/null || break
      sleep 0.1
    done
    if kill -0 "$server_pid" 2>/dev/null; then
      kill -KILL "$server_pid" 2>/dev/null || true
    fi
    wait "$server_pid" 2>/dev/null || true
  fi
  server_pid=''
}
trap cleanup EXIT INT TERM

wait_ready() {
  local port=$1
  for _ in $(seq 1 100); do
    if ! kill -0 "$server_pid" 2>/dev/null; then
      return 1
    fi
    if curl --noproxy '*' --max-time 1 -fsS \
      "http://127.0.0.1:${port}/" -o /dev/null; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

run_target() {
  local target=$1
  local binary=$2
  local port=$3
  local storage=${4:-}
  local server_log="$OUTDIR/${target}.log"

  echo "starting $target on port $port"
  if [[ -n "$storage" ]]; then
    BIND_HOST=127.0.0.1 PORT="$port" URING_WORKERS="$WORKERS" \
      URING_ENTRIES="$ENTRIES" BUFFER_STORAGE="$storage" \
      BUFFER_CONSUMER="$CONSUMER" SHARED_BUFFER_CAPACITY="$SHARED_BUFFER_CAPACITY" \
      "$binary" >"$server_log" 2>&1 &
  else
    BIND_HOST=127.0.0.1 PORT="$port" URING_WORKERS="$WORKERS" \
      URING_ENTRIES="$ENTRIES" SHARED_BUFFER_CAPACITY="$SHARED_BUFFER_CAPACITY" \
      "$binary" >"$server_log" 2>&1 &
  fi
  server_pid=$!

  if ! wait_ready "$port"; then
    echo "$target did not become ready; see $server_log" >&2
    return 1
  fi

  for concurrency in $LEVELS; do
    echo "$target warmup c=$concurrency"
    "$WRK" -t"$THREADS" -c"$concurrency" -d"$WARMUP" \
      --timeout "$TIMEOUT" "http://127.0.0.1:${port}/" \
      >"$OUTDIR/raw/${target}-${concurrency}-warmup.txt" 2>&1

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

  cleanup
}

run_target regular "$BASELINE_BIN" "$BASE_PORT"
run_target multishot-vector "$MULTISHOT_BIN" "$((BASE_PORT + 1))" vector
run_target multishot-mmap "$MULTISHOT_BIN" "$((BASE_PORT + 2))" mmap

echo "results: $OUTDIR"

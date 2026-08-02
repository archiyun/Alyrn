#!/usr/bin/env bash
set -euo pipefail

# Unified fixed HTTP keep-alive benchmark for the current checkout.
# The adapters all use the same request framing and 512-byte response. Results
# and generated wrk output stay outside the repository by default.

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
OUTDIR=${OUTDIR:-/tmp/coropact-network-libraries-$(date +%Y%m%d-%H%M%S)}
BUILD_DIR=${BUILD_DIR:-"$ROOT_DIR/build-uring"}
RUST_TARGET_DIR=${RUST_TARGET_DIR:-"$ROOT_DIR/benchmarks/network-libraries-rust/target/release"}
WRK=${WRK:-wrk}
THREADS=${THREADS:-8}
LEVELS=${LEVELS:-"100 200 500 1000 2000 5000 10000"}
WARMUP=${WARMUP:-5s}
DURATION=${DURATION:-10s}
ROUNDS=${ROUNDS:-3}
TIMEOUT=${TIMEOUT:-5s}
WORKERS=${WORKERS:-4}
ENTRIES=${ENTRIES:-8192}
FRAME_POOL=${FRAME_POOL:-1}
REACTOR_TRIGGER_MODE=${REACTOR_TRIGGER_MODE:-et}
PORT_BASE=${PORT_BASE:-19090}
TARGETS=${TARGETS:-"reactor luring raw-liburing asio monoio compio libaio libuv libevent libev"}

REACTOR_BIN=${REACTOR_BIN:-"$BUILD_DIR/examples/net/demo_bench_http_reactor"}
LURING_BIN=${LURING_BIN:-"$BUILD_DIR/examples/luring/demo_bench_http_luring"}
RAW_LIBURING_BIN=${RAW_LIBURING_BIN:-"$BUILD_DIR/examples/luring/demo_bench_http_liburing"}
ASIO_BIN=${ASIO_BIN:-"$BUILD_DIR/examples/net/demo_bench_http_asio"}
MONOIO_BIN=${MONOIO_BIN:-"$RUST_TARGET_DIR/bench-http-monoio"}
COMPIO_BIN=${COMPIO_BIN:-"$RUST_TARGET_DIR/bench-http-compio"}
LIBAIO_BIN=${LIBAIO_BIN:-"$BUILD_DIR/examples/net/demo_bench_http_libaio"}
LIBUV_BIN=${LIBUV_BIN:-"$BUILD_DIR/examples/net/demo_bench_http_libuv"}
LIBEVENT_BIN=${LIBEVENT_BIN:-"$BUILD_DIR/examples/net/demo_bench_http_libevent"}
LIBEV_BIN=${LIBEV_BIN:-"$BUILD_DIR/examples/net/demo_bench_http_libev"}

mkdir -p "$OUTDIR/raw"
printf 'target,concurrency,round,cpu_percent,rss_kb\n' >"$OUTDIR/resources.csv"
printf 'target,concurrency,round,log\n' >"$OUTDIR/runs.csv"

server_pid=''
cleanup() {
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    kill -TERM "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

binary_for() {
  case "$1" in
    reactor) printf '%s\n' "$REACTOR_BIN" ;;
    luring) printf '%s\n' "$LURING_BIN" ;;
    raw-liburing) printf '%s\n' "$RAW_LIBURING_BIN" ;;
    asio) printf '%s\n' "$ASIO_BIN" ;;
    monoio) printf '%s\n' "$MONOIO_BIN" ;;
    compio) printf '%s\n' "$COMPIO_BIN" ;;
    libaio) printf '%s\n' "$LIBAIO_BIN" ;;
    libuv) printf '%s\n' "$LIBUV_BIN" ;;
    libevent) printf '%s\n' "$LIBEVENT_BIN" ;;
    libev) printf '%s\n' "$LIBEV_BIN" ;;
    *) echo "unknown target: $1" >&2; return 1 ;;
  esac
}

start_target() {
  local target=$1
  local port=$2
  local binary
  binary=$(binary_for "$target")
  local log="$OUTDIR/${target}.log"

  case "$target" in
    reactor)
      PORT="$port" REACTOR_WORKERS="$WORKERS" FRAME_POOL="$FRAME_POOL" \
        REACTOR_TRIGGER_MODE="$REACTOR_TRIGGER_MODE" \
        "$binary" >"$log" 2>&1 & ;;
    luring)
      BIND_HOST=127.0.0.1 PORT="$port" URING_WORKERS="$WORKERS" URING_ENTRIES="$ENTRIES" \
        FRAME_POOL="$FRAME_POOL" "$binary" >"$log" 2>&1 & ;;
    raw-liburing)
      PORT="$port" URING_WORKERS="$WORKERS" URING_ENTRIES="$ENTRIES" \
        "$binary" >"$log" 2>&1 & ;;
    asio)
      PORT="$port" ASIO_WORKERS="$WORKERS" "$binary" >"$log" 2>&1 & ;;
    monoio)
      PORT="$port" MONOIO_WORKERS="$WORKERS" "$binary" >"$log" 2>&1 & ;;
    compio)
      PORT="$port" COMPIO_WORKERS="$WORKERS" "$binary" >"$log" 2>&1 & ;;
    libaio)
      PORT="$port" LIBAIO_WORKERS="$WORKERS" "$binary" >"$log" 2>&1 & ;;
    libuv)
      PORT="$port" LIBUV_WORKERS="$WORKERS" "$binary" >"$log" 2>&1 & ;;
    libevent)
      PORT="$port" LIBEVENT_WORKERS="$WORKERS" "$binary" >"$log" 2>&1 & ;;
    libev)
      PORT="$port" LIBEV_WORKERS="$WORKERS" "$binary" >"$log" 2>&1 & ;;
  esac
  server_pid=$!
}

run_target() {
  local target=$1
  local port=$2
  local server_log="$OUTDIR/${target}.log"

  echo "starting $target on port $port"
  start_target "$target" "$port"

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
      "http://127.0.0.1:${port}/" >"$OUTDIR/raw/${target}-${concurrency}-warmup.txt" 2>&1 || true

    for round in $(seq 1 "$ROUNDS"); do
      local raw="$OUTDIR/raw/${target}-${concurrency}-r${round}.txt"
      echo "$target round=$round c=$concurrency"
      "$WRK" -t"$THREADS" -c"$concurrency" -d"$DURATION" --latency \
        --timeout "$TIMEOUT" "http://127.0.0.1:${port}/" >"$raw" 2>&1 || true
      printf '%s,%s,%s,%s\n' "$target" "$concurrency" "$round" "$raw" >>"$OUTDIR/runs.csv"

      local cpu='NA' rss='NA'
      if kill -0 "$server_pid" 2>/dev/null; then
        read -r cpu rss < <(ps -p "$server_pid" -o %cpu=,rss=)
      fi
      printf '%s,%s,%s,%s,%s\n' "$target" "$concurrency" "$round" "$cpu" "$rss" \
        >>"$OUTDIR/resources.csv"
    done
  done

  kill -TERM "$server_pid" 2>/dev/null || true
  wait "$server_pid" 2>/dev/null || true
  server_pid=''
}

index=0
for target in $TARGETS; do
  binary=$(binary_for "$target")
  if [[ ! -x "$binary" ]]; then
    echo "missing executable for $target: $binary" >&2
    exit 1
  fi
  run_target "$target" "$((PORT_BASE + index))"
  index=$((index + 1))
done

echo "results: $OUTDIR"

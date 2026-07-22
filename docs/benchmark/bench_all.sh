#!/usr/bin/env bash
# docs/benchmark/bench_all.sh
# One-shot orchestrator for the C10k gateway-vs-nginx benchmark.
# Brings up the shared upstream backends, then benchmarks the runtime gateway
# and the nginx reverse-proxy gateway under identical conditions.
#
# Prereqs: nginx + wrk installed; project built in ./build-release.
#   cmake -B build-release -G Ninja -DCMAKE_BUILD_TYPE=Release
#   cmake --build build-release -j"$(nproc)" --target demo_bench_gateway_multi
#
# Usage (from repo root):
#   docs/benchmark/bench_all.sh
#
# Env knobs are forwarded to run_bench.sh (DURATION, ROUNDS, THREADS, LEVELS...).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BD="$ROOT/docs/benchmark"
GW_BIN="$ROOT/build-release/examples/gateway/demo_bench_gateway_multi"
cleanup() {
  set +e
  nginx -s stop -c "$BD/nginx_gateway.conf"  2>/dev/null
  nginx -s stop -c "$BD/nginx_upstream.conf" 2>/dev/null
  [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null
  wait 2>/dev/null
}
trap cleanup EXIT

echo "==> ulimit -n: $(ulimit -n)"
[[ -x "$GW_BIN" ]] || { echo "missing $GW_BIN; build first" >&2; exit 1; }

echo "==> starting upstream nginx (9001-9004)"
nginx -t -c "$BD/nginx_upstream.conf"
nginx    -c "$BD/nginx_upstream.conf"
sleep 1
curl -fs http://127.0.0.1:9001/ >/dev/null && echo "    upstream ok ($(curl -s http://127.0.0.1:9001/ | wc -c) bytes)"

# ---- runtime gateway on :8080 ----
echo "==> starting runtime gateway (:8080)"
UPSTREAM_PORTS=9001,9002,9003,9004 LB_ALGO=round_robin PORT=8080 \
  "$GW_BIN" &
GW_PID=$!
sleep 1
curl -fs http://127.0.0.1:8080/ >/dev/null && echo "    gateway ok"
"$BD/run_bench.sh" http://127.0.0.1:8080/ gateway "demo_bench_gateway_multi"
kill "$GW_PID" 2>/dev/null; wait "$GW_PID" 2>/dev/null || true; GW_PID=""

# ---- nginx gateway on :8088 ----
echo "==> starting nginx gateway (:8088)"
nginx -t -c "$BD/nginx_gateway.conf"
nginx    -c "$BD/nginx_gateway.conf"
sleep 1
curl -fs http://127.0.0.1:8088/ >/dev/null && echo "    nginx gateway ok"
# Sample only the gateway nginx master + its workers (pidfile based) so the
# shared upstream nginx workers are excluded from CPU/RSS.
"$BD/run_bench.sh" http://127.0.0.1:8088/ nginx "@/tmp/nginx_gateway.pid"
nginx -s stop -c "$BD/nginx_gateway.conf"

echo "==> all done. CSVs under $BD/results/"

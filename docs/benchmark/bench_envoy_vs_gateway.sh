#!/usr/bin/env bash
# docs/benchmark/bench_envoy_vs_gateway.sh
# One-shot orchestrator for gateway-vs-Envoy reverse-proxy benchmarking.
#
# Prereqs: nginx + wrk installed; project built in ./build-release.
# Envoy can be supplied via ENVOY_BIN when it is not installed on PATH.
#
# Usage:
#   ENVOY_BIN=/path/to/envoy docs/benchmark/bench_envoy_vs_gateway.sh
#
# Env knobs are forwarded to run_bench.sh (DURATION, ROUNDS, THREADS, LEVELS...).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BD="$ROOT/docs/benchmark"
OUTDIR=${OUTDIR:-"$BD/results-envoy-vs-gateway"}
GW_BIN="$ROOT/build-release/examples/gateway/demo_bench_gateway_multi"
ENVOY_BIN=${ENVOY_BIN:-envoy}
ENVOY_CONCURRENCY=${ENVOY_CONCURRENCY:-4}
NOFILE=${NOFILE:-200000}

mkdir -p "$OUTDIR"
mkdir -p /tmp/nginx_upstream_tmp/client \
         /tmp/nginx_upstream_tmp/proxy \
         /tmp/nginx_upstream_tmp/fastcgi \
         /tmp/nginx_upstream_tmp/uwsgi \
         /tmp/nginx_upstream_tmp/scgi

cleanup() {
  set +e
  nginx -s stop -c "$BD/nginx_upstream.conf" 2>/dev/null
  [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null
  [[ -n "${ENVOY_PID:-}" ]] && kill "$ENVOY_PID" 2>/dev/null
  wait 2>/dev/null
  rm -f /tmp/coropact_gateway.pid /tmp/envoy_gateway.pid
}
trap cleanup EXIT

ulimit -n "$NOFILE" 2>/dev/null || true

echo "==> ulimit -n: $(ulimit -n)"
[[ -x "$GW_BIN" ]] || { echo "missing $GW_BIN; build first" >&2; exit 1; }
[[ -x "$ENVOY_BIN" ]] || { echo "missing executable ENVOY_BIN=$ENVOY_BIN" >&2; exit 1; }

echo "==> validating envoy config"
"$ENVOY_BIN" --mode validate -c "$BD/envoy_gateway.yaml" >/dev/null

echo "==> starting upstream nginx (9001-9004)"
nginx -s stop -c "$BD/nginx_upstream.conf" 2>/dev/null || true
nginx -t -c "$BD/nginx_upstream.conf"
nginx    -c "$BD/nginx_upstream.conf"
sleep 1
curl -fs http://127.0.0.1:9001/ >/dev/null
echo "    upstream ok ($(curl -s http://127.0.0.1:9001/ | wc -c) bytes)"

echo "==> starting runtime gateway (:8080)"
UPSTREAM_PORTS=9001,9002,9003,9004 LB_ALGO=round_robin PORT=8080 \
  "$GW_BIN" > "$OUTDIR/gateway.log" 2>&1 &
GW_PID=$!
echo "$GW_PID" > /tmp/coropact_gateway.pid
sleep 1
curl -fs http://127.0.0.1:8080/ >/dev/null
echo "    gateway ok"
"$BD/run_bench.sh" http://127.0.0.1:8080/ gateway "@/tmp/coropact_gateway.pid" "$OUTDIR"
kill "$GW_PID" 2>/dev/null; wait "$GW_PID" 2>/dev/null || true; GW_PID=""

echo "==> starting envoy (:8090)"
"$ENVOY_BIN" -c "$BD/envoy_gateway.yaml" \
  --concurrency "$ENVOY_CONCURRENCY" \
  --log-level warn > "$OUTDIR/envoy.log" 2>&1 &
ENVOY_PID=$!
echo "$ENVOY_PID" > /tmp/envoy_gateway.pid
sleep 2
curl -fs http://127.0.0.1:8090/ >/dev/null
echo "    envoy ok"
"$BD/run_bench.sh" http://127.0.0.1:8090/ envoy "@/tmp/envoy_gateway.pid" "$OUTDIR"
kill "$ENVOY_PID" 2>/dev/null; wait "$ENVOY_PID" 2>/dev/null || true; ENVOY_PID=""

echo "==> all done. CSVs under $OUTDIR/"

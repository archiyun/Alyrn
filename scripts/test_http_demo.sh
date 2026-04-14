#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/build-tests}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-18081}"
IO_THREADS="${IO_THREADS:-2}"
STATIC_ROOT="${STATIC_ROOT:-$ROOT_DIR/examples/www}"
LOG_FILE="${LOG_FILE:-$BUILD_DIR/demo_http_server.log}"

SERVER_PID=""

cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

print_section() {
  printf '\n== %s ==\n' "$1"
}

wait_for_server() {
  local attempts=40
  local i
  for ((i = 1; i <= attempts; ++i)); do
    if curl -fsS --http1.1 "http://${HOST}:${PORT}/api/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  echo "server did not become ready at http://${HOST}:${PORT}" >&2
  return 1
}

show_network_state() {
  print_section "socket state"
  if command -v ss >/dev/null 2>&1; then
    ss -tanp | grep ":${PORT}" || true
  elif command -v netstat >/dev/null 2>&1; then
    netstat -tan | grep "[.:]${PORT}[[:space:]]" || true
  else
    echo "ss/netstat not available"
  fi

  if command -v lsof >/dev/null 2>&1; then
    lsof -n -P -iTCP:"${PORT}" || true
  else
    echo "lsof not available"
  fi
}

require_cmd cmake
require_cmd curl

mkdir -p "${BUILD_DIR}"

print_section "build"
cmake -S "${ROOT_DIR}" -B "${BUILD_DIR}" >/dev/null
cmake --build "${BUILD_DIR}" --target demo_http_server >/dev/null

rm -f "${LOG_FILE}"

print_section "start server"
(
  cd "${BUILD_DIR}"
  HOST="${HOST}" \
  PORT="${PORT}" \
  IO_THREADS="${IO_THREADS}" \
  ./examples/demo_http_server
) &
SERVER_PID=$!
echo "pid=${SERVER_PID} host=${HOST} port=${PORT}"

wait_for_server

print_section "health check"
curl -i --http1.1 "http://${HOST}:${PORT}/api/health"

print_section "json echo"
curl -i --http1.1 \
  -H 'Content-Type: application/json' \
  -d '{"hello":"runtime"}' \
  "http://${HOST}:${PORT}/api/echo?src=script"

print_section "404"
curl -i --http1.1 "http://${HOST}:${PORT}/missing" || true

print_section "405"
curl -i --http1.1 -X PUT "http://${HOST}:${PORT}/api/health" || true

print_section "keep-alive reuse"
curl -sv --http1.1 \
  -H 'Connection: keep-alive' \
  "http://${HOST}:${PORT}/api/health" \
  "http://${HOST}:${PORT}/api/health" \
  -o /dev/null 2>&1 | grep -E 'Re-using existing connection|Connected to|Connection #0' || true

show_network_state

print_section "recent log"
tail -n 20 "${LOG_FILE}" || true

print_section "manual commands"
cat <<EOF
curl -i --http1.1 http://${HOST}:${PORT}/api/health
curl -i --http1.1 -d '{"x":1}' http://${HOST}:${PORT}/api/echo
curl -i --http1.1 -X PUT http://${HOST}:${PORT}/api/health
tail -f ${LOG_FILE}
ss -tanp | grep :${PORT}
lsof -n -P -iTCP:${PORT}
EOF

#!/usr/bin/env bash
# bench_poller.sh — Compare QPS across epoll / poll / select backends.
#
# Round 1 (CONNS_LOW):  all three backends — baseline comparison
# Round 2 (CONNS_HIGH): epoll + poll only — select skipped (FD_SETSIZE ceiling)
#
# Usage:
#   ./scripts/bench_poller.sh              # auto-build, default params
#   ./scripts/bench_poller.sh --no-build   # skip cmake build
#   CONNS_LOW=50 CONNS_HIGH=300 ./scripts/bench_poller.sh

set -euo pipefail

# ── tunables (env-overridable) ────────────────────────────────────────────────
CONNS_LOW="${CONNS_LOW:-100}"
CONNS_HIGH="${CONNS_HIGH:-500}"
DURATION="${DURATION:-10s}"
WRK_THREADS="${WRK_THREADS:-4}"
IO_THREADS="${IO_THREADS:-4}"
PORT="${PORT:-19091}"
HOST="${HOST:-127.0.0.1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build-bench"
SERVER_BIN="${BUILD_DIR}/examples/demo_http_server"
TARGET_URL="http://${HOST}:${PORT}/api/health"
SERVER_PID=""

# ── colours ───────────────────────────────────────────────────────────────────
BOLD=$'\033[1m'; DIM=$'\033[2m'
RED=$'\033[0;31m'; GREEN=$'\033[0;32m'
YELLOW=$'\033[1;33m'; CYAN=$'\033[0;36m'; NC=$'\033[0m'

log()  { echo -e "${CYAN}[bench]${NC} $*"; }
ok()   { echo -e "${GREEN}  ✓${NC}  $*"; }
warn() { echo -e "${YELLOW}  !${NC}  $*"; }
die()  { echo -e "${RED}[ERR]${NC} $*" >&2; exit 1; }
hr()   { printf '%0.s─' {1..55}; echo; }

# ── build ─────────────────────────────────────────────────────────────────────
build_server() {
    log "Configuring Release build → ${BUILD_DIR}"
    cmake -B "${BUILD_DIR}" -S "${ROOT_DIR}" \
          -DCMAKE_BUILD_TYPE=Release \
          -DBUILD_TESTS=OFF \
          -DBUILD_EXAMPLES=ON \
          -DCMAKE_CXX_FLAGS="-O2" \
          > /dev/null 2>&1 || die "cmake configure failed"
    log "Compiling demo_http_server…"
    cmake --build "${BUILD_DIR}" --target demo_http_server -j"$(nproc)" \
          > /dev/null 2>&1 || die "cmake build failed"
    ok "Built: ${SERVER_BIN}"
}

# ── server lifecycle ──────────────────────────────────────────────────────────
start_server() {
    local poller="$1"
    RUNTIME_POLLER="${poller}" IO_THREADS="${IO_THREADS}" PORT="${PORT}" \
        "${SERVER_BIN}" > /dev/null 2>&1 &
    SERVER_PID=$!
    # wait until the HTTP port is accepting requests (max 6s)
    local i=0
    while (( i++ < 30 )); do
        if curl -sf "${TARGET_URL}" > /dev/null 2>&1; then return 0; fi
        sleep 0.2
    done
    kill_server
    return 1
}

kill_server() {
    if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
        kill "${SERVER_PID}" 2>/dev/null || true
        wait "${SERVER_PID}" 2>/dev/null || true
    fi
    SERVER_PID=""
}

cleanup() { kill_server; }
trap cleanup EXIT INT TERM

# ── wrk runner ────────────────────────────────────────────────────────────────
# Outputs two lines: QPS and avg latency
run_wrk() {
    local conns="$1"
    local raw
    raw=$(wrk -t"${WRK_THREADS}" -c"${conns}" -d"${DURATION}" \
              --latency "${TARGET_URL}" 2>&1)

    local qps lat
    qps=$(echo "${raw}"    | awk '/Requests\/sec/{print $2}')
    # "    Latency   2.82ms ..." — 4-space indent, second field is avg latency value
    lat=$(echo "${raw}"    | awk '/^    Latency/{print $2}')

    echo "${qps:-ERR}|${lat:-ERR}"
}

# ── single benchmark run ──────────────────────────────────────────────────────
# Args: label poller connections
# Prints  "label|qps|latency" to stdout via the global array below
declare -A RES_QPS=()
declare -A RES_LAT=()

bench_one() {
    local label="$1" poller="$2" conns="$3"
    log "Starting server [RUNTIME_POLLER=${poller}]…"
    if ! start_server "${poller}"; then
        warn "Server failed to start for ${label}"
        RES_QPS["${label}"]="FAIL"
        RES_LAT["${label}"]="FAIL"
        return
    fi
    ok "Server ready (PID=${SERVER_PID})"

    # brief warm-up
    for _ in 1 2 3; do curl -sf "${TARGET_URL}" > /dev/null 2>&1 || true; done
    sleep 0.3

    log "Running wrk  -t${WRK_THREADS} -c${conns} -d${DURATION}…"
    local result
    result=$(run_wrk "${conns}")

    RES_QPS["${label}"]="${result%%|*}"
    RES_LAT["${label}"]="${result##*|}"

    kill_server
    ok "Done — QPS=${RES_QPS[${label}]}  latency=${RES_LAT[${label}]}"
    sleep 0.8
}

# ── print table ───────────────────────────────────────────────────────────────
print_table() {
    local -n labels_ref=$1
    printf "\n"
    printf "  ${BOLD}%-14s  %-8s  %-14s  %s${NC}\n" \
           "Backend" "Conns" "QPS (req/s)" "Avg Latency"
    hr
    for key in "${labels_ref[@]}"; do
        printf "  %-14s  %-8s  %-14s  %s\n" \
               "${key}" \
               "${key_conns[${key}]}" \
               "${RES_QPS[${key}]:-—}" \
               "${RES_LAT[${key}]:-—}"
    done
    hr
}

# ── main ──────────────────────────────────────────────────────────────────────
main() {
    local do_build=1
    for arg in "$@"; do
        [[ "${arg}" == "--no-build" ]] && do_build=0
    done

    echo ""
    echo -e "${BOLD}╔═══════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║       Poller Backend QPS Benchmark                ║${NC}"
    echo -e "${BOLD}╚═══════════════════════════════════════════════════╝${NC}"
    echo -e "${DIM}  endpoint  : GET /api/health${NC}"
    echo -e "${DIM}  duration  : ${DURATION} per run${NC}"
    echo -e "${DIM}  wrk threads: ${WRK_THREADS}   io threads: ${IO_THREADS}${NC}"
    echo -e "${DIM}  round-1 conns: ${CONNS_LOW}  round-2 conns: ${CONNS_HIGH}${NC}"
    echo ""

    command -v wrk > /dev/null 2>&1 || die "wrk not found. Install: sudo apt install wrk  or  brew install wrk"

    if (( do_build )) || [[ ! -x "${SERVER_BIN}" ]]; then
        build_server
    else
        log "Skipping build (--no-build). Binary: ${SERVER_BIN}"
    fi
    echo ""

    # ── Round 1: all three backends, low concurrency ──────────────────────────
    echo -e "${BOLD}── Round 1 — ${CONNS_LOW} concurrent connections (all backends) ──${NC}"
    echo ""

    declare -A key_conns=()
    local round1_labels=("epoll[r1]" "poll[r1]" "select[r1]")
    key_conns["epoll[r1]"]="${CONNS_LOW}"
    key_conns["poll[r1]"]="${CONNS_LOW}"
    key_conns["select[r1]"]="${CONNS_LOW}"

    bench_one "epoll[r1]"  "epoll"  "${CONNS_LOW}"
    echo ""
    bench_one "poll[r1]"   "poll"   "${CONNS_LOW}"
    echo ""
    bench_one "select[r1]" "select" "${CONNS_LOW}"

    print_table round1_labels

    # ── Round 2: high concurrency, epoll + poll only ──────────────────────────
    echo ""
    echo -e "${BOLD}── Round 2 — ${CONNS_HIGH} concurrent connections (epoll & poll) ──${NC}"
    warn "select skipped: FD_SETSIZE=1024 caps usable connections at ~1000"
    echo ""

    local round2_labels=("epoll[r2]" "poll[r2]")
    key_conns["epoll[r2]"]="${CONNS_HIGH}"
    key_conns["poll[r2]"]="${CONNS_HIGH}"

    bench_one "epoll[r2]" "epoll" "${CONNS_HIGH}"
    echo ""
    bench_one "poll[r2]"  "poll"  "${CONNS_HIGH}"

    print_table round2_labels

    # ── Key takeaways ─────────────────────────────────────────────────────────
    echo ""
    echo -e "${BOLD}── Notes ──────────────────────────────────────────────${NC}"
    echo -e "  epoll  O(active events) — scales to C10K+, ideal for prod"
    echo -e "  poll   O(n) kernel scan — portable, fine for ≤ a few hundred conns"
    echo -e "  select O(n) scan + FD_SETSIZE hard cap — teaching / legacy compat"
    echo ""
}

main "$@"

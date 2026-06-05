#!/usr/bin/env bash
# docs/benchmark/run_bench.sh
# Sweep wrk concurrency levels against one target and record results.
#
# Usage:
#   run_bench.sh <url> <label> <cpu_match_regex> [outdir]
#
#   <url>              e.g. http://127.0.0.1:8080/
#   <label>           e.g. gateway | nginx   (used for filenames)
#   <cpu_match_regex> how to find the server process(es) to sample CPU/RSS for.
#                     Either a pgrep-style regex (e.g. demo_bench_gateway_multi)
#                     or "@/path/to/pidfile" to sample that master PID + all of
#                     its descendants (use this for nginx so the *shared*
#                     upstream nginx workers are not counted).
#   [outdir]          results dir (default: docs/benchmark/results)
#
# Env knobs:
#   DURATION   wrk test duration         (default 15s)
#   ROUNDS     rounds per level          (default 3)
#   THREADS    wrk threads               (default 4, matches server threads)
#   LEVELS     space-separated conn list (default "100 500 1000 5000 10000")
#   WARMUP     warmup seconds per level  (default 3)
#
# Emits:
#   <outdir>/<label>.csv             one row per (level,round) + avg rows
#   <outdir>/raw/<label>-<c>-r<n>.txt  raw wrk output
set -euo pipefail

URL=${1:?url}
LABEL=${2:?label}
CPU_RE=${3:?cpu match regex}
OUTDIR=${4:-"$(cd "$(dirname "$0")" && pwd)/results"}

DURATION=${DURATION:-15s}
ROUNDS=${ROUNDS:-3}
THREADS=${THREADS:-4}
LEVELS=${LEVELS:-"100 500 1000 5000 10000"}
WARMUP=${WARMUP:-3}

mkdir -p "$OUTDIR/raw"
CSV="$OUTDIR/$LABEL.csv"
echo "level,round,rps,lat_avg_ms,p50_ms,p90_ms,p99_ms,non2xx,sock_err,timeout,cpu_pct_avg,rss_mb_max" > "$CSV"

# Parse a wrk latency value like "1.23ms" / "950.00us" / "1.20s" into milliseconds.
to_ms() {
  local v=$1
  if   [[ $v == *us ]]; then awk -v x="${v%us}" 'BEGIN{printf "%.3f", x/1000}'
  elif [[ $v == *ms ]]; then awk -v x="${v%ms}" 'BEGIN{printf "%.3f", x}'
  elif [[ $v == *s  ]]; then awk -v x="${v%s}"  'BEGIN{printf "%.3f", x*1000}'
  else echo "$v"; fi
}

# Resolve the set of PIDs to sample: a master PID (from a pidfile) plus all its
# descendants, or every process whose args match a regex.
target_pids() {
  local spec=$1
  if [[ $spec == @* ]]; then
    local pidfile=${spec#@} master
    master=$(cat "$pidfile" 2>/dev/null) || return 0
    [[ -n $master ]] || return 0
    # master + children (nginx workers are direct children of the master)
    echo "$master"
    pgrep -P "$master" 2>/dev/null || true
  else
    pgrep -f "$spec" 2>/dev/null || true
  fi
}

CLK_TCK=$(getconf CLK_TCK 2>/dev/null || echo 100)

# Sum (utime+stime) jiffies across the target pids (threads roll up into the
# process's stat, so a multi-threaded gateway is captured by its single pid).
busy_jiffies() {
  local total=0 p u s
  for p in "$@"; do
    read -r u s < <(awk '{print $14, $15}' "/proc/$p/stat" 2>/dev/null) || continue
    [[ -n ${u:-} ]] && total=$((total + u + s))
  done
  echo "$total"
}

# Sum RSS (kB) across the target pids.
sum_rss_kb() {
  local total=0 p v
  for p in "$@"; do
    v=$(awk '/^VmRSS:/{print $2}' "/proc/$p/status" 2>/dev/null) || continue
    [[ -n ${v:-} ]] && total=$((total + v))
  done
  echo "$total"
}

# Background CPU/RSS sampler. Each second writes "cpu_pct summed_rss_kb" where
# cpu_pct is the true interval CPU (delta jiffies over wall time; can exceed 100
# across cores). We average cpu and take max rss across the run.
sample_cpu() {
  local spec=$1 out=$2
  : > "$out"
  local prev cur now last_t dt
  read -ra pids <<< "$(target_pids "$spec" | tr '\n' ' ')"
  prev=$(busy_jiffies "${pids[@]}")
  last_t=$(date +%s.%N)
  while :; do
    sleep 1
    read -ra pids <<< "$(target_pids "$spec" | tr '\n' ' ')"
    cur=$(busy_jiffies "${pids[@]}")
    now=$(date +%s.%N)
    dt=$(awk -v a="$last_t" -v b="$now" 'BEGIN{printf "%.4f", b-a}')
    awk -v d="$((cur - prev))" -v tck="$CLK_TCK" -v dt="$dt" -v rss="$(sum_rss_kb "${pids[@]}")" \
        'BEGIN{ if(dt>0) printf "%.1f %d\n", 100*(d/tck)/dt, rss }' >> "$out"
    prev=$cur; last_t=$now
  done
}

run_one() {
  local conn=$1 round=$2
  local raw="$OUTDIR/raw/$LABEL-$conn-r$round.txt"
  local samp; samp=$(mktemp)

  sample_cpu "$CPU_RE" "$samp" &
  local sampler=$!

  wrk -t"$THREADS" -c"$conn" -d"$DURATION" --latency --timeout 5s "$URL" > "$raw" 2>&1 || true

  kill "$sampler" 2>/dev/null || true; wait "$sampler" 2>/dev/null || true

  local rps lat_avg p50 p90 p99 non2xx sock_err timeout cpu_avg rss_max
  rps=$(awk '/^Requests\/sec:/{print $2}' "$raw"); rps=${rps:-0}
  lat_avg=$(to_ms "$(awk '/Latency/ && /Stdev/{next} /^[[:space:]]*Latency[[:space:]]/{print $2; exit}' "$raw")")
  p50=$(to_ms "$(awk '/^[[:space:]]*50%/{print $2}' "$raw")")
  p90=$(to_ms "$(awk '/^[[:space:]]*90%/{print $2}' "$raw")")
  p99=$(to_ms "$(awk '/^[[:space:]]*99%/{print $2}' "$raw")")
  non2xx=$(awk -F'[: ]+' '/Non-2xx or 3xx responses/{print $(NF)}' "$raw"); non2xx=${non2xx:-0}
  sock_err=$(awk '/Socket errors/{c=$4+$6+$8; gsub(",","",c); print $4+$6+$8}' "$raw" | tr -d ',' ); sock_err=${sock_err:-0}
  timeout=$(awk -F'timeout ' '/Socket errors/{print $2+0}' "$raw"); timeout=${timeout:-0}
  cpu_avg=$(awk '{c+=$1; n++} END{if(n>0) printf "%.1f", c/n; else print 0}' "$samp")
  rss_max=$(awk 'BEGIN{m=0} {if($2>m)m=$2} END{printf "%.1f", m/1024}' "$samp")

  rm -f "$samp"
  echo "$conn,$round,$rps,$lat_avg,$p50,$p90,$p99,$non2xx,$sock_err,$timeout,$cpu_avg,$rss_max" | tee -a "$CSV"
}

echo "### benchmark target=$LABEL url=$URL threads=$THREADS dur=$DURATION rounds=$ROUNDS"
for c in $LEVELS; do
  echo "--- warmup ${WARMUP}s @ c=$c ---"
  wrk -t"$THREADS" -c"$c" -d"${WARMUP}s" --timeout 5s "$URL" >/dev/null 2>&1 || true
  for r in $(seq 1 "$ROUNDS"); do
    run_one "$c" "$r"
  done
done

echo "### done -> $CSV"

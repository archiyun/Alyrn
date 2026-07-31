#!/usr/bin/env bash
set -euo pipefail

OUTDIR=${1:?usage: summarize_network_libraries.sh OUTDIR}
RUNS="$OUTDIR/runs.csv"
SUMMARY="$OUTDIR/summary.csv"
AVERAGES="$OUTDIR/averages.csv"

printf 'target,concurrency,round,rps,p99_ms,connect_errors,read_errors,write_errors,timeouts,non_2xx\n' >"$SUMMARY"
while IFS=, read -r target concurrency round raw; do
  [[ "$target" == "target" ]] && continue
  result=$(awk '
    function percentile_ms(value, unit) {
      if (unit == "us") return value / 1000.0
      if (unit == "s") return value * 1000.0
      return value
    }
    /99%/ {
      value = $2
      unit = value
      sub(/^[0-9.]+/, "", unit)
      sub(/[^0-9.].*$/, "", value)
      p99 = percentile_ms(value + 0, unit)
    }
    /Requests\/sec:/ { rps = $2 }
    /Socket errors:/ {
      for (i = 1; i <= NF; ++i) {
        if ($i == "connect") connect_errors = $(i + 1) + 0
        if ($i == "read") read_errors = $(i + 1) + 0
        if ($i == "write") write_errors = $(i + 1) + 0
        if ($i == "timeout") timeouts = $(i + 1) + 0
      }
    }
    /Non-2xx or 3xx responses:/ { non_2xx = $NF + 0 }
    END {
      printf "%.6f,%.6f,%d,%d,%d,%d,%d", rps + 0, p99 + 0, \
        connect_errors + 0, read_errors + 0, write_errors + 0, timeouts + 0, non_2xx + 0
    }
  ' "$raw")
  printf '%s,%s,%s,%s\n' "$target" "$concurrency" "$round" "$result" >>"$SUMMARY"
done <"$RUNS"

printf 'target,concurrency,rounds,avg_rps,avg_p99_ms,total_connect_errors,total_read_errors,total_write_errors,total_timeouts,total_non_2xx\n' >"$AVERAGES"
awk -F, '
  NR == 1 { next }
  {
    key = $1 SUBSEP $2
    if (!(key in seen)) order[++count] = key
    seen[key] = 1
    rps[key] += $4
    p99[key] += $5
    connect_errors[key] += $6
    read_errors[key] += $7
    write_errors[key] += $8
    timeouts[key] += $9
    non_2xx[key] += $10
    rounds[key]++
  }
  END {
    for (i = 1; i <= count; ++i) {
      split(order[i], fields, SUBSEP)
      key = order[i]
      printf "%s,%s,%d,%.2f,%.2f,%d,%d,%d,%d,%d\n", fields[1], fields[2], rounds[key], \
        rps[key] / rounds[key], p99[key] / rounds[key], connect_errors[key], \
        read_errors[key], write_errors[key], timeouts[key], non_2xx[key]
    }
  }
' "$SUMMARY" >>"$AVERAGES"

cat "$AVERAGES"

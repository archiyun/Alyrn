# Envoy vs Gateway Benchmark Report

This report records the local reverse-proxy benchmark used to compare Envoy
against the project gateway.

## Method

- Upstream backend: `docs/benchmark/nginx_upstream.conf`.
- Backend ports: `127.0.0.1:9001-9004`.
- Backend payload: fixed 512-byte JSON response.
- Project gateway: `build-release/examples/gateway/demo_bench_gateway_multi`.
- Project gateway listen address: `127.0.0.1:8080`.
- Project gateway workers: `IO_THREADS=4`.
- Envoy version: `1.38.2`.
- Envoy config: `docs/benchmark/envoy_gateway.yaml`.
- Envoy listen address: `127.0.0.1:8090`.
- Envoy workers: `--concurrency 4`.
- Load generator: `wrk -t4 --latency`.
- File descriptor limit: `ulimit -n 200000`.

The benchmark orchestration script is:

```bash
docs/benchmark/bench_envoy_vs_gateway.sh
```

The adjusted run used:

```bash
DURATION=8s ROUNDS=2 WARMUP=2 THREADS=4 LEVELS="100 500 1000 5000 10000" \
MAX_CONCURRENT_REQUESTS=20000 \
OUTDIR=docs/benchmark/results-envoy-vs-gateway-max-concurrent-20000 \
ENVOY_BIN=/tmp/envoyproxy-bin/usr/bin/envoy \
docs/benchmark/bench_envoy_vs_gateway.sh
```

## Default Bulkhead Results

These results use the gateway benchmark default
`max_concurrent_requests=1024`.

| Connections | Gateway RPS | Envoy RPS | Gateway / Envoy | Gateway p99 ms | Envoy p99 ms | Gateway non-2xx | Envoy non-2xx |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 300043.14 | 112116.47 | 2.68 | 1.025 | 2.235 | 0 | 0 |
| 500 | 234449.40 | 86171.07 | 2.72 | 3.105 | 12.190 | 0 | 0 |
| 1000 | 210180.26 | 79665.39 | 2.64 | 6.550 | 23.365 | 0 | 0 |
| 5000 | 60854.03 | 66919.18 | 0.91 | 353.890 | 111.020 | 158482 | 0 |
| 10000 | 50741.26 | 65644.65 | 0.77 | 926.375 | 227.295 | 107465 | 0 |

At 5000 and 10000 client connections, the gateway returns many non-2xx
responses because the default upstream bulkhead limit is reached.

Raw data is under:

```text
docs/benchmark/results-envoy-vs-gateway/
```

## Adjusted Bulkhead Results

These results use `MAX_CONCURRENT_REQUESTS=20000` for the benchmark gateway.

| Connections | Gateway RPS | Envoy RPS | Gateway / Envoy | Gateway p99 ms | Envoy p99 ms | Gateway non-2xx | Envoy non-2xx |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 307091.74 | 117723.33 | 2.61 | 1.160 | 2.055 | 0 | 0 |
| 500 | 241067.83 | 89360.41 | 2.70 | 3.100 | 12.195 | 0 | 0 |
| 1000 | 216066.95 | 82587.58 | 2.62 | 6.340 | 21.565 | 0 | 0 |
| 5000 | 123826.12 | 68180.89 | 1.82 | 79.750 | 87.670 | 0 | 0 |
| 10000 | 98703.51 | 66785.94 | 1.48 | 170.690 | 195.450 | 0 | 0 |

Raw data is under:

```text
docs/benchmark/results-envoy-vs-gateway-max-concurrent-20000/
```

## Summary

With the default benchmark bulkhead of 1024 concurrent upstream requests, the
gateway outperforms Envoy up to 1000 connections but rejects excess load at
5000 and 10000 connections. Raising the benchmark bulkhead to 20000 removes the
non-2xx responses in the high-concurrency runs.

With `MAX_CONCURRENT_REQUESTS=20000`, the gateway has zero non-2xx responses
across all measured levels. It reaches 2.61x to 2.70x Envoy throughput from 100
to 1000 connections, 1.82x at 5000 connections, and 1.48x at 10000 connections,
with lower p99 latency and lower RSS at every measured level.

This is a short local benchmark. For release-quality numbers, run longer
durations and more rounds on an isolated host with CPU frequency and background
load controlled.

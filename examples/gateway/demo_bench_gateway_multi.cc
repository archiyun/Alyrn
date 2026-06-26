// examples/demo_bench_gateway_multi.cc
// 多上游网关 benchmark, 与 nginx 同条件对比.
//
// 与 demo_bench_gateway 区别:
//   - 支持多个 upstream peer (env: UPSTREAM_PORTS=9001,9002,9003,9004)
//   - 可选负载均衡算法 (env: LB_ALGO=round_robin|p2c|least_connection|...)
//
// 与 nginx 对比设置: 上游一律打到 nginx 多端口监听返回 ~512B JSON.
//
// 启动顺序:
//   ① 上游 nginx (4 端口, 见 README/nginx_upstream.conf 一并配)
//   ② 我们: UPSTREAM_PORTS=9001,9002,9003,9004 LB_ALGO=round_robin \
//          IO_THREADS=4 PORT=8080 ./build-release/examples/demo_bench_gateway_multi
//   ③ nginx 网关: 配置在 /tmp/nginx_gw_multi.conf, listen 8088
//
// 压测:
//   wrk -t4 -c100 -d15s --latency http://127.0.0.1:8080/
//   wrk -t4 -c100 -d15s --latency http://127.0.0.1:8088/

#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "vexo/gateway/gateway_server.h"
#include "vexo/gateway/upstream.h"
#include "vexo/gateway/upstream_peer.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/inet_address.h"

namespace {

std::vector<uint16_t> ParsePorts(std::string_view csv) {
  std::vector<uint16_t> out;
  std::size_t i = 0;
  while (i < csv.size()) {
    std::size_t j = csv.find(',', i);
    if (j == std::string_view::npos) j = csv.size();
    out.push_back(
        static_cast<uint16_t>(std::atoi(std::string(csv.substr(i, j - i)).c_str())));
    i = j + 1;
  }
  return out;
}

const char* EnvOr(const char* key, const char* def) {
  const char* v = std::getenv(key);
  return v ? v : def;
}

int EnvInt(const char* key, int def) {
  const char* v = std::getenv(key);
  return v ? std::atoi(v) : def;
}

std::size_t EnvSize(const char* key, std::size_t def) {
  const char* v = std::getenv(key);
  if (!v) return def;
  char* end = nullptr;
  unsigned long long parsed = std::strtoull(v, &end, 10);
  return end != v ? static_cast<std::size_t>(parsed) : def;
}

}  // namespace

int main() {
  const int      io_threads  = EnvInt("IO_THREADS", 4);
  const uint16_t listen_port = static_cast<uint16_t>(EnvInt("PORT", 8080));
  const auto     ports_csv   = std::string(EnvOr("UPSTREAM_PORTS", "9001"));
  const auto     algo        = std::string(EnvOr("LB_ALGO", "round_robin"));
  const auto     max_concurrent = EnvSize("MAX_CONCURRENT_REQUESTS", 1024);

  const auto ports = ParsePorts(ports_csv);
  if (ports.empty()) {
    std::fprintf(stderr, "no upstream ports parsed from '%s'\n", ports_csv.c_str());
    return 1;
  }

  std::signal(SIGPIPE, SIG_IGN);

  vexo::gateway::UpstreamRegistry reg;
  vexo::gateway::UpstreamConfig upstream_cfg;
  upstream_cfg.name = "backend";
  upstream_cfg.max_concurrent_requests = max_concurrent;
  auto us = std::make_shared<vexo::gateway::Upstream>(std::move(upstream_cfg));
  for (uint16_t p : ports) {
    us->AddPeer(std::make_shared<vexo::gateway::UpstreamPeer>(
        vexo::gateway::UpstreamPeerConfig{
            .name = "127.0.0.1:" + std::to_string(p),
            .host = "127.0.0.1",
            .port = p}));
  }
  reg.Add(us);

  vexo::net::EventLoop loop;
  vexo::gateway::GatewayServer gw(
      &loop,
      vexo::net::InetAddress(listen_port),
      "BenchGatewayMulti",
      reg);
  gw.set_thread_num(io_threads);
  gw.set_pool_config({.max_idle_per_peer = 64});

  gw.AddProxyRoute("/", "backend", algo);

  gw.Start();
  std::printf("BenchGatewayMulti listen=%u peers=[%s] algo=%s io_threads=%d max_concurrent=%zu\n",
              listen_port, ports_csv.c_str(), algo.c_str(), io_threads, max_concurrent);
  loop.Loop();
  return 0;
}

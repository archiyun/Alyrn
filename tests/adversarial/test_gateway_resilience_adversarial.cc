// Adversarial regression tests for gateway resilience components.
//
// These tests encode defensive invariants. They are intentionally stricter
// than smoke tests and are expected to fail when an implementation can be
// driven into a false-health or resource-exhaustion state.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

#include "vexo/gateway/circuit_breaker.h"
#include "vexo/gateway/health_check_config.h"
#include "vexo/gateway/health_checker.h"
#include "vexo/gateway/rate_limiter.h"
#include "vexo/gateway/upstream.h"
#include "vexo/gateway/upstream_peer.h"
#include "vexo/gateway/upstream_registry.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/inet_address.h"
#include "vexo/net/tcp_server.h"

namespace {

using namespace std::chrono_literals;

bool Expect(bool condition, const char* message) {
  if (!condition) {
    std::cerr << "[VULNERABLE] " << message << '\n';
    return false;
  }
  return true;
}

void Passed(const char* name) {
  std::cout << "[RESISTED] " << name << '\n';
}

std::uint16_t ReserveLoopbackPort() {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return 0;

  int on = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(0);
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return 0;
  }

  socklen_t len = static_cast<socklen_t>(sizeof(addr));
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
    ::close(fd);
    return 0;
  }

  const auto port = ntohs(addr.sin_port);
  ::close(fd);
  return port;
}

bool WaitForAtLeast(const std::atomic<int>& value, int target,
                    std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (value.load(std::memory_order_relaxed) >= target) return true;
    std::this_thread::sleep_for(5ms);
  }
  return value.load(std::memory_order_relaxed) >= target;
}

struct HealthTestEnv {
  std::thread loop_thread;
  vexo::net::EventLoop* loop{nullptr};
};

template <typename ConfigureServer>
HealthTestEnv StartHealthTest(
    std::uint16_t port,
    const std::shared_ptr<vexo::gateway::UpstreamPeer>& peer,
    vexo::gateway::HealthCheckConfig cfg,
    ConfigureServer configure_server) {
  auto upstream = std::make_shared<vexo::gateway::Upstream>(
      vexo::gateway::UpstreamConfig{.name = "adversarial-upstream"});
  upstream->AddPeer(peer);

  auto registry = std::make_shared<vexo::gateway::UpstreamRegistry>();
  registry->Add(upstream);

  std::promise<vexo::net::EventLoop*> loop_promise;
  auto loop_future = loop_promise.get_future();
  std::promise<void> started_promise;
  auto started_future = started_promise.get_future();

  std::thread thread(
      [port, cfg, registry, configure_server = std::move(configure_server),
       loop_promise = std::move(loop_promise),
       started_promise = std::move(started_promise)]() mutable {
        vexo::net::EventLoop loop;
        vexo::net::TcpServer server(
            &loop, vexo::net::InetAddress(port), "adversarial-health-backend");
        configure_server(server);
        server.Start();

        vexo::gateway::HealthChecker checker(&loop, *registry, cfg);
        checker.Start();

        loop_promise.set_value(&loop);
        started_promise.set_value();
        loop.Loop();
        checker.Stop();
      });

  auto* loop = loop_future.get();
  started_future.wait();
  return HealthTestEnv{.loop_thread = std::move(thread), .loop = loop};
}

void StopHealthTest(HealthTestEnv& env) {
  if (env.loop) env.loop->Quit();
  if (env.loop_thread.joinable()) env.loop_thread.join();
}

bool TestCircuitBreakerRequiresConsecutiveSuccesses() {
  vexo::gateway::CircuitBreakerConfig cfg;
  cfg.failure_threshold = 100;
  cfg.success_threshold = 2;
  vexo::gateway::CircuitBreaker breaker(cfg);

  breaker.OnFailure();
  breaker.OnSuccess();
  breaker.OnFailure();  // Must break the success streak.
  breaker.OnSuccess();

  if (!Expect(
          breaker.failure_count() == 2,
          "circuit breaker resets failures after non-consecutive successes")) {
    return false;
  }

  Passed("TestCircuitBreakerRequiresConsecutiveSuccesses");
  return true;
}

bool TestPerIPBucketLimitIsHard() {
  vexo::gateway::RateLimiterConfig cfg;
  cfg.per_ip_enabled = true;
  cfg.per_ip_rate = 0.000001;
  cfg.per_ip_burst = 1.0;
  cfg.per_ip_max_buckets = 8;
  vexo::gateway::RateLimiter limiter(cfg);

  // Simulate a high-cardinality identity spray. Every bucket remains active,
  // so an eviction policy that only drops full buckets cannot enforce the cap.
  for (int i = 0; i < 256; ++i) {
    limiter.AllowPerIP("198.51.100." + std::to_string(i));
  }

  if (!Expect(
          limiter.per_ip_bucket_count() <= cfg.per_ip_max_buckets,
          "per-IP bucket spray exceeds the configured cap and grows memory")) {
    std::cerr << "  configured cap=" << cfg.per_ip_max_buckets
              << " live buckets=" << limiter.per_ip_bucket_count() << '\n';
    return false;
  }

  Passed("TestPerIPBucketLimitIsHard");
  return true;
}

bool TestUpstreamBulkheadIsHard() {
  vexo::gateway::UpstreamConfig cfg;
  cfg.name = "bulkhead";
  cfg.max_concurrent_requests = 2;
  vexo::gateway::Upstream upstream(cfg);

  if (!Expect(upstream.TryAcquireRequestSlot(), "bulkhead slot 1 must pass")) {
    return false;
  }
  if (!Expect(upstream.TryAcquireRequestSlot(), "bulkhead slot 2 must pass")) {
    return false;
  }
  if (!Expect(!upstream.TryAcquireRequestSlot(),
              "bulkhead must reject above max_concurrent_requests")) {
    return false;
  }
  if (!Expect(upstream.active_requests() == 2,
              "rejected request must not inflate active slot count")) {
    return false;
  }

  upstream.ReleaseRequestSlot();
  if (!Expect(upstream.TryAcquireRequestSlot(),
              "released bulkhead slot must be reusable")) {
    return false;
  }
  upstream.ReleaseRequestSlot();
  upstream.ReleaseRequestSlot();

  if (!Expect(upstream.active_requests() == 0,
              "all bulkhead slots must be released exactly once")) {
    return false;
  }

  Passed("TestUpstreamBulkheadIsHard");
  return true;
}

bool TestTruncated200DoesNotForgeHealth() {
  const std::uint16_t port = ReserveLoopbackPort();
  if (!Expect(port != 0, "failed to reserve loopback port")) return false;

  auto peer = std::make_shared<vexo::gateway::UpstreamPeer>(
      vexo::gateway::UpstreamPeerConfig{
          .name = "truncated-200",
          .host = "127.0.0.1",
          .port = port,
      });
  peer->state().down.store(true, std::memory_order_relaxed);

  std::atomic<int> accepted{0};
  vexo::gateway::HealthCheckConfig cfg;
  cfg.interval_sec = 0.05;
  cfg.timeout_sec = 0.5;
  cfg.healthy_threshold = 1;
  cfg.unhealthy_threshold = 3;

  auto env = StartHealthTest(
      port, peer, cfg,
      [&accepted](vexo::net::TcpServer& server) {
        server.set_connection_callback(
            [&accepted](const vexo::net::TcpServer::TcpConnectionPtr& conn) {
              if (!conn->Connected()) return;
              accepted.fetch_add(1, std::memory_order_relaxed);
              // Exactly the prefix inspected by HealthChecker. There is no
              // complete status line, header block, or response body.
              conn->Send(std::string_view{"HTTP/1.1 200"});
            });
      });

  const bool connected = WaitForAtLeast(accepted, 1, 1500ms);
  std::this_thread::sleep_for(50ms);
  const bool still_down = peer->state().down.load(std::memory_order_relaxed);
  StopHealthTest(env);

  if (!Expect(connected, "health checker never reached malicious backend")) {
    return false;
  }
  if (!Expect(
          still_down,
          "a 12-byte 'HTTP/1.1 200' prefix forged a successful health check")) {
    return false;
  }

  Passed("TestTruncated200DoesNotForgeHealth");
  return true;
}

bool TestConnectionFailureBreaksHealthSuccessStreak() {
  const std::uint16_t port = ReserveLoopbackPort();
  if (!Expect(port != 0, "failed to reserve loopback port")) return false;

  auto peer = std::make_shared<vexo::gateway::UpstreamPeer>(
      vexo::gateway::UpstreamPeerConfig{
          .name = "non-consecutive-health",
          .host = "127.0.0.1",
          .port = port,
      });
  peer->state().down.store(true, std::memory_order_relaxed);

  std::atomic<int> accepted{0};
  vexo::gateway::HealthCheckConfig cfg;
  cfg.interval_sec = 0.1;
  cfg.timeout_sec = 0.5;
  cfg.healthy_threshold = 2;
  cfg.unhealthy_threshold = 100;

  auto env = StartHealthTest(
      port, peer, cfg,
      [&accepted](vexo::net::TcpServer& server) {
        server.set_connection_callback(
            [&accepted](const vexo::net::TcpServer::TcpConnectionPtr& conn) {
              if (!conn->Connected()) return;
              const int probe =
                  accepted.fetch_add(1, std::memory_order_relaxed) + 1;
              if (probe == 1 || probe == 3) {
                conn->Send(std::string_view{
                    "HTTP/1.1 200 OK\r\n"
                    "Content-Length: 0\r\n"
                    "Connection: close\r\n\r\n"});
              }
              conn->Shutdown();
            });
      });

  const bool saw_sequence = WaitForAtLeast(accepted, 3, 2s);
  std::this_thread::sleep_for(30ms);
  const bool still_down = peer->state().down.load(std::memory_order_relaxed);
  StopHealthTest(env);

  if (!Expect(saw_sequence, "health checker did not run the scripted probes")) {
    return false;
  }
  if (!Expect(
          still_down,
          "connection failure did not break the consecutive-success streak")) {
    return false;
  }

  Passed("TestConnectionFailureBreaksHealthSuccessStreak");
  return true;
}

bool TestHealthChecksDoNotOverlapPerPeer() {
  const std::uint16_t port = ReserveLoopbackPort();
  if (!Expect(port != 0, "failed to reserve loopback port")) return false;

  auto peer = std::make_shared<vexo::gateway::UpstreamPeer>(
      vexo::gateway::UpstreamPeerConfig{
          .name = "slow-health-backend",
          .host = "127.0.0.1",
          .port = port,
      });

  std::atomic<int> active{0};
  std::atomic<int> max_active{0};
  std::atomic<int> accepted{0};

  vexo::gateway::HealthCheckConfig cfg;
  cfg.interval_sec = 0.02;
  cfg.timeout_sec = 0.5;
  cfg.healthy_threshold = 2;
  cfg.unhealthy_threshold = 3;

  auto env = StartHealthTest(
      port, peer, cfg,
      [&active, &max_active, &accepted](vexo::net::TcpServer& server) {
        server.set_connection_callback(
            [&active, &max_active, &accepted](
                const vexo::net::TcpServer::TcpConnectionPtr& conn) {
              if (conn->Connected()) {
                accepted.fetch_add(1, std::memory_order_relaxed);
                const int current =
                    active.fetch_add(1, std::memory_order_relaxed) + 1;
                int observed = max_active.load(std::memory_order_relaxed);
                while (current > observed &&
                       !max_active.compare_exchange_weak(
                           observed, current, std::memory_order_relaxed)) {
                }
                // Intentionally never respond. Each probe should remain in
                // flight until timeout, exposing overlapping scheduling.
              } else {
                active.fetch_sub(1, std::memory_order_relaxed);
              }
            });
      });

  const bool first_probe_started = WaitForAtLeast(accepted, 1, 1s);
  // Stay inside the first probe's 500ms timeout. A vulnerable scheduler will
  // start a new connection every 20ms; a single-flight checker stays at one.
  std::this_thread::sleep_for(250ms);
  const int connections = accepted.load(std::memory_order_relaxed);
  const int peak = max_active.load(std::memory_order_relaxed);
  StopHealthTest(env);

  if (!Expect(first_probe_started, "health checker never started a probe")) {
    return false;
  }
  if (!Expect(
          connections == 1 && peak <= 1,
          "health checker overlaps probes for one peer and can exhaust sockets")) {
    std::cerr << "  connections before first timeout=" << connections << '\n';
    std::cerr << "  peak concurrent probes=" << peak << '\n';
    return false;
  }

  Passed("TestHealthChecksDoNotOverlapPerPeer");
  return true;
}

}  // namespace

int main() {
  int passed = 0;
  int total = 0;

#define RUN(test)                 \
  do {                            \
    ++total;                      \
    if (test()) ++passed;         \
  } while (false)

  RUN(TestCircuitBreakerRequiresConsecutiveSuccesses);
  RUN(TestPerIPBucketLimitIsHard);
  RUN(TestUpstreamBulkheadIsHard);
  RUN(TestTruncated200DoesNotForgeHealth);
  RUN(TestConnectionFailureBreaksHealthSuccessStreak);
  RUN(TestHealthChecksDoNotOverlapPerPeer);

  std::cout << "===========================\n"
            << passed << "/" << total << " adversarial invariants held.\n";
  return passed == total ? 0 : 1;
}

// Regression test for HealthChecker callback lifetime.
//
// Build and run with:
//   cmake -S . -B build-asan -DBUILD_TESTS=ON \
//     -DRUNTIME_SANITIZER=address,undefined
//   cmake --build build-asan --target health_checker_lifetime_reproducer
//   ./build-asan/tests/health_checker_lifetime_reproducer
//
// The test stops and destroys HealthChecker while one probe is in flight, then
// closes the backend connection. Delayed callbacks must neither access the
// destroyed checker nor mutate peer health after Stop().

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
#include <thread>

#include "runtime/gateway/health_check_config.h"
#include "runtime/gateway/health_checker.h"
#include "runtime/gateway/upstream.h"
#include "runtime/gateway/upstream_peer.h"
#include "runtime/gateway/upstream_registry.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/event_loop_thread.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/tcp_server.h"

namespace {

using namespace std::chrono_literals;

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

}  // namespace

int main() {
  const std::uint16_t port = ReserveLoopbackPort();
  if (port == 0) {
    std::cerr << "failed to reserve loopback port\n";
    return 2;
  }

  auto peer = std::make_shared<runtime::gateway::UpstreamPeer>(
      runtime::gateway::UpstreamPeerConfig{
          .name = "lifetime-target",
          .host = "127.0.0.1",
          .port = port,
      });
  auto upstream = std::make_shared<runtime::gateway::Upstream>(
      runtime::gateway::UpstreamConfig{.name = "lifetime-upstream"});
  upstream->AddPeer(peer);
  runtime::gateway::UpstreamRegistry registry;
  registry.Add(upstream);

  runtime::net::EventLoopThread loop_thread;
  runtime::net::EventLoop* loop = loop_thread.StartLoop();

  std::unique_ptr<runtime::net::TcpServer> server;
  std::unique_ptr<runtime::gateway::HealthChecker> checker;
  runtime::net::TcpServer::TcpConnectionPtr backend_conn;
  std::atomic<int> accepted{0};

  std::promise<void> started_promise;
  auto started = started_promise.get_future();
  loop->RunInLoop([&] {
    server = std::make_unique<runtime::net::TcpServer>(
        loop, runtime::net::InetAddress(port), "lifetime-backend");
    server->set_connection_callback(
        [&](const runtime::net::TcpServer::TcpConnectionPtr& conn) {
          if (conn->Connected()) {
            backend_conn = conn;
            accepted.fetch_add(1, std::memory_order_relaxed);
          }
        });
    server->Start();

    runtime::gateway::HealthCheckConfig cfg;
    cfg.interval_sec = 0.02;
    cfg.timeout_sec = 10.0;
    checker =
        std::make_unique<runtime::gateway::HealthChecker>(loop, registry, cfg);
    checker->Start();
    started_promise.set_value();
  });
  started.wait();

  if (!WaitForAtLeast(accepted, 1, 2s)) {
    std::cerr << "health checker did not establish a probe\n";
    loop->RunInLoop([&] {
      checker.reset();
      server.reset();
      loop->Quit();
    });
    return 2;
  }

  std::promise<void> destroyed_promise;
  auto destroyed = destroyed_promise.get_future();
  loop->RunInLoop([&] {
    checker->Stop();
    checker.reset();
    destroyed_promise.set_value();
  });
  destroyed.wait();

  // Trigger the in-flight probe's disconnect callback after HealthChecker has
  // been destroyed.
  loop->RunInLoop([&] {
    if (backend_conn) backend_conn->Shutdown();
  });

  std::this_thread::sleep_for(250ms);
  const auto fails_after_stop =
      peer->state().fails.load(std::memory_order_relaxed);
  const bool down_after_stop =
      peer->state().down.load(std::memory_order_relaxed);

  std::promise<void> cleanup_promise;
  auto cleanup = cleanup_promise.get_future();
  loop->RunInLoop([&] {
    backend_conn.reset();
    server.reset();
    cleanup_promise.set_value();
    loop->Quit();
  });
  cleanup.wait();

  if (fails_after_stop != 0 || down_after_stop) {
    std::cerr << "stale health callback mutated peer after Stop(): fails="
              << fails_after_stop << " down=" << down_after_stop << '\n';
    return 1;
  }

  std::cout << "HealthChecker callbacks are inert after Stop().\n";
  return 0;
}

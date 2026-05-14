// HealthChecker 主动健康检查冒烟测试
//
// 编译
// cmake --build build-tests --target health_checker_smoke_test -j$(nproc)
// 运行
// ./build-tests/tests/health_checker_smoke_test

#include "runtime/gateway/health_check_config.h"
#include "runtime/gateway/health_checker.h"
#include "runtime/gateway/upstream.h"
#include "runtime/gateway/upstream_peer.h"
#include "runtime/gateway/upstream_registry.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/tcp_server.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace {

using namespace std::chrono_literals;

bool Expect(bool condition, const char* msg) {
  if (!condition) {
    std::cerr << "[FAIL] " << msg << '\n';
    return false;
  }
  return true;
}

void Passed(const char* name) {
  std::cout << "[PASS] " << name << '\n';
}

// 预留一个 loopback 端口
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

  const std::uint16_t port = ntohs(addr.sin_port);
  ::close(fd);
  return port;
}

void WaitFor(std::atomic<bool>& flag, bool expected, int timeout_ms) {
  for (int i = 0; i < timeout_ms / 20; ++i) {
    if (flag.load(std::memory_order_relaxed) == expected) return;
    std::this_thread::sleep_for(20ms);
  }
}

// 测试环境：在独立线程中运行一个 EventLoop，其上同时跑 mock HTTP 后端 + HealthChecker
// EventLoop 由 loop_thread 持有并析构（析构要求 IsInLoopThread）。
// main thread 通过 RunInLoop 向 loop 投递 Quit 命令。
struct TestEnv {
  std::thread loop_thread;
  runtime::net::EventLoop* loop{nullptr};  // owned by loop_thread
  std::shared_ptr<runtime::gateway::UpstreamPeer> peer;
  std::atomic<bool>* peer_down_flag{nullptr};
};

TestEnv StartTestEnv(
    std::uint16_t port,
    const std::string& http_status_line,
    runtime::gateway::HealthCheckConfig cfg,
    std::shared_ptr<runtime::gateway::UpstreamPeer> peer) {

  auto upstream = std::make_shared<runtime::gateway::Upstream>(
      runtime::gateway::UpstreamConfig{.name = "test-upstream"});
  upstream->AddPeer(peer);

  auto registry = std::make_shared<runtime::gateway::UpstreamRegistry>();
  registry->Add(upstream);

  std::promise<runtime::net::EventLoop*> loop_promise;
  auto loop_future = loop_promise.get_future();
  std::promise<void> started;
  auto started_future = started.get_future();

  std::thread t([port, http_status_line, cfg, registry,
                 loop_promise = std::move(loop_promise),
                 started = std::move(started)]() mutable {
    runtime::net::EventLoop loop;

    std::string response = http_status_line +
        "\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

    runtime::net::TcpServer server(
        &loop,
        runtime::net::InetAddress(port, "127.0.0.1"),
        "mock-backend");

    server.SetConnectionCallback(
        [response](const runtime::net::TcpServer::TcpConnectionPtr& conn) {
          if (conn->Connected()) {
            conn->Send(response);
            conn->Shutdown();
          }
        });

    server.Start();

    runtime::gateway::HealthChecker checker(&loop, *registry, cfg);
    checker.Start();

    loop_promise.set_value(&loop);
    started.set_value();
    loop.Loop();
  });

  auto* loop = loop_future.get();
  started_future.wait();

  return TestEnv{
      .loop_thread = std::move(t),
      .loop = loop,
      .peer = peer,
      .peer_down_flag = &peer->State().down,
  };
}

void StopTestEnv(TestEnv& env) {
  if (env.loop) {
    // 从 main 线程向 loop 投递 Quit，RunInLoop 通过 eventfd 跨线程安全唤醒
    env.loop->RunInLoop([loop = env.loop] { loop->Quit(); });
  }
  if (env.loop_thread.joinable()) {
    env.loop_thread.join();
  }
}

// ================================================================
// 测试: HealthCheckConfig 默认值
// ================================================================
bool TestHealthCheckConfigDefaults() {
  runtime::gateway::HealthCheckConfig cfg;
  if (!Expect(cfg.path == "/health", "default path should be /health")) return false;
  if (!Expect(cfg.interval_sec == 10.0, "default interval should be 10s")) return false;
  if (!Expect(cfg.timeout_sec == 3.0, "default timeout should be 3s")) return false;
  if (!Expect(cfg.unhealthy_threshold == 3, "default unhealthy_threshold should be 3")) return false;
  if (!Expect(cfg.healthy_threshold == 2, "default healthy_threshold should be 2")) return false;
  Passed("TestHealthCheckConfigDefaults");
  return true;
}

// ================================================================
// 测试: 后端返回 200，健康检查连续成功 → peer 恢复
// ================================================================
bool TestRecoveryAfterConsecutiveSuccesses() {
  const std::uint16_t port = ReserveLoopbackPort();
  if (!Expect(port != 0, "should reserve a port")) return false;

  auto peer = std::make_shared<runtime::gateway::UpstreamPeer>(
      runtime::gateway::UpstreamPeerConfig{
          .name = "127.0.0.1:" + std::to_string(port),
          .host = "127.0.0.1",
          .port = port,
      });
  peer->State().down.store(true, std::memory_order_relaxed);

  runtime::gateway::HealthCheckConfig cfg;
  cfg.path = "/health";
  cfg.interval_sec = 0.2;
  cfg.timeout_sec = 1.5;
  cfg.healthy_threshold = 2;
  cfg.unhealthy_threshold = 3;

  auto env = StartTestEnv(port, "HTTP/1.1 200 OK", cfg, peer);

  WaitFor(*env.peer_down_flag, false, 3000);
  bool recovered = !env.peer_down_flag->load(std::memory_order_relaxed);

  StopTestEnv(env);

  if (!Expect(recovered, "peer should recover after consecutive 200 responses")) return false;
  Passed("TestRecoveryAfterConsecutiveSuccesses");
  return true;
}

// ================================================================
// 测试: 后端返回 503，健康检查连续失败 → peer 被摘除
// ================================================================
bool TestMarkedDownAfterConsecutiveFailures() {
  const std::uint16_t port = ReserveLoopbackPort();
  if (!Expect(port != 0, "should reserve a port")) return false;

  auto peer = std::make_shared<runtime::gateway::UpstreamPeer>(
      runtime::gateway::UpstreamPeerConfig{
          .name = "127.0.0.1:" + std::to_string(port),
          .host = "127.0.0.1",
          .port = port,
      });

  runtime::gateway::HealthCheckConfig cfg;
  cfg.path = "/health";
  cfg.interval_sec = 0.2;
  cfg.timeout_sec = 1.5;
  cfg.healthy_threshold = 2;
  cfg.unhealthy_threshold = 2;

  auto env = StartTestEnv(port, "HTTP/1.1 503 Service Unavailable", cfg, peer);

  WaitFor(*env.peer_down_flag, true, 3000);
  bool marked_down = env.peer_down_flag->load(std::memory_order_relaxed);

  StopTestEnv(env);

  if (!Expect(marked_down, "peer should be marked down after consecutive 503 responses")) return false;
  Passed("TestMarkedDownAfterConsecutiveFailures");
  return true;
}

// ================================================================
// 测试: down 状态经过 200 响应后恢复为 up
// ================================================================
bool TestFullRecoveryCycle() {
  const std::uint16_t port = ReserveLoopbackPort();
  if (!Expect(port != 0, "should reserve a port")) return false;

  auto peer = std::make_shared<runtime::gateway::UpstreamPeer>(
      runtime::gateway::UpstreamPeerConfig{
          .name = "127.0.0.1:" + std::to_string(port),
          .host = "127.0.0.1",
          .port = port,
      });
  peer->State().down.store(true, std::memory_order_relaxed);

  runtime::gateway::HealthCheckConfig cfg;
  cfg.path = "/health";
  cfg.interval_sec = 0.2;
  cfg.timeout_sec = 1.5;
  cfg.healthy_threshold = 2;
  cfg.unhealthy_threshold = 3;

  auto env = StartTestEnv(port, "HTTP/1.1 200 OK", cfg, peer);

  WaitFor(*env.peer_down_flag, false, 3000);
  bool recovered = !env.peer_down_flag->load(std::memory_order_relaxed);

  StopTestEnv(env);

  if (!Expect(recovered, "peer should go down→up after backend returns 200")) return false;
  Passed("TestFullRecoveryCycle");
  return true;
}

}  // namespace

int main() {
  int passed = 0;
  int total = 0;

#define RUN(test) do { ++total; if (test()) ++passed; } while (0)

  RUN(TestHealthCheckConfigDefaults);
  RUN(TestRecoveryAfterConsecutiveSuccesses);
  RUN(TestMarkedDownAfterConsecutiveFailures);
  RUN(TestFullRecoveryCycle);

  std::cout << "===========================\n";
  std::cout << passed << "/" << total << " tests passed.\n";

  return (passed == total) ? 0 : 1;
}

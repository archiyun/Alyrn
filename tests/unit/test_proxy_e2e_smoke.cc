// Proxy E2E smoke test
//
// 端到端验证 GatewayServer 的代理路径：
//   1. 转发到 upstream 的请求行包含完整 path + query
//   2. 当 upstream 返回 Content-Length 响应时连接进入 pool 并被复用
//   3. 哈希负载均衡首选节点连接失败时会切换到其他节点
//   4. 非幂等请求已经发出后不会在其他节点重放
//
// 编译: cmake --build build-tests --target proxy_e2e_smoke_test -j$(nproc)
// 运行: ./build-tests/tests/proxy_e2e_smoke_test

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "vexo/ds/murmurhash32.h"
#include "vexo/gateway/gateway_server.h"
#include "vexo/gateway/upstream.h"
#include "vexo/gateway/upstream_peer.h"
#include "vexo/gateway/upstream_registry.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/event_loop_scheduler.h"
#include "vexo/net/event_loop_thread.h"
#include "vexo/net/inet_address.h"
#include "vexo/net/reactor_connect.h"
#include "vexo/net/reactor_listener.h"
#include "vexo/net/tcp_server.h"

using namespace std::chrono_literals;

uint16_t ReservePort() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
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
  socklen_t len = sizeof(addr);
  ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
  uint16_t port = ntohs(addr.sin_port);
  ::close(fd);
  return port;
}

namespace {

using TestGateway =
    vexo::gateway::GatewayServer<vexo::net::ReactorListener, vexo::net::ReactorConnector>;

struct GatewayRuntime {
  std::unique_ptr<vexo::net::EventLoopScheduler> scheduler;
  std::unique_ptr<vexo::net::ReactorListener> listener;
  std::unique_ptr<vexo::net::ReactorConnector> connector;
  std::unique_ptr<TestGateway> gateway;
};

bool Expect(bool ok, const char* msg) {
  if (!ok) std::cerr << "[FAIL] " << msg << '\n';
  return ok;
}

void Passed(const char* name) { std::cout << "[PASS] " << name << '\n'; }

template <typename F>
void RunInLoopAndWait(vexo::net::EventLoop* loop, F&& fn) {
  std::promise<void> done_promise;
  auto done = done_promise.get_future();
  loop->RunInLoop([fn = std::forward<F>(fn), &done_promise]() mutable {
    fn();
    done_promise.set_value();
  });
  done.wait();
}

void InitGateway(GatewayRuntime& runtime, vexo::net::EventLoop* loop, uint16_t port,
                 std::string name, vexo::gateway::UpstreamRegistry& registry) {
  runtime.scheduler = std::make_unique<vexo::net::EventLoopScheduler>(loop);
  runtime.listener =
      std::make_unique<vexo::net::ReactorListener>(loop, vexo::net::InetAddress(port));
  runtime.connector = std::make_unique<vexo::net::ReactorConnector>(loop);
  runtime.gateway = std::make_unique<TestGateway>(*runtime.listener, *runtime.scheduler,
                                                  std::move(name), registry, *runtime.connector);
}

vexo::coro::Task<void> StopGatewayTask(TestGateway* gateway, std::promise<void>* done) {
  co_await gateway->Stop();
  done->set_value();
}

void StopGateway(vexo::net::EventLoop* loop, GatewayRuntime& runtime) {
  std::promise<void> done_promise;
  auto done = done_promise.get_future();
  loop->RunInLoop([&runtime, &done_promise] {
    vexo::coro::Spawn(*runtime.scheduler, StopGatewayTask(runtime.gateway.get(), &done_promise))
        .Detach();
  });
  done.wait();
  RunInLoopAndWait(loop, [&] {
    runtime.gateway.reset();
    runtime.connector.reset();
    runtime.listener.reset();
    runtime.scheduler.reset();
  });
}

// 简单的 blocking 客户端：写完整请求并读到 EOF 或固定字节数。
std::string BlockingHttpCall(uint16_t port, const std::string& req, size_t expect_max = 4096) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    ::close(fd);
    return "";
  }
  timeval timeout{.tv_sec = 3, .tv_usec = 0};
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  ::send(fd, req.data(), req.size(), 0);

  std::string out;
  char buf[1024];
  while (out.size() < expect_max) {
    ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
    if (n <= 0) break;
    out.append(buf, n);
    // 已读到完整响应 (\r\n\r\n + 已知 body) 就退出
    auto h = out.find("\r\n\r\n");
    if (h != std::string::npos) {
      auto cl = out.find("Content-Length:");
      if (cl != std::string::npos && cl < h) {
        size_t v_start = cl + 15;
        while (v_start < h && (out[v_start] == ' ' || out[v_start] == '\t')) ++v_start;
        size_t body_len = std::strtoul(out.c_str() + v_start, nullptr, 10);
        if (out.size() >= h + 4 + body_len) break;
      }
    }
  }
  ::close(fd);
  return out;
}

// upstream stub：把每次收到的 request 完整记录下来，回固定 Content-Length 响应。
struct UpstreamStub {
  vexo::net::EventLoopThread loop_thr;
  vexo::net::EventLoop* loop_holder{nullptr};
  std::unique_ptr<vexo::net::TcpServer> server;
  std::mutex mu;
  std::vector<std::string> requests;  // 收到的每条完整请求 (按 \r\n\r\n 切)
  std::atomic<int> accept_count{0};
  std::atomic<int> request_count{0};
  bool respond{true};
  bool close_after_request{false};

  void Start(uint16_t port) {
    auto* loop = loop_thr.StartLoop();
    loop_holder = loop;
    vexo::net::InetAddress addr(port);
    RunInLoopAndWait(loop, [this, loop, addr] {
      server = std::make_unique<vexo::net::TcpServer>(loop, addr, "upstream-stub");
      server->set_connection_callback(
          [this](const vexo::net::TcpConnection::TcpConnectionPtr& conn) {
            if (conn->Connected()) accept_count.fetch_add(1);
          });
      server->set_message_callback([this](const vexo::net::TcpConnection::TcpConnectionPtr& conn,
                                          vexo::net::Buffer& buf, vexo::time::Timestamp) {
        while (true) {
          std::string_view view(buf.Peek(), buf.readable_bytes());
          auto pos = view.find("\r\n\r\n");
          if (pos == std::string_view::npos) return;
          {
            std::lock_guard lk{mu};
            requests.emplace_back(view.substr(0, pos + 4));
          }
          buf.Retrieve(pos + 4);
          request_count.fetch_add(1);
          if (close_after_request) {
            conn->Shutdown();
            return;
          }
          if (!respond) continue;
          const std::string body = "hello world";
          std::string resp =
              "HTTP/1.1 200 OK\r\n"
              "Content-Type: text/plain\r\n"
              "Content-Length: " +
              std::to_string(body.size()) + "\r\n\r\n" + body;
          conn->Send(resp);
        }
      });
      server->Start();
    });
  }
};

void AddPeersWithHashSelectingFirst(const std::shared_ptr<vexo::gateway::Upstream>& upstream,
                                    const std::shared_ptr<vexo::gateway::UpstreamPeer>& selected,
                                    const std::shared_ptr<vexo::gateway::UpstreamPeer>& failover) {
  const auto hash = vexo::ds::MurmurHash3("127.0.0.1");
  if ((hash % 2) == 0) {
    upstream->AddPeer(selected);
    upstream->AddPeer(failover);
  } else {
    upstream->AddPeer(failover);
    upstream->AddPeer(selected);
  }
}

bool TestProxyPreservesRequestLineAndReusesConnection() {
  const uint16_t up_port = ReservePort();
  const uint16_t gw_port = ReservePort();
  if (!Expect(up_port && gw_port, "must reserve ephemeral ports")) return false;

  // 1. upstream stub 起来
  UpstreamStub stub;
  stub.Start(up_port);

  // 2. 网关起来 (单线程,简化生命周期)
  vexo::gateway::UpstreamRegistry reg;
  auto up =
      std::make_shared<vexo::gateway::Upstream>(vexo::gateway::UpstreamConfig{.name = "stub"});
  up->AddPeer(std::make_shared<vexo::gateway::UpstreamPeer>(
      vexo::gateway::UpstreamPeerConfig{.name = "stub-1", .host = "127.0.0.1", .port = up_port}));
  reg.Add(up);

  vexo::net::EventLoopThread gw_thr;
  auto* gw_loop = gw_thr.StartLoop();
  GatewayRuntime gw;
  RunInLoopAndWait(gw_loop, [&] {
    InitGateway(gw, gw_loop, gw_port, "gw", reg);
    gw.gateway->AddProxyRoute("/api/", "stub", "round_robin");
    gw.gateway->Start();
  });

  // 3. 发两个请求，验证：
  //    (a) upstream 收到 "GET /api/foo?x=1 HTTP/1.1"
  //    (b) 第二个请求 keep-alive 复用了 TCP 连接 (accept_count == 1)
  const std::string req1 =
      "GET /api/foo?x=1 HTTP/1.1\r\n"
      "Host: gw\r\n"
      "Connection: keep-alive, X-Remove-Me\r\n"
      "X-Remove-Me: secret\r\n"
      "Keep-Alive: timeout=5\r\n"
      "X-Forwarded-For: 192.0.2.10\r\n"
      "X-Real-IP: spoofed\r\n"
      "X-Forwarded-Proto: https\r\n"
      "X-Forwarded-Host: spoofed.example\r\n"
      "Via: 1.0 old-gateway\r\n"
      "X-Request-ID: client-request-id\r\n"
      "\r\n";
  auto resp1 = BlockingHttpCall(gw_port, req1);
  if (!Expect(resp1.find("200 OK") != std::string::npos, "first response must be 200 OK"))
    return false;
  if (!Expect(resp1.find("hello world") != std::string::npos, "first response must contain body"))
    return false;

  // 通过同一个客户端 TCP 不容易复用 (会建一个新连接给 upstream)；
  // 真正要验的是 upstream 侧是否同一条连接处理了两个请求。
  // 客户端发第二次请求 (新 TCP 连接), 网关在 IO 线程从 pool 复用 upstream conn。
  std::this_thread::sleep_for(50ms);
  const std::string req2 = "GET /api/bar HTTP/1.1\r\nHost: gw\r\nConnection: keep-alive\r\n\r\n";
  auto resp2 = BlockingHttpCall(gw_port, req2);
  if (!Expect(resp2.find("200 OK") != std::string::npos, "second response must be 200 OK"))
    return false;

  // 检查 upstream 看到的请求行
  std::this_thread::sleep_for(50ms);
  std::vector<std::string> recs;
  {
    std::lock_guard lk{stub.mu};
    recs = stub.requests;
  }
  if (!Expect(recs.size() >= 2, "upstream must receive at least 2 requests")) {
    std::cerr << "  recs.size()=" << recs.size() << '\n';
    return false;
  }
  if (!Expect(recs[0].starts_with("GET /api/foo?x=1 HTTP/1.1"),
              "first upstream request line must preserve path + query")) {
    std::cerr << "  got: " << recs[0].substr(0, 64) << '\n';
    return false;
  }
  if (!Expect(recs[1].starts_with("GET /api/bar HTTP/1.1"),
              "second upstream request line must preserve path")) {
    std::cerr << "  got: " << recs[1].substr(0, 64) << '\n';
    return false;
  }
  if (!Expect(recs[0].find("x-remove-me:") == std::string::npos &&
                  recs[0].find("keep-alive: timeout=5") == std::string::npos,
              "hop-by-hop and Connection-nominated headers must be removed")) {
    std::cerr << "  got: " << recs[0] << '\n';
    return false;
  }
  if (!Expect(recs[0].find("x-forwarded-for: 192.0.2.10, 127.0.0.1\r\n") != std::string::npos,
              "gateway must append the client IP to X-Forwarded-For")) {
    std::cerr << "  got: " << recs[0] << '\n';
    return false;
  }
  if (!Expect(recs[0].find("x-real-ip: 127.0.0.1\r\n") != std::string::npos &&
                  recs[0].find("x-forwarded-proto: http\r\n") != std::string::npos &&
                  recs[0].find("x-forwarded-host: gw\r\n") != std::string::npos,
              "gateway-owned forwarded headers must replace spoofed values")) {
    std::cerr << "  got: " << recs[0] << '\n';
    return false;
  }
  if (!Expect(recs[0].find("via: 1.0 old-gateway, 1.1 gw\r\n") != std::string::npos,
              "gateway must append itself to Via")) {
    std::cerr << "  got: " << recs[0] << '\n';
    return false;
  }
  if (!Expect(recs[0].find("x-request-id: client-request-id\r\n") != std::string::npos,
              "existing request ID must be preserved")) {
    std::cerr << "  got: " << recs[0] << '\n';
    return false;
  }
  // 连接复用：accept_count 应该 == 1
  if (!Expect(stub.accept_count.load() == 1,
              "upstream conn must be pooled and reused (accept_count==1)")) {
    std::cerr << "  accept_count=" << stub.accept_count.load() << '\n';
    return false;
  }

  Passed("TestProxyPreservesRequestLineAndReusesConnection");
  // gw 和 stub.server 都在 loop 线程上创建，必须也在 loop 线程上析构
  StopGateway(gw_loop, gw);
  auto* stub_loop = stub.loop_holder;
  RunInLoopAndWait(stub_loop, [&] { stub.server.reset(); });
  return true;
}

bool TestPrefixBoundary() {
  const uint16_t up_port = ReservePort();
  const uint16_t gw_port = ReservePort();
  if (!Expect(up_port && gw_port, "must reserve ephemeral ports")) return false;

  // 路由 /api 不应匹配 /apifoo
  UpstreamStub stub;
  stub.Start(up_port);

  vexo::gateway::UpstreamRegistry reg;
  auto up =
      std::make_shared<vexo::gateway::Upstream>(vexo::gateway::UpstreamConfig{.name = "stub"});
  up->AddPeer(std::make_shared<vexo::gateway::UpstreamPeer>(
      vexo::gateway::UpstreamPeerConfig{.name = "stub-1", .host = "127.0.0.1", .port = up_port}));
  reg.Add(up);

  vexo::net::EventLoopThread gw_thr;
  auto* gw_loop = gw_thr.StartLoop();
  GatewayRuntime gw;
  RunInLoopAndWait(gw_loop, [&] {
    InitGateway(gw, gw_loop, gw_port, "gw2", reg);
    gw.gateway->AddProxyRoute("/api", "stub", "round_robin");
    gw.gateway->Start();
  });

  // /apifoo 不应代理，应该 404
  const std::string req = "GET /apifoo HTTP/1.1\r\nHost: gw2\r\nConnection: close\r\n\r\n";
  auto resp = BlockingHttpCall(gw_port, req);
  if (!Expect(resp.find("404") != std::string::npos,
              "/apifoo must NOT match prefix /api (expect 404)")) {
    std::cerr << "  resp head: " << resp.substr(0, 64) << '\n';
    return false;
  }
  // /api/foo 应该代理成功
  const std::string req_ok = "GET /api/foo HTTP/1.1\r\nHost: gw2\r\nConnection: close\r\n\r\n";
  auto resp_ok = BlockingHttpCall(gw_port, req_ok);
  if (!Expect(resp_ok.find("200 OK") != std::string::npos, "/api/foo must match prefix /api"))
    return false;

  Passed("TestPrefixBoundary");
  StopGateway(gw_loop, gw);
  auto* stub_loop = stub.loop_holder;
  RunInLoopAndWait(stub_loop, [&] { stub.server.reset(); });
  return true;
}

bool TestProxyDeadlineReleasesBulkheadSlot() {
  const uint16_t up_port = ReservePort();
  const uint16_t gw_port = ReservePort();
  if (!Expect(up_port && gw_port, "must reserve ephemeral ports")) return false;

  UpstreamStub stub;
  stub.respond = false;
  stub.Start(up_port);

  vexo::gateway::UpstreamRegistry reg;
  vexo::gateway::UpstreamConfig cfg;
  cfg.name = "slow";
  cfg.max_concurrent_requests = 1;
  cfg.request_timeout = 100ms;
  auto up = std::make_shared<vexo::gateway::Upstream>(cfg);
  auto peer = std::make_shared<vexo::gateway::UpstreamPeer>(
      vexo::gateway::UpstreamPeerConfig{.name = "slow-1", .host = "127.0.0.1", .port = up_port});
  up->AddPeer(peer);
  reg.Add(up);

  vexo::net::EventLoopThread gw_thr;
  auto* gw_loop = gw_thr.StartLoop();
  GatewayRuntime gw;
  RunInLoopAndWait(gw_loop, [&] {
    InitGateway(gw, gw_loop, gw_port, "deadline-gw", reg);
    gw.gateway->AddProxyRoute("/slow", "slow", "round_robin");
    gw.gateway->Start();
  });

  const std::string req = "GET /slow HTTP/1.1\r\nHost: deadline-gw\r\nConnection: close\r\n\r\n";
  const auto started = std::chrono::steady_clock::now();
  const auto first = BlockingHttpCall(gw_port, req);
  const auto elapsed = std::chrono::steady_clock::now() - started;

  if (!Expect(first.find("502 Bad Gateway") != std::string::npos,
              "slow upstream must be cut off with 502"))
    return false;
  if (!Expect(elapsed < 1500ms, "upstream deadline must bound client latency")) return false;

  for (int i = 0; i < 100 && (peer->active_request() != 0 || up->active_requests() != 0); ++i) {
    std::this_thread::sleep_for(5ms);
  }
  if (!Expect(peer->active_request() == 0, "deadline must release peer active accounting"))
    return false;
  if (!Expect(up->active_requests() == 0, "deadline must release upstream bulkhead slot"))
    return false;

  // A second request must acquire the same single slot and reach the backend;
  // a leaked slot would fail immediately with the gateway's 503 fallback.
  const int before = stub.request_count.load();
  const auto second = BlockingHttpCall(gw_port, req);
  if (!Expect(second.find("502 Bad Gateway") != std::string::npos,
              "bulkhead slot must be reusable after timeout"))
    return false;
  if (!Expect(stub.request_count.load() > before,
              "second request must reach backend after slot release"))
    return false;

  Passed("TestProxyDeadlineReleasesBulkheadSlot");
  StopGateway(gw_loop, gw);
  auto* stub_loop = stub.loop_holder;
  RunInLoopAndWait(stub_loop, [&] { stub.server.reset(); });
  return true;
}

bool TestIPHashConnectFailureFailsOver() {
  const uint16_t closing_port = ReservePort();
  const uint16_t good_port = ReservePort();
  const uint16_t gw_port = ReservePort();
  if (!Expect(closing_port && good_port && gw_port, "must reserve failover ports")) {
    return false;
  }

  UpstreamStub closing;
  closing.respond = false;
  closing.close_after_request = true;
  closing.Start(closing_port);
  UpstreamStub good;
  good.Start(good_port);

  vexo::gateway::UpstreamRegistry reg;
  vexo::gateway::UpstreamConfig cfg;
  cfg.name = "hash-failover";
  cfg.request_timeout = 1s;
  auto up = std::make_shared<vexo::gateway::Upstream>(cfg);
  auto bad_peer = std::make_shared<vexo::gateway::UpstreamPeer>(vexo::gateway::UpstreamPeerConfig{
      .name = "accept-then-close", .host = "127.0.0.1", .port = closing_port});
  auto good_peer = std::make_shared<vexo::gateway::UpstreamPeer>(
      vexo::gateway::UpstreamPeerConfig{.name = "healthy", .host = "127.0.0.1", .port = good_port});
  AddPeersWithHashSelectingFirst(up, bad_peer, good_peer);
  reg.Add(up);

  vexo::net::EventLoopThread gw_thr;
  auto* gw_loop = gw_thr.StartLoop();
  GatewayRuntime gw;
  RunInLoopAndWait(gw_loop, [&] {
    InitGateway(gw, gw_loop, gw_port, "hash-failover-gw", reg);
    gw.gateway->AddProxyRoute("/hash", "hash-failover", "ip_hash");
    gw.gateway->Start();
  });

  bool ok = true;
  const auto response = BlockingHttpCall(gw_port,
                                         "GET /hash HTTP/1.1\r\nHost: hash-failover-gw\r\n"
                                         "Connection: close\r\n\r\n");
  ok &= Expect(response.find("200 OK") != std::string::npos,
               "ip_hash retry must reach a different healthy peer");
  ok &= Expect(closing.request_count.load() == 1,
               "hash-selected peer must receive the first attempt once");
  ok &= Expect(good.request_count.load() == 1,
               "healthy failover peer must receive the retry exactly once");

  if (ok) Passed("TestIPHashConnectFailureFailsOver");
  StopGateway(gw_loop, gw);
  RunInLoopAndWait(closing.loop_holder, [&] { closing.server.reset(); });
  RunInLoopAndWait(good.loop_holder, [&] { good.server.reset(); });
  return ok;
}

bool TestPostIsNotReplayedAfterFlush() {
  const uint16_t closing_port = ReservePort();
  const uint16_t good_port = ReservePort();
  const uint16_t gw_port = ReservePort();
  if (!Expect(closing_port && good_port && gw_port, "must reserve non-idempotent retry ports")) {
    return false;
  }

  UpstreamStub closing;
  closing.respond = false;
  closing.close_after_request = true;
  closing.Start(closing_port);
  UpstreamStub good;
  good.Start(good_port);

  vexo::gateway::UpstreamRegistry reg;
  vexo::gateway::UpstreamConfig cfg;
  cfg.name = "post-no-replay";
  cfg.request_timeout = 1s;
  auto up = std::make_shared<vexo::gateway::Upstream>(cfg);
  auto closing_peer =
      std::make_shared<vexo::gateway::UpstreamPeer>(vexo::gateway::UpstreamPeerConfig{
          .name = "accept-then-close", .host = "127.0.0.1", .port = closing_port});
  auto good_peer = std::make_shared<vexo::gateway::UpstreamPeer>(vexo::gateway::UpstreamPeerConfig{
      .name = "must-not-receive", .host = "127.0.0.1", .port = good_port});
  AddPeersWithHashSelectingFirst(up, closing_peer, good_peer);
  reg.Add(up);

  vexo::net::EventLoopThread gw_thr;
  auto* gw_loop = gw_thr.StartLoop();
  GatewayRuntime gw;
  RunInLoopAndWait(gw_loop, [&] {
    InitGateway(gw, gw_loop, gw_port, "post-no-replay-gw", reg);
    gw.gateway->AddProxyRoute("/write", "post-no-replay", "ip_hash");
    gw.gateway->Start();
  });

  bool ok = true;
  const auto response = BlockingHttpCall(gw_port,
                                         "POST /write HTTP/1.1\r\nHost: post-no-replay-gw\r\n"
                                         "Content-Length: 7\r\nConnection: close\r\n\r\npayload");
  ok &= Expect(response.find("502 Bad Gateway") != std::string::npos,
               "flushed POST must fail instead of being replayed");
  ok &= Expect(closing.request_count.load() == 1, "first peer must receive the POST exactly once");
  ok &= Expect(good.request_count.load() == 0, "failover peer must never receive a replayed POST");

  if (ok) Passed("TestPostIsNotReplayedAfterFlush");
  StopGateway(gw_loop, gw);
  RunInLoopAndWait(closing.loop_holder, [&] { closing.server.reset(); });
  RunInLoopAndWait(good.loop_holder, [&] { good.server.reset(); });
  return ok;
}

}  // namespace

int main() {
  std::setvbuf(stdout, nullptr, _IONBF, 0);
  std::setvbuf(stderr, nullptr, _IONBF, 0);
  int passed = 0, total = 0;
#define RUN(t)         \
  do {                 \
    ++total;           \
    if (t()) ++passed; \
  } while (0)
  RUN(TestProxyPreservesRequestLineAndReusesConnection);
  RUN(TestPrefixBoundary);
  RUN(TestProxyDeadlineReleasesBulkheadSlot);
  RUN(TestIPHashConnectFailureFailsOver);
  RUN(TestPostIsNotReplayedAfterFlush);
  std::cout << "===========================\n" << passed << "/" << total << " tests passed.\n";
  return passed == total ? 0 : 1;
}

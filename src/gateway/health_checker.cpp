#include "runtime/gateway/health_checker.h"
#include "runtime/log/logger.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/tcp_client.h"
#include "runtime/net/buffer.h"
#include "runtime/time/timestamp.h"

#include <string>

namespace runtime::gateway {

HealthChecker::HealthChecker(runtime::net::EventLoop* loop, 
                             UpstreamRegistry& registry, 
                             HealthCheckConfig cfg) 
  : loop_(loop), registry_(registry), cfg_(std::move(cfg)) {}


void HealthChecker::Start() {
  if (running_) return;
  running_ = true;
  timer_id_ = loop_->RunEvery(cfg_.interval_sec, [this] { CheckAll(); });
  LOG_INFO() << "health_checker: started, interval=" << cfg_.interval_sec << "s";
}
void HealthChecker::Stop() {
  if (!running_) return;
  running_ = false;
  loop_->Cancel(timer_id_);
}

void HealthChecker::CheckAll() {
  for (const auto& [_, upstream] : registry_.All()) {
    for (const auto& peer : upstream->Peers()) {
      CheckOne(peer);
    }
  }
}

void HealthChecker::CheckOne(std::shared_ptr<UpstreamPeer> peer) {
  auto client = std::make_shared<runtime::net::TcpClient>(
    loop_,
    runtime::net::InetAddress(peer->Config().port, peer->Config().host),
    "health->" + peer->Config().name);
  
  const std::string request = 
    "GET " + cfg_.path + " HTTP/1.1\r\n"
    "Host: " + peer->Config().host + "\r\n"
    "Connection: close\r\n"; 
  auto& ok_count = consecutive_ok_[peer->Config().name];
  client->SetConnectionCallback(
    [client, request](const runtime::net::TcpConnection::TcpConnectionPtr& conn) {
      if (conn->Connected()) conn->Send(request);
    }
  );

  client->SetMessageCallback(
    [peer, client, &ok_count, healthy_threshold = cfg_.healthy_threshold](
      const runtime::net::TcpConnection::TcpConnectionPtr& conn,
      runtime::net::Buffer& buf,
      runtime::time::Timestamp) {
        const bool ok = buf.ReadableBytes() >= 12 && std::string_view(buf.Peek(), 12) == "HTTP/1.1 200";
        buf.RetrieveAll();
        conn->Shutdown();

        if (ok) {
          ++ok_count;
          if (ok_count >= healthy_threshold && 
            peer->State().down.load(std::memory_order_relaxed)) {
            peer->State().down.store(false, std::memory_order_relaxed);
            peer->State().fails.store(0, std::memory_order_relaxed);
            ok_count = 0;
            LOG_INFO() << "health_checker: upstream peer " << peer->Config().name << " recovered";
          }
        } else {
          ok_count = 0;
        }
      });
  client->Connect();

  loop_->RunAfter(cfg_.timeout_sec, [client] {
    if (client->connection() && client->connection()->Connected()) {
      client->Disconnect();
    }
  });
}
}  // namespace runtime::gateway

#include "runtime/gateway/health_checker.h"
#include "runtime/gateway/service_registry.h"
#include "runtime/log/logger.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/tcp_client.h"
#include "runtime/net/buffer.h"
#include "runtime/time/timestamp.h"

#include <string>

namespace runtime::gateway {

HealthChecker::HealthChecker(runtime::net::EventLoop* loop, 
                             ServiceRegistry& registry, 
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
  for (const auto& [_, group] : registry_.All()) {
    for (const auto& backend : group->Backends()) {
      CheckOne(backend);
    }
  }
}

void HealthChecker::CheckOne(std::shared_ptr<Backend> backend) {
  auto client = std::make_shared<runtime::net::TcpClient>(
    loop_,
    runtime::net::InetAddress(backend->config_.port, backend->config_.host),
    "health->" + backend->config_.id);
  
  const std::string request = 
    "GET " + cfg_.path + " HTTP/1.1\r\n"
    "Host: " + backend->config_.host + "\r\n"
    "Connection: close\r\n"; 
  auto& ok_count = consecutive_ok_[backend->config_.id];
  client->SetConnectionCallback(
    [client, request](const runtime::net::TcpConnection::TcpConnectionPtr& conn) {
      if (conn->Connected()) conn->Send(request);
    }
  );

  client->SetMessageCallback(
    [backend, client, &ok_count, healthy_threshold = cfg_.healthy_threshold](
      const runtime::net::TcpConnection::TcpConnectionPtr& conn,
      runtime::net::Buffer& buf,
      runtime::time::Timestamp) {
        const bool ok = buf.ReadableBytes() >= 12 && std::string_view(buf.Peek(), 12) == "HTTP/1.1 200";
        buf.RetrieveAll();
        conn->Shutdown();

        if (ok) {
          ++ok_count;
          if (ok_count >= healthy_threshold && 
            !backend->state_.healthy.load(std::memory_order_relaxed)) {
            backend->state_.healthy.store(true, std::memory_order_relaxed);
            backend->state_.fail_count.store(0, std::memory_order_relaxed);
            ok_count = 0;
            LOG_INFO() << "health_checker: backend " << backend->config_.id << " recovered";
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

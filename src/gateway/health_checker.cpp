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
  using TcpConnectionPtr = runtime::net::TcpConnection::TcpConnectionPtr;
  std::string name = peer->Config().name;
  // 1. 创建临时 TcpClient
  auto client = std::make_shared<runtime::net::TcpClient>(
    loop_,
    runtime::net::InetAddress(peer->Config().port, peer->Config().host),
    "health->" + name);
  
  // 2. 构造 HTTP 请求
  const std::string request = 
    "GET " + cfg_.path + " HTTP/1.1\r\n"
    "Host: " + peer->Config().host + "\r\n"
    "Connection: close\r\n\r\n"; 

  
  // 3. 连接成功回调 -> 发送回调
  auto done = std::make_shared<bool>(false);
  client->SetConnectionCallback(
    [this, peer, client, request, name, done](const TcpConnectionPtr& conn) {
      if (conn->Connected()) {
        conn->Send(request);
      }
      else if (!*done) {
        // MessageCallback 从未触发，连接层面就失败了
        auto& fail_count = consecutive_fail_[name];
        if (++fail_count >= cfg_.unhealthy_threshold &&
            !peer->State().down.load(std::memory_order_relaxed)) {
          peer->State().down.store(true, std::memory_order_relaxed);
        }
      }
    });

  // 4. 消息回调 -> 检验响应头 + 判定状态
  client->SetMessageCallback(
    [this, peer, client, name, healthy_threshold = cfg_.healthy_threshold,
     unhealthy_threshold = cfg_.unhealthy_threshold, done](
      const TcpConnectionPtr& conn,
      runtime::net::Buffer& buf,
      runtime::time::Timestamp) {
        // "HTTP/1.1 200" => 12 bytes
        // 字节数不够 等下一次回调
        if (buf.ReadableBytes() < 12) return;

        *done = true; // 防止 ConnectionCallback else 重复计数

        const bool ok = std::string_view(buf.Peek(), 12) == "HTTP/1.1 200";
        buf.RetrieveAll();
        conn->Shutdown();

        auto& ok_count = consecutive_ok_[name];
        if (ok) {
          consecutive_fail_[name] = 0;
          ++ok_count;
          // 连续成功次数大于上线阈值直接复活
          if (ok_count >= healthy_threshold && 
              peer->State().down.load(std::memory_order_relaxed)) {
            peer->State().down.store(false, std::memory_order_relaxed);
            peer->State().fails.store(0, std::memory_order_relaxed);
            ok_count = 0;
            LOG_INFO() << "health_checker: upstream peer " << name << " recovered";
          }
        } else {
          ok_count = 0;
          // 连续失败超过下线阈值直接去世
          auto& fail_count = consecutive_fail_[name];
          ++fail_count;
          if (fail_count >= unhealthy_threshold &&
              !peer->State().down.load(std::memory_order_relaxed)) {
            peer->State().down.store(true, std::memory_order_relaxed);
            LOG_WARN() << "health_checker: upstream peer " << name << " marked down";
          }
        }
      });
  // 5. 发起连接
  client->Connect();

  // 6. 超时兜底
  loop_->RunAfter(cfg_.timeout_sec, [client] {
    if (client->connection() && client->connection()->Connected()) {
      client->Disconnect();
    }
  });
}
}  // namespace runtime::gateway

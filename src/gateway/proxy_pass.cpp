#include "runtime/gateway/proxy_pass.h"
#include "runtime/http/http_types.h"
#include "runtime/log/logger.h"
#include "runtime/net/inet_address.h"
#include "runtime/time/timestamp.h"

#include <atomic>
#include <sstream>

namespace runtime::gateway {

// ProxySession
ProxySession::ProxySession(const TcpConnectionPtr& client_conn, std::shared_ptr<Backend> backend, std::string upstream_request)
  : client_weak_(client_conn),
    backend_(std::move(backend)),
    upstream_request_(std::move(upstream_request)) {
  backend_->state_.active_requests.fetch_add(1, std::memory_order_relaxed);
  backend_->state_.total_requests.fetch_add(1, std::memory_order_relaxed);
}

ProxySession::~ProxySession() {
  backend_->state_.active_requests.fetch_sub(1, std::memory_order_relaxed);
}

void ProxySession::Start() {
  auto client = client_weak_.lock();
  if (!client) return;

  runtime::net::InetAddress upstream_addr(backend_->config_.port, backend_->config_.host);
  upstream_ = std::make_unique<runtime::net::TcpClient>(client->GetLoop(), upstream_addr, "proxy->" + backend_->config_.id);

  // weak_from_this 避免 session->TcpClient->callback -> session
  auto weak_self = weak_from_this();

  upstream_->SetConnectionCallback([weak_self](const TcpConnectionPtr& up) {
    if (auto self = weak_self.lock()) self->OnUpstreamConnChange(up);
  });

  upstream_->SetMessageCallback([weak_self]
  (const TcpConnectionPtr& up, runtime::net::Buffer& buf, runtime::time::Timestamp ts){
    if (auto self = weak_self.lock()) self->OnUpstreamMessage(up, buf, ts);
  });

  upstream_->Connect();
}

void ProxySession::OnUpstreamConnChange(const TcpConnectionPtr& up_conn) {
  if (up_conn->Connected()) {
    up_conn->Send(upstream_request_);
    LOG_INFO() << "proxy: upstream connected " << backend_->config_.id;
    return;
  }

  // Connected()==false：连接断开
  // responded_==true  → 上游正常关闭，客户端已收到完整响应，无需处理
  // responded_==false → 真正的错误（连接失败 / 上游提前断开）
  if (!responded_) {
    const uint64_t fails = 
      backend_->state_.fail_count.fetch_add(1, std::memory_order_relaxed) + 1;
    if (static_cast<int>(fails) >= backend_->config_.max_fails) {
      backend_->state_.healthy.store(false, std::memory_order_relaxed);
      LOG_WARN() << "proxy: backend " << backend_->config_.id
                 << " marked unhealthy (fails=" << fails << ")";
    }
    Send502();
  }
}

void ProxySession::OnUpstreamMessage(const TcpConnectionPtr& /*up*/,
                                     runtime::net::Buffer& buf,
                                     runtime::time::Timestamp /*ts*/) {
  auto client = client_weak_.lock();
  if (!client) { buf.RetrieveAll(); return; }

  // 流式透传
  std::string chunk(buf.Peek(), buf.ReadableBytes());
  buf.RetrieveAll();
  responded_ = true;
  client->Send(chunk);
}                     

void ProxySession::Send502() {
  auto client = client_weak_.lock();
  if (!client) return;

  static constexpr std::string_view kResp = 
    "HTTP/1.1 502 Bad Gateway\r\n"
    "Content-Type: text/plain\r\n"
    "Content-Length: 11\r\n"
    "Connection: close\r\n"
    "\r\n"
    "Bad Gateway";
  client->Send(std::string(kResp));
  client->Shutdown();
  LOG_WARN() << "proxy: 502 -> client, upstream=" << backend_->config_.id; 
}

// ProxyPass

std::shared_ptr<ProxySession>
ProxyPass::Forward(const TcpConnectionPtr &client_conn, 
                   const runtime::http::HttpRequest &request, 
                   std::shared_ptr<Backend> backend) {
  auto session = std::make_shared<ProxySession>(client_conn, backend, BuildRequest(request, *backend));
  session->Start();
  return session;
}

std::string ProxyPass::BuildRequest(const runtime::http::HttpRequest& req,
                                    const Backend& backend) {
  std::ostringstream oss;
  oss << runtime::http::MethodToString(req.GetMethod()) << ' ' << req.Path();
  if (!req.Query().empty()) oss << '?' << req.Query();
  oss << " HTTP/1.1\r\n";

  for (const auto& [k, v] : req.Headers()) {
    if (k == "host") continue;
    oss << k << ": " << v << "\r\n";
  }
  oss << "host: " << backend.config_.host << ":" << backend.config_.port << "\r\n";
  
  const auto xff = req.GetHeader("x-forwarded-for");
  if (!xff.empty()) oss << "X-Forwarded-For: " << xff << "\r\n";

  oss << "\r\n";
  if (!req.Body().empty()) oss << req.Body();
  return oss.str();
}
} // namespace runtime::gateway
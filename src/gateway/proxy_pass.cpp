#include "runtime/gateway/proxy_pass.h"
#include "runtime/http/http_types.h"
#include "runtime/log/logger.h"
#include "runtime/net/inet_address.h"
#include "runtime/time/timestamp.h"

#include <atomic>
#include <sstream>

namespace runtime::gateway {

// UpstreamRequest
UpstreamRequest::UpstreamRequest(const TcpConnectionPtr& client_conn, 
                 std::shared_ptr<UpstreamPeer> peer, 
                 std::string request_bytes)
  : client_weak_(client_conn),
    peer_(std::move(peer)),
    request_bytes_(std::move(request_bytes)) {
  peer_->State().active.fetch_add(1, std::memory_order_relaxed);
  peer_->State().requests.fetch_add(1, std::memory_order_relaxed);
}

UpstreamRequest::~UpstreamRequest() {
  peer_->State().active.fetch_sub(1, std::memory_order_relaxed);
}

void UpstreamRequest::Start() {
  auto client = client_weak_.lock();
  if (!client) return;

  runtime::net::InetAddress upstream_addr(peer_->Config().port, peer_->Config().host);
  upstream_ = std::make_unique<runtime::net::TcpClient>(client->GetLoop(), upstream_addr, "proxy->" + peer_->Config().name);

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

void UpstreamRequest::OnUpstreamConnChange(const TcpConnectionPtr& up_conn) {
  if (up_conn->Connected()) {
    phase_ = Phase::kSendingRequest;
    up_conn->Send(request_bytes_);
    LOG_INFO() << "proxy: upstream connected " << peer_->Config().name;
    return;
  }

  // Connected()==false：连接断开
  // kForwardingBody 阶段的断开是 upstream 正常关闭，不算错误
  if (phase_ != Phase::kForwardingBody && phase_ != Phase::kDone) {
    const uint64_t fails = 
      peer_->State().fails.fetch_add(1, std::memory_order_relaxed) + 1;
    if (static_cast<int>(fails) >= peer_->Config().max_fails) {
      peer_->State().down.store(true, std::memory_order_relaxed);
      LOG_WARN() << "proxy: upstream peer " << peer_->Config().name
                 << " marked unhealthy (fails=" << fails << ")";
    }
    Send502();
  }
  phase_ = Phase::kDone;
}

void UpstreamRequest::OnUpstreamMessage(const TcpConnectionPtr& /*up*/,
                                     runtime::net::Buffer& buf,
                                     runtime::time::Timestamp /*ts*/) {
  auto client = client_weak_.lock();
  if (!client) { buf.RetrieveAll(); return; }

  if (phase_ == Phase::kSendingRequest) {
    phase_ = Phase::kReadingHeaders;
  }
  if (phase_ == Phase::kReadingHeaders) {
    const char* end = buf.FindCRLFCRLF();
    if (!end) return;

    std::string_view raw_headers(buf.Peek(), end - buf.Peek() + 4);
    std::string rewritten = RewriteHeaders(raw_headers);
    client->Send(rewritten);

    buf.Retrieve(raw_headers.size());
    phase_ = Phase::kForwardingBody;
    // fall through: 剩余 buf 里可能已经有 body
  }
  if (phase_ == Phase::kForwardingBody && buf.ReadableBytes() > 0) {
    // 流式透传
    std::string chunk(buf.Peek(), buf.ReadableBytes());
    buf.RetrieveAll();
    client->Send(chunk);
  }
}                     

void UpstreamRequest::Send502() {
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
  LOG_WARN() << "proxy: 502 -> client, upstream=" << peer_->Config().name; 
}

std::string UpstreamRequest::RewriteHeaders(std::string_view raw) {
  std::string out;
  out.reserve(raw.size() + 64);

  const char* p   = raw.data();
  const char* end = raw.data() + raw.size();

  while (p < end) {
    const char* crlf = static_cast<const char*>(
        ::memmem(p, static_cast<std::size_t>(end - p), "\r\n", 2));
    if (!crlf) break;

    std::string_view line(p, static_cast<std::size_t>(crlf - p));
    p = crlf + 2;

    if (line.size() >= 7 && (line[0] == 'S' || line[0] == 's') &&
        line.substr(0, 7) == "Server:") {
      out += "Server: runtime-gateway\r\n";
    } else {
      out += line;
      out += "\r\n";
    }

    if (line.empty()) break;  // 空行 = \r\n\r\n 的第二个 \r\n，header 结束
  }
  return out;
}

// -- ProxyPass --

std::shared_ptr<UpstreamRequest>
ProxyPass::Forward(const TcpConnectionPtr &client_conn, 
                   const runtime::http::HttpRequest &request, 
                   std::shared_ptr<UpstreamPeer> peer) {
  auto session = std::make_shared<UpstreamRequest>(client_conn, peer, BuildRequest(request, *peer));
  session->Start();
  return session;
}

std::string ProxyPass::BuildRequest(const runtime::http::HttpRequest& req,
                                    const UpstreamPeer& peer) {
  std::ostringstream oss;
  oss << runtime::http::MethodToString(req.GetMethod()) << ' ' << req.Path();
  if (!req.Query().empty()) oss << '?' << req.Query();
  oss << " HTTP/1.1\r\n";

  for (const auto& [k, v] : req.Headers()) {
    if (k == "host") continue;
    oss << k << ": " << v << "\r\n";
  }
  oss << "host: " << peer.Config().host << ":" << peer.Config().port << "\r\n";
  oss << "\r\n";
  if (!req.Body().empty()) oss << req.Body();
  return oss.str();
}
} // namespace runtime::gateway

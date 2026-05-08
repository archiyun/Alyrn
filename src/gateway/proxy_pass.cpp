#include "runtime/gateway/proxy_pass.h"
#include "runtime/gateway/upstream_conn_pool.h"
#include "runtime/http/http_types.h"
#include "runtime/log/logger.h"
#include "runtime/net/inet_address.h"
#include "runtime/time/timestamp.h"

#include <atomic>
#include <sstream>

namespace runtime::gateway {

// -- UpstreamRequest --

UpstreamRequest::UpstreamRequest(const TcpConnectionPtr& client_conn,
                                 Upstream& upstream,
                                 LoadBalancer& lb,
                                 UpstreamConnPool& pool,
                                 std::shared_ptr<UpstreamPeer> first_peer,
                                 std::string request_bytes,
                                 int max_retries)
  : client_weak_(client_conn),
    upstream_(upstream),
    lb_(lb),
    pool_(pool),
    peer_(std::move(first_peer)),
    request_bytes_(std::move(request_bytes)),
    retries_left_(max_retries) {
  peer_->State().active.fetch_add(1, std::memory_order_relaxed);
  peer_->State().requests.fetch_add(1, std::memory_order_relaxed);
}

UpstreamRequest::~UpstreamRequest() {
  if (peer_) peer_->State().active.fetch_sub(1, std::memory_order_relaxed);
}

void UpstreamRequest::Start() {
  if (auto pooled = pool_.Acquire(peer_->Config().name)) {
    ConnectToWithPool(peer_, std::move(pooled));
    return;
  }
  ConnectTo(std::move(peer_));
}

void UpstreamRequest::ConnectToWithPool(std::shared_ptr<UpstreamPeer> peer,
                                        TcpConnectionPtr pooled_conn) {
  pooled_conn_ = std::move(pooled_conn);
  peer_ = std::move(peer);
  phase_ = Phase::kSendingRequest;

  // 重新注册回调 (池里的连接之前的回调已过期)
  auto weak_self = weak_from_this();
  pooled_conn_->SetMessageCallback([weak_self](
    const TcpConnectionPtr& up, runtime::net::Buffer& buf, runtime::time::Timestamp ts) {
      if (auto self = weak_self.lock()) self->OnUpstreamMessage(up, buf, ts);
    }
  );

  pooled_conn_->SetCloseCallback([weak_self](const TcpConnectionPtr& up) {
    if (auto self = weak_self.lock()) self->OnUpstreamConnChange(up);
  });
  pooled_conn_->Send(request_bytes_);
  LOG_INFO() << "proxy: reuse pooled conn -> " << peer_->Config().name;
}

void UpstreamRequest::ConnectTo(std::shared_ptr<UpstreamPeer> peer) {
  // retry 路径：换掉旧 peer，把 active/requests 迁移到新 peer
  if (peer_ && peer_.get() != peer.get()) {
    peer_->State().active.fetch_sub(1, std::memory_order_relaxed);
    peer->State().active.fetch_add(1, std::memory_order_relaxed);
    peer->State().requests.fetch_add(1, std::memory_order_relaxed);
  }
  peer_ = std::move(peer);
  phase_ = Phase::kConnecting;

  auto client = client_weak_.lock();
  if (!client) return;

  runtime::net::InetAddress addr(peer_->Config().port, peer_->Config().host);
  upstream_conn_ = std::make_unique<runtime::net::TcpClient>(
    client->GetLoop(), addr, "proxy->" + peer_->Config().name);

  auto weak_self = weak_from_this();
  upstream_conn_->SetConnectionCallback([weak_self](const TcpConnectionPtr& up) {
    if (auto self = weak_self.lock()) self->OnUpstreamConnChange(up);
  });
  upstream_conn_->SetMessageCallback([weak_self](
    const TcpConnectionPtr& up, runtime::net::Buffer& buf, runtime::time::Timestamp ts) {
      if (auto self = weak_self.lock()) self->OnUpstreamMessage(up, buf, ts);
    }
  );
  upstream_conn_->Connect();
}

void UpstreamRequest::OnUpstreamConnChange(const TcpConnectionPtr& up_conn) {
  if (up_conn->Connected()) {
    phase_ = Phase::kSendingRequest;
    up_conn->Send(request_bytes_);
    LOG_INFO() << "proxy: upstream connected " << peer_->Config().name;
    return;
  }

  // Connected()==false：连接断开
  // kForwardingBody 阶段的断开是 upstream 正常关闭，归还连接到池
  if (phase_ == Phase::kForwardingBody || phase_ == Phase::kDone) {
    Finalize();
    return;
  }
  // 连接阶段或发送阶段断开：更新失败计数
  const uint64_t fails =
      peer_->State().fails.fetch_add(1, std::memory_order_relaxed) + 1;
  if (static_cast<int>(fails) >= peer_->Config().max_fails) {
    peer_->State().down.store(true, std::memory_order_relaxed);
    LOG_WARN() << "proxy: peer " << peer_->Config().name
               << " marked down (fails=" << fails << ")";
  }
  if (retries_left_ -- > 0) {
    auto next = lb_.Select(upstream_);
    if (next && next.get() != peer_.get()) {
      LOG_WARN() << "proxy: retry " << peer_->Config().name
                 << " -> " << next->Config().name
                 << " (retries_left=" << retries_left_ << ")";
      ConnectTo(std::move(next));
      return;
    }
  }

  Send502();
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
    client->Send(RewriteHeaders(raw_headers));
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

void UpstreamRequest::Finalize() {
  phase_ = Phase::kDone;
  TcpConnectionPtr conn_to_return;
  if (pooled_conn_ && pooled_conn_->Connected()) {
    conn_to_return = pooled_conn_;
    pooled_conn_.reset();
  } else if (upstream_conn_ && upstream_conn_->connection() &&
             upstream_conn_->connection()->Connected()) {
    conn_to_return = upstream_conn_->connection();
  }
  if (conn_to_return) {
    pool_.Release(peer_->Config().name, std::move(conn_to_return));
  }
}

// -- ProxyPass --

std::shared_ptr<UpstreamRequest>
ProxyPass::Forward(const TcpConnectionPtr& client_conn,
                   const runtime::http::HttpRequest& request,
                   Upstream& upstream,
                   LoadBalancer& lb,
                   UpstreamConnPool& pool) {
  auto first_peer = lb.Select(upstream);
  if (!first_peer) return nullptr;
  auto req = std::make_shared<UpstreamRequest>(
      client_conn, upstream, lb, pool, first_peer, BuildRequest(request, *first_peer));
  req->Start();
  return req;
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

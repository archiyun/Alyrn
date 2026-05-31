// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
// Proxy request lifecycle. UpstreamRequest models one upstream interaction as
// a small state machine driven by network callbacks on the gateway's IO thread:
//
//   kConnecting       -> waiting for the upstream TCP connect to complete
//   kSendingRequest   -> connection up, request bytes flushed, awaiting bytes
//   kReadingHeaders   -> accumulating bytes until \r\n\r\n is seen
//   kForwardingBody   -> response headers relayed; body now streams through
//   kDone             -> response finished, connection either pooled or shut
//
// Lifetime: created by ProxyPass::Forward, returned to the gateway as a
// shared_ptr, and held in the client connection's ConnCtx until the response
// completes or the client disconnects. All callbacks capture weak_self so the
// object can outlive any single in-flight callback without UAF.
#include "runtime/gateway/proxy_pass.h"

#include <atomic>
#include <charconv>
#include <chrono>
#include <cstring>

#include "runtime/gateway/upstream_conn_pool.h"
#include "runtime/http/http_types.h"
#include "runtime/log/logger.h"
#include "runtime/net/inet_address.h"
#include "runtime/time/timestamp.h"

namespace runtime::gateway {

namespace {
// Monotonic milliseconds since some unspecified epoch, used to stamp
// UpstreamPeerState::checked_ms for the fail_timeout window.
uint64_t NowMs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}
}  // namespace

// -- UpstreamRequest --

UpstreamRequest::UpstreamRequest(const TcpConnectionPtr& client_conn,
                                 Upstream& upstream,
                                 LoadBalancer& lb,
                                 UpstreamConnPool& pool,
                                 std::shared_ptr<UpstreamPeer> first_peer,
                                 RequestContext request_ctx,
                                 std::string request_bytes,
                                 CircuitBreaker* cb,
                                 int max_retries,
                                 runtime::http::Method request_method)
  : client_weak_(client_conn),
    upstream_(upstream),
    lb_(lb),
    pool_(pool),
    peer_(std::move(first_peer)),
    request_ctx_(std::move(request_ctx)),
    request_bytes_(std::move(request_bytes)),
    cb_(cb),
    retries_left_(max_retries),
    request_method_(request_method) {
  peer_->state().active.fetch_add(1, std::memory_order_relaxed);
  peer_->state().requests.fetch_add(1, std::memory_order_relaxed);
}

UpstreamRequest::~UpstreamRequest() {
  if (peer_) peer_->state().active.fetch_sub(1, std::memory_order_relaxed);
}

void UpstreamRequest::Start() {
  if (auto pooled = pool_.Acquire(peer_->config().name)) {
    ConnectToWithPool(peer_, std::move(pooled));
    return;
  }
  ConnectTo(std::move(peer_));
}

void UpstreamRequest::ConnectToWithPool(std::shared_ptr<UpstreamPeer> peer,
                                        std::unique_ptr<runtime::net::TcpClient> pooled_client) {
  upstream_conn_ = std::move(pooled_client);
  peer_ = std::move(peer);
  phase_ = Phase::kSendingRequest;
  AttachCallbacks();
  // The pooled connection is already established, so TcpClient::set_message_callback
  // (which only takes effect for *future* connections) is not enough — we must
  // also rebind the live TcpConnection's own callbacks. Without this, incoming
  // bytes would still be dispatched to the swallow-noop installed by Finalize().
  auto& conn = *upstream_conn_->connection();
  auto weak_self = weak_from_this();
  conn.set_message_callback(
      [weak_self](const TcpConnectionPtr& up, runtime::net::Buffer& buf,
                  runtime::time::Timestamp ts) {
        if (auto self = weak_self.lock()) self->OnUpstreamMessage(up, buf, ts);
      });
  conn.set_close_callback([weak_self](const TcpConnectionPtr& up) {
    if (auto self = weak_self.lock()) self->OnUpstreamConnChange(up);
  });
  conn.Send(request_bytes_);
}

void UpstreamRequest::ConnectTo(std::shared_ptr<UpstreamPeer> peer) {
  // Retry path: when switching peers, move the active/requests bookkeeping
  // off the old peer and onto the new one so LeastConnection stays accurate.
  if (peer_ && peer_.get() != peer.get()) {
    peer_->state().active.fetch_sub(1, std::memory_order_relaxed);
    peer->state().active.fetch_add(1, std::memory_order_relaxed);
    peer->state().requests.fetch_add(1, std::memory_order_relaxed);
  }
  peer_ = std::move(peer);
  phase_ = Phase::kConnecting;

  auto client = client_weak_.lock();
  if (!client) return;

  runtime::net::InetAddress addr(peer_->config().port, peer_->config().host);
  upstream_conn_ = std::make_unique<runtime::net::TcpClient>(
    client->loop(), addr, "proxy->" + peer_->config().name);
  AttachCallbacks();
  upstream_conn_->Connect();
}

void UpstreamRequest::AttachCallbacks() {
  auto weak_self = weak_from_this();
  upstream_conn_->set_connection_callback([weak_self](const TcpConnectionPtr& up) {
    if (auto self = weak_self.lock()) self->OnUpstreamConnChange(up);
  });
  upstream_conn_->set_message_callback([weak_self](
    const TcpConnectionPtr& up, runtime::net::Buffer& buf, runtime::time::Timestamp ts) {
      if (auto self = weak_self.lock()) self->OnUpstreamMessage(up, buf, ts);
    }
  );
}

void UpstreamRequest::OnUpstreamConnChange(const TcpConnectionPtr& up_conn) {
  if (up_conn->Connected()) {
    up_conn->set_tcp_no_delay(true);
    phase_ = Phase::kSendingRequest;
    up_conn->Send(request_bytes_);
    return;
  }

  // Connected()==false means the upstream socket closed.
  // In the body-streaming or done phase this is the normal end-of-response —
  // hand off to Finalize() which decides whether to pool the conn or drop it.
  if (phase_ == Phase::kForwardingBody || phase_ == Phase::kDone) {
    Finalize();
    return;
  }
  // Connect or send phase: record a failure. OnFailure bumps fails/checked_ms
  // and decays effective_weight by 1; the inline max_fails check then flips
  // the hard down flag once the threshold is crossed.
  peer_->OnFailure(NowMs());
  const auto fails = peer_->state().fails.load(std::memory_order_relaxed);
  if (static_cast<int>(fails) >= peer_->config().max_fails) {
    peer_->state().down.store(true, std::memory_order_relaxed);
    LOG_WARN() << "proxy: peer " << peer_->config().name
               << " marked down (fails=" << fails << ")";
  }
  if (retries_left_-- > 0) {
    auto next = lb_.Select(upstream_, request_ctx_);
    if (next && next.get() != peer_.get()) {
      LOG_WARN() << "proxy: retry " << peer_->config().name
                 << " -> " << next->config().name
                 << " (retries_left=" << retries_left_ << ")";
      ConnectTo(std::move(next));
      return;
    }
  }
  // Retries exhausted: notify the per-upstream circuit breaker (if any) so
  // it can transition Closed -> Open after enough consecutive failures.
  if (cb_) cb_->OnFailure();

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

  // Coalesce headers + first body chunk from this callback into a single
  // Send -> single write() syscall. We append body bytes onto the same
  // rewritten buffer so SendInLoop sees one contiguous payload.
  std::string outbound;
  bool finalize_after = false;

  if (phase_ == Phase::kReadingHeaders) {
    const char* crlf = reinterpret_cast<const char*>(
      std::memchr(buf.Peek(), '\r', buf.readable_bytes())
    );
    const char* end = buf.FindCRLFCRLF();
    if (!end) return;

    int status = 0;
    if (crlf) {
      std::string_view first_line(buf.Peek(), crlf - buf.Peek());
      if (first_line.size() >= 12 && first_line[8] == ' ') {
        auto code_sv = first_line.substr(9, 3);
        std::from_chars(code_sv.data(), code_sv.data() + code_sv.size(), status);
      }
    }
    if (cb_) {
      if (status >= 500) cb_->OnFailure();
      else               cb_->OnSuccess();
    }

    std::string_view raw_headers(buf.Peek(), end - buf.Peek() + 4);
    ParseFraming(raw_headers, status);
    // Reserve once for headers + the body bytes we already have in buf so the
    // subsequent body append below stays in the same allocation.
    outbound.reserve(raw_headers.size() + 64 +
                     (buf.readable_bytes() - raw_headers.size()));
    RewriteHeaders(raw_headers, outbound);
    buf.Retrieve(raw_headers.size());
    phase_ = Phase::kForwardingBody;

    if (framing_ == BodyFraming::kNoBody ||
        (framing_ == BodyFraming::kContentLength && body_remaining_ == 0)) {
      client->Send(std::string_view(outbound));
      Finalize();
      return;
    }
  }

  if (phase_ == Phase::kForwardingBody && buf.readable_bytes() > 0) {
    const uint64_t n = (framing_ == BodyFraming::kContentLength)
                         ? std::min<uint64_t>(buf.readable_bytes(), body_remaining_)
                         : buf.readable_bytes();
    outbound.append(buf.Peek(), n);
    buf.Retrieve(n);
    if (framing_ == BodyFraming::kContentLength) {
      body_remaining_ -= n;
      if (body_remaining_ == 0) finalize_after = true;
    }
  }

  if (!outbound.empty()) client->Send(std::string_view(outbound));
  if (finalize_after) Finalize();
}

void UpstreamRequest::ParseFraming(std::string_view raw_headers, int status) {
  // RFC 7230 §3.3.3: HEAD responses and 1xx/204/304 status codes never carry a body.
  if (request_method_ == runtime::http::Method::Head ||
      status == 204 || status == 304 ||
      (status >= 100 && status < 200)) {
    framing_ = BodyFraming::kNoBody;
    body_remaining_ = 0;
    upstream_keepalive_ = true;
    return;
  }

  // Walk headers line-by-line to extract Content-Length / Transfer-Encoding / Connection.
  // Defaults: close-delimited body, HTTP/1.1 keep-alive unless explicitly closed.
  framing_ = BodyFraming::kCloseDelimited;
  body_remaining_ = 0;
  upstream_keepalive_ = true;

  const char* p = raw_headers.data();
  const char* end = raw_headers.data() + raw_headers.size();
  // Skip the status line.
  if (const char* eol = static_cast<const char*>(std::memchr(p, '\n', end - p))) {
    p = eol + 1;
  }
  while (p < end) {
    const char* eol = static_cast<const char*>(std::memchr(p, '\n', end - p));
    if (!eol) break;
    std::string_view line(p, eol - p);
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
    p = eol + 1;
    if (line.empty()) break;

    auto colon = line.find(':');
    if (colon == std::string_view::npos) continue;
    std::string_view name = line.substr(0, colon);
    std::string_view value = line.substr(colon + 1);
    while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) {
      value.remove_prefix(1);
    }

    auto ieq = [](std::string_view a, std::string_view b) {
      if (a.size() != b.size()) return false;
      for (size_t i = 0; i < a.size(); ++i) {
        char ca = a[i], cb = b[i];
        if (ca >= 'A' && ca <= 'Z') ca = static_cast<char>(ca + 32);
        if (cb >= 'A' && cb <= 'Z') cb = static_cast<char>(cb + 32);
        if (ca != cb) return false;
      }
      return true;
    };

    if (ieq(name, "content-length")) {
      uint64_t n = 0;
      auto [ptr, ec] = std::from_chars(value.data(), value.data() + value.size(), n);
      if (ec == std::errc{}) {
        framing_ = BodyFraming::kContentLength;
        body_remaining_ = n;
      }
    } else if (ieq(name, "transfer-encoding")) {
      // Any occurrence of "chunked" wins (overrides Content-Length per RFC).
      // v1 does NOT actually decode chunks — chunked responses fall back to
      // close-delimited streaming and the connection is not pooled afterward.
      for (size_t i = 0; i + 7 <= value.size(); ++i) {
        if (ieq(value.substr(i, 7), "chunked")) {
          framing_ = BodyFraming::kChunked;
          break;
        }
      }
    } else if (ieq(name, "connection")) {
      if (ieq(value, "close")) upstream_keepalive_ = false;
    }
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
  client->Send(kResp);
  client->Shutdown();
  LOG_WARN() << "proxy: 502 -> client, upstream=" << peer_->config().name;
}

void UpstreamRequest::RewriteHeaders(std::string_view raw, std::string& out) {
  out.reserve(raw.size() + 64);

  const char* p   = raw.data();
  const char* end = raw.data() + raw.size();

  auto ieq = [](std::string_view a, std::string_view b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
      char ca = a[i], cb = b[i];
      if (ca >= 'A' && ca <= 'Z') ca = static_cast<char>(ca + 32);
      if (cb >= 'A' && cb <= 'Z') cb = static_cast<char>(cb + 32);
      if (ca != cb) return false;
    }
    return true;
  };

  while (p < end) {
    const char* cr = static_cast<const char*>(std::memchr(p, '\r', end - p));
    if (!cr || cr + 1 >= end || cr[1] != '\n') break;

    std::string_view line(p, static_cast<std::size_t>(cr - p));
    p = cr + 2;

    const auto colon = line.find(':');
    if (colon != std::string_view::npos &&
        ieq(line.substr(0, colon), "server")) {
      out += "Server: runtime-gateway\r\n";
    } else {
      out += line;
      out += "\r\n";
    }

    if (line.empty()) break;  // Empty line marks the end of the header block.
  }
}

void UpstreamRequest::Finalize() {
  phase_ = Phase::kDone;
  if (!upstream_conn_ || !upstream_conn_->connection() ||
      !upstream_conn_->connection()->Connected()) {
    return;
  }
  // Only reuse the upstream socket when we know exactly where the response ends
  // AND the peer agreed to keep-alive. chunked/close-delimited/explicit "close"
  // disqualify reuse — the simplest correct behavior is to drop the connection.
  const bool reusable =
      upstream_keepalive_ &&
      (framing_ == BodyFraming::kContentLength || framing_ == BodyFraming::kNoBody);
  if (!reusable) {
    upstream_conn_->Disconnect();
    return;
  }
  // Before returning the connection to the pool, install swallow-noop callbacks.
  // weak_self would still lock() successfully as long as this UpstreamRequest
  // is alive; without unbinding, a stray FIN or extra bytes on the pooled conn
  // would still be forwarded to *this* request's client connection.
  auto& conn = *upstream_conn_->connection();
  conn.set_message_callback(
      [](const TcpConnectionPtr&, runtime::net::Buffer& b, runtime::time::Timestamp) {
        b.RetrieveAll();
      });
  conn.set_close_callback([](const TcpConnectionPtr&) {});
  pool_.Release(peer_->config().name, std::move(upstream_conn_));
}

// -- ProxyPass --

// Entry point used by GatewayServer when a request matches a Proxy route.
// Picks the initial peer, builds the upstream request bytes, and kicks off
// the asynchronous UpstreamRequest. The caller MUST keep the returned
// shared_ptr alive — dropping it before Start() finishes would destroy the
// only owner and cancel the request.
std::shared_ptr<UpstreamRequest>
ProxyPass::Forward(const TcpConnectionPtr& client_conn,
                   const runtime::http::HttpRequest& request,
                   Upstream& upstream,
                   LoadBalancer& lb,
                   UpstreamConnPool& pool,
                   const RequestContext& ctx,
                   CircuitBreaker* cb) {
  auto first_peer = lb.Select(upstream, ctx);
  if (!first_peer) return nullptr;
  auto req = std::make_shared<UpstreamRequest>(
      client_conn, upstream, lb, pool, first_peer, ctx,
      BuildRequest(request, *first_peer),
      cb, /*max_retries=*/2, request.method());
  req->Start();
  return req;
}

// Serialize the inbound HttpRequest into the byte stream we send upstream.
// Strips the client's Host/Connection (we rewrite both) and pins the
// outgoing connection to keep-alive so the conn pool can reuse it.
std::string ProxyPass::BuildRequest(const runtime::http::HttpRequest& req,
                                    const UpstreamPeer& peer) {
  std::string out;
  out.reserve(256);
  out += runtime::http::MethodToString(req.method());
  out += ' ';
  out += req.path().empty() ? "/" : req.path();
  if (!req.query().empty()) {
    out += '?';
    out += req.query();
  }
  out += " HTTP/1.1\r\n";

  for (const auto& [k, v] : req.headers()) {
    // double protection
    if (k == "host" || k == "connection" ||
      k == "keep-alive" || k == "proxy-connection" ||
      k == "proxy-authenticate" || k == "proxy-authorization" ||
      k == "te" || k == "trailer" || k == "transfer-encoding" || k == "upgrade") {
    continue;
  }
    out += k; out += ": "; out += v; out += "\r\n";
  }
  out += "host: ";
  out += peer.host_port();
  out += "\r\nconnection: keep-alive\r\n\r\n";
  if (!req.body().empty()) out += req.body();
  return out;
}

}  // namespace runtime::gateway

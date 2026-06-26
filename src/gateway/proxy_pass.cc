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
#include "vexo/gateway/proxy_pass.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstring>

#include "vexo/gateway/upstream_conn_pool.h"
#include "vexo/http/http_types.h"
#include "vexo/log/logger.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/inet_address.h"
#include "vexo/net/net_utils.h"
#include "vexo/time/timestamp.h"

namespace vexo::gateway {

namespace {
// Monotonic milliseconds since some unspecified epoch, used to stamp
// UpstreamPeerState::checked_ms for the fail_timeout window.
uint64_t NowMs() {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now().time_since_epoch())
                                   .count());
}

// RFC 7231 idempotent methods: replaying them upstream has no extra effect, so
// they are safe to retry on another peer even after the bytes were flushed.
// POST/PATCH/CONNECT (and Invalid) are not — a replay could double-execute.
bool IsIdempotent(vexo::http::Method m) {
  using vexo::http::Method;
  switch (m) {
    case Method::Get:
    case Method::Head:
    case Method::Put:
    case Method::Delete:
    case Method::Options:
    case Method::Trace:
      return true;
    default:
      return false;
  }
}

bool AsciiCaseEqual(std::string_view lhs, std::string_view rhs) {
  if (lhs.size() != rhs.size()) return false;
  for (std::size_t i = 0; i < lhs.size(); ++i) {
    unsigned char a = static_cast<unsigned char>(lhs[i]);
    unsigned char b = static_cast<unsigned char>(rhs[i]);
    if (a >= 'A' && a <= 'Z') a = static_cast<unsigned char>(a + ('a' - 'A'));
    if (b >= 'A' && b <= 'Z') b = static_cast<unsigned char>(b + ('a' - 'A'));
    if (a != b) return false;
  }
  return true;
}

bool NamedByConnection(std::string_view header, std::string_view connection) {
  while (!connection.empty()) {
    const auto comma = connection.find(',');
    auto token = connection.substr(0, comma);
    while (!token.empty() && (token.front() == ' ' || token.front() == '\t')) {
      token.remove_prefix(1);
    }
    while (!token.empty() && (token.back() == ' ' || token.back() == '\t')) {
      token.remove_suffix(1);
    }
    if (AsciiCaseEqual(header, token)) return true;
    if (comma == std::string_view::npos) break;
    connection.remove_prefix(comma + 1);
  }
  return false;
}

bool IsHopByHop(std::string_view header) {
  static constexpr std::array<std::string_view, 9> kHeaders = {
      "connection",
      "keep-alive",
      "proxy-connection",
      "proxy-authenticate",
      "proxy-authorization",
      "te",
      "trailer",
      "transfer-encoding",
      "upgrade",
  };
  return std::find(kHeaders.begin(), kHeaders.end(), header) != kHeaders.end();
}

void AppendHeader(std::string& out, std::string_view name, std::string_view value) {
  out += name;
  out += ": ";
  out += value;
  out += "\r\n";
}
}  // namespace

// -- UpstreamRequest --

UpstreamRequest::UpstreamRequest(const TcpConnectionPtr& client_conn, Upstream& upstream,
                                 LoadBalancer& lb, UpstreamConnPool& pool,
                                 std::shared_ptr<UpstreamPeer> first_peer,
                                 RequestContext request_ctx, std::string request_bytes,
                                 CircuitBreaker* cb, int max_retries,
                                 vexo::http::Method request_method)
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
  CancelDeadline();
  ReleaseAccounting();
  // Backstop: a request admitted by the breaker MUST resolve to exactly one
  // OnSuccess/OnFailure. If we never reported (client disconnected mid-flight,
  // weak_self expired, ...), count it as a failure so a HALF_OPEN probe slot is
  // released and the breaker can re-open and retry instead of stalling forever.
  ReportToBreaker(false);
}

void UpstreamRequest::ReportToBreaker(bool success) {
  if (!cb_ || cb_reported_) return;
  cb_reported_ = true;
  if (success) {
    cb_->OnSuccess();
  } else {
    cb_->OnFailure();
  }
}

// Linear scan for any other available peer. Not weight-aware, but this is the
// degraded failover path: the affinity/primary target is already failing, so
// reaching a different healthy node matters more than honoring the LB policy.
std::shared_ptr<UpstreamPeer> UpstreamRequest::SelectFailoverPeer() {
  const uint64_t now_ms = NowMs();
  for (const auto& p : upstream_.peers()) {
    if (p.get() == peer_.get()) continue;
    if (p->AvailableAt(now_ms)) return p;
  }
  return nullptr;
}

void UpstreamRequest::Start() {
  ArmDeadline();
  if (auto pooled = pool_.Acquire(peer_.get())) {
    ConnectToWithPool(peer_, std::move(pooled));
    return;
  }
  ConnectTo(std::move(peer_));
}

void UpstreamRequest::ConnectToWithPool(std::shared_ptr<UpstreamPeer> peer,
                                        std::unique_ptr<vexo::net::TcpClient> pooled_client) {
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
      [weak_self](const TcpConnectionPtr& up, vexo::net::Buffer& buf, vexo::time::Timestamp ts) {
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

  auto address = vexo::net::ParseIPv4Address(peer_->config().host, peer_->config().port);
  if (!address) {
    peer_->OnFailure(NowMs());
    ReportToBreaker(false);
    LOG_ERROR() << "proxy: invalid IPv4 address for peer " << peer_->config().name << ": "
                << peer_->config().host << " error=" << address.error().message();
    Send502();
    phase_ = Phase::kDone;
    CancelDeadline();
    ReleaseAccounting();
    return;
  }
  upstream_conn_ = std::make_unique<vexo::net::TcpClient>(client->loop(), *address,
                                                          "proxy->" + peer_->config().name);
  AttachCallbacks();
  upstream_conn_->Connect();
}

void UpstreamRequest::AttachCallbacks() {
  auto weak_self = weak_from_this();
  upstream_conn_->set_connection_callback([weak_self](const TcpConnectionPtr& up) {
    if (auto self = weak_self.lock()) self->OnUpstreamConnChange(up);
  });
  upstream_conn_->set_message_callback(
      [weak_self](const TcpConnectionPtr& up, vexo::net::Buffer& buf, vexo::time::Timestamp ts) {
        if (auto self = weak_self.lock()) self->OnUpstreamMessage(up, buf, ts);
      });
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
  // Connect or send phase: record a passive failure. OnFailure bumps
  // fails/checked_ms and decays effective_weight by 1. Recovery is governed by
  // AvailableAt()'s max_fails + fail_timeout cooldown — we deliberately do NOT
  // touch state_.down here. That hard flag is owned exclusively by the active
  // HealthChecker; writing it from the proxy path would make the cooldown
  // unreachable and strand the peer forever when no HealthChecker is running.
  peer_->OnFailure(NowMs());

  // Only retry when it is safe: the request was never put on the wire
  // (kConnecting failed) OR the method is idempotent. Replaying a POST/PATCH
  // that was already flushed could double-execute it, so those fail fast once
  // the bytes leave (matches nginx not retrying non-idempotent methods).
  const bool request_on_wire = (phase_ != Phase::kConnecting);
  if ((!request_on_wire || IsIdempotent(request_method_)) && retries_left_-- > 0) {
    // lb_.Select() with the same ctx re-picks the SAME peer for hash-based
    // strategies (ip_hash/consistent/maglev) — that would make the retry a
    // no-op. Fall back to any other available peer: the affinity target is
    // down, so failing over to a different node is the correct degraded path.
    auto next = lb_.Select(upstream_, request_ctx_);
    if (!next || next.get() == peer_.get()) next = SelectFailoverPeer();
    if (next && next.get() != peer_.get()) {
      LOG_WARN() << "proxy: retry " << peer_->config().name << " -> " << next->config().name
                 << " (retries_left=" << retries_left_ << ")";
      // This callback runs before TcpClient's close callback. Replacing
      // upstream_conn_ synchronously would destroy that TcpClient while
      // TcpConnection::HandleClose() is still about to call back into it.
      // Defer the replacement until the current close event has unwound.
      auto weak_self = weak_from_this();
      up_conn->loop()->QueueInLoop([weak_self, next = std::move(next)]() mutable {
        if (auto self = weak_self.lock(); self && self->phase_ != Phase::kDone) {
          self->ConnectTo(std::move(next));
        }
      });
      return;
    }
  }
  // Retries exhausted or unsafe: notify the per-upstream circuit breaker (if
  // any) so it can transition Closed -> Open after enough consecutive failures.
  ReportToBreaker(false);

  Send502();
  phase_ = Phase::kDone;
  CancelDeadline();
  ReleaseAccounting();
}

void UpstreamRequest::OnUpstreamMessage(const TcpConnectionPtr& /*up*/, vexo::net::Buffer& buf,
                                        vexo::time::Timestamp /*ts*/) {
  auto client = client_weak_.lock();
  if (!client) {
    buf.RetrieveAll();
    return;
  }

  if (phase_ == Phase::kSendingRequest) {
    phase_ = Phase::kReadingHeaders;
  }

  // Coalesce headers + first body chunk from this callback into a single
  // Send -> single write() syscall. We append body bytes onto the same
  // rewritten buffer so SendInLoop sees one contiguous payload.
  std::string outbound;
  bool finalize_after = false;

  if (phase_ == Phase::kReadingHeaders) {
    const char* crlf =
        reinterpret_cast<const char*>(std::memchr(buf.Peek(), '\r', buf.readable_bytes()));
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
    if (status >= 500) {
      ReportToBreaker(false);
    } else {
      // Completed a full round-trip to the peer: clear passive fails and let
      // effective_weight climb back so SWRR re-promotes a recovered peer.
      peer_->OnSuccess();
      ReportToBreaker(true);
    }

    std::string_view raw_headers(buf.Peek(), end - buf.Peek() + 4);
    ParseFraming(raw_headers, status);
    // Reserve once for headers + the body bytes we already have in buf so the
    // subsequent body append below stays in the same allocation.
    outbound.reserve(raw_headers.size() + 64 + (buf.readable_bytes() - raw_headers.size()));
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
  if (request_method_ == vexo::http::Method::Head || status == 204 || status == 304 ||
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

void UpstreamRequest::ArmDeadline() {
  auto client = client_weak_.lock();
  if (!client) return;

  request_loop_ = client->loop();
  const auto timeout = std::max(upstream_.config().request_timeout, std::chrono::milliseconds{1});
  deadline_armed_ = true;
  auto weak_self = weak_from_this();
  deadline_timer_ =
      request_loop_->RunAfter(std::chrono::duration<double>(timeout).count(), [weak_self] {
        if (auto self = weak_self.lock()) self->OnDeadline();
      });
}

void UpstreamRequest::CancelDeadline() {
  if (!deadline_armed_ || !request_loop_) return;
  deadline_armed_ = false;
  request_loop_->Cancel(deadline_timer_);
}

void UpstreamRequest::OnDeadline() {
  if (phase_ == Phase::kDone) return;
  deadline_armed_ = false;

  peer_->OnFailure(NowMs());
  ReportToBreaker(false);
  phase_ = Phase::kDone;
  if (upstream_conn_) upstream_conn_->Disconnect();
  Send502();
  ReleaseAccounting();
}

void UpstreamRequest::ReleaseAccounting() {
  if (accounting_released_) return;
  accounting_released_ = true;
  if (peer_) {
    peer_->state().active.fetch_sub(1, std::memory_order_relaxed);
  }
  upstream_.ReleaseRequestSlot();
}

void UpstreamRequest::RewriteHeaders(std::string_view raw, std::string& out) {
  out.reserve(raw.size() + 64);

  const char* p = raw.data();
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
    if (colon != std::string_view::npos && ieq(line.substr(0, colon), "server")) {
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
  CancelDeadline();
  ReleaseAccounting();
  if (!upstream_conn_ || !upstream_conn_->connection() ||
      !upstream_conn_->connection()->Connected()) {
    return;
  }
  // Only reuse the upstream socket when we know exactly where the response ends
  // AND the peer agreed to keep-alive. chunked/close-delimited/explicit "close"
  // disqualify reuse — the simplest correct behavior is to drop the connection.
  const bool reusable = upstream_keepalive_ && (framing_ == BodyFraming::kContentLength ||
                                                framing_ == BodyFraming::kNoBody);
  if (!reusable) {
    upstream_conn_->Disconnect();
    return;
  }
  // Before returning the connection to the pool, install swallow-noop callbacks.
  // weak_self would still lock() successfully as long as this UpstreamRequest
  // is alive; without unbinding, a stray FIN or extra bytes on the pooled conn
  // would still be forwarded to *this* request's client connection.
  auto& conn = *upstream_conn_->connection();
  conn.set_message_callback([](const TcpConnectionPtr&, vexo::net::Buffer& b,
                               vexo::time::Timestamp) { b.RetrieveAll(); });
  conn.set_close_callback([](const TcpConnectionPtr&) {});
  pool_.Release(peer_.get(), std::move(upstream_conn_));
}

// -- ProxyPass --

// Entry point used by GatewayServer when a request matches a Proxy route.
// Picks the initial peer, builds the upstream request bytes, and kicks off
// the asynchronous UpstreamRequest. The caller MUST keep the returned
// shared_ptr alive — dropping it before Start() finishes would destroy the
// only owner and cancel the request.
std::shared_ptr<UpstreamRequest> ProxyPass::Forward(const TcpConnectionPtr& client_conn,
                                                    const vexo::http::HttpRequest& request,
                                                    Upstream& upstream, LoadBalancer& lb,
                                                    UpstreamConnPool& pool,
                                                    const RequestContext& ctx, CircuitBreaker* cb,
                                                    ForwardedHeaderContext forwarded) {
  if (!upstream.TryAcquireRequestSlot()) {
    if (cb) cb->OnFailure();
    return nullptr;
  }

  auto first_peer = lb.Select(upstream, ctx);
  if (!first_peer) {
    upstream.ReleaseRequestSlot();
    if (cb) cb->OnFailure();
    return nullptr;
  }

  std::shared_ptr<UpstreamRequest> req;
  try {
    req = std::make_shared<UpstreamRequest>(client_conn, upstream, lb, pool, first_peer, ctx,
                                            BuildRequest(request, *first_peer, forwarded), cb,
                                            /*max_retries=*/2, request.method());
  } catch (...) {
    upstream.ReleaseRequestSlot();
    throw;
  }
  req->Start();
  return req;
}

// Serialize the inbound HttpRequest into the byte stream we send upstream.
// Filters and rewrites gateway-owned headers during the same traversal used
// for serialization. The inbound HttpRequest remains untouched.
std::string ProxyPass::BuildRequest(const vexo::http::HttpRequest& req, const UpstreamPeer& peer,
                                    ForwardedHeaderContext forwarded) {
  std::string out;
  out.reserve(256);
  out += vexo::http::MethodToString(req.method());
  out += ' ';
  out += req.path().empty() ? "/" : req.path();
  if (!req.query().empty()) {
    out += '?';
    out += req.query();
  }
  out += " HTTP/1.1\r\n";

  const std::string_view connection = req.connection();
  const std::string_view original_host = req.host();
  std::string_view previous_xff;
  std::string_view previous_via;
  std::string_view existing_request_id;

  for (const auto& [k, v] : req.headers()) {
    if (k == "host" || IsHopByHop(k) || NamedByConnection(k, connection)) {
      continue;
    }
    if (k == "x-forwarded-for") {
      previous_xff = v;
      continue;
    }
    if (k == "via") {
      previous_via = v;
      continue;
    }
    if (k == "x-request-id") {
      existing_request_id = v;
      continue;
    }
    if (k == "x-real-ip" || k == "x-forwarded-proto" || k == "x-forwarded-host" ||
        k == "x-forwarded-port") {
      continue;
    }
    AppendHeader(out, k, v);
  }

  if (!previous_xff.empty() && !forwarded.client_ip.empty()) {
    out += "x-forwarded-for: ";
    out += previous_xff;
    out += ", ";
    out += forwarded.client_ip;
    out += "\r\n";
  } else if (!previous_xff.empty()) {
    AppendHeader(out, "x-forwarded-for", previous_xff);
  } else if (!forwarded.client_ip.empty()) {
    AppendHeader(out, "x-forwarded-for", forwarded.client_ip);
  }
  if (!forwarded.client_ip.empty()) {
    AppendHeader(out, "x-real-ip", forwarded.client_ip);
  }
  if (!forwarded.scheme.empty()) {
    AppendHeader(out, "x-forwarded-proto", forwarded.scheme);
  }
  if (!original_host.empty()) {
    AppendHeader(out, "x-forwarded-host", original_host);
  }

  if (!previous_via.empty() && !forwarded.gateway_name.empty()) {
    out += "via: ";
    out += previous_via;
    out += ", 1.1 ";
    out += forwarded.gateway_name;
    out += "\r\n";
  } else if (!previous_via.empty()) {
    AppendHeader(out, "via", previous_via);
  } else if (!forwarded.gateway_name.empty()) {
    out += "via: 1.1 ";
    out += forwarded.gateway_name;
    out += "\r\n";
  }

  if (!existing_request_id.empty()) {
    AppendHeader(out, "x-request-id", existing_request_id);
  } else if (!forwarded.request_id.empty()) {
    AppendHeader(out, "x-request-id", forwarded.request_id);
  }

  AppendHeader(out, "host", peer.host_port());
  out += "connection: keep-alive\r\n\r\n";
  if (!req.body().empty()) out += req.body();
  return out;
}

}  // namespace vexo::gateway

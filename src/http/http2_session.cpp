// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/http/http2_session.h"
#include "runtime/net/tcp_connection.h"

#include "header_utils.h"

#include <algorithm>
#include <cstring>
#include <utility>
#include <vector>

namespace runtime::http {

// ──────────────────────────────────────────────────────────────────────────────
// Construction / destruction
// ──────────────────────────────────────────────────────────────────────────────

Http2Session::Http2Session(std::shared_ptr<runtime::net::TcpConnection> conn,
                           DispatchFn dispatch,
                           WireSendFn wire_send)
    : conn_(std::move(conn)),
      dispatch_(std::move(dispatch)),
      wire_send_(std::move(wire_send)) {

  nghttp2_session_callbacks* cbs;
  nghttp2_session_callbacks_new(&cbs);
  nghttp2_session_callbacks_set_on_header_callback    (cbs, OnHeaderCb);
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cbs, OnDataChunkCb);
  nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, OnFrameRecvCb);
  nghttp2_session_callbacks_set_on_stream_close_callback(cbs, OnStreamCloseCb);
  nghttp2_session_callbacks_set_send_callback         (cbs, SendCb);
  nghttp2_session_server_new(&session_, cbs, this);
  nghttp2_session_callbacks_del(cbs);

  // Server connection preface: send SETTINGS immediately after accept
  nghttp2_settings_entry settings[] = {
    {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 256},
    {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE,    1 << 20},  // 1 MB per stream
  };
  nghttp2_submit_settings(session_, NGHTTP2_FLAG_NONE,
                          settings, std::size(settings));
  Flush();
}

Http2Session::~Http2Session() {
  if (session_) nghttp2_session_del(session_);
}

// ──────────────────────────────────────────────────────────────────────────────
// Data path
// ──────────────────────────────────────────────────────────────────────────────

bool Http2Session::Feed(runtime::net::Buffer& buf) {
  const ssize_t consumed = nghttp2_session_mem_recv(
      session_,
      reinterpret_cast<const uint8_t*>(buf.Peek()),
      buf.ReadableBytes());

  if (consumed < 0) return false;    // NGHTTP2_ERR_*: fatal, close connection

  buf.Retrieve(static_cast<std::size_t>(consumed));
  Flush();
  return true;
}

void Http2Session::SendResponse(int32_t stream_id, const HttpResponse& resp) {
  // All strings that back nghttp2_nv pointers must outlive nghttp2_session_send
  // (called inside Flush). Keep them as named locals in this scope.
  const std::string& body = resp.GetBody();
  std::vector<std::pair<std::string, std::string>> headers;
  headers.emplace_back(":status",
                       std::to_string(static_cast<int>(resp.GetStatusCode())));

  std::vector<nghttp2_nv> nva;
  auto add = [&](const std::string& k, const std::string& v) {
    nva.push_back({
      const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(k.data())),
      const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(v.data())),
      k.size(), v.size(), NGHTTP2_NV_FLAG_NONE
    });
  };

  for (const auto& [key, value] : resp.GetHeaders()) {
    std::string lower = detail::LowerCopy(key);
    // Restricted list covers framing, hop-by-hop, and pseudo headers — plus
    // date/server, which we re-emit below from the framework's own values.
    if (detail::IsRestrictedResponseHeader(lower)) continue;
    headers.emplace_back(std::move(lower), value);
  }

  // Framework-managed headers (RFC 9110 §6.6.1 requires Date).
  headers.emplace_back("date", detail::FormatHttpDateNow());
  headers.emplace_back("server", std::string(detail::kServerSignature));

  if (!body.empty()) {
    headers.emplace_back("content-length", std::to_string(body.size()));
  }

  nva.reserve(headers.size());
  for (const auto& [key, value] : headers) {
    add(key, value);
  }

  struct BodySource {
    std::string body;
    std::size_t offset{0};
  };

  // DATA provider: nghttp2 pulls body bytes on demand during Flush().
  // Ownership transfers into the read_callback; it deletes after EOF.
  nghttp2_data_provider prd{};
  BodySource* body_source = nullptr;

  if (!body.empty()) {
    body_source = new BodySource{body, 0};
    prd.source.ptr = body_source;
    prd.read_callback = [](nghttp2_session*, int32_t, uint8_t* out,
                           size_t length, uint32_t* data_flags,
                           nghttp2_data_source* src, void*) -> ssize_t {
      auto* source = static_cast<BodySource*>(src->ptr);
      const std::size_t remaining = source->body.size() - source->offset;
      const std::size_t n = std::min(length, remaining);

      std::memcpy(out, source->body.data() + source->offset, n);
      source->offset += n;

      if (source->offset == source->body.size()) {
        *data_flags |= NGHTTP2_DATA_FLAG_EOF;
        delete source;
        src->ptr = nullptr;
      }

      return static_cast<ssize_t>(n);
    };
  }

  const int rv = nghttp2_submit_response(session_, stream_id,
                                         nva.data(), nva.size(),
                                         body.empty() ? nullptr : &prd);
  if (rv < 0) {
    // Submission failed; read_callback will never fire, so clean up manually.
    delete body_source;
    return;
  }

  Flush();
}

void Http2Session::Flush() {
  // nghttp2_session_send drives SendCb until the send queue is empty.
  nghttp2_session_send(session_);
}

// ──────────────────────────────────────────────────────────────────────────────
// nghttp2 callbacks
// ──────────────────────────────────────────────────────────────────────────────

// Called once per header field in a HEADERS frame.
int Http2Session::OnHeaderCb(nghttp2_session*,
                             const nghttp2_frame* frame,
                             const uint8_t* name,  size_t namelen,
                             const uint8_t* value, size_t valuelen,
                             uint8_t, void* userdata) {
  auto* self = static_cast<Http2Session*>(userdata);
  auto& st   = self->streams_[frame->hd.stream_id];

  std::string_view k(reinterpret_cast<const char*>(name),  namelen);
  std::string_view v(reinterpret_cast<const char*>(value), valuelen);

  // HTTP/2 pseudo-headers start with ':' and map to request-line fields.
  if      (k == ":method") {
    if      (v == "GET")    st.request.SetMethod(Method::Get);
    else if (v == "POST")   st.request.SetMethod(Method::Post);
    else if (v == "PUT")    st.request.SetMethod(Method::Put);
    else if (v == "DELETE") st.request.SetMethod(Method::Delete);
    else if (v == "PATCH")  st.request.SetMethod(Method::Patch);
    else if (v == "HEAD")   st.request.SetMethod(Method::Head);
    else if (v == "OPTIONS") st.request.SetMethod(Method::Options);
    st.request.SetVersion(Version::Http20);
  } else if (k == ":path") {
    const auto q = v.find('?');
    if (q == std::string_view::npos) {
      st.request.SetPath(std::string(v));
    } else {
      st.request.SetPath(std::string(v.substr(0, q)));
      st.request.SetQuery(std::string(v.substr(q + 1)));
    }
  } else if (k == ":authority") {
    st.request.AddHeader("host", v);
  } else if (!k.empty() && k[0] != ':') {
    // Regular header (AddHeader normalises key to lowercase)
    st.request.AddHeader(k, v);
  }
  // Ignore :scheme (not needed for routing)

  return 0;
}

// Called for each chunk of DATA frame payload.
int Http2Session::OnDataChunkCb(nghttp2_session*, uint8_t,
                                int32_t stream_id,
                                const uint8_t* data, size_t len,
                                void* userdata) {
  auto* self = static_cast<Http2Session*>(userdata);
  auto  it   = self->streams_.find(stream_id);
  if (it == self->streams_.end()) return 0;

  it->second.body_buf.append(reinterpret_cast<const char*>(data), len);
  return 0;
}

// Called after a complete frame has been received.
// We dispatch when END_STREAM is set on a HEADERS or DATA frame.
int Http2Session::OnFrameRecvCb(nghttp2_session*,
                                const nghttp2_frame* frame,
                                void* userdata) {
  if (!(frame->hd.flags & NGHTTP2_FLAG_END_STREAM)) return 0;
  if (frame->hd.type != NGHTTP2_HEADERS &&
      frame->hd.type != NGHTTP2_DATA)               return 0;

  auto* self = static_cast<Http2Session*>(userdata);
  auto  it   = self->streams_.find(frame->hd.stream_id);
  if (it == self->streams_.end()) return 0;

  auto& st = it->second;
  if (!st.body_buf.empty())
    st.request.SetBody(std::move(st.body_buf));

  HttpResponse resp(false);  // close_connection=false: HTTP/2 manages lifetime
  self->dispatch_(st.request, resp, self->conn_, frame->hd.stream_id);
  return 0;
}

// Called when a stream is fully closed (RST_STREAM or natural end).
int Http2Session::OnStreamCloseCb(nghttp2_session*, int32_t stream_id,
                                  uint32_t, void* userdata) {
  auto* self = static_cast<Http2Session*>(userdata);
  self->streams_.erase(stream_id);
  return 0;
}

// Called by nghttp2 whenever it has bytes to send.
// We forward directly into TcpConnection's output buffer.
ssize_t Http2Session::SendCb(nghttp2_session*, const uint8_t* data,
                             size_t length, int /*flags*/, void* userdata) {
  auto* self = static_cast<Http2Session*>(userdata);
  std::string wire(reinterpret_cast<const char*>(data), length);
  if (self->wire_send_) {
    self->wire_send_(std::move(wire));
  } else if (self->conn_) {
    self->conn_->Send(wire);
  }
  return static_cast<ssize_t>(length);
}

}  // namespace runtime::http

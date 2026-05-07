#pragma once

#include "runtime/http/http_request.h"
#include "runtime/http/http_response.h"
#include "runtime/net/buffer.h"
#include "runtime/net/tcp_connection.h"

#include <nghttp2/nghttp2.h>

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

namespace runtime::net { class TcpConnection; }

namespace runtime::http {

// DispatchFn is called when a complete HTTP/2 request arrives on a stream.
// stream_id is needed to route the response back through nghttp2.
using DispatchFn = std::function<void(HttpRequest&, HttpResponse&, std::shared_ptr<runtime::net::TcpConnection>, int32_t stream_id)>;
using WireSendFn = std::function<void(std::string)>;

// Http2Session owns one nghttp2_session per TcpConnection.
// It assembles HttpRequest objects from incoming frames ans submits
// HttpResponse objects back as HEADERS + DATA frames.
class Http2Session : public runtime::base::NonCopyable {
public:
  Http2Session(std::shared_ptr<runtime::net::TcpConnection> conn,
               DispatchFn dispatch,
               WireSendFn wire_send = {});
  ~Http2Session();

  // Feed TLS-decrypted bytes into nghttp2. Returns false on fatal error.
  bool Feed(runtime::net::Buffer& buf);

  // Submit a response for stream_id. Called Called from the IO thread.
  void SendResponse(int32_t stream_id, const HttpResponse& resp);

private:
  struct StreamState {
    HttpRequest request;
    std::string body_buf;
  };

  // nghttp2 callback trampolines
  static int OnHeaderCb(nghttp2_session*, const nghttp2_frame*,
                        const uint8_t* name, size_t namelen,
                        const uint8_t* value, size_t valuelen,
                        uint8_t, void* userdata);
  static int OnDataChunkCb(nghttp2_session*, uint8_t, int32_t stream_id,
                           const uint8_t* data, size_t len, void* userdata);
  static int OnFrameRecvCb(nghttp2_session*, const nghttp2_frame*, void* userdata);
  static int OnStreamCloseCb(nghttp2_session*, int32_t stream_id,
                             uint32_t error_code, void* userdata);
  static ssize_t SendCb(nghttp2_session*, const uint8_t* data, size_t length, int flags, void* userdata);

  // Triggers nghttp2_session_sned -> SendCb -> conn_ -> Send
  void Flush();

  nghttp2_session* session_{nullptr};
  std::shared_ptr<runtime::net::TcpConnection> conn_;
  DispatchFn dispatch_;
  WireSendFn wire_send_;
  std::unordered_map<int32_t, StreamState> streams_;
};

} // namespace runtime::http

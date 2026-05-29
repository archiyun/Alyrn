#include <gtest/gtest.h>
#include <nghttp2/nghttp2.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "runtime/http/http2_session.h"
#include "runtime/net/buffer.h"

namespace {

class H2Client {
public:
  H2Client() {
    nghttp2_session_callbacks* cbs = nullptr;
    nghttp2_session_callbacks_new(&cbs);
    nghttp2_session_callbacks_set_send_callback(cbs, SendCb);
    nghttp2_session_callbacks_set_on_header_callback(cbs, OnHeaderCb);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cbs,
                                                              OnDataChunkCb);
    nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, OnFrameRecvCb);
    nghttp2_session_client_new(&session_, cbs, this);
    nghttp2_session_callbacks_del(cbs);
    nghttp2_submit_settings(session_, NGHTTP2_FLAG_NONE, nullptr, 0);
  }

  ~H2Client() {
    if (session_) nghttp2_session_del(session_);
  }

  int32_t SubmitRequest(
      std::vector<std::pair<std::string, std::string>> headers,
      std::string body = {}) {
    request_headers_ = std::move(headers);
    request_body_ = std::move(body);
    request_offset_ = 0;

    std::vector<nghttp2_nv> nva;
    nva.reserve(request_headers_.size());
    for (const auto& [name, value] : request_headers_) {
      nva.push_back({
          const_cast<uint8_t*>(
              reinterpret_cast<const uint8_t*>(name.data())),
          const_cast<uint8_t*>(
              reinterpret_cast<const uint8_t*>(value.data())),
          name.size(),
          value.size(),
          NGHTTP2_NV_FLAG_NONE,
      });
    }

    nghttp2_data_provider prd{};
    prd.source.ptr = this;
    prd.read_callback = ReadRequestBodyCb;

    const int32_t stream_id = nghttp2_submit_request(
        session_, nullptr, nva.data(), nva.size(),
        request_body_.empty() ? nullptr : &prd, nullptr);
    EXPECT_GT(stream_id, 0);
    Flush();
    return stream_id;
  }

  void FeedFromServer(const std::string& wire) {
    if (wire.empty()) return;
    const ssize_t rv = nghttp2_session_mem_recv(
        session_, reinterpret_cast<const uint8_t*>(wire.data()), wire.size());
    ASSERT_GE(rv, 0);
    Flush();
  }

  std::string TakeOutbound() {
    std::string out;
    out.swap(outbound_);
    return out;
  }

  const std::unordered_map<std::string, std::string>& ResponseHeaders() const {
    return response_headers_;
  }

  const std::string& ResponseBody() const { return response_body_; }
  bool ResponseComplete() const { return response_complete_; }

private:
  void Flush() { ASSERT_EQ(nghttp2_session_send(session_), 0); }

  static ssize_t SendCb(nghttp2_session*, const uint8_t* data, size_t length,
                        int, void* userdata) {
    auto* self = static_cast<H2Client*>(userdata);
    self->outbound_.append(reinterpret_cast<const char*>(data), length);
    return static_cast<ssize_t>(length);
  }

  static ssize_t ReadRequestBodyCb(nghttp2_session*, int32_t, uint8_t* out,
                                   size_t length, uint32_t* data_flags,
                                   nghttp2_data_source* src, void*) {
    auto* self = static_cast<H2Client*>(src->ptr);
    const std::size_t remaining =
        self->request_body_.size() - self->request_offset_;
    const std::size_t n = std::min(length, remaining);
    std::memcpy(out, self->request_body_.data() + self->request_offset_, n);
    self->request_offset_ += n;
    if (self->request_offset_ == self->request_body_.size()) {
      *data_flags |= NGHTTP2_DATA_FLAG_EOF;
    }
    return static_cast<ssize_t>(n);
  }

  static int OnHeaderCb(nghttp2_session*, const nghttp2_frame* frame,
                        const uint8_t* name, size_t namelen,
                        const uint8_t* value, size_t valuelen, uint8_t,
                        void* userdata) {
    if (frame->hd.type != NGHTTP2_HEADERS ||
        frame->headers.cat != NGHTTP2_HCAT_RESPONSE) {
      return 0;
    }

    auto* self = static_cast<H2Client*>(userdata);
    self->response_headers_.insert_or_assign(
        std::string(reinterpret_cast<const char*>(name), namelen),
        std::string(reinterpret_cast<const char*>(value), valuelen));
    return 0;
  }

  static int OnDataChunkCb(nghttp2_session*, uint8_t, int32_t,
                           const uint8_t* data, size_t len, void* userdata) {
    auto* self = static_cast<H2Client*>(userdata);
    self->response_body_.append(reinterpret_cast<const char*>(data), len);
    return 0;
  }

  static int OnFrameRecvCb(nghttp2_session*, const nghttp2_frame* frame,
                           void* userdata) {
    if ((frame->hd.flags & NGHTTP2_FLAG_END_STREAM) &&
        (frame->hd.type == NGHTTP2_HEADERS || frame->hd.type == NGHTTP2_DATA)) {
      auto* self = static_cast<H2Client*>(userdata);
      self->response_complete_ = true;
    }
    return 0;
  }

  nghttp2_session* session_{nullptr};
  std::string outbound_;
  std::vector<std::pair<std::string, std::string>> request_headers_;
  std::string request_body_;
  std::size_t request_offset_{0};
  std::unordered_map<std::string, std::string> response_headers_;
  std::string response_body_;
  bool response_complete_{false};
};

void FeedServer(runtime::http::Http2Session& server, const std::string& wire) {
  runtime::net::Buffer buf;
  buf.Append(wire);
  ASSERT_TRUE(server.Feed(buf));
  EXPECT_EQ(buf.ReadableBytes(), 0u);
}

TEST(Http2SessionIntegrationTest, MapsRequestPseudoHeadersAndBody) {
  std::string server_to_client;
  std::unique_ptr<runtime::http::Http2Session> server;
  int dispatch_count = 0;

  server = std::make_unique<runtime::http::Http2Session>(
      nullptr,
      [&](runtime::http::HttpRequest& req, runtime::http::HttpResponse& resp,
          std::shared_ptr<runtime::net::TcpConnection>, int32_t stream_id) {
        ++dispatch_count;
        EXPECT_EQ(req.GetMethod(), runtime::http::Method::Options);
        EXPECT_EQ(req.GetVersion(), runtime::http::Version::Http20);
        EXPECT_EQ(req.GetPath(), "/api/items");
        EXPECT_EQ(req.GetQuery(), "debug=1");
        EXPECT_EQ(req.GetHeader("host"), "example.test");
        EXPECT_EQ(req.GetHeader("x-client"), "h2-test");
        EXPECT_EQ(req.GetBody(), "payload-body");

        resp.SetStatusCode(runtime::http::StatusCode::Created);
        resp.SetContentType("text/plain");
        resp.AddHeader("X-Reply", "ok");
        resp.SetBody("created");
        server->SendResponse(stream_id, resp);
      },
      [&](std::string wire) { server_to_client += wire; });

  H2Client client;

  client.SubmitRequest({
      {":method", "OPTIONS"},
      {":scheme", "https"},
      {":authority", "example.test"},
      {":path", "/api/items?debug=1"},
      {"x-client", "h2-test"},
  }, "payload-body");

  FeedServer(*server, client.TakeOutbound());
  client.FeedFromServer(server_to_client);

  EXPECT_EQ(dispatch_count, 1);
  EXPECT_TRUE(client.ResponseComplete());
  EXPECT_EQ(client.ResponseHeaders().at(":status"), "201");
  EXPECT_EQ(client.ResponseHeaders().at("content-type"), "text/plain");
  EXPECT_EQ(client.ResponseHeaders().at("x-reply"), "ok");
  EXPECT_EQ(client.ResponseBody(), "created");
}

TEST(Http2SessionIntegrationTest, SendsLargeBodiesAndFiltersResponseHeaders) {
  std::string server_to_client;
  std::unique_ptr<runtime::http::Http2Session> server;
  const std::string large_body(48 * 1024, 'x');

  server = std::make_unique<runtime::http::Http2Session>(
      nullptr,
      [&](runtime::http::HttpRequest&, runtime::http::HttpResponse& resp,
          std::shared_ptr<runtime::net::TcpConnection>, int32_t stream_id) {
        resp.SetStatusCode(runtime::http::StatusCode::Ok);
        resp.AddHeader("X-Trace-Id", "trace-123");
        resp.AddHeader("Connection", "close");
        resp.AddHeader("Transfer-Encoding", "chunked");
        resp.AddHeader("content-length", "1");
        resp.SetBody(large_body);
        server->SendResponse(stream_id, resp);
      },
      [&](std::string wire) { server_to_client += wire; });

  H2Client client;

  client.SubmitRequest({
      {":method", "GET"},
      {":scheme", "https"},
      {":authority", "example.test"},
      {":path", "/large"},
  });

  FeedServer(*server, client.TakeOutbound());
  client.FeedFromServer(server_to_client);

  ASSERT_TRUE(client.ResponseComplete());
  EXPECT_EQ(client.ResponseHeaders().at(":status"), "200");
  EXPECT_EQ(client.ResponseHeaders().at("content-length"),
            std::to_string(large_body.size()));
  EXPECT_EQ(client.ResponseHeaders().at("x-trace-id"), "trace-123");
  EXPECT_FALSE(client.ResponseHeaders().contains("connection"));
  EXPECT_FALSE(client.ResponseHeaders().contains("transfer-encoding"));
  EXPECT_EQ(client.ResponseBody(), large_body);
}

TEST(Http2SessionIntegrationTest, CompletesEmptyBodyResponsesWithoutDataProvider) {
  std::string server_to_client;
  std::unique_ptr<runtime::http::Http2Session> server;

  server = std::make_unique<runtime::http::Http2Session>(
      nullptr,
      [&](runtime::http::HttpRequest&, runtime::http::HttpResponse& resp,
          std::shared_ptr<runtime::net::TcpConnection>, int32_t stream_id) {
        resp.SetStatusCode(runtime::http::StatusCode::NoContent);
        resp.AddHeader("X-Empty", "true");
        server->SendResponse(stream_id, resp);
      },
      [&](std::string wire) { server_to_client += wire; });

  H2Client client;

  client.SubmitRequest({
      {":method", "GET"},
      {":scheme", "https"},
      {":authority", "example.test"},
      {":path", "/empty"},
  });

  FeedServer(*server, client.TakeOutbound());
  client.FeedFromServer(server_to_client);

  ASSERT_TRUE(client.ResponseComplete());
  EXPECT_EQ(client.ResponseHeaders().at(":status"), "204");
  EXPECT_EQ(client.ResponseHeaders().at("x-empty"), "true");
  EXPECT_FALSE(client.ResponseHeaders().contains("content-length"));
  EXPECT_TRUE(client.ResponseBody().empty());
}

}  // namespace

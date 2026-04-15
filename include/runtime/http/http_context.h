#pragma once

#include "runtime/time/timestamp.h"
#include "runtime/net/buffer.h"
#include "runtime/http/http_request.h"

namespace runtime::http {

// HttpContext incrementally parses bytes from one TCP connection into an
// HttpRequest.
class HttpContext {
public:
  enum class ParseState {
    ExpectRequestLine,
    ExpectHeaders,
    ExpectBody,
    GotAll,
  };

  bool ParseRequest(runtime::net::Buffer& buf, runtime::time::Timestamp ts);
  bool GotAll() const { return state_ == ParseState::GotAll; }
  void Reset();

  const HttpRequest& Request() const { return request_; }
  HttpRequest& MutableRequest() { return request_; }

private:
  bool ProcessRequestLine(std::string_view line);
  bool ProcessHeaderLine(std::string_view line);

  bool ParseMethod(std::string_view method_sv);
  bool ParseVersion(std::string_view version_sv);

  ParseState state_{ParseState::ExpectRequestLine};
  HttpRequest request_;
  std::size_t body_bytes_expected_{0};
};

}  // namespace runtime::http

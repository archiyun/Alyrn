// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <utility>

#include "vexo/http/http_request.h"
#include "vexo/http/parse_status.h"
#include "vexo/net/buffer.h"
#include "vexo/time/timestamp.h"

namespace vexo::http {

// HttpContext incrementally parses bytes from one TCP connection into an
// HttpRequest.
class HttpContext {
public:
  ParseStatus ParseRequest(vexo::net::Buffer& buf, vexo::time::Timestamp ts);
  bool GotAll() const { return state_ == ParseState::GotAll; }
  void Reset();

  const HttpRequest& request() const { return request_; }

  // Moves the parsed request out of the context. After this call, the
  // context's internal HttpRequest is in a moved-from state; the caller
  // MUST invoke Reset() before parsing the next pipelined request.
  HttpRequest TakeRequest() { return std::move(request_); }

private:
  enum class ParseState : uint8_t {
    ExpectRequestLine,
    ExpectHeaders,
    ExpectBody,
    GotAll,
  };

  ParseStatus ProcessRequestLine(std::string_view line);
  ParseStatus ProcessHeaderLine(std::string_view line);

  bool ParseMethod(std::string_view method_sv);
  bool ParseVersion(std::string_view version_sv);

  ParseState state_{ParseState::ExpectRequestLine};
  HttpRequest request_;
  std::size_t body_bytes_expected_{0};

  std::size_t header_count_{0};
  std::size_t header_bytes_{0};
  bool content_length_seen_{false};
};

}  // namespace vexo::http

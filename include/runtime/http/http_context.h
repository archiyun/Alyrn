// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <utility>

#include "runtime/http/http_request.h"
#include "runtime/http/http_types.h"
#include "runtime/net/buffer.h"
#include "runtime/time/timestamp.h"

namespace runtime::http {

// Result of one ParseRequest call. Distinct error variants let the server
// emit accurate status codes (413 / 414 / 431 / 501 / 505) instead of a
// blanket 400.
enum class ParseStatus : uint8_t {
  Continue,         // Need more bytes; call again on next read.
  GotAll,           // A complete request is available via Request().
  BadRequest,       // 400 — malformed framing (bad line, dup CL, TE present...)
  UriTooLong,       // 414 — request line / URI over the cap.
  HeaderTooLarge,   // 431 — header line/count/total bytes over the cap.
  PayloadTooLarge,  // 413 — Content-Length declared value over the cap.
  BadMethod,        // 501 — method token not recognized.
  BadVersion,       // 505 — HTTP version token not supported.
};

// Maps a ParseStatus error to the HTTP status code a server should emit.
// Callers should only invoke this for error variants (not Continue/GotAll).
StatusCode ParseStatusToStatusCode(ParseStatus s) noexcept;

// HttpContext incrementally parses bytes from one TCP connection into an
// HttpRequest.
class HttpContext {
public:
  ParseStatus ParseRequest(runtime::net::Buffer& buf,
                           runtime::time::Timestamp ts);
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
  bool        content_length_seen_{false};
};

}  // namespace runtime::http

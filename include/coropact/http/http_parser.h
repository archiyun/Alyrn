// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <string>
#include <string_view>

#include "coropact/http/http_request.h"
#include "coropact/http/parse_status.h"

namespace coropact::http {

// Incremental HTTP/1 request parser over plain byte slices. This is the
// backend-neutral parser used by stream-based servers; it does not depend on
// a transport implementation or any event-loop type.
class HttpParser {
public:
  ParseStatus Feed(std::string_view bytes);
  ParseStatus ParseAvailable();

  bool GotAll() const { return got_all_; }
  // Returns the parser-owned request without moving it. The reference remains
  // valid until Reset() or the next parsing operation. This is the hot-path
  // API for servers that finish handling a request before parsing the next
  // pipelined request.
  HttpRequest& CurrentRequest() { return request_; }
  const HttpRequest& CurrentRequest() const { return request_; }
  HttpRequest TakeRequest();
  void Reset();

private:
  ParseStatus ParseRequestLine(std::string_view line);
  ParseStatus ParseHeaderLine(std::string_view line);

  bool ParseMethod(std::string_view method_sv);
  bool ParseVersion(std::string_view version_sv);

  std::string pending_;
  HttpRequest request_;
  std::size_t body_bytes_expected_{0};
  std::size_t header_count_{0};
  std::size_t header_bytes_{0};
  bool content_length_seen_{false};
  bool got_all_{false};
};

}  // namespace coropact::http

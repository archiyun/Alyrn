// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <string>
#include <string_view>
#include <unordered_map>

#include "runtime/http/http_types.h"

namespace runtime::http {

// HttpResponse stores one HTTP response and serializes it into an HTTP/1.1
// message.
class HttpResponse {
 public:
  // When close_connection is true, the caller is expected to close the
  // connection after sending the response.
  explicit HttpResponse(bool close_connection);

  void set_status_code(StatusCode code);
  void set_body(std::string body);
  void set_content_type(std::string_view content_type);
  void AddHeader(std::string_view key, std::string_view value);
  void set_close_connection(bool close);

  bool close_connection() const;

  StatusCode status_code() const { return status_code_; }
  const std::string& body() const { return body_; }
  const std::unordered_map<std::string, std::string>& headers() const { return headers_; }
  std::string content_type() const {
    auto it = headers_.find("Content-Type");
    return it != headers_.end() ? it->second : std::string{};
  }

  // Serializes the response in HTTP/1.1 format.
  // This implementation always emits Content-Length and does not support
  // chunked transfer encoding.
  std::string ToString() const;

 private:
  StatusCode status_code_{StatusCode::Ok};
  bool close_connection_{true};
  std::unordered_map<std::string, std::string> headers_;
  std::string body_;
};

}  // namespace runtime::http

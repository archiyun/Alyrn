// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
#include "runtime/http/http_response.h"

#include <cctype>

namespace runtime::http {

namespace {

std::string ToLower(std::string_view sv) {
  std::string out{sv};
  for (char& c : out) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return out;
}

}  // namespace

HttpResponse::HttpResponse(bool close_connection)
    : close_connection_(close_connection) {}

void HttpResponse::SetStatusCode(StatusCode code) { status_code_ = code; }

void HttpResponse::SetBody(std::string body) { body_ = std::move(body); }

void HttpResponse::SetContentType(std::string_view content_type) {
  headers_["Content-Type"] = std::string(content_type);
}

void HttpResponse::AddHeader(std::string_view key, std::string_view value) {
  std::string k{key};
  // Content-Length and Connection are managed by the HTTP layer.
  const std::string lower = ToLower(k);
  if (lower == "content-length" || lower == "connection") return;
  headers_.insert_or_assign(k, std::string(value));
}

void HttpResponse::SetCloseConnection(bool close) { close_connection_ = close; }

bool HttpResponse::CloseConnection() const { return close_connection_; }

std::string HttpResponse::ToString() const {
  std::string out;
  out.reserve(256 + body_.size());

  out += "HTTP/1.1 ";
  out += std::to_string(static_cast<int>(status_code_));
  out += ' ';
  out += StatusMessage(status_code_);
  out += "\r\n";

  out += "Content-Length: ";
  out += std::to_string(body_.size());
  out += "\r\n";

  out += close_connection_ ? "Connection: close\r\n"
                           : "Connection: keep-alive\r\n";

  for (const auto &[key, value] : headers_) {
    out += key;
    out += ": ";
    out += value;
    out += "\r\n";
  }

  out += "\r\n";

  out += body_;

  return out;
}

}  // namespace runtime::http

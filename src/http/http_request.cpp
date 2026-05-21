// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
#include "runtime/http/http_request.h"

#include "header_utils.h"

namespace runtime::http {

void HttpRequest::AddHeader(std::string_view field, std::string_view value) {
  headers_.emplace(detail::LowerCopy(field), detail::Trim(value));
}

std::string_view HttpRequest::GetHeader(std::string_view field) const {
  const auto it = headers_.find(detail::LowerCopy(field));
  if (it == headers_.end()) {
    return {};
  }
  return it->second;
}

void HttpRequest::SetHeader(std::string_view field, std::string_view value) {
  headers_[detail::LowerCopy(field)] = detail::Trim(value);
}

bool HttpRequest::RemoveHeader(std::string_view field) {
  return headers_.erase(detail::LowerCopy(field)) > 0;
}

bool HttpRequest::KeepAlive() const {
  const auto conn = GetHeader("connection");
  if (static_cast<uint8_t>(version_) < static_cast<uint8_t>(Version::Http10)) {
    // HTTP/1.1 HTTP/2.0 HTTP/3.0 keeps connections alive by default unless the client sends
    // Connection: close.
    return conn != "close";
  }
  return conn == "keep-alive";
}

void HttpRequest::Reset() {
  method_ = Method::Invalid;
  version_ = Version::Unknown;
  path_params_.clear();
  path_.clear();
  query_.clear();
  body_.clear();
  headers_.clear();
  receive_time_ = {};
}

}  // namespace runtime::http

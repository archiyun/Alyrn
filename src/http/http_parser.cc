// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/http/http_parser.h"

#include <charconv>
#include <string_view>
#include <utility>

namespace coropact::http {
namespace {

constexpr std::size_t kMaxRequestLine = 8 * 1024;
constexpr std::size_t kMaxHeaderLine = 8 * 1024;
constexpr std::size_t kMaxHeaderCount = 100;
constexpr std::size_t kMaxHeaderBytes = 32 * 1024;
constexpr std::size_t kMaxBodyBytes = 8 * 1024 * 1024;

}  // namespace

ParseStatus HttpParser::Feed(std::string_view bytes) {
  pending_.append(bytes);
  return ParseAvailable();
}

ParseStatus HttpParser::ParseAvailable() {
  if (got_all_) {
    return ParseStatus::GotAll;
  }

  Reset();

  const std::size_t line_end = pending_.find("\r\n");
  if (line_end == std::string::npos) {
    if (pending_.size() > kMaxRequestLine) return ParseStatus::UriTooLong;
    return ParseStatus::Continue;
  }
  if (line_end > kMaxRequestLine) return ParseStatus::UriTooLong;

  std::string_view line(pending_.data(), line_end);
  if (auto st = ParseRequestLine(line); st != ParseStatus::Continue) {
    return st;
  }

  const std::size_t headers_begin = line_end + 2;
  const std::size_t headers_end = pending_.find("\r\n\r\n", headers_begin);
  if (headers_end == std::string::npos) {
    if (pending_.size() - headers_begin > kMaxHeaderBytes) {
      return ParseStatus::HeaderTooLarge;
    }
    return ParseStatus::Continue;
  }

  std::size_t pos = headers_begin;
  while (pos < headers_end) {
    const std::size_t end = pending_.find("\r\n", pos);
    if (end == std::string::npos || end > headers_end) return ParseStatus::BadRequest;
    if (end - pos > kMaxHeaderLine) return ParseStatus::HeaderTooLarge;
    std::string_view header_line(pending_.data() + pos, end - pos);
    if (auto st = ParseHeaderLine(header_line); st != ParseStatus::Continue) {
      return st;
    }
    pos = end + 2;
  }

  const std::size_t body_begin = headers_end + 4;
  if (pending_.size() - body_begin < body_bytes_expected_) {
    return ParseStatus::Continue;
  }

  if (body_bytes_expected_ != 0) {
    request_.set_body(std::string_view(pending_.data() + body_begin, body_bytes_expected_));
  }

  pending_.erase(0, body_begin + body_bytes_expected_);
  got_all_ = true;
  return ParseStatus::GotAll;
}

HttpRequest HttpParser::TakeRequest() {
  HttpRequest out = std::move(request_);
  Reset();
  return out;
}

void HttpParser::Reset() {
  request_.Reset();
  body_bytes_expected_ = 0;
  header_count_ = 0;
  header_bytes_ = 0;
  content_length_seen_ = false;
  got_all_ = false;
}

bool HttpParser::ParseMethod(std::string_view method_sv) {
  switch (method_sv.size()) {
    case 3:
      if (method_sv == "GET") {
        request_.set_method(Method::Get);
        return true;
      }
      if (method_sv == "PUT") {
        request_.set_method(Method::Put);
        return true;
      }
      break;
    case 4:
      if (method_sv == "POST") {
        request_.set_method(Method::Post);
        return true;
      }
      if (method_sv == "HEAD") {
        request_.set_method(Method::Head);
        return true;
      }
      break;
    case 5:
      if (method_sv == "PATCH") {
        request_.set_method(Method::Patch);
        return true;
      }
      if (method_sv == "TRACE") {
        request_.set_method(Method::Trace);
        return true;
      }
      break;
    case 6:
      if (method_sv == "DELETE") {
        request_.set_method(Method::Delete);
        return true;
      }
      break;
    case 7:
      if (method_sv == "OPTIONS") {
        request_.set_method(Method::Options);
        return true;
      }
      if (method_sv == "CONNECT") {
        request_.set_method(Method::Connect);
        return true;
      }
      break;
    default:
      break;
  }
  return false;
}

bool HttpParser::ParseVersion(std::string_view version_sv) {
  if (version_sv == "HTTP/1.1") {
    request_.set_version(Version::Http11);
    return true;
  }
  if (version_sv == "HTTP/1.0") {
    request_.set_version(Version::Http10);
    return true;
  }
  return false;
}

ParseStatus HttpParser::ParseRequestLine(std::string_view line) {
  const auto m_end = line.find(' ');
  if (m_end == std::string_view::npos) return ParseStatus::BadRequest;

  const auto uri_end = line.find(' ', m_end + 1);
  if (uri_end == std::string_view::npos) return ParseStatus::BadRequest;

  const std::string_view method_sv = line.substr(0, m_end);
  const std::string_view uri_sv = line.substr(m_end + 1, uri_end - m_end - 1);
  const std::string_view version_sv = line.substr(uri_end + 1);

  if (!ParseMethod(method_sv)) return ParseStatus::BadMethod;
  if (!ParseVersion(version_sv)) return ParseStatus::BadVersion;

  const auto q_pos = uri_sv.find('?');
  if (q_pos == std::string_view::npos) {
    request_.set_path(uri_sv);
    request_.set_query("");
  } else {
    request_.set_path(uri_sv.substr(0, q_pos));
    request_.set_query(uri_sv.substr(q_pos + 1));
  }
  return ParseStatus::Continue;
}

ParseStatus HttpParser::ParseHeaderLine(std::string_view line) {
  if (++header_count_ > kMaxHeaderCount) return ParseStatus::HeaderTooLarge;
  header_bytes_ += line.size();
  if (header_bytes_ > kMaxHeaderBytes) return ParseStatus::HeaderTooLarge;

  const auto colon = line.find(':');
  if (colon == std::string_view::npos) return ParseStatus::BadRequest;

  std::string_view field = line.substr(0, colon);
  std::string_view value = line.substr(colon + 1);
  while (!value.empty() && value.front() == ' ') value.remove_prefix(1);
  if (field.empty()) return ParseStatus::BadRequest;

  auto lower_field = request_.MakeHeaderKey(field);

  if (lower_field == "transfer-encoding") return ParseStatus::BadRequest;

  if (lower_field == "content-length") {
    if (content_length_seen_) return ParseStatus::BadRequest;
    content_length_seen_ = true;

    std::size_t len = 0;
    const auto [ptr, ec] = std::from_chars(value.data(), value.data() + value.size(), len);
    if (ec != std::errc{} || ptr != value.data() + value.size()) {
      return ParseStatus::BadRequest;
    }
    if (len > kMaxBodyBytes) return ParseStatus::PayloadTooLarge;
    body_bytes_expected_ = len;
  }

  request_.AddHeaderLowered(std::move(lower_field), value);
  return ParseStatus::Continue;
}

}  // namespace coropact::http

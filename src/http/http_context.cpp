// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/http/http_context.h"

#include "header_utils.h"

#include <charconv>
#include <string_view>

namespace runtime::http {

namespace {

// Resource limits to bound memory use on malformed or abusive clients.
constexpr std::size_t kMaxRequestLine = 8 * 1024;
constexpr std::size_t kMaxHeaderLine  = 8 * 1024;
constexpr std::size_t kMaxHeaderCount = 100;
constexpr std::size_t kMaxHeaderBytes = 32 * 1024;
constexpr std::size_t kMaxBodyBytes   = 8 * 1024 * 1024;

}  // namespace

StatusCode ParseStatusToStatusCode(ParseStatus s) noexcept {
  switch (s) {
    case ParseStatus::UriTooLong:      return StatusCode::UriTooLong;
    case ParseStatus::HeaderTooLarge:  return StatusCode::RequestHeaderFieldsTooLarge;
    case ParseStatus::PayloadTooLarge: return StatusCode::PayloadTooLarge;
    case ParseStatus::BadMethod:       return StatusCode::NotImplemented;
    case ParseStatus::BadVersion:      return StatusCode::HttpVersionNotSupported;
    case ParseStatus::BadRequest:
    case ParseStatus::Continue:
    case ParseStatus::GotAll:
      break;
  }
  return StatusCode::BadRequest;
}

// Parses the method token. Drives the lookup off MethodToString so the
// wire-format spelling lives in exactly one place (http_types.cpp).
// HTTP method names are case-sensitive uppercase tokens (RFC 9110 §9.1).
bool HttpContext::ParseMethod(std::string_view method_sv) {
  for (Method m : kAllRequestMethods) {
    if (method_sv == MethodToString(m)) {
      request_.SetMethod(m);
      return true;
    }
  }
  return false;
}

// Only HTTP/1.x is accepted here — HTTP/2 uses a binary preface detected
// outside this parser, so an "HTTP/2.0" text token is rejected.
bool HttpContext::ParseVersion(std::string_view version_sv) {
  if (version_sv == "HTTP/1.1") { request_.SetVersion(Version::Http11); return true; }
  if (version_sv == "HTTP/1.0") { request_.SetVersion(Version::Http10); return true; }
  return false;
}

ParseStatus HttpContext::ParseRequest(runtime::net::Buffer& buf,
                                      runtime::time::Timestamp ts) {
  while (state_ != ParseState::GotAll) {
    if (state_ == ParseState::ExpectRequestLine ||
        state_ == ParseState::ExpectHeaders) {
      const char* begin = buf.Peek();
      const char* crlf = buf.FindCRLF();
      if (crlf == nullptr) {
        // No CRLF yet — but if the buffer has already grown past the line
        // limit without producing one, the peer is sending garbage.
        const std::size_t pending = buf.ReadableBytes();
        if (state_ == ParseState::ExpectRequestLine) {
          if (pending > kMaxRequestLine) return ParseStatus::UriTooLong;
        } else {
          if (pending > kMaxHeaderLine)  return ParseStatus::HeaderTooLarge;
        }
        return ParseStatus::Continue;
      }

      std::string_view line(begin, crlf - begin);
      buf.RetrieveUntil(crlf + 2);

      if (state_ == ParseState::ExpectRequestLine) {
        if (line.size() > kMaxRequestLine) return ParseStatus::UriTooLong;
        if (auto st = ProcessRequestLine(line); st != ParseStatus::Continue) {
          return st;
        }
        state_ = ParseState::ExpectHeaders;
      } else {
        if (line.empty()) {
          if (body_bytes_expected_ == 0) {
            request_.SetReceiveTime(ts);
            state_ = ParseState::GotAll;
          } else {
            state_ = ParseState::ExpectBody;
          }
        } else {
          if (auto st = ProcessHeaderLine(line); st != ParseStatus::Continue) {
            return st;
          }
        }
      }
    } else if (state_ == ParseState::ExpectBody) {
      if (buf.ReadableBytes() < body_bytes_expected_) return ParseStatus::Continue;

      request_.SetBody(buf.RetrieveAsString(body_bytes_expected_));
      request_.SetReceiveTime(ts);
      state_ = ParseState::GotAll;
    }
  }
  return ParseStatus::GotAll;
}

// Helpers return Continue on success (meaning ParseRequest should keep
// looping). Any other ParseStatus value is an error that should propagate.
ParseStatus HttpContext::ProcessRequestLine(std::string_view line) {
  const auto m_end = line.find(' ');
  if (m_end == std::string_view::npos) return ParseStatus::BadRequest;

  const auto uri_end = line.find(' ', m_end + 1);
  if (uri_end == std::string_view::npos) return ParseStatus::BadRequest;

  const std::string_view method_sv  = line.substr(0, m_end);
  const std::string_view uri_sv     = line.substr(m_end + 1, uri_end - m_end - 1);
  const std::string_view version_sv = line.substr(uri_end + 1);

  if (!ParseMethod(method_sv))   return ParseStatus::BadMethod;
  if (!ParseVersion(version_sv)) return ParseStatus::BadVersion;

  const auto q_pos = uri_sv.find('?');
  if (q_pos == std::string_view::npos) {
    request_.SetPath(std::string(uri_sv));
    request_.SetQuery("");
  } else {
    request_.SetPath(std::string(uri_sv.substr(0, q_pos)));
    request_.SetQuery(std::string(uri_sv.substr(q_pos + 1)));
  }
  return ParseStatus::Continue;
}

ParseStatus HttpContext::ProcessHeaderLine(std::string_view line) {
  // Header-side resource limits — bound memory before any allocation.
  if (line.size() > kMaxHeaderLine)      return ParseStatus::HeaderTooLarge;
  if (++header_count_ > kMaxHeaderCount) return ParseStatus::HeaderTooLarge;
  header_bytes_ += line.size();
  if (header_bytes_ > kMaxHeaderBytes)   return ParseStatus::HeaderTooLarge;

  const auto colon = line.find(':');
  if (colon == std::string_view::npos) return ParseStatus::BadRequest;

  std::string_view field = line.substr(0, colon);
  std::string_view value = line.substr(colon + 1);
  while (!value.empty() && value.front() == ' ') value.remove_prefix(1);
  if (field.empty()) return ParseStatus::BadRequest;

  const std::string lower_field = detail::LowerCopy(field);

  // RFC 9112 §6.1: any Transfer-Encoding is rejected — we don't implement
  // chunked, and allowing TE alongside Content-Length is the canonical
  // request-smuggling vector.
  if (lower_field == "transfer-encoding") return ParseStatus::BadRequest;

  if (lower_field == "content-length") {
    // RFC 9112 §6.3.1: multiple Content-Length headers must be rejected.
    // We reject any duplicate, conflict or not — simpler and strictly safer.
    if (content_length_seen_) return ParseStatus::BadRequest;
    content_length_seen_ = true;

    std::size_t len = 0;
    const auto [ptr, ec] =
        std::from_chars(value.data(), value.data() + value.size(), len);
    if (ec != std::errc{} || ptr != value.data() + value.size()) {
      return ParseStatus::BadRequest;
    }
    if (len > kMaxBodyBytes) return ParseStatus::PayloadTooLarge;
    body_bytes_expected_ = len;
  }

  request_.AddHeader(field, value);
  return ParseStatus::Continue;
}

void HttpContext::Reset() {
  state_ = ParseState::ExpectRequestLine;
  body_bytes_expected_ = 0;
  header_count_ = 0;
  header_bytes_ = 0;
  content_length_seen_ = false;
  request_.Reset();
}

}  // namespace runtime::http

#include "runtime/http/http_context.h"

#include <algorithm>
#include <charconv>

namespace runtime::http {

// Parses the request method token into the internal enum representation.
bool HttpContext::ParseMethod(std::string_view method_sv) {
  if (method_sv == "GET")
    request_.SetMethod(Method::Get);
  else if (method_sv == "POST")
    request_.SetMethod(Method::Post);
  else if (method_sv == "PUT")
    request_.SetMethod(Method::Put);
  else if (method_sv == "DELETE")
    request_.SetMethod(Method::Delete);
  else if (method_sv == "HEAD")
    request_.SetMethod(Method::Head);
  else if (method_sv == "OPTIONS")
    request_.SetMethod(Method::Options);
  else if (method_sv == "PATCH")
    request_.SetMethod(Method::Patch);
  else
    return false;
  return true;
}

// Parses the HTTP version token.
bool HttpContext::ParseVersion(std::string_view version_sv) {
  if (version_sv == "HTTP/1.1")
    request_.SetVersion(Version::Http11);
  else if (version_sv == "HTTP/1.0")
    request_.SetVersion(Version::Http10);
  else
    return false;
  return true;
}

// Incrementally parses bytes from buf.
//
// Returns true when the input is either valid so far or still incomplete.
// Returns false only when the byte stream is definitively malformed.
bool HttpContext::ParseRequest(runtime::net::Buffer& buf,
                               runtime::time::Timestamp ts) {
  while (state_ != ParseState::GotAll) {
    if (state_ == ParseState::ExpectRequestLine ||
        state_ == ParseState::ExpectHeaders) {
      const char* begin = buf.Peek();
      const char* end = begin + buf.ReadableBytes();
      const char* crlf = std::search(begin, end, "\r\n", "\r\n" + 2);

      if (crlf == end)
        return true;

      std::string_view line(begin, crlf - begin);
      buf.RetrieveUntil(crlf + 2);

      if (state_ == ParseState::ExpectRequestLine) {
        if (!ProcessRequestLine(line))
          return false;
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
          if (!ProcessHeaderLine(line))
            return false;
        }
      }
    } else if (state_ == ParseState::ExpectBody) {
      if (buf.ReadableBytes() < body_bytes_expected_)
        return true;

      request_.SetBody(buf.RetrieveAsString(body_bytes_expected_));
      request_.SetReceiveTime(ts);
      state_ = ParseState::GotAll;
    }
  }
  return true;
}

bool HttpContext::ProcessRequestLine(std::string_view line) {
  const auto m_end = line.find(' ');
  if (m_end == std::string_view::npos)
    return false;

  const auto uri_end = line.find(' ', m_end + 1);
  if (uri_end == std::string_view::npos)
    return false;

  const std::string_view method_sv = line.substr(0, m_end);
  const std::string_view uri_sv = line.substr(m_end + 1, uri_end - m_end - 1);
  const std::string_view version_sv = line.substr(uri_end + 1);

  if (!ParseMethod(method_sv)) {
    return false;
  }
  if (!ParseVersion(version_sv)) {
    return false;
  }

  const auto q_pos = uri_sv.find('?');
  if (q_pos == std::string_view::npos) {
    request_.SetPath(std::string(uri_sv));
    request_.SetQuery("");
  } else {
    request_.SetPath(std::string(uri_sv.substr(0, q_pos)));
    request_.SetQuery(std::string(uri_sv.substr(q_pos + 1)));
  }

  return true;
}

bool HttpContext::ProcessHeaderLine(std::string_view line) {
  const auto colon = line.find(":");
  if (colon == std::string_view::npos)
    return false;

  std::string_view field = line.substr(0, colon);
  std::string_view value = line.substr(colon + 1);

  while (!value.empty() && value.front() == ' ')
    value.remove_prefix(1);

  if (field.empty())
    return false;

  request_.AddHeader(field, value);

  const auto cl = request_.GetHeader("content-length");
  if (!cl.empty()) {
    std::size_t len = 0;
    const auto [ptr, ec] =
        std::from_chars(cl.data(), cl.data() + cl.size(), len);
    if (ec != std::errc{} || ptr != cl.data() + cl.size())
      return false;
    // Cap request bodies to avoid unbounded memory growth on malformed or
    // abusive clients.
    if (len > 8 * 1024 * 1024)
      return false;
    body_bytes_expected_ = len;
  }
  return true;
}

void HttpContext::Reset() {
  state_ = ParseState::ExpectRequestLine;
  body_bytes_expected_ = 0;
  request_.Reset();
}

}  // namespace runtime::http

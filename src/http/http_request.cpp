#include "runtime/http/http_request.h"

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

std::string Trim(std::string_view sv) {
  std::size_t begin{0};
  std::size_t end{sv.size()};
  while (begin < end && sv[begin] == ' ') ++begin;
  while (begin < end && sv[end-1] == ' ') --end;
  return std::string(sv.substr(begin, end - begin));
}
}  // namespace

void HttpRequest::AddHeader(std::string_view field, std::string_view value) {
  headers_.emplace(ToLower(field), Trim(value));
}

std::string_view HttpRequest::GetHeader(std::string_view field) const {
  const auto it = headers_.find(ToLower(field));
  if (it == headers_.end()) {
    return {};
  }
  return it->second;
}

void HttpRequest::SetHeader(std::string_view field, std::string_view value) {
  headers_[ToLower(field)] = Trim(value);
}

bool HttpRequest::RemoveHeader(std::string_view field) {
  return headers_.erase(ToLower(field)) > 0;
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

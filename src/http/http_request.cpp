#include "runtime/http/http_request.h"
#include <cctype>

namespace runtime::http {

namespace {

std::string ToLower(std::string_view sv) {
  std::string out{sv};
  for (char &c : out) {
    c = tolower(c);
  }
  return out;
}
} // namespace

void HttpRequest::AddHeader(std::string_view field, std::string_view value) {
    // field 统一转小写
    headers_.emplace(ToLower(field), std::string(value));
}

std::string_view HttpRequest::GetHeader(std::string_view field) const {
    const auto it = headers_.find(ToLower(field));
    if (it == headers_.end()) {
        return {}; // null string_view
    }
    return it->second;
}

// 头部行
// connection: close or keep-alive
bool HttpRequest::KeepAlive() const {
    const auto conn = GetHeader("connection");
    if (version_ == Version::Http11) {
        // http1.1默认开启长连接， 除非显示关闭
        return conn != "close";
    }
    // http1.0 默认close, 除非显示开启 keep-alive
    return conn == "keep-alive";
}

void HttpRequest::Reset() {
    method_ = Method::Invalid;
    version_ = Version::Unknown;
    path_.clear();
    query_.clear();
    body_.clear();
    headers_.clear();
    receive_time_ = {};
}

} // namespace runtime::http
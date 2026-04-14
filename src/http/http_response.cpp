#include "runtime/http/http_response.h"

namespace runtime::http {

HttpResponse::HttpResponse(bool close_connection)
    : close_connection_(close_connection) {}

void HttpResponse::SetStatusCode(StatusCode code) { status_code_ = code; }

void HttpResponse::SetBody(std::string body) { body_ = std::move(body); }

void HttpResponse::SetContentType(std::string_view content_type) {
  headers_["Content-Type"] = std::string(content_type);
}

void HttpResponse::AddHeader(std::string_view key, std::string_view value) {
  headers_.emplace(std::string(key), std::string(value));
}

void HttpResponse::SetCloseConnection(bool close) { close_connection_ = close; }

bool HttpResponse::CloseConnection() const { return close_connection_; }

// 生成一条完整的 HTTP/1.1响应报文
// 当前实现始终输出 Content-Length, 不支持 chunked transfer encoding
std::string HttpResponse::ToString() const {
  std::string out;
  out.reserve(256 + body_.size());

  out += "HTTP/1.1 ";
  out += std::to_string(static_cast<int>(status_code_));
  out += ' ';
  out += StatusMessage(status_code_);
  out += "\r\n";

  // Content-Length 由当前 body 大小自动推导， 避免手动调用方手动维护
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
} // namespace runtime::http
#pragma once

#include "runtime/http/http_types.h"
#include "runtime/time/timestamp.h"

#include <string>
#include <unordered_map>

namespace runtime::http {

class HttpRequest {
public:
  // http请求行  ex: GET /somedir/page.html HTTP/1.1
  void SetMethod(Method m) { method_ = m; }
  Method GetMethod() const { return method_; }

  void SetVersion(Version v) { version_ = v; }
  Version GetVersion() const { return version_; }

  void SetPath(std::string p) { path_ = std::move(p); }
  const std::string &Path() const { return path_; }

  void SetQuery(std::string q) { query_ = std::move(q); }
  const std::string &Query() const { return query_; }

  // http: Headers
  /**
   * @param: field: value
   * Host: www.example.com
   * Connection: close
   * User-agent: Mozilla/5.0
   * Accept-language: fr
   *
   * field 大小写敏感
   */
  void AddHeader(std::string_view field, std::string_view value);

  // 查找时大小写不敏感
  std::string_view GetHeader(std::string_view field) const;
  const std::unordered_map<std::string, std::string> &Headers() const {
    return headers_;
  }

  // http: Body
  void SetBody(std::string b) { body_ = std::move(b); }
  const std::string &Body() const { return body_; }

  // 其它... 比如 keep-alive
  bool KeepAlive() const;

  void SetPathParams(std::unordered_map<std::string, std::string> p) {
    path_params_ = std::move(p);
  }

  std::string_view PathParam(std::string_view key) const {
    auto it = path_params_.find(std::string(key));
    if (it == path_params_.end()) return {};
    return it->second;
  }
  
  void SetReceiveTime(runtime::time::Timestamp ts) { receive_time_ = ts; }
  runtime::time::Timestamp ReceiveTime() const { return receive_time_; }

  void Reset();

private:
  Method method_{Method::Invalid};
  Version version_{Version::Unknown};
  std::unordered_map<std::string, std::string> path_params_;
  std::string path_;
  std::string query_;
  std::string body_;
  std::unordered_map<std::string, std::string> headers_; // key 小写
  runtime::time::Timestamp receive_time_;
};
} // namespace runtime::http
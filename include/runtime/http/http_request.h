#pragma once

#include "runtime/http/http_types.h"
#include "runtime/time/timestamp.h"

#include <string>
#include <string_view>
#include <unordered_map>

namespace runtime::http {

// HttpRequest stores one parsed HTTP request.
class HttpRequest {
public:
  // Sets and returns request-line fields.
  void SetMethod(Method m) { method_ = m; }
  Method GetMethod() const { return method_; }

  void SetVersion(Version v) { version_ = v; }
  Version GetVersion() const { return version_; }

  void SetPath(std::string p) { path_ = std::move(p); }
  const std::string& Path() const { return path_; }

  void SetQuery(std::string q) { query_ = std::move(q); }
  const std::string& Query() const { return query_; }

  // Adds a header line. The field name is normalized to lowercase.
  void AddHeader(std::string_view field, std::string_view value);

  // Looks up a header value with case-insensitive field matching.
  std::string_view GetHeader(std::string_view field) const;
  const std::unordered_map<std::string, std::string>& Headers() const {
    return headers_;
  }

  // Sets and returns the message body.
  void SetBody(std::string b) { body_ = std::move(b); }
  const std::string& Body() const { return body_; }

  // Returns true if the request semantics keep the connection alive.
  bool KeepAlive() const;

  void SetPathParams(std::unordered_map<std::string, std::string> p) {
    path_params_ = std::move(p);
  }

  std::string_view PathParam(std::string_view key) const {
    auto it = path_params_.find(std::string(key));
    if (it == path_params_.end())
      return {};
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
  std::unordered_map<std::string, std::string> headers_;
  runtime::time::Timestamp receive_time_;
};

}  // namespace runtime::http

// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
#pragma once

#include "runtime/memory/pmr_pool_resource.h"
#include "runtime/memory/pool.h"

#include "runtime/http/http_types.h"
#include "runtime/time/timestamp.h"

#include <memory>
#include <string_view>
#include <utility>
#include <vector>

namespace runtime::http {

// HttpRequest stores one parsed HTTP request.
class HttpRequest {
public:
  HttpRequest();
  ~HttpRequest() = default;

  HttpRequest(const HttpRequest&)            = delete;
  HttpRequest& operator=(const HttpRequest&) = delete;
  HttpRequest(HttpRequest&&) noexcept            = default;
  HttpRequest& operator=(HttpRequest&&) noexcept = default;

  void SetMethod(Method m) { method_ = m; }
  Method GetMethod() const { return method_; }

  void SetVersion(Version v) { version_ = v; }
  Version GetVersion() const { return version_; }

  void SetPath(std::string_view p) { path_.assign(p); }
  std::string_view GetPath() const { return path_; }

  void SetQuery(std::string_view q) { query_.assign(q); }
  std::string_view GetQuery() const { return query_; }

  void AddHeader(std::string_view field, std::string_view value);
  void SetHeader(std::string_view field, std::string_view value);
  bool RemoveHeader(std::string_view field);

  std::string_view GetHeader(std::string_view field) const;
  const HttpMap<HttpString, HttpString>& GetHeaders() const {
    return headers_;
  }

  void SetBody(std::string_view b) { body_.assign(b); }
  std::string_view GetBody() const { return body_; }

  bool KeepAlive() const;

  // path_params_ is a plain std::vector (not HttpVector) because Router
  // produces std::vector<PathParam> and PathParam itself holds std::string
  // members; pmr-fying just the spine of this 0~2-entry vector is not
  // worth the type ripple through the router.
  void SetPathParams(std::vector<PathParam> p) {
    path_params_ = std::move(p);
  }

  // Linear scan: path_params_ typically holds 0~2 entries, where linear
  // search beats hashing both in instructions and in allocation cost.
  std::string_view GetPathParam(std::string_view key) const {
    for (const auto& p : path_params_) {
      if (p.key == key) return p.value;
    }
    return {};
  }

  void SetReceiveTime(runtime::time::Timestamp ts) { receive_time_ = ts; }
  runtime::time::Timestamp GetReceiveTime() const { return receive_time_; }

  void Reset();

private:
  std::unique_ptr<runtime::memory::Pool> pool_;
  std::unique_ptr<runtime::memory::PoolResource> res_;  
  Method method_{Method::Invalid};       // GET / POST / PUT ...
  Version version_{Version::Unknown};    // HTTP/1.0 HTTP/1.1
  HttpString path_;                     // e.g. /users/123
  HttpString query_;                    // e.g. name=abc&age=18
  HttpString body_;
  HttpMap<HttpString, HttpString> headers_; // Host / Content-Length / Connection ...
  std::vector<PathParam> path_params_; // e.g. /users/:id -> id = 123
  runtime::time::Timestamp receive_time_;
};

}  // namespace runtime::http

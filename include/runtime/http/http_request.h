// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "runtime/http/http_types.h"
#include "runtime/memory/pmr_pool_resource.h"
#include "runtime/memory/pool.h"
#include "runtime/time/timestamp.h"

namespace runtime::http {

// HttpRequest stores one parsed HTTP request.
class HttpRequest {
 public:
  HttpRequest();
  ~HttpRequest() = default;

  HttpRequest(const HttpRequest&) = delete;
  HttpRequest& operator=(const HttpRequest&) = delete;
  HttpRequest(HttpRequest&&) noexcept = default;
  HttpRequest& operator=(HttpRequest&&) noexcept = default;

  void set_method(Method m) { method_ = m; }
  Method method() const { return method_; }

  void set_version(Version v) { version_ = v; }
  Version version() const { return version_; }

  void set_path(std::string_view p) { path_.assign(p); }
  std::string_view path() const { return path_; }

  void set_query(std::string_view q) { query_.assign(q); }
  std::string_view query() const { return query_; }

  void AddHeader(std::string_view field, std::string_view value);
  void set_header(std::string_view field, std::string_view value);
  bool RemoveHeader(std::string_view field);

  std::string_view header(std::string_view field) const;
  const HttpMap<HttpString, HttpString>& headers() const { return headers_; }

  void set_body(std::string_view b) { body_.assign(b); }
  std::string_view body() const { return body_; }

  bool keep_alive() const;

  // path_params_ is a plain std::vector (not HttpVector) because Router
  // produces std::vector<PathParam> and PathParam itself holds std::string
  // members; pmr-fying just the spine of this 0~2-entry vector is not
  // worth the type ripple through the router.
  void set_path_params(std::vector<PathParam> p) { path_params_ = std::move(p); }

  // Linear scan: path_params_ typically holds 0~2 entries, where linear
  // search beats hashing both in instructions and in allocation cost.
  std::string_view path_param(std::string_view key) const {
    for (const auto& p : path_params_) {
      if (p.key == key) return p.value;
    }
    return {};
  }

  void set_receive_time(runtime::time::Timestamp ts) { receive_time_ = ts; }
  runtime::time::Timestamp receive_time() const { return receive_time_; }

  void Reset();

 private:
  runtime::memory::Pool::Ptr pool_;
  std::unique_ptr<runtime::memory::PoolResource> res_;
  Method method_{Method::Invalid};     // GET / POST / PUT ...
  Version version_{Version::Unknown};  // HTTP/1.0 HTTP/1.1
  HttpString path_;                    // e.g. /users/123
  HttpString query_;                   // e.g. name=abc&age=18
  HttpString body_;
  HttpMap<HttpString, HttpString> headers_;  // Host / Content-Length / Connection ...
  std::vector<PathParam> path_params_;       // e.g. /users/:id -> id = 123
  runtime::time::Timestamp receive_time_;
};

}  // namespace runtime::http
